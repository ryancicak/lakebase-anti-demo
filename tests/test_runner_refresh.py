from __future__ import annotations

import asyncio
import copy
from types import SimpleNamespace

import pytest

from server import connection_spike_live, lifecycle


class _Manifest:
    def __init__(self) -> None:
        self.run_id = "ad-refresh-test"
        self.owner = "operator@databricks.com"
        self.status = "ready"
        self.manifest_version = 7
        self.installation_id = "00000000-0000-0000-0000-000000000001"
        self.round5 = SimpleNamespace(
            runner_instance_id="i-runner",
            runner_instance_profile_arn="arn:aws:iam::123456789012:instance-profile/runner",
            trust_bundle_path="/opt/lakebase-anti-demo/round5/round5-ca.pem",
            trust_bundle_sha256="c" * 64,
            harness_sha256="a" * 64,
        )
        self.round4 = {"seal": "unchanged-four"}
        self.round6 = {"seal": "unchanged-six"}

    def require_round5_resources(self):
        return self.round5

    def model_copy(self, *, deep: bool):
        assert deep is True
        return copy.deepcopy(self)


@pytest.fixture
def source(monkeypatch):
    assets = {
        "connection_spike_runner.py": "1" * 64,
        "run_connection_spike.sh": "2" * 64,
        "requirements-round5.txt": "3" * 64,
    }
    harness = "b" * 64
    monkeypatch.setattr(connection_spike_live, "runner_asset_sha256s", lambda: dict(assets))
    monkeypatch.setattr(connection_spike_live, "runner_harness_sha256", lambda: harness)
    return assets, harness


def test_runner_refresh_installs_verifies_and_reseals_only_round5(
    monkeypatch,
    source,
) -> None:
    assets, harness = source
    manifest = _Manifest()
    installed = iter(
        [
            ({"connection_spike_runner.py": "0" * 64}, "a" * 64, "c" * 64),
            (assets, harness, "c" * 64),
        ]
    )
    installs: list[str] = []
    saved: list[_Manifest] = []
    monkeypatch.setattr(
        lifecycle,
        "_round5_runner_asset_checksums",
        lambda *_a, **_k: next(installed),
    )
    monkeypatch.setattr(
        lifecycle,
        "_install_round5_runner_assets",
        lambda _session, *, runner_instance_id: installs.append(runner_instance_id),
    )
    monkeypatch.setattr(
        lifecycle,
        "_reseal_round5_harness",
        lambda sealed, digest: SimpleNamespace(**{**vars(sealed), "harness_sha256": digest}),
    )
    monkeypatch.setattr(lifecycle, "save_manifest", lambda candidate: saved.append(candidate))

    result = lifecycle._refresh_round5_runner_locked(manifest, object())

    assert installs == ["i-runner"]
    assert result is saved[0]
    assert result.round5.harness_sha256 == harness
    assert result.round4 == manifest.round4
    assert result.round6 == manifest.round6
    assert manifest.round5.harness_sha256 == "a" * 64


def test_runner_install_failure_leaves_old_seal_untouched(monkeypatch, source) -> None:
    manifest = _Manifest()
    monkeypatch.setattr(
        lifecycle,
        "_round5_runner_asset_checksums",
        lambda *_a, **_k: ({"connection_spike_runner.py": "0" * 64}, "a" * 64, "c" * 64),
    )
    monkeypatch.setattr(
        lifecycle,
        "_install_round5_runner_assets",
        lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("provider secret")),
    )
    monkeypatch.setattr(
        lifecycle,
        "save_manifest",
        lambda *_a, **_k: pytest.fail("a failed install must not advance the seal"),
    )

    with pytest.raises(RuntimeError, match="category=runner_install_failed") as raised:
        lifecycle._refresh_round5_runner_locked(manifest, object())

    assert "provider secret" not in str(raised.value)
    assert manifest.round5.harness_sha256 == "a" * 64


def test_post_install_hash_mismatch_leaves_old_seal_untouched(monkeypatch, source) -> None:
    manifest = _Manifest()
    checks = iter(
        [
            ({"connection_spike_runner.py": "0" * 64}, "a" * 64, "c" * 64),
            ({"connection_spike_runner.py": "f" * 64}, "e" * 64, "c" * 64),
        ]
    )
    monkeypatch.setattr(lifecycle, "_round5_runner_asset_checksums", lambda *_a, **_k: next(checks))
    monkeypatch.setattr(lifecycle, "_install_round5_runner_assets", lambda *_a, **_k: None)
    monkeypatch.setattr(
        lifecycle,
        "save_manifest",
        lambda *_a, **_k: pytest.fail("unverified EC2 bytes must not advance the seal"),
    )

    with pytest.raises(RuntimeError, match="category=runner_hash_mismatch"):
        lifecycle._refresh_round5_runner_locked(manifest, object())

    assert manifest.round5.harness_sha256 == "a" * 64


def test_per_file_ec2_checksum_output_is_required(monkeypatch) -> None:
    assets = {
        name: str(index) * 64
        for index, name in enumerate(connection_spike_live.RUNNER_ASSETS, start=1)
    }
    output = "\n".join(
        [
            *(f"ASSET={name}:{digest}" for name, digest in assets.items()),
            f"HARNESS={'a' * 64}",
            f"TRUST={'b' * 64}",
        ]
    )
    monkeypatch.setattr(lifecycle, "_run_round5_ssm_command", lambda *_a, **_k: output)
    session = SimpleNamespace(client=lambda _name: object())

    assert lifecycle._round5_runner_asset_checksums(
        session,
        runner_instance_id="i-runner",
        trust_bundle_path="/safe/round5-ca.pem",
    ) == (assets, "a" * 64, "b" * 64)

    monkeypatch.setattr(
        lifecycle,
        "_run_round5_ssm_command",
        lambda *_a, **_k: f"HARNESS={'a' * 64}\nTRUST={'b' * 64}",
    )
    with pytest.raises(RuntimeError, match="invalid output"):
        lifecycle._round5_runner_asset_checksums(
            session,
            runner_instance_id="i-runner",
            trust_bundle_path="/safe/round5-ca.pem",
        )


def test_active_round5_ring_refuses_before_install(monkeypatch) -> None:
    class Held(Exception):
        def __init__(self):
            self.lease = SimpleNamespace(
                operator=SimpleNamespace(email="owner@example.com", display_name="Owner"),
                phase="running",
            )

    class Store:
        ring_key = "round5"

        async def initialize(self):
            return None

        async def claim(self, **_kwargs):
            raise Held()

        async def close(self):
            return None

    monkeypatch.setattr(lifecycle, "apply_manifest_environment", lambda _manifest: None)
    monkeypatch.setattr(
        lifecycle,
        "_round5_refresh_ring_keys",
        lambda _manifest: [("round5", "Round 5 main ring")],
    )
    monkeypatch.setattr("server.coordination.LeaseHeldError", Held)
    monkeypatch.setattr("server.coordination.build_lease_store", lambda **_kwargs: Store())
    monkeypatch.setattr(
        lifecycle,
        "_refresh_round5_runner_locked",
        lambda *_a, **_k: pytest.fail("an active ring must refuse before install"),
    )

    with pytest.raises(RuntimeError, match="Round 5 main ring is active"):
        asyncio.run(
            lifecycle._refresh_round5_runner_under_fence(
                _Manifest(),
                object(),
                timeout_seconds=300,
            )
        )
