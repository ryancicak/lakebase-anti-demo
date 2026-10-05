"""A Round 5 runner keeps its sealed trust bundle, and setup adopts one AWS republished.

The trust bundle is the host's CA bundle plus AWS's RDS global bundle, which setup downloaded
every time it configured a runner. On 2026-09-29 AWS republished that bundle (Last-Modified
23:32:50 UTC). The next re-run of setup rebuilt both runners' bundles from it and then refused
them for differing from the seal, leaving the runners holding a bundle the seal did not name. So
every installation's next setup would have broken its own Round 5. These pin the two halves of
the fix: a runner whose bundle still matches the seal keeps it, and when both runners had to be
rebuilt, the re-seal adopts the bundle they now hold instead of refusing its own work.
"""

from __future__ import annotations

import pytest

import server.connection_spike_live as connection_spike_live
import server.lifecycle as lifecycle
from tests.test_round5_credential_drift import _round5_manifest, _stub_reseal_preconditions

NEW_BUNDLE = "7" * 64


def _passing_topology(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        lifecycle,
        "_round5_topology_check",
        lambda candidate, resources=None: lifecycle.Check(
            "round5_secret_free_topology", True, "clean baseline"
        ),
    )


def test_a_reseal_hands_each_runner_the_sealed_bundle_to_keep(tmp_path, monkeypatch):
    manifest = _round5_manifest(tmp_path)
    sealed = manifest.require_round5_resources()
    _stub_reseal_preconditions(monkeypatch, manifest)
    _passing_topology(monkeypatch)
    calls: list[dict] = []

    def configure(*args, **kwargs):
        calls.append(kwargs)
        return sealed.trust_bundle_sha256

    monkeypatch.setattr(lifecycle, "_configure_round5_runner", configure)
    resealed = lifecycle._prepare_and_reseal_round5(manifest, timeout=1)

    assert [call["resident_lane_id"] for call in calls] == ["lakebase", "competitor"]
    assert all(call["keep_trust_bundle_sha256"] == sealed.trust_bundle_sha256 for call in calls)
    assert resealed.require_round5_resources().trust_bundle_sha256 == sealed.trust_bundle_sha256


def test_a_reseal_adopts_the_bundle_both_runners_rebuilt(tmp_path, monkeypatch, capsys):
    manifest = _round5_manifest(tmp_path)
    sealed = manifest.require_round5_resources()
    assert sealed.trust_bundle_sha256 != NEW_BUNDLE
    _stub_reseal_preconditions(monkeypatch, manifest)
    _passing_topology(monkeypatch)
    monkeypatch.setattr(lifecycle, "_configure_round5_runner", lambda *a, **k: NEW_BUNDLE)
    verified: list[str] = []

    def setup_request(*args, **kwargs):
        payload = kwargs.get("payload") or {}
        if payload.get("action") == "verify":
            verified.append(payload["trust_bundle_sha256"])
        return {
            "public_key_sha256": (
                sealed.competitor_runner_public_key_sha256
                if kwargs.get("runner_instance_id") == sealed.competitor_runner_instance_id
                else sealed.runner_public_key_sha256
            )
        }

    monkeypatch.setattr(lifecycle, "_round5_setup_request", setup_request)
    resealed = lifecycle._prepare_and_reseal_round5(manifest, timeout=1)

    adopted = resealed.require_round5_resources()
    assert adopted.trust_bundle_sha256 == NEW_BUNDLE
    # Everything after the adoption verified against the bundle the runners hold.
    assert verified and set(verified) == {NEW_BUNDLE}
    # Only the trust bundle moved: the credentials and the runners are as they were sealed.
    assert adopted.aurora_credential_sha256 == sealed.aurora_credential_sha256
    assert adopted.runner_instance_id == sealed.runner_instance_id
    assert adopted.baseline_sha256 != sealed.baseline_sha256
    assert "RESEAL Round 5's trust bundle" in capsys.readouterr().out


def test_runners_that_hold_different_bundles_are_still_refused(tmp_path, monkeypatch):
    manifest = _round5_manifest(tmp_path)
    _stub_reseal_preconditions(monkeypatch, manifest)
    answers = iter([NEW_BUNDLE, "e" * 64])
    monkeypatch.setattr(lifecycle, "_configure_round5_runner", lambda *a, **k: next(answers))
    with pytest.raises(RuntimeError, match="installed different trust bundles"):
        lifecycle._prepare_and_reseal_round5(manifest, timeout=1)


def _trust_command(monkeypatch, keep: str | None) -> str:
    """The shell `_configure_round5_runner` sends to rebuild or keep the trust bundle."""

    harness = "b" * 64
    assets = {"connection_spike_runner.py": "a" * 64}
    sent: list[str] = []

    class Session:
        region_name = "us-west-2"

        def client(self, name):
            return object()

    def command(unused_ssm, *, commands, **unused):
        joined = "\n".join(commands)
        if "systemctl stop" in joined:
            return "OLD_PID=17\nOLD_PROCESS_BOOT=process-old\n"
        if "enable --now" in joined:
            return f"NEW_PID=22\nNEW_PROCESS_BOOT=process-new\nLOADED_HARNESS={harness}\n"
        sent.append(joined)
        return ""

    monkeypatch.setattr(lifecycle, "_run_round5_ssm_command", command)
    monkeypatch.setattr(lifecycle, "_install_round5_runner_assets", lambda *a, **k: None)
    monkeypatch.setattr(
        lifecycle, "_round5_runner_asset_checksums", lambda *a, **k: (assets, harness, "c" * 64)
    )
    monkeypatch.setattr(connection_spike_live, "runner_asset_sha256s", lambda: assets)
    lifecycle._configure_round5_runner(
        Session(),
        runner_instance_id="i-0123456789abcdef0",
        expected_harness_sha256=harness,
        resident_lane_id="lakebase",
        resident_control_queue_url="https://sqs.us-west-2.amazonaws.com/123456789012/q.fifo",
        resident_control_secret_arn="arn:aws:secretsmanager:us-west-2:123456789012:secret:c",
        keep_trust_bundle_sha256=keep,
    )
    (trust,) = sent
    return trust


def test_a_sealed_runner_keeps_a_bundle_that_still_matches(monkeypatch):
    trust = _trust_command(monkeypatch, keep="f" * 64)
    assert "sha256sum" in trust and f"'{'f' * 64}'" in trust
    assert "TRUST_BUNDLE=kept" in trust
    # The download is only the fallback, for a bundle that no longer matches.
    assert trust.index("TRUST_BUNDLE=kept") < trust.index("global-bundle.pem")


def test_a_first_install_always_builds_its_bundle(monkeypatch):
    trust = _trust_command(monkeypatch, keep=None)
    assert "TRUST_BUNDLE=kept" not in trust
    assert "global-bundle.pem" in trust


def test_a_kept_checksum_is_a_sha256_before_it_reaches_a_shell(monkeypatch):
    with pytest.raises(RuntimeError, match="not a sha256"):
        _trust_command(monkeypatch, keep="'; rm -rf / #")
