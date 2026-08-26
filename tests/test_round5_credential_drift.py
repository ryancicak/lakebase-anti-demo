"""The seal must describe the credentials the runner actually holds.

The failure these pin happened on 2026-08-24 and cost two seven-minute bouts.
A Round 5 re-seal rotates the runner's Aurora and RDS baseline credentials over
SSM -- an irreversible write to files on that instance -- and only afterwards
runs `_round5_topology_check`. That check failed, the manifest write never
happened, and the installation was left sealing digests for credentials that no
longer existed. Nothing detected it. `doctor` passed, `/readyz` passed, the
catalog advertised Round 5 as ready, and the only symptom was the competitor
lane dying on `baseline_auth_hash_invalid` after the bell while the Lakebase
lane won in 2.6 seconds.

Two separate defects, so two separate groups of tests below:

*   the *record* was conditional on a later gate, when the thing it records had
    already happened unconditionally; and
*   nothing ever compared the sealed digests against the runner, even though
    that comparison is one SSM round trip with no database connection in it.

The second is the more valuable of the two. It catches the whole class rather
than the one instance -- any future divergence between seal and runner, however
it arises -- and it would have turned today's two failed bouts into a refusal
before either one started.
"""

from __future__ import annotations

import pytest

import server.connection_spike_live as connection_spike_live
import server.lifecycle as lifecycle

#: Every field on the seal that the re-seal compares against Terraform before it
#: will touch the runner. Built from the sealed model at call time rather than
#: retyped, for the same reason the digests below are: a hand-written copy
#: agrees with itself, not with the code under test.
_COMPARED_OUTPUT_FIELDS = (
    "aurora_direct_host",
    "aurora_cluster_id",
    "aurora_cluster_resource_id",
    "aurora_writer_instance_id",
    "aurora_master_secret_arn",
    "rds_direct_host",
    "rds_master_secret_arn",
    "rds_resource_id",
    "vpc_id",
    "proxy_subnet_ids",
    "control_role_arn",
    "control_role_trusted_principal_arn",
    "proxy_service_role_arn",
    "proxy_service_policy_name",
    "aurora_proxy_secret_arn",
    "rds_proxy_secret_arn",
    "runner_permissions_boundary_arn",
    "runner_instance_id",
    "runner_instance_profile_arn",
    "runner_role_arn",
    "runner_subnet_id",
    "runner_security_group_id",
    "runner_egress_rule_id",
    "bout_name_prefix",
)

ROTATED_AURORA = "c" * 64
ROTATED_RDS = "2" * 64


def _round5_manifest(tmp_path):
    from test_manifest import _v7_manifest

    manifest = _v7_manifest(tmp_path)
    assert manifest.round5_ready, "fixture must seal Round 5"
    return manifest


def _stub_reseal_preconditions(monkeypatch: pytest.MonkeyPatch, manifest) -> list:
    """Let `_prepare_and_reseal_round5` reach its rotation without touching AWS.

    Everything stubbed here is a precondition the re-seal checks *before* it
    mutates the runner. The rotation itself, the re-seal arithmetic and the
    manifest write are left real, because they are what is under test.
    """

    sealed = manifest.require_round5_resources()
    outputs = {field: getattr(sealed, field) for field in _COMPARED_OUTPUT_FIELDS}
    # Terraform derives this from the same variables as the control role's
    # `ec2:CreateTags` condition, so it tracks `manifest.expires_at` rather than
    # whatever the previous seal happened to record.
    outputs["ownership_tags"] = {
        **lifecycle._required_round_tags(manifest, "r5"),
        "managed-by": "round5-lifecycle",
    }
    saved: list[tuple[str, str, str]] = []

    monkeypatch.setattr(lifecycle, "_terraform_outputs", lambda candidate: outputs)
    monkeypatch.setattr(lifecycle, "_required_round5_outputs", lambda values: values)
    monkeypatch.setattr(lifecycle, "_aws_session", lambda candidate: _Session())
    monkeypatch.setattr(
        lifecycle, "_round5_aurora_cluster_resource_id", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(lifecycle, "_wait_round5_runner_ready", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        lifecycle,
        "_configure_round5_runner",
        lambda *args, **kwargs: sealed.trust_bundle_sha256,
    )
    monkeypatch.setattr(
        connection_spike_live,
        "runner_harness_sha256",
        lambda *args, **kwargs: sealed.harness_sha256,
    )
    monkeypatch.setattr(
        lifecycle,
        "_round5_setup_request",
        lambda *args, **kwargs: {"public_key_sha256": sealed.runner_public_key_sha256},
    )
    # The irreversible half: by the time this returns, the runner's Aurora and
    # RDS credential files hold new secrets whose digests are these.
    monkeypatch.setattr(
        lifecycle,
        "_prepare_and_reassert_round5_aws_credentials",
        lambda *args, **kwargs: {"aurora": ROTATED_AURORA, "rds": ROTATED_RDS},
    )

    def record(candidate, path=None):
        round5 = candidate.round5
        saved.append(
            (
                candidate.status,
                round5.aurora_credential_sha256,
                round5.rds_credential_sha256,
            )
        )
        return path

    monkeypatch.setattr(lifecycle, "save_manifest", record)
    return saved


def test_the_rotated_digests_are_recorded_even_when_the_gate_refuses(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The exact shape of the 2026-08-24 failure, now with the record kept.

    The runner's credentials have already been replaced when the gate runs, so a
    manifest that omits the new digests is not the careful outcome -- it is the
    false one. It describes files that no longer exist, and the next bout dies
    after the bell proving it.
    """

    manifest = _round5_manifest(tmp_path)
    stale_aurora = manifest.require_round5_resources().aurora_credential_sha256
    saved = _stub_reseal_preconditions(monkeypatch, manifest)
    monkeypatch.setattr(
        lifecycle,
        "_round5_topology_check",
        lambda candidate, resources=None: lifecycle.Check(
            "round5_secret_free_topology", False, "runner is not online in SSM"
        ),
    )

    with pytest.raises(RuntimeError) as refusal:
        lifecycle._prepare_and_reseal_round5(manifest, timeout=1)

    # As loud as it was before: the same exception type, naming the same check.
    assert "Round 5 secret-free doctor failed" in str(refusal.value)
    assert "runner is not online in SSM" in str(refusal.value)

    # ... and the digests reached the manifest anyway.
    assert saved, "a failed gate must not discard the record of the rotation"
    _, aurora, rds = saved[0]
    assert (aurora, rds) == (ROTATED_AURORA, ROTATED_RDS)
    assert aurora != stale_aurora
    assert manifest.require_round5_resources().aurora_credential_sha256 == ROTATED_AURORA


def test_a_refused_gate_never_leaves_the_installation_advertising_ready(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Recording reality must not be mistaken for approving it.

    Persisting the digests moves a write to before the gate. That write must not
    also promote the installation, or a failed topology would advertise itself as
    ready and a bout could arm against it -- trading one silent failure for a
    louder one.
    """

    manifest = _round5_manifest(tmp_path)
    saved = _stub_reseal_preconditions(monkeypatch, manifest)
    monkeypatch.setattr(
        lifecycle,
        "_round5_topology_check",
        lambda candidate, resources=None: lifecycle.Check(
            "round5_secret_free_topology", False, "Lakebase native login is not enabled"
        ),
    )

    with pytest.raises(RuntimeError):
        lifecycle._prepare_and_reseal_round5(manifest, timeout=1)

    assert [status for status, _, _ in saved] == ["seeding"]
    assert manifest.status != "ready"


def test_a_passing_gate_still_seals_and_publishes_exactly_as_before(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The success path is unchanged, which is what makes the fix safe to land."""

    manifest = _round5_manifest(tmp_path)
    saved = _stub_reseal_preconditions(monkeypatch, manifest)
    monkeypatch.setattr(
        lifecycle,
        "_round5_topology_check",
        lambda candidate, resources=None: lifecycle.Check(
            "round5_secret_free_topology", True, "clean baseline"
        ),
    )

    resealed = lifecycle._prepare_and_reseal_round5(manifest, timeout=1)

    assert resealed.require_round5_resources().aurora_credential_sha256 == ROTATED_AURORA
    assert resealed.require_round5_resources().rds_credential_sha256 == ROTATED_RDS
    # Round 6 is sealed in the v7 fixture, so readiness is deliberately withheld
    # until its canary re-runs -- the behaviour that predates this change.
    assert [status for status, _, _ in saved] == ["seeding", "seeding"]


def test_every_credential_the_runner_stores_has_a_sealed_digest_to_compare(
    tmp_path,
) -> None:
    """The anti-drift claim the doctor check rests on.

    `_round5_runner_credential_digests` asks the runner for one digest per entry
    in its own `BASELINE_CREDENTIAL_PATHS`, then compares each against
    `<lane>_credential_sha256` on the seal. That correspondence is the whole
    check, and nothing else in the tree enforces it: adding a fourth credential
    to the runner without adding its digest to the seal would make the new lane
    silently uncompared, which is precisely the gap being closed.
    """

    runner = pytest.importorskip("runner.connection_spike_runner")
    sealed = _round5_manifest(tmp_path).require_round5_resources()

    for lane in runner.BASELINE_CREDENTIAL_PATHS:
        assert hasattr(sealed, f"{lane}_credential_sha256"), (
            f"the runner stores a {lane} credential that no sealed digest covers"
        )

    # The names the remote script reads. Renaming any of them in the runner must
    # break here rather than at 3am against a live installation.
    for name in (
        "BASELINE_CREDENTIAL_PATHS",
        "AWS_CREDENTIAL_IDS",
        "RDS_BASELINE_KEYS",
        "BASELINE_DATABASE_KEYS",
        "_read_root_json",
        "_canonical_json",
    ):
        assert hasattr(runner, name), f"the digest doctor reads runner.{name}"


def test_the_digest_doctor_refuses_output_it_cannot_trust(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A truncated or empty SSM reply must not read as "nothing has drifted".

    An empty mapping compares clean against every seal, so the failure mode this
    forbids is the check quietly approving an installation it never measured.
    """

    def reply(text: str):
        monkeypatch.setattr(
            lifecycle, "_run_round5_ssm_command", lambda *args, **kwargs: text
        )
        return lifecycle._round5_runner_credential_digests(
            _Session(), runner_instance_id="i-0123456789abcdef0"
        )

    good = reply(
        f"DIGEST_AURORA={ROTATED_AURORA}\n"
        f"DIGEST_LAKEBASE={'a' * 64}\n"
        f"DIGEST_RDS={ROTATED_RDS}\n"
    )
    assert good == {
        "aurora": ROTATED_AURORA,
        "lakebase": "a" * 64,
        "rds": ROTATED_RDS,
    }

    for untrustworthy in ("", "DIGEST_AURORA=\n", "DIGEST_AURORA=not-a-digest\n"):
        with pytest.raises(RuntimeError, match="credential doctor"):
            reply(untrustworthy)


def test_a_renewed_expiry_reaches_the_tags_every_per_bout_rule_carries(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The 2026-08-24 evening failure, one drift later than the digests.

    `renew` moves `expires_at` and re-applies `round5_control.tf`, which
    conditions `ec2:CreateTags` on `security-group-rule/*` on that exact value.
    The re-seal used to copy the previous `ownership_tags` forward untouched, so
    the manifest kept tagging per-bout rules with the superseded expiry and the
    grant became an implicit deny -- `UnauthorizedOperation` on the third
    journaled mutation, roughly two seconds into the bout.
    """

    manifest = _round5_manifest(tmp_path)
    before = manifest.require_round5_resources().ownership_tags.expires_at
    manifest.expires_at = manifest.expires_at.replace(year=manifest.expires_at.year + 1)
    renewed = lifecycle._utc_tag(manifest.expires_at)
    assert renewed != before, "the fixture must actually move the expiry"

    _stub_reseal_preconditions(monkeypatch, manifest)
    monkeypatch.setattr(
        lifecycle,
        "_round5_topology_check",
        lambda candidate, resources=None: lifecycle.Check(
            "round5_secret_free_topology", True, "clean baseline"
        ),
    )

    resealed = lifecycle._prepare_and_reseal_round5(manifest, timeout=1)

    tags = resealed.require_round5_resources().ownership_tags
    assert tags.expires_at == renewed
    assert tags.as_aws_tags()["expires-at"] == renewed


def test_the_doctor_refuses_tags_the_control_role_would_deny(tmp_path) -> None:
    """Cheap, read-only, and the check that turns a dead bout into a refusal.

    One `GetRolePolicy`. The sealed tags either match what the applied policy
    allows or the bout cannot create a single per-bout security-group rule, and
    there is no reason to discover which after the bell.
    """

    sealed = _round5_manifest(tmp_path).require_round5_resources()
    allowed = sealed.ownership_tags.as_aws_tags()

    def policy(**tags: str):
        return {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "ec2:CreateTags",
                    "Resource": "arn:aws:ec2:us-west-2:1234:security-group-rule/*",
                    "Condition": {
                        "StringEquals": {
                            f"aws:RequestTag/{key}": value for key, value in tags.items()
                        }
                        | {"ec2:CreateAction": "AuthorizeSecurityGroupIngress"},
                        "Null": {"aws:RequestTag/anti-demo-bout-id": "false"},
                    },
                }
            ],
        }

    lifecycle._require_round5_tags_the_control_role_allows(_Iam(policy(**allowed)), sealed)

    stale = dict(allowed, **{"expires-at": "2026-01-01T00:00:00Z"})
    with pytest.raises(RuntimeError) as refusal:
        lifecycle._require_round5_tags_the_control_role_allows(_Iam(policy(**stale)), sealed)

    detail = str(refusal.value)
    assert "expires-at" in detail
    assert "2026-01-01T00:00:00Z" in detail
    assert allowed["expires-at"] in detail
    # Names the consequence, not just the mismatch: this text is what an
    # operator reads instead of "The Round 5 setup phase failed".
    assert "ec2:CreateTags" in detail


class _Iam:
    """An IAM client stand-in carrying one inline policy on the control role."""

    def __init__(self, document: dict) -> None:
        self._document = document

    def list_role_policies(self, RoleName: str) -> dict:  # noqa: N803
        return {"PolicyNames": ["control"]}

    def get_role_policy(self, RoleName: str, PolicyName: str) -> dict:  # noqa: N803
        return {"PolicyDocument": self._document}


class _Session:
    """A boto3 session stand-in that hands back inert clients."""

    def __init__(self) -> None:
        self.clients: list[str] = []

    def client(self, name: str):
        self.clients.append(name)
        return object()
