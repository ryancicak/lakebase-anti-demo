"""The SQL warehouse `SELECT 1` preflight: prove CAN_USE before the fleet is built.

A clean install once selected a SQL warehouse the service principal could *see*
but not *run* on, and found out roughly twenty minutes and a full AWS fleet
later, when Round 4 issued its first Delta statement against it. Selecting a
warehouse proves visibility; only executing something proves usability. These
tests pin the read-only `SELECT 1` that closes that gap: it runs before the
first billable step, it probes the exact warehouse setup will use (the sealed
one on a resume, the environment's on a first provision), it never leaks a
secret, and it reframes the control plane's error around the CAN_USE grant an
operator can actually act on.

The bootstrap.sh side reads the warehouse's presence for free before the gate
(tests/bootstrap_stub_harness.sh::case_warehouse_presence_preflight); this
read-only `SELECT 1` proof runs from `./antidemo setup`, after PROVISION is
confirmed and before Terraform, where a small warehouse-resume cost is already
sanctioned.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

import server.lifecycle as lifecycle


class TestResolveSetupWarehouse:
    """Fresh and resume ask for the warehouse differently; the probe must too."""

    def test_a_sealed_round4_warehouse_wins_over_the_environment(self, monkeypatch):
        # A resume is bound to the warehouse in its manifest, so an unrelated
        # DATABRICKS_WAREHOUSE_ID in the resuming shell must not redirect the probe.
        monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "wh-from-the-shell")
        manifest = SimpleNamespace(round4=SimpleNamespace(warehouse_id="wh-sealed"))
        assert lifecycle._resolve_setup_warehouse_id(manifest) == "wh-sealed"

    def test_a_first_provision_reads_the_environment(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "wh-from-the-shell")
        manifest = SimpleNamespace(round4=None)
        assert lifecycle._resolve_setup_warehouse_id(manifest) == "wh-from-the-shell"

    def test_no_seal_and_no_environment_is_nothing_to_prove(self, monkeypatch):
        # A first provision that has not been given the variable. Round 4 refuses
        # that later with the message that names it; the probe has nothing yet.
        monkeypatch.delenv("DATABRICKS_WAREHOUSE_ID", raising=False)
        assert lifecycle._resolve_setup_warehouse_id(SimpleNamespace(round4=None)) is None

    def test_a_legacy_seal_without_a_warehouse_falls_back_to_the_environment(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "wh-env")
        manifest = SimpleNamespace(round4=SimpleNamespace(warehouse_id=""))
        assert lifecycle._resolve_setup_warehouse_id(manifest) == "wh-env"


class TestProbeSqlWarehouse:
    def test_a_usable_warehouse_runs_select_one_on_the_exact_target(self, monkeypatch):
        captured: list[tuple] = []

        def fake_statement(profile, warehouse_id, statement, **kwargs):
            captured.append((profile, warehouse_id, statement, kwargs))
            return {"status": {"state": "SUCCEEDED"}}

        monkeypatch.setattr(lifecycle, "_sql_statement", fake_statement)
        lifecycle._probe_sql_warehouse("anti-demo-x", "wh-42")

        assert len(captured) == 1
        profile, warehouse_id, statement, kwargs = captured[0]
        assert profile == "anti-demo-x"
        assert warehouse_id == "wh-42"
        assert statement == "SELECT 1", "the probe must be a read, and this one specifically"
        # Bounded like every other setup statement: a stuck warehouse cannot make
        # the probe hang past its per-attempt budget or retry without limit.
        assert kwargs.get("max_attempts") == 4
        assert kwargs.get("timeout") == 120

    def test_a_can_use_denial_is_reframed_around_the_warehouse_and_the_grant(self, monkeypatch):
        def denied(*_args, **_kwargs):
            raise RuntimeError(
                "Databricks SQL statement failed with state FAILED (warehouse=wh-42): "
                "PERMISSION_DENIED: User does not have CAN_USE on warehouse wh-42"
            )

        monkeypatch.setattr(lifecycle, "_sql_statement", denied)
        with pytest.raises(RuntimeError) as excinfo:
            lifecycle._probe_sql_warehouse("prof", "wh-42")

        message = str(excinfo.value)
        assert "wh-42" in message
        assert "CAN_USE" in message
        # The operator is told what this probe is sparing them: the same failure
        # after the fleet is built and billing.
        assert "after the AWS fleet is already provisioned" in message
        # And the control plane's own words are carried through.
        assert "PERMISSION_DENIED" in message

    def test_the_probe_never_leaks_a_secret_the_control_plane_echoed(self, monkeypatch):
        # `_sql_statement` surfaces the control plane's message; if a future error
        # ever carried a credential, the probe must scrub it like every other
        # Databricks-facing message in this module does.
        def leaky(*_args, **_kwargs):
            raise RuntimeError(
                "warehouse start failed for client_secret=dose_TOPSECRET9: 500 error"
            )

        monkeypatch.setattr(lifecycle, "_sql_statement", leaky)
        with pytest.raises(RuntimeError) as excinfo:
            lifecycle._probe_sql_warehouse("prof", "wh")

        message = str(excinfo.value)
        assert "dose_TOPSECRET9" not in message
        assert "[redacted]" in message

    def test_a_still_resuming_warehouse_degrades_rather_than_blocks(self, monkeypatch, capsys):
        """A cold warehouse is not an unusable one, and must not fail a valid install.

        CAN_USE is checked at submission, so a denial returns a fast terminal
        FAILED (which this raises on). A warehouse the principal *can* use but that
        is still cold-starting only ever times out -- and a stopped non-serverless
        warehouse can take minutes. Blocking that operator would be the exact
        false-positive this preflight must not produce, so a timeout warns and
        proceeds; Round 4 waits for the same resume.
        """

        def times_out(*_args, **_kwargs):
            raise lifecycle._SqlStatementTimeout("Databricks SQL statement timed out")

        monkeypatch.setattr(lifecycle, "_sql_statement", times_out)
        # Must NOT raise.
        lifecycle._probe_sql_warehouse("prof", "wh-cold")
        out = capsys.readouterr().out
        assert "still resuming" in out
        assert "wh-cold" in out

    def test_a_terminal_failure_whose_message_mentions_a_timeout_still_fails(self, monkeypatch):
        # The boundary the degrade must not cross: a terminal FAILED whose
        # control-plane text merely CONTAINS "timed out" (a lock wait, a
        # downstream timeout) is a real failure -- only `_SqlStatementTimeout`
        # (the per-attempt deadline itself) degrades. A substring match on the
        # message would wrongly proceed here and spend on a doomed warehouse.
        def denied_mentioning_timeout(*_args, **_kwargs):
            raise RuntimeError(
                "Databricks SQL statement failed with state FAILED (warehouse=wh): "
                "lock wait timed out; the transaction could not proceed"
            )

        monkeypatch.setattr(lifecycle, "_sql_statement", denied_mentioning_timeout)
        with pytest.raises(RuntimeError, match="not usable"):
            lifecycle._probe_sql_warehouse("prof", "wh")

    def test_a_plain_can_use_denial_still_fails(self, monkeypatch):
        def denied(*_args, **_kwargs):
            raise RuntimeError(
                "Databricks SQL statement failed with state FAILED: PERMISSION_DENIED CAN_USE"
            )

        monkeypatch.setattr(lifecycle, "_sql_statement", denied)
        with pytest.raises(RuntimeError, match="not usable"):
            lifecycle._probe_sql_warehouse("prof", "wh")


class TestProvisionProbesBeforeSpend:
    """A first provision proves the warehouse before it touches AWS at all."""

    def _stub_identity(self, monkeypatch):
        monkeypatch.setattr(
            lifecycle,
            "select_setup_auth",
            lambda environment, requested: SimpleNamespace(mode="profile", profile="sandbox"),
        )
        monkeypatch.setattr(
            lifecycle, "_verify_databricks_identity", lambda profile: "operator@databricks.com"
        )

    def test_the_warehouse_is_probed_after_the_free_checks_and_before_terraform(
        self, monkeypatch, tmp_path
    ):
        monkeypatch.setenv("ANTI_DEMO_MANIFEST", str(tmp_path / "manifest.json"))
        monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "wh-99")
        self._stub_identity(monkeypatch)
        order: list[str] = []
        monkeypatch.setattr(
            lifecycle, "_verify_aws_identity", lambda *_a, **_k: order.append("aws")
        )
        # The egress seal makes a network call; stub it so this stays offline.
        monkeypatch.setattr(
            lifecycle, "_seal_initial_serverless_egress", lambda region: (None, None)
        )
        monkeypatch.setattr(
            lifecycle,
            "_probe_sql_warehouse",
            lambda profile, warehouse_id: order.append(f"probe:{warehouse_id}"),
        )

        # `_complete_provision` is where the first billable Terraform apply lives;
        # raising here pins the probe just before it, after the free checks.
        def stop_at_terraform(*_args, **_kwargs):
            order.append("complete")
            raise RuntimeError("stop the test right before terraform")

        monkeypatch.setattr(lifecycle, "_complete_provision", stop_at_terraform)

        with pytest.raises(RuntimeError, match="stop the test right before terraform"):
            lifecycle.provision(
                databricks_profile="fe-vm-test",
                aws_profile="sandbox",
                aws_region="us-west-2",
                expected_account="123456789012",
                owner="operator@databricks.com",
                operator_cidr="203.0.113.10/32",
                ttl_hours=72,
                zero_timeout_seconds=1,
            )

        assert order == ["aws", "probe:wh-99", "complete"], (
            "the warehouse is probed only after the free identity/CIDR/egress checks "
            "have had their chance to abort, and still before the billable Terraform apply"
        )

    def test_an_unusable_warehouse_stops_the_provision_before_terraform(
        self, monkeypatch, tmp_path
    ):
        monkeypatch.setenv("ANTI_DEMO_MANIFEST", str(tmp_path / "manifest.json"))
        monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "wh-broken")
        self._stub_identity(monkeypatch)
        monkeypatch.setattr(lifecycle, "_verify_aws_identity", lambda *_a, **_k: None)
        monkeypatch.setattr(
            lifecycle, "_seal_initial_serverless_egress", lambda region: (None, None)
        )

        def unusable(*_a, **_k):
            raise RuntimeError("warehouse wh-broken is not usable")

        monkeypatch.setattr(lifecycle, "_probe_sql_warehouse", unusable)

        # Terraform must not run past an unusable-warehouse verdict.
        def forbidden_terraform(*_args, **_kwargs):
            raise AssertionError("Terraform ran past an unusable-warehouse verdict")

        monkeypatch.setattr(lifecycle, "_complete_provision", forbidden_terraform)

        with pytest.raises(RuntimeError, match="wh-broken is not usable"):
            lifecycle.provision(
                databricks_profile="fe-vm-test",
                aws_profile="sandbox",
                aws_region="us-west-2",
                expected_account="123456789012",
                owner="operator@databricks.com",
                operator_cidr="203.0.113.10/32",
                ttl_hours=72,
                zero_timeout_seconds=1,
            )

    def test_a_provision_without_the_variable_does_not_probe(self, monkeypatch, tmp_path):
        # Nothing to prove yet: Round 4 refuses a missing DATABRICKS_WAREHOUSE_ID
        # later by name, and the probe must not invent a warehouse to check.
        monkeypatch.setenv("ANTI_DEMO_MANIFEST", str(tmp_path / "manifest.json"))
        monkeypatch.delenv("DATABRICKS_WAREHOUSE_ID", raising=False)
        self._stub_identity(monkeypatch)
        monkeypatch.setattr(lifecycle, "_verify_aws_identity", lambda *_a, **_k: None)
        monkeypatch.setattr(
            lifecycle, "_seal_initial_serverless_egress", lambda region: (None, None)
        )
        monkeypatch.setattr(
            lifecycle,
            "_probe_sql_warehouse",
            lambda *_a, **_k: pytest.fail("probed a warehouse that was never selected"),
        )

        def stop_at_terraform(*_a, **_k):
            raise RuntimeError("stop after the skipped probe")

        monkeypatch.setattr(lifecycle, "_complete_provision", stop_at_terraform)

        with pytest.raises(RuntimeError, match="stop after the skipped probe"):
            lifecycle.provision(
                databricks_profile="fe-vm-test",
                aws_profile="sandbox",
                aws_region="us-west-2",
                expected_account="123456789012",
                owner="operator@databricks.com",
                operator_cidr="203.0.113.10/32",
                ttl_hours=72,
                zero_timeout_seconds=1,
            )
