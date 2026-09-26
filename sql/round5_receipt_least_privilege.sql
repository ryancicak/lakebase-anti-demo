-- Round 5 remediation (B): least-privilege for the bout receipt history.
--
-- The receipt store writes two kinds of row into anti_demo_coordination.bout_receipt:
--   * declarations and terminal failures  -- append-only, INSERT ... ON CONFLICT
--     DO NOTHING; these are the IMMUTABLE record of what happened.
--   * one 'cleanup_update' overlay per bout -- the single MUTABLE fact (a retry may
--     later recover), INSERT ... ON CONFLICT DO UPDATE.
--
-- Granting the deployed app role table-wide UPDATE (so it can do the cleanup
-- upsert) would also let it rewrite the immutable declaration/terminal rows.
-- Instead, the mutable path is confined to a SECURITY DEFINER function owned by
-- the privileged coordination owner. The app role gets only:
--     schema USAGE, table SELECT + INSERT, function EXECUTE   (NOT table UPDATE).
--
-- Apply as the coordination schema OWNER (the privileged setup path, e.g.
-- `antidemo setup`). Idempotent. Substitute :"app_role" with the limited login
-- the deployed app connects as (ANTI_DEMO_COORDINATION_USER). psql:
--     psql "$OWNER_DSN" -v app_role=anti_demo_coordination_app \
--          -f sql/round5_receipt_least_privilege.sql

CREATE SCHEMA IF NOT EXISTS anti_demo_coordination;

-- The store's initialize() creates this on the owner path; kept IF NOT EXISTS so
-- the migration is self-contained and order-independent.
CREATE TABLE IF NOT EXISTS anti_demo_coordination.bout_receipt (
    session_id    text        NOT NULL,
    round_id      text        NOT NULL,
    sealing_event text        NOT NULL,
    receipt       text        NOT NULL,
    run_id        text,
    outcome       text        NOT NULL,
    sealed_at     timestamptz NOT NULL,
    document      jsonb       NOT NULL,
    written_at    timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (session_id, round_id, sealing_event)
);

-- The one mutable write, owned by the privileged coordination owner. sealing_event
-- is FIXED to 'cleanup_update' inside the function, not a parameter, so this
-- definer-owned UPDATE can never reach a declaration/terminal row: those carry a
-- different sealing_event and therefore a different primary key, so the
-- INSERT ... ON CONFLICT can only ever match the single cleanup_update overlay.
-- The WHERE clause is byte-for-byte the store's own guard (never move a newer
-- cleanup backward; only supersede on a changed cleanup_failure).
CREATE OR REPLACE FUNCTION anti_demo_coordination.bout_receipt_cleanup_upsert_v1(
    p_session_id text,
    p_round_id   text,
    p_receipt    text,
    p_run_id     text,
    p_outcome    text,
    p_sealed_at  timestamptz,
    p_document   jsonb
) RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    v_rows integer;
BEGIN
    INSERT INTO anti_demo_coordination.bout_receipt AS r (
        session_id, round_id, sealing_event, receipt, run_id, outcome, sealed_at, document
    ) VALUES (
        p_session_id, p_round_id, 'cleanup_update', p_receipt, p_run_id, p_outcome, p_sealed_at, p_document
    )
    ON CONFLICT (session_id, round_id, sealing_event) DO UPDATE
        SET receipt   = EXCLUDED.receipt,
            run_id    = EXCLUDED.run_id,
            outcome   = EXCLUDED.outcome,
            sealed_at = EXCLUDED.sealed_at,
            document  = EXCLUDED.document
        WHERE r.sealed_at <= EXCLUDED.sealed_at
          AND (r.document -> 'receipt' -> 'cleanup_failure'
               IS DISTINCT FROM EXCLUDED.document -> 'receipt' -> 'cleanup_failure');
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RETURN v_rows > 0;
END;
$function$;

REVOKE ALL ON FUNCTION anti_demo_coordination.bout_receipt_cleanup_upsert_v1(
    text, text, text, text, text, timestamptz, jsonb
) FROM PUBLIC;

-- App role: the minimum EFFECTIVE set. CONNECT on the database is granted with
-- the login itself; here it gets schema USAGE, table SELECT + INSERT (declarations
-- and the readback), and EXECUTE on the definer function (the cleanup overlay).
GRANT USAGE  ON SCHEMA anti_demo_coordination TO :"app_role";
GRANT SELECT, INSERT ON anti_demo_coordination.bout_receipt TO :"app_role";
GRANT EXECUTE ON FUNCTION anti_demo_coordination.bout_receipt_cleanup_upsert_v1(
    text, text, text, text, text, timestamptz, jsonb
) TO :"app_role";

-- The point of the exercise: the app role must NOT hold table-wide UPDATE, so it
-- cannot rewrite an immutable declaration/terminal receipt. Mutable cleanup state
-- is reachable ONLY through the definer function above.
REVOKE UPDATE ON anti_demo_coordination.bout_receipt FROM :"app_role";
