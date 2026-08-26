CREATE TABLE IF NOT EXISTS public.anti_demo_probe (
    probe_id UUID PRIMARY KEY,
    expected_value TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

