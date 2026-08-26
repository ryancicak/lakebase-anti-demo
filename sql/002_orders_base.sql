CREATE TABLE IF NOT EXISTS public.orders (
    order_id UUID PRIMARY KEY,
    customer_email TEXT NOT NULL,
    total_cents INTEGER NOT NULL CHECK (total_cents >= 0),
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- Reset the owned source to the agreed pre-migration contract. This statement
-- only ever runs against disposable Anti-Demo resources.
ALTER TABLE public.orders DROP COLUMN IF EXISTS delivery_instructions;

TRUNCATE TABLE public.orders;

INSERT INTO public.orders (
    order_id,
    customer_email,
    total_cents,
    status,
    created_at
) VALUES (
    '00000000-0000-4000-8000-000000000001',
    'ringside@example.com',
    4299,
    'ready',
    '2026-08-17T00:00:00Z'
);
