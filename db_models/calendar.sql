CREATE TABLE IF NOT EXISTS calendar (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    last_job_id UUID REFERENCES job(id),
    date DATE NOT NULL,
    exchange_id UUID NOT NULL REFERENCES exchange(id),
    open BOOLEAN NOT NULL,
    created_on TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_on TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE(date, exchange_id)
);