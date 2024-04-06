CREATE TABLE exchange_listing (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    exchange_id UUID NOT NULL REFERENCES exchange(id),
    last_job_id UUID REFERENCES job(id),
    ticker VARCHAR NOT NULL,
    isin VARCHAR UNIQUE,
    name VARCHAR,
    asset_type VARCHAR NOT NULL CHECK (asset_type IN ('EQUITY', 'ETF', 'MF')),
    listing_date DATE NOT NULL,
    de_listing_date DATE,
    is_active BOOLEAN NOT NULL,
    created_on TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_on TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE(exchange_id, ticker)
);