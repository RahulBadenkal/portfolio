CREATE TABLE exchange (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    last_job_id UUID REFERENCES job(id),
    ticker VARCHAR NOT NULL UNIQUE,
    name VARCHAR NOT NULL,
    country_code VARCHAR NOT NULL,
    created_on TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_on TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


--INSERT INTO exchange (ticker, name, country_code)
--VALUES
--    ('NYSE', 'The New York Stock Exchange', 'USA'),
--    ('NASDAQ', 'National Association of Securities Dealers Automated Quotations', 'USA'),
--    ('NSE', 'National Stock Exchange of India Limited', 'IND');