CREATE TABLE job (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR NOT NULL CHECK (name in ('exchange_listing_sync')),
    stage VARCHAR NOT NULL CHECK (stage IN ('started', 'completed')) DEFAULT 'started',
    finished_status VARCHAR CHECK (finished_status IN ('success', 'partial-success', 'failure')),
    duration DOUBLE PRECISION GENERATED ALWAYS AS (
        CASE
            WHEN stage = 'completed' THEN EXTRACT(EPOCH FROM (updated_on - created_on))
            ELSE NULL
        END
    ) STORED,
    log VARCHAR,
    created_on TIMESTAMPTZ NOT NULL,
    updated_on TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_job_name ON job(name);
CREATE INDEX idx_job_stage ON job(stage);
CREATE INDEX idx_job_finished_status ON job(finished_status);
CREATE INDEX idx_job_duration ON job(duration);
CREATE INDEX idx_job_created_on ON job(created_on);
CREATE INDEX idx_job_updated_on ON job(updated_on);
