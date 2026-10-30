-- +goose Up
CREATE TABLE jobs (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    status      text NOT NULL CHECK (status IN ('queued', 'running', 'completed', 'cancelled', 'failed')),
    created_at  timestamptz NOT NULL DEFAULT now(),
    started_at  timestamptz,
    finished_at timestamptz,
    error       text
);

CREATE INDEX jobs_unfinished_idx ON jobs (status) WHERE status IN ('queued', 'running');

CREATE TABLE job_files (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id       uuid NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    position     integer NOT NULL CHECK (position >= 0),
    path         text NOT NULL,
    status       text NOT NULL CHECK (status IN ('ok', 'ok_with_errors', 'partial', 'failed', 'cancelled')),
    valid_rows   bigint NOT NULL CHECK (valid_rows >= 0),
    invalid_rows bigint NOT NULL CHECK (invalid_rows >= 0),
    last_line    bigint NOT NULL CHECK (last_line >= 0),
    error        text,
    close_error  text,
    error_counts jsonb NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (job_id, position),
    UNIQUE (job_id, path)
);

CREATE TABLE row_errors (
    job_file_id bigint NOT NULL REFERENCES job_files (id) ON DELETE CASCADE,
    line        bigint NOT NULL CHECK (line > 0),
    column_name text,
    kind        text NOT NULL,
    message     text NOT NULL,
    PRIMARY KEY (job_file_id, line)
);

CREATE TABLE sales_daily (
    job_id       uuid NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    sale_date    date NOT NULL,
    outlet_id    text NOT NULL,
    product_id   text NOT NULL,
    transactions bigint NOT NULL CHECK (transactions > 0),
    quantity     bigint NOT NULL,
    amount       bigint NOT NULL,
    PRIMARY KEY (job_id, sale_date, outlet_id, product_id)
);

-- +goose Down
DROP TABLE sales_daily;
DROP TABLE row_errors;
DROP TABLE job_files;
DROP TABLE jobs;
