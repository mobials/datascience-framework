CREATE TABLE IF NOT EXISTS s3 (
    id bigserial,
	file text NOT NULL,
	last_modified timestamptz NOT NULL,
	script text not null,
    scanned timestamptz not null,
	CONSTRAINT s3_unq_idx UNIQUE (file,script),
	CONSTRAINT s3_pk PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS avr_widget_impressions(
    s3_id bigint REFERENCES s3(id) ON DELETE CASCADE,
    date timestamptz,
    master_business_id uuid,
    integration_settings_id uuid,
    ip_address text,
    product text,
    device_type text,
    referrer_url text,
    CONSTRAINT avr_widget_impressions_pk PRIMARY KEY (ip_address,date,product,integration_settings_id,master_business_id,device_type)
);

CREATE INDEX IF NOT EXISTS avr_widget_impressions_s3_id_idx ON avr_widget_impressions (s3_id);
CREATE INDEX IF NOT EXISTS avr_widget_impressions_date_idx ON avr_widget_impressions (date);

CREATE TABLE IF NOT EXISTS authenticom_sales_data (
    s3_id bigint REFERENCES s3(id) ON DELETE CASCADE,
    payload jsonb
);

ALTER TABLE avr_widget_impressions ADD COLUMN IF NOT EXISTS referrer_url TEXT;


CREATE TABLE IF NOT EXISTS tradesii_leads (
    s3_id bigint REFERENCES s3(id) ON DELETE CASCADE,
    payload jsonb
);

CREATE UNIQUE INDEX IF NOT EXISTS tradesii_leads_event_id_unq_idx ON tradesii_leads(((payload->>'event_id')::uuid));

CREATE TABLE IF NOT EXISTS credsii_leads (
    s3_id bigint REFERENCES s3(id) ON DELETE CASCADE,
    payload jsonb
);

CREATE UNIQUE INDEX IF NOT EXISTS credsii_leads_event_id_unq_idx ON credsii_leads(((payload->>'event_id')::uuid));

CREATE TABLE IF NOT EXISTS insuresii_leads (
    s3_id bigint REFERENCES s3(id) ON DELETE CASCADE,
    payload jsonb
);

CREATE UNIQUE INDEX IF NOT EXISTS insuresii_leads_event_id_unq_idx ON insuresii_leads(((payload->>'event_id')::uuid));

CREATE TABLE IF NOT EXISTS reservesii_reservations (
    s3_id bigint REFERENCES s3(id) ON DELETE CASCADE,
    payload jsonb
);

CREATE UNIQUE INDEX IF NOT EXISTS reservesii_reservations_event_id_unq_idx ON reservesii_reservations(((payload->>'event_id')::uuid));

CREATE TABLE IF NOT EXISTS scheduler
(
    script text NOT NULL,
    start_date timestamptz NOT NULL,
    frequency interval NOT NULL,
    last_run timestamptz,
    last_update timestamptz,
    status text,
    run_time interval,
    CONSTRAINT scheduler_pk PRIMARY KEY (script)
);

INSERT INTO scheduler (script,start_date,frequency) VALUES ('avr_widget_impressions','2020-05-23','15 minute') ON CONFLICT ON CONSTRAINT scheduler_pk DO NOTHING;

CREATE OR REPLACE VIEW v_table_size as
	SELECT a.oid,
        a.table_schema,
        a.table_name,
        a.row_estimate,
        a.total_bytes,
        a.index_bytes,
        a.toast_bytes,
        a.table_bytes,
        pg_size_pretty(a.total_bytes) AS total,
        pg_size_pretty(a.index_bytes) AS index,
        pg_size_pretty(a.toast_bytes) AS toast,
        pg_size_pretty(a.table_bytes) AS "table"
   FROM ( SELECT a_1.oid,
            a_1.table_schema,
            a_1.table_name,
            a_1.row_estimate,
            a_1.total_bytes,
            a_1.index_bytes,
            a_1.toast_bytes,
            a_1.total_bytes - a_1.index_bytes - COALESCE(a_1.toast_bytes, 0::bigint) AS table_bytes
           FROM ( SELECT c.oid,
                    n.nspname AS table_schema,
                    c.relname AS table_name,
                    c.reltuples AS row_estimate,
                    pg_total_relation_size(c.oid::regclass) AS total_bytes,
                    pg_indexes_size(c.oid::regclass) AS index_bytes,
                    pg_total_relation_size(c.reltoastrelid::regclass) AS toast_bytes
                   FROM pg_class c
                     LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
                  WHERE c.relkind = 'r'::"char") a_1) a
  ORDER BY a.total_bytes DESC;

CREATE OR REPLACE VIEW v_relation_size AS
    SELECT n.nspname,
        c.relname,
        pg_size_pretty(pg_relation_size(c.oid::regclass)) AS size
   FROM pg_class c
     LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname <> ALL (ARRAY['pg_catalog'::name, 'information_schema'::name])
  ORDER BY (pg_relation_size(c.oid::regclass)) desc;

CREATE OR REPLACE VIEW v_active_queries AS
    SELECT
        datid,
        datname,
        pid,
        usesysid,
        usename,
        application_name,
        client_addr,
        client_hostname,
        client_port,
        backend_start,
        xact_start,
        query_start,
        state_change,
        wait_event_type,
        wait_event,
        state,
        backend_xid,
        backend_xmin,
        query,
        backend_type,
        now() - query_start AS running_time
    FROM
        pg_stat_activity
    WHERE
        state = 'active';


CREATE OR REPLACE VIEW v_blocking_statements AS
    SELECT blocked_locks.pid AS blocked_pid,
    blocked_activity.usename AS blocked_user,
    blocking_locks.pid AS blocking_pid,
    blocking_activity.usename AS blocking_user,
    blocked_activity.query AS blocked_statement,
    blocking_activity.query AS current_statement_in_blocking_process
   FROM pg_locks blocked_locks
     JOIN pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
     JOIN pg_locks blocking_locks ON blocking_locks.locktype = blocked_locks.locktype AND NOT blocking_locks.database IS DISTINCT FROM blocked_locks.database AND NOT blocking_locks.relation IS DISTINCT FROM blocked_locks.relation AND NOT blocking_locks.page IS DISTINCT FROM blocked_locks.page AND NOT blocking_locks.tuple IS DISTINCT FROM blocked_locks.tuple AND NOT blocking_locks.virtualxid IS DISTINCT FROM blocked_locks.virtualxid AND NOT blocking_locks.transactionid IS DISTINCT FROM blocked_locks.transactionid AND NOT blocking_locks.classid IS DISTINCT FROM blocked_locks.classid AND NOT blocking_locks.objid IS DISTINCT FROM blocked_locks.objid AND NOT blocking_locks.objsubid IS DISTINCT FROM blocked_locks.objsubid AND blocking_locks.pid <> blocked_locks.pid
     JOIN pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
  WHERE NOT blocked_locks.granted;

CREATE OR REPLACE VIEW v_vacuum_stats AS
    SELECT
        relid,
        schemaname,
        relname,
        seq_scan,
        seq_tup_read,
        idx_scan,
        idx_tup_fetch,
        n_tup_ins,
        n_tup_upd,
        n_tup_del,
        n_tup_hot_upd,
        n_live_tup,
        n_dead_tup,
        n_mod_since_analyze,
        last_vacuum,
        last_autovacuum,
        last_analyze,
        last_autoanalyze,
        vacuum_count,
        autovacuum_count,
        analyze_count,
        autoanalyze_count
   FROM
        pg_stat_user_tables
  ORDER BY
        n_dead_tup DESC;

CREATE TABLE IF NOT EXISTS zuora_invoice_item_created (
    s3_id bigint REFERENCES s3(id) ON DELETE CASCADE,
    payload jsonb
);

CREATE UNIQUE INDEX IF NOT EXISTS zuora_invoice_item_created_unq_idx ON zuora_invoice_item_created(((payload->>'event_id')::uuid));

CREATE TABLE IF NOT EXISTS zuora_credit_memo_posted (
    s3_id bigint REFERENCES s3(id) ON DELETE CASCADE,
    payload jsonb
);

CREATE UNIQUE INDEX IF NOT EXISTS zuora_credit_memo_posted_unq_idx ON zuora_credit_memo_posted(((payload->>'event_id')::uuid));

INSERT INTO scheduler (script,start_date,frequency) VALUES ('zuora_credit_memo_posted','2020-05-23','1 day') ON CONFLICT ON CONSTRAINT scheduler_pk DO NOTHING;
INSERT INTO scheduler (script,start_date,frequency) VALUES ('zuora_invoice_item_created','2020-05-23','1 day') ON CONFLICT ON CONSTRAINT scheduler_pk DO NOTHING;