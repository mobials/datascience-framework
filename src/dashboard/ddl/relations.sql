CREATE TABLE IF NOT EXISTS s3 (
    id bigserial,
	file text NOT NULL,
	last_modified timestamptz NOT NULL,
	script text not null,
    scanned timestamptz not null,
	CONSTRAINT s3_unq_idx UNIQUE (file,script),
	CONSTRAINT s3_pk PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS zuora_invoice_item_created (
    s3_id bigint REFERENCES s3(id) ON DELETE CASCADE,
    payload jsonb
);

CREATE UNIQUE INDEX IF NOT EXISTS zuora_invoice_item_created_unq_idx ON zuora_invoice_item_created(((payload->>'event_id')::uuid));

CREATE OR REPLACE VIEW v_active_queries AS
    SELECT pg_stat_activity.datid,
    pg_stat_activity.datname,
    pg_stat_activity.pid,
    pg_stat_activity.usesysid,
    pg_stat_activity.usename,
    pg_stat_activity.application_name,
    pg_stat_activity.client_addr,
    pg_stat_activity.client_hostname,
    pg_stat_activity.client_port,
    pg_stat_activity.backend_start,
    pg_stat_activity.xact_start,
    pg_stat_activity.query_start,
    pg_stat_activity.state_change,
    pg_stat_activity.wait_event_type,
    pg_stat_activity.wait_event,
    pg_stat_activity.state,
    pg_stat_activity.backend_xid,
    pg_stat_activity.backend_xmin,
    pg_stat_activity.query,
    pg_stat_activity.backend_type,
    now() - pg_stat_activity.query_start AS running_time
   FROM pg_stat_activity
  WHERE pg_stat_activity.state = 'active'::text;


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
    SELECT pg_stat_user_tables.relid,
    pg_stat_user_tables.schemaname,
    pg_stat_user_tables.relname,
    pg_stat_user_tables.seq_scan,
    pg_stat_user_tables.seq_tup_read,
    pg_stat_user_tables.idx_scan,
    pg_stat_user_tables.idx_tup_fetch,
    pg_stat_user_tables.n_tup_ins,
    pg_stat_user_tables.n_tup_upd,
    pg_stat_user_tables.n_tup_del,
    pg_stat_user_tables.n_tup_hot_upd,
    pg_stat_user_tables.n_live_tup,
    pg_stat_user_tables.n_dead_tup,
    pg_stat_user_tables.n_mod_since_analyze,
    pg_stat_user_tables.last_vacuum,
    pg_stat_user_tables.last_autovacuum,
    pg_stat_user_tables.last_analyze,
    pg_stat_user_tables.last_autoanalyze,
    pg_stat_user_tables.vacuum_count,
    pg_stat_user_tables.autovacuum_count,
    pg_stat_user_tables.analyze_count,
    pg_stat_user_tables.autoanalyze_count
   FROM pg_stat_user_tables
  ORDER BY pg_stat_user_tables.n_dead_tup DESC;