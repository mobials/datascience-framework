CREATE TABLE IF NOT EXISTS s3 (
    id bigserial,
	file text NOT NULL,
	last_modified timestamptz NOT NULL,
	script text NOT NULL,
    scanned timestamptz NOT NULL,
	CONSTRAINT s3_unq_idx UNIQUE (file,script),
	CONSTRAINT s3_pk PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS cdc (
    s3_id bigint REFERENCES s3(id) ON DELETE CASCADE,
	status_date timestamptz NOT NULL,
	vin text NOT NULL,
	taxonomy_vin text NOT NULL,
	body_type text NULL,
	trim_orig text NULL,
	miles double precision NOT NULL,
	price double precision NOT NULL,
	CONSTRAINT cdc_vin_pk PRIMARY KEY (vin)
);

CREATE INDEX IF NOT EXISTS cdc_tax_vin_idx ON cdc(taxonomy_vin);

CREATE TABLE IF NOT EXISTS dataone (
    s3_id BIGINT references  s3(id) ON DELETE CASCADE,
    vin_pattern TEXT NOT NULL,
    vehicle_id INT NOT NULL,
    market TEXT,
    year INT NOT NULL,
    make TEXT NOT NULL,
    model TEXT NOT NULL,
    trim TEXT NOT NULL,
    style TEXT NOT NULL,
    body_type TEXT NOT NULL,
    msrp double precision NULL,
    CONSTRAINT  dataone_vin_pat_veh_id_idx PRIMARY KEY (vin_pattern,vehicle_id)
);

CREATE TABLE IF NOT EXISTS sessions
(
	id BIGSERIAL NOT NULL,
	session_info jsonb NOT NULL,
	validated boolean default false,
	CONSTRAINT sessions_pk PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS list_price_models
(
    session_id  BIGINT REFERENCES sessions (id) ON DELETE CASCADE,
    vin_pattern text  NOT NULL,
    vehicle_id  BIGINT   NOT NULL,
    model_info        jsonb NOT NULL,
    CONSTRAINT list_price_models_pk PRIMARY KEY (session_id,vin_pattern,vehicle_id)
);

CREATE TABLE IF NOT EXISTS list_price_model_training_data
(
  session_id BIGINT REFERENCES sessions (id) ON DELETE CASCADE,
  vin_pattern text NOT NULL,
  vehicle_id BIGINT NOT NULL,
  dataone_s3_id BIGINT NOT NULL,
  cdc_s3_id BIGINT NOT NULL,
  vin text NOT NULL,
  status text NOT NULL
);

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
    SELECT
        blocked_locks.pid AS blocked_pid,
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

CREATE OR REPLACE VIEW v_dataone_t1 AS
    SELECT
        dataone.vin_pattern,
        dataone.vehicle_id,
        dataone."year",
        dataone.make,
        dataone.model,
        dataone.trim,
        dataone."style",
        dataone.body_type,
        dataone.msrp,
        avg(dataone.msrp) OVER (PARTITION BY dataone.vin_pattern, dataone.market) AS vin_pattern_market_avg_msrp,
        count(*) OVER (PARTITION BY dataone.vin_pattern) AS trims,
        s3_id
    FROM
        dataone;

CREATE OR REPLACE VIEW v_training_set AS
    SELECT
        v_dataone_t1.vin_pattern,
        v_dataone_t1.vehicle_id,
        cdc.vin,
        v_dataone_t1."year",
        v_dataone_t1.make,
        v_dataone_t1.model,
        v_dataone_t1.trim,
        v_dataone_t1."style",
        v_dataone_t1.body_type,
        v_dataone_t1.msrp,
        cdc.miles as mileage,
        cdc.price,
        count(*) OVER (PARTITION BY vin_pattern,vehicle_id) AS vehicles,
        rank() OVER (PARTITION BY v_dataone_t1.vin_pattern,v_dataone_t1.vehicle_id ORDER BY cdc.status_date DESC) AS rank,
        v_dataone_t1.s3_id as dataone_s3_id,
        cdc.s3_id as cdc_s3_id
    FROM
        v_dataone_t1,
        cdc
    WHERE
        cdc.taxonomy_vin = v_dataone_t1.vin_pattern
    AND
    (
            v_dataone_t1.trims = 1
        OR
        (
            (
                (
                        v_dataone_t1.vin_pattern_market_avg_msrp > 0
                    AND
                        abs(v_dataone_t1.msrp - v_dataone_t1.vin_pattern_market_avg_msrp) / v_dataone_t1.vin_pattern_market_avg_msrp < 0.05
                )
                OR
                    lower(v_dataone_t1."trim") ~~ (('%'::text || lower(cdc.trim_orig)) || '%'::text)
            )
            AND
                v_dataone_t1.body_type = cdc.body_type
        )
    );

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

INSERT INTO scheduler (script,start_date,frequency) VALUES ('cdc','2020-05-23','1 day') ON CONFLICT ON CONSTRAINT scheduler_pk DO NOTHING;
INSERT INTO scheduler (script,start_date,frequency) VALUES ('dataone','2020-05-23','1 day') ON CONFLICT ON CONSTRAINT scheduler_pk DO NOTHING;
INSERT INTO scheduler (script,start_date,frequency) VALUES ('list_price_estimator','2020-05-18','1 week') ON CONFLICT ON CONSTRAINT scheduler_pk DO NOTHING;

CREATE OR REPLACE VIEW v_models AS
    SELECT
        session_id,
        vehicle_id,
        (model_info->>'intercept')::float8 as list_price_intercept,
        model_info->'coefficients' as list_price_coefficients,
        -2500.0::float8 as trade_value_intercept,
        json_build_object('estimated_list_price',power(extract(year from current_date) - (model_info->>'year')::int + 2,-0.06)) as trade_value_coefficients,
        (model_info->>'year')::int as year,
        model_info->>'make' as make,
        model_info->>'model' as model,
        model_info->>'trim' as trim,
        model_info->>'style' as style,
        (model_info->>'size')::int as vehicles,
        model_info->>'body_type' as body_type,
        (model_info->>'msrp')::float8 as msrp,
        0.9::float8 as multiplier,
        (model_info->>'minimum_list_price')::float8 as minimum_list_price,
        (model_info->>'maximum_list_price')::float8 as maximum_list_price,
        0.0::float8 as minimum_mileage_threshold
    FROM
        list_price_models;

CREATE OR REPLACE VIEW v_latest_models AS
    SELECT
        session_id,
        vehicle_id,
        (model_info->>'intercept')::float8 as list_price_intercept,
        model_info->'coefficients' as list_price_coefficients,
        -2500.0::float8 as trade_value_intercept,
        json_build_object('estimated_list_price',power(extract(year from current_date) - (model_info->>'year')::int + 2,-0.06)) as trade_value_coefficients,
        (model_info->>'year')::int as year,
        model_info->>'make' as make,
        model_info->>'model' as model,
        model_info->>'trim' as trim,
        model_info->>'style' as style,
        (model_info->>'size')::int as vehicles,
        model_info->>'body_type' as body_type,
        (model_info->>'msrp')::float8 as msrp,
        0.9::float8 as multiplier,
        (model_info->>'minimum_list_price')::float8 as minimum_list_price,
        (model_info->>'maximum_list_price')::float8 as maximum_list_price,
        0.0::float8 as minimum_mileage_threshold
    FROM
        list_price_models
    WHERE
	    session_id = (SELECT max(id) FROM sessions WHERE validated = true);