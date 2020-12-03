import sys
sys.path.insert(0,'../..')
import boto3
import io
import json
import os
import datetime
import psycopg2
import src.settings
import postgreshandler

queries = [
    '''
        CREATE SCHEMA IF NOT EXISTS s3;
    ''',
    '''
        CREATE SCHEMA IF NOT EXISTS autoverify;
    ''',
    '''
        CREATE SCHEMA IF NOT EXISTS zuora;
    ''',
    '''
        CREATE SCHEMA IF NOT EXISTS operations;
    ''',
    '''
        CREATE SCHEMA IF NOT EXISTS utility;
    ''',
    '''
       CREATE SCHEMA IF NOT EXISTS sage;
    ''',
    '''
        CREATE SCHEMA IF NOT EXISTS redash;
    ''',
    '''
        CREATE SCHEMA IF NOT EXISTS miscellaneous;
    ''',
    '''
        CREATE SCHEMA IF NOT EXISTS vendors;
    ''',
    '''
        CREATE TABLE IF NOT EXISTS miscellaneous.dealer_socket_sales_reports_gross_profit
        (
            dealer text not null,
            purchase_number bigint not null,
            purchase_date timestamptz not null,
            payload jsonb,
            CONSTRAINT dealer_socket_sales_reports_gross_profit_pkey PRIMARY KEY (dealer,purchase_number,purchase_date)
        );
    ''',
    '''
        CREATE TABLE IF NOT EXISTS operations.scheduler
        (
            schema text not null,
            script text NOT NULL,
            start_date timestamptz NOT NULL,
            frequency interval NOT NULL,
            last_run timestamptz,
            last_update timestamptz,
            status text,
            run_time interval,
            CONSTRAINT scheduler_pk PRIMARY KEY (script)
        );
    ''',
    '''
        INSERT INTO operations.scheduler (schema,script,start_date,frequency)
        VALUES ('autoverify','mpm_leads','2020-07-01','15 minute')
        ON CONFLICT ON CONSTRAINT scheduler_pk
        DO NOTHING;
    ''',
    '''
        INSERT INTO operations.scheduler (schema,script,start_date,frequency)
        VALUES ('autoverify','mpm_integration_settings','2020-07-01','15 minute')
         ON CONFLICT ON CONSTRAINT scheduler_pk
         DO NOTHING;
    ''',
    '''
        INSERT INTO operations.scheduler (schema,script,start_date,frequency)
        VALUES ('autoverify','tradesii_businesses','2020-07-01','15 minute')
        ON CONFLICT ON CONSTRAINT scheduler_pk
        DO NOTHING;
    ''',
    '''
        INSERT INTO operations.scheduler (schema,script,start_date,frequency)
        VALUES ('autoverify','tradesii_leads','2020-07-01','15 minute')
        ON CONFLICT ON CONSTRAINT scheduler_pk
        DO NOTHING;
    ''',
    '''
        INSERT INTO operations.scheduler (schema,script,start_date,frequency)
        VALUES ('autoverify','tradesii_business_profiles','2020-07-01','15 minute')
        ON CONFLICT ON CONSTRAINT scheduler_pk
        DO NOTHING;
    ''',
    '''
        INSERT INTO operations.scheduler (schema,script,start_date,frequency)
        VALUES ('autoverify','sda_master_businesses','2020-07-01','15 minute')
        ON CONFLICT ON CONSTRAINT scheduler_pk
        DO NOTHING;
    ''',
    '''
        INSERT INTO operations.scheduler (schema,script,start_date,frequency)
        VALUES ('autoverify','credsii_leads','2020-07-01','15 minute')
        ON CONFLICT ON CONSTRAINT scheduler_pk
        DO NOTHING;
    ''',
    '''
        INSERT INTO operations.scheduler (schema,script,start_date,frequency)
        VALUES ('autoverify','credsii_reports','2020-07-01','15 minute')
        ON CONFLICT ON CONSTRAINT scheduler_pk
        DO NOTHING;
    ''',
    '''
        INSERT INTO operations.scheduler (schema,script,start_date,frequency)
        VALUES ('autoverify','credsii_businesses','2020-07-01','15 minute')
        ON CONFLICT ON CONSTRAINT scheduler_pk
        DO NOTHING;
    ''',
    '''
        INSERT INTO operations.scheduler (schema,script,start_date,frequency)
        VALUES ('autoverify','credsii_business_profiles','2020-07-01','15 minute')
        ON CONFLICT ON CONSTRAINT scheduler_pk
        DO NOTHING;
    ''',
    '''
        INSERT INTO operations.scheduler (schema,script,start_date,frequency)
        VALUES ('s3','avr_widget_impressions','2020-01-01','15 minute')
        ON CONFLICT ON CONSTRAINT scheduler_pk
        DO NOTHING;
    ''',
    '''
        INSERT INTO operations.scheduler (schema,script,start_date,frequency)
        VALUES ('s3','avr_button_widget_was_rendered','2020-01-01','15 minute')
        ON CONFLICT ON CONSTRAINT scheduler_pk
        DO NOTHING;
    ''',
    '''
        INSERT INTO operations.scheduler (schema,script,start_date,frequency)
        VALUES ('s3','integrations_widget_was_rendered','2020-08-01','15 minute')
        ON CONFLICT ON CONSTRAINT scheduler_pk
        DO NOTHING;
    ''',
    '''
        INSERT INTO operations.scheduler (schema,script,start_date,frequency)
        VALUES ('public','avr_unique_impressions_ip_daily','2020-01-01','15 minute')
        ON CONFLICT ON CONSTRAINT scheduler_pk
        DO NOTHING;
    ''',
    '''
        INSERT INTO operations.scheduler (schema,script,start_date,frequency)
        VALUES ('public','avr_unique_impressions_ip_weekly','2020-01-01','15 minute')
        ON CONFLICT ON CONSTRAINT scheduler_pk
        DO NOTHING;
    ''',
    '''
        INSERT INTO operations.scheduler (schema,script,start_date,frequency)
        VALUES ('s3','integrations_button_widget_was_rendered','2020-08-01','15 minute')
        ON CONFLICT ON CONSTRAINT scheduler_pk
        DO NOTHING;
    ''',
    '''
        INSERT INTO operations.scheduler (schema,script,start_date,frequency)
        VALUES ('autoverify','insuresii_leads','2020-08-01','15 minute')
        ON CONFLICT ON CONSTRAINT scheduler_pk
        DO NOTHING;
    ''',
    '''
        INSERT INTO operations.scheduler (schema,script,start_date,frequency)
        VALUES ('autoverify','insuresii_quotes','2020-08-01','15 minute')
        ON CONFLICT ON CONSTRAINT scheduler_pk
        DO NOTHING;
    ''',
    '''
        INSERT INTO operations.scheduler (schema,script,start_date,frequency)
        VALUES ('autoverify','insuresii_business_profiles','2020-08-01','15 minute')
        ON CONFLICT ON CONSTRAINT scheduler_pk
        DO NOTHING;
    ''',
    '''
        INSERT INTO operations.scheduler (schema,script,start_date,frequency)
        VALUES ('autoverify','insuresii_businesses','2020-08-01','15 minute')
        ON CONFLICT ON CONSTRAINT scheduler_pk
        DO NOTHING;
    ''',
    '''
        INSERT INTO operations.scheduler (schema,script,start_date,frequency)
        VALUES ('public','avr_unique_impressions_ip_monthly','2020-01-01','15 minute')
        ON CONFLICT ON CONSTRAINT scheduler_pk
        DO NOTHING;
    ''',
    '''
        INSERT INTO operations.scheduler (schema,script,start_date,frequency)
        VALUES ('public','avr_unique_impressions_ip_quarterly','2020-01-01','15 minute')
        ON CONFLICT ON CONSTRAINT scheduler_pk
        DO NOTHING;
    ''',
    '''
        INSERT INTO operations.scheduler (schema,script,start_date,frequency)
        VALUES ('zuora','zuora','2020-01-01','3 hour')
        ON CONFLICT ON CONSTRAINT scheduler_pk
        DO NOTHING;
    ''',
    '''
        INSERT INTO operations.scheduler (schema,script,start_date,frequency)
        VALUES ('s3','authenticom_sales_data','2020-01-01','3 hour')
        ON CONFLICT ON CONSTRAINT scheduler_pk
        DO NOTHING;
    ''',
    '''
        INSERT INTO operations.scheduler (schema,script,start_date,frequency)
        VALUES ('autoverify','accident_check_reports','2020-01-01','15 minute')
        ON CONFLICT ON CONSTRAINT scheduler_pk
        DO NOTHING;
    ''',
    '''
        INSERT INTO operations.scheduler (schema,script,start_date,frequency)
        VALUES ('autoverify','m_mpm_lead_details','2020-01-01','3 hour')
        ON CONFLICT ON CONSTRAINT scheduler_pk
        DO NOTHING;
    ''',
    '''
        INSERT INTO operations.scheduler (schema,script,start_date,frequency)
        VALUES ('vendors','dataone','2020-01-01','1 day')
        ON CONFLICT ON CONSTRAINT scheduler_pk
        DO NOTHING;
    ''',
    '''
        INSERT INTO operations.scheduler (schema,script,start_date,frequency)
        VALUES ('vendors','marketcheck_ca','2020-01-01','1 day')
        ON CONFLICT ON CONSTRAINT scheduler_pk
        DO NOTHING;
    ''',
    '''
        INSERT INTO operations.scheduler (schema,script,start_date,frequency)
        VALUES ('vendors','marketcheck_us','2020-01-01','1 day')
        ON CONFLICT ON CONSTRAINT scheduler_pk
        DO NOTHING;
    ''',
    '''
        INSERT INTO operations.scheduler (schema,script,start_date,frequency)
        VALUES ('autoverify','dashboard_lead_content','2020-01-01 00:30:00','3 hours')
        ON CONFLICT ON CONSTRAINT scheduler_pk
        DO NOTHING;
    ''',
    '''
        INSERT INTO operations.scheduler (schema,script,start_date,frequency)
        VALUES ('autoverify','mpm_lead_details','2020-01-01','3 hour')
        ON CONFLICT ON CONSTRAINT scheduler_pk
        DO NOTHING;
    ''',
    '''
        INSERT INTO operations.scheduler (schema,script,start_date,frequency)
        VALUES ('public','dashboard_refresh','2020-01-01 00:06:00','6 hours')
        ON CONFLICT ON CONSTRAINT scheduler_pk
        DO NOTHING;
    '''
    '''
        CREATE TABLE IF NOT EXISTS autoverify.mpm_leads
        (
            id uuid primary key,
            created_at timestamptz not null,
            updated_at timestamptz not null,
            payload jsonb not null
        );
        ALTER TABLE autoverify.mpm_leads
        SET (autovacuum_vacuum_scale_factor = 0, autovacuum_vacuum_threshold = 10000);
        CREATE INDEX IF NOT EXISTS mpm_leads_created_at_idx
        ON autoverify.mpm_leads (created_at);
        CREATE INDEX IF NOT EXISTS mpm_leads_updated_at_idx
        ON autoverify.mpm_leads (updated_at);
    ''',
    '''
        CREATE TABLE IF NOT EXISTS autoverify.mpm_integration_settings
        (
            id uuid primary key,
            created_at timestamptz not null,
            updated_at timestamptz not null,
            payload jsonb not null
        );
    ''',
    '''
        CREATE TABLE IF NOT EXISTS autoverify.tradesii_leads
        (
            id uuid primary key,
            created_at timestamptz not null,
            payload jsonb not null
        );
        ALTER TABLE autoverify.tradesii_leads
        SET (autovacuum_vacuum_scale_factor = 0, autovacuum_vacuum_threshold = 10000);
    ''',
    '''
        CREATE TABLE IF NOT EXISTS autoverify.tradesii_businesses
        (
            id uuid primary key,
            created_at timestamptz not null,
            updated_at timestamptz,
            payload jsonb not null
        );
    ''',
    '''
        CREATE TABLE IF NOT EXISTS autoverify.tradesii_business_profiles
        (
            id uuid primary key,
            created_at timestamptz not null,
            updated_at timestamptz,
            payload jsonb not null
        );
    ''',
    '''
        CREATE TABLE IF NOT EXISTS autoverify.sda_master_businesses
        (
            id uuid primary key,
            created_at timestamptz,
            updated_at timestamptz not null,
            payload jsonb not null
        );
    ''',
    '''
        CREATE TABLE IF NOT EXISTS autoverify.credsii_leads
        (
            id uuid primary key,
            created_at timestamptz not null,
            updated_at timestamptz not null,
            payload jsonb not null
        );
        ALTER TABLE autoverify.credsii_leads
        SET (autovacuum_vacuum_scale_factor = 0, autovacuum_vacuum_threshold = 10000);
    ''',
    '''
        CREATE TABLE IF NOT EXISTS autoverify.credsii_reports
        (
            id uuid primary key,
            created_at timestamptz not null,
            updated_at timestamptz not null,
            payload jsonb not null
        );
        ALTER TABLE autoverify.credsii_reports
        SET (autovacuum_vacuum_scale_factor = 0, autovacuum_vacuum_threshold = 10000);
    ''',
    '''
        CREATE TABLE IF NOT EXISTS autoverify.credsii_businesses
        (
            id uuid primary key,
            created_at timestamptz not null,
            updated_at timestamptz,
            payload jsonb not null
        );
    ''',
    '''
        CREATE TABLE IF NOT EXISTS autoverify.credsii_business_profiles
        (
            id uuid primary key,
            created_at timestamptz not null,
            updated_at timestamptz,
            payload jsonb not null
        );
    ''',
    '''
        CREATE TABLE IF NOT EXISTS s3.scanned_files
        (
            id bigserial,
            file text NOT NULL,
            last_modified timestamptz NOT NULL,
            script text not null,
            scanned timestamptz not null,
            CONSTRAINT s3_unq_idx UNIQUE (file,script),
            CONSTRAINT s3_pk PRIMARY KEY (id)
        );
    ''',
    '''
        CREATE TABLE IF NOT EXISTS s3.avr_widget_impressions
        (
            s3_id bigint REFERENCES s3.scanned_files(id) ON DELETE CASCADE,
            event_id uuid,
            happened_at timestamptz,
            payload jsonb
        );
        CREATE INDEX IF NOT EXISTS avr_widget_impressions_s3_id_idx
        ON s3.avr_widget_impressions (s3_id);
        CREATE INDEX IF NOT EXISTS avr_widget_impressions_idx
        ON s3.avr_widget_impressions (happened_at);
    ''',
    '''
        CREATE TABLE IF NOT EXISTS s3.avr_button_widget_was_rendered
        (
            s3_id bigint REFERENCES s3.scanned_files(id) ON DELETE CASCADE,
            event_id uuid,
            happened_at timestamptz,
            payload jsonb
        );
        CREATE INDEX IF NOT EXISTS avr_button_widget_was_rendered_s3_id_idx
        ON s3.avr_button_widget_was_rendered (s3_id);
        CREATE INDEX IF NOT EXISTS avr_button_widget_was_rendered_idx
        ON s3.avr_button_widget_was_rendered (happened_at);
    ''',
    '''
        CREATE TABLE IF NOT EXISTS s3.integrations_widget_was_rendered
        (
            s3_id bigint REFERENCES s3.scanned_files(id) ON DELETE CASCADE,
            event_id uuid,
            happened_at timestamptz not null,
            payload jsonb not null
        );
        CREATE INDEX IF NOT EXISTS integrations_widget_was_rendered_s3_id_idx
        ON s3.integrations_widget_was_rendered (s3_id);
        CREATE INDEX IF NOT EXISTS integrations_widget_was_rendered_date_idx
        ON s3.integrations_widget_was_rendered (happened_at);
    ''',
    '''
        CREATE TABLE IF NOT EXISTS s3.integrations_button_widget_was_rendered
        (
            s3_id bigint REFERENCES s3.scanned_files(id) ON DELETE CASCADE,
            event_id uuid,
            happened_at timestamptz not null,
            payload jsonb not null
        );
        CREATE INDEX IF NOT EXISTS integrations_button_widget_was_rendered_s3_id_idx
        ON s3.integrations_button_widget_was_rendered (s3_id);
        CREATE INDEX IF NOT EXISTS integrations_button_widget_was_rendered_date_idx
        ON s3.integrations_button_widget_was_rendered (happened_at);
    ''',
    '''
        CREATE TABLE IF NOT EXISTS avr_unique_impressions_ip_daily
        (
            master_business_id uuid,
            date timestamptz,
            impressions int8,
            constraint avr_unique_impressions_ip_daily_pk primary key (master_business_id,date)
        );
        CREATE INDEX IF NOT EXISTS avr_unique_impressions_ip_daily_date_idx
        ON avr_unique_impressions_ip_daily(date);
    ''',
    '''
        CREATE TABLE IF NOT EXISTS avr_unique_impressions_ip_weekly
        (
            master_business_id uuid,
            date timestamptz,
            impressions int8,
            constraint avr_unique_impressions_ip_weekly_pk primary key (master_business_id,date)
        );
        CREATE INDEX IF NOT EXISTS avr_unique_impressions_ip_weekly_date_idx
        ON avr_unique_impressions_ip_weekly(date);
    ''',
    '''
        CREATE TABLE IF NOT EXISTS avr_unique_impressions_ip_monthly
        (
            master_business_id uuid,
            date timestamptz,
            impressions int8,
            constraint avr_unique_impressions_ip_monthly_pk primary key (master_business_id,date)
        );
        CREATE INDEX IF NOT EXISTS avr_unique_impressions_ip_monthly_date_idx
        ON avr_unique_impressions_ip_monthly(date);
    ''',
    '''
        CREATE TABLE IF NOT EXISTS avr_unique_impressions_ip_quarterly
        (
            master_business_id uuid,
            date timestamptz,
            impressions int8,
            constraint avr_unique_impressions_ip_quarterly_pk primary key (master_business_id,date)
        );
        CREATE INDEX IF NOT EXISTS avr_unique_impressions_ip_quarterly_date_idx
        ON avr_unique_impressions_ip_quarterly(date);
    ''',
    '''
        CREATE TABLE IF NOT EXISTS autoverify.insuresii_leads
        (
            id uuid primary key,
            created_at timestamptz not null,
            updated_at timestamptz not null,
            payload jsonb not null
        );
        ALTER TABLE autoverify.insuresii_leads
        SET (autovacuum_vacuum_scale_factor = 0, autovacuum_vacuum_threshold = 10000);
    ''',
    '''
        CREATE TABLE IF NOT EXISTS autoverify.insuresii_quotes
        (
            id uuid primary key,
            created_at timestamptz not null,
            updated_at timestamptz not null,
            payload jsonb not null
        );
        ALTER TABLE autoverify.insuresii_quotes
        SET (autovacuum_vacuum_scale_factor = 0, autovacuum_vacuum_threshold = 10000);
    ''',
    '''
        CREATE TABLE IF NOT EXISTS autoverify.insuresii_business_profiles
        (
            id uuid primary key,
            payload jsonb not null
        );
    ''',
    '''
        CREATE TABLE IF NOT EXISTS autoverify.insuresii_businesses
        (
            id uuid primary key,
            payload jsonb not null
        );
    ''',
    '''
        CREATE TABLE IF NOT EXISTS autoverify.accident_check_reports
        (
            id uuid primary key,
            created_at timestamptz not null,
            payload jsonb not null
        );
    ''',
    '''
        CREATE OR REPLACE VIEW utility.v_table_size as
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
        FROM
        (
            SELECT
                a_1.oid,
                a_1.table_schema,
                a_1.table_name,
                a_1.row_estimate,
                a_1.total_bytes,
                a_1.index_bytes,
                a_1.toast_bytes,
                a_1.total_bytes - a_1.index_bytes - COALESCE(a_1.toast_bytes, 0::bigint) AS table_bytes
            FROM
            (
                SELECT
                    c.oid,
                    n.nspname AS table_schema,
                    c.relname AS table_name,
                    c.reltuples AS row_estimate,
                    pg_total_relation_size(c.oid::regclass) AS total_bytes,
                    pg_indexes_size(c.oid::regclass) AS index_bytes,
                    pg_total_relation_size(c.reltoastrelid::regclass) AS toast_bytes
                FROM
                    pg_class c
                 LEFT JOIN pg_namespace n
                 ON n.oid = c.relnamespace
                  WHERE c.relkind = 'r'::"char"
            ) a_1
        ) a
        ORDER BY a.total_bytes DESC;
    ''',
    '''
        CREATE OR REPLACE VIEW utility.v_relation_size AS
        SELECT n.nspname,
            c.relname,
            pg_size_pretty(pg_relation_size(c.oid::regclass)) AS size
        FROM pg_class c
         LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname <> ALL (ARRAY['pg_catalog'::name, 'information_schema'::name])
        ORDER BY (pg_relation_size(c.oid::regclass)) desc;
    ''',
    '''
        CREATE OR REPLACE VIEW utility.v_active_queries AS
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
    ''',
    '''
        CREATE OR REPLACE VIEW utility.v_blocking_statements AS
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
    ''',
    '''
        CREATE OR REPLACE VIEW utility.v_vacuum_stats AS
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
    ''',
    '''
        CREATE TABLE IF NOT EXISTS zuora.account
        (
            id text primary key,
            updateddate timestamptz not null,
            payload jsonb
        );
    ''',
    '''
        CREATE TABLE IF NOT EXISTS zuora.contact
        (
            id text primary key,
            updateddate timestamptz not null,
            payload jsonb
        );
    ''',
    '''
        CREATE TABLE IF NOT EXISTS zuora.invoice
        (
            id text primary key,
            updateddate timestamptz not null,
            payload jsonb
        );
    ''',
    '''
        CREATE TABLE IF NOT EXISTS zuora.invoiceitem
        (
            id text primary key,
            updateddate timestamptz not null,
            payload jsonb
        );
    ''',
    '''
        CREATE TABLE IF NOT EXISTS zuora.creditmemo
        (
            id text primary key,
            updateddate timestamptz not null,
            payload jsonb
        );
    ''',
    '''
        CREATE TABLE IF NOT EXISTS zuora.order
        (
            id text primary key,
            updateddate timestamptz not null,
            payload jsonb
        );
    ''',
    '''
        CREATE TABLE IF NOT EXISTS zuora.subscription
        (
            id text primary key,
            updateddate timestamptz not null,
            payload jsonb
        );
    ''',
    '''
        CREATE TABLE IF NOT EXISTS vendors.dataone
        (
            s3_id BIGINT references s3.scanned_files(id) ON DELETE CASCADE,
            vin_pattern TEXT NOT NULL,
            vehicle_id INT NOT NULL,
            payload jsonb,
            CONSTRAINT  dataone_vin_pat_veh_id_idx PRIMARY KEY (vin_pattern,vehicle_id)
        );
    ''',
    '''
        CREATE TABLE IF NOT EXISTS vendors.marketcheck_ca
        (
            s3_id BIGINT references s3.scanned_files(id) ON DELETE CASCADE,
            id text,
            dealer_id integer,
            vin text,
            price double precision,
            miles double precision,
            taxonomy_vin text,
            scraped_at timestamptz,
            status_date timestamptz,
            zip text,
            latitude double precision,
            longitude double precision,
            city text,
            state text
        );
        CREATE INDEX IF NOT EXISTS marketcheck_ca_s3_id_idx ON vendors.marketcheck_ca (s3_id);
        CREATE INDEX IF NOT EXISTS marketcheck_ca_status_date_idx ON vendors.marketcheck_ca (status_date);
        CREATE INDEX IF NOT EXISTS marketcheck_ca_taxonomy_vin_idx ON vendors.marketcheck_ca (taxonomy_vin);
    ''',
    '''
        CREATE TABLE IF NOT EXISTS vendors.marketcheck_ca_used
        (
            s3_id BIGINT references s3.scanned_files(id) ON DELETE CASCADE,
            id text,
            dealer_id integer,
            vin text,
            price double precision,
            miles double precision,
            taxonomy_vin text,
            scraped_at timestamptz,
            status_date timestamptz,
            zip text,
            latitude double precision,
            longitude double precision,
            city text,
            state text
        );
        CREATE INDEX IF NOT EXISTS marketcheck_ca_used_s3_id_idx ON vendors.marketcheck_ca_used (s3_id);
        CREATE INDEX IF NOT EXISTS marketcheck_ca_used_status_date_idx ON vendors.marketcheck_ca_used (status_date);
    ''',
    '''
        CREATE TABLE IF NOT EXISTS vendors.marketcheck_ca_new
        (
            s3_id BIGINT references s3.scanned_files(id) ON DELETE CASCADE,
            id text,
            dealer_id integer,
            vin text,
            price double precision,
            miles double precision,
            taxonomy_vin text,
            scraped_at timestamptz,
            status_date timestamptz,
            zip text,
            latitude double precision,
            longitude double precision,
            city text,
            state text
        );
        CREATE INDEX IF NOT EXISTS marketcheck_ca_new_s3_id_idx ON vendors.marketcheck_ca_new (s3_id);
        CREATE INDEX IF NOT EXISTS marketcheck_ca_new_status_date_idx ON vendors.marketcheck_ca_new (status_date);
    ''',
    '''
        CREATE TABLE IF NOT EXISTS vendors.marketcheck_us
        (
            s3_id BIGINT references s3.scanned_files(id) ON DELETE CASCADE,
            id text,
            dealer_id integer,
            vin text,
            price double precision,
            miles double precision,
            taxonomy_vin text,
            scraped_at timestamptz,
            status_date timestamptz,
            zip text,
            latitude double precision,
            longitude double precision,
            city text,
            state text
        );
        CREATE INDEX IF NOT EXISTS marketcheck_us_s3_id_idx ON vendors.marketcheck_us (s3_id);
        CREATE INDEX IF NOT EXISTS marketcheck_us_status_date_idx ON vendors.marketcheck_us (status_date);
    ''',
    '''
        CREATE TABLE IF NOT EXISTS vendors.marketcheck_us_used
        (
            s3_id BIGINT references s3.scanned_files(id) ON DELETE CASCADE,
            id text,
            dealer_id integer,
            vin text,
            price double precision,
            miles double precision,
            taxonomy_vin text,
            scraped_at timestamptz,
            status_date timestamptz,
            zip text,
            latitude double precision,
            longitude double precision,
            city text,
            state text
        );
        CREATE INDEX IF NOT EXISTS marketcheck_us_used_s3_id_idx ON vendors.marketcheck_us_used (s3_id);
        CREATE INDEX IF NOT EXISTS marketcheck_us_used_status_date_idx ON vendors.marketcheck_us_used (status_date);
    ''',
    '''
        CREATE TABLE IF NOT EXISTS vendors.marketcheck_us_new
        (
            s3_id BIGINT references s3.scanned_files(id) ON DELETE CASCADE,
            id text,
            dealer_id integer,
            vin text,
            price double precision,
            miles double precision,
            taxonomy_vin text,
            scraped_at timestamptz,
            status_date timestamptz,
            zip text,
            latitude double precision,
            longitude double precision,
            city text,
            state text
        );
        CREATE INDEX IF NOT EXISTS marketcheck_us_new_s3_id_idx ON vendors.marketcheck_us_new (s3_id);
        CREATE INDEX IF NOT EXISTS marketcheck_us_new_status_date_idx ON vendors.marketcheck_us_new (status_date);
    ''',
    '''
        create or replace view zuora.v_account as
        select
            id,
            updateddate,
            case when payload->>'Account.Mrr' = '' then null else (payload->>'Account.Mrr')::numeric end as mrr,
            case when payload->>'Account.Name' = '' then null else payload->>'Account.Name' end as name,
            case when payload->>'Account.Batch' = '' then null else payload->>'Account.Batch' end as batch,
            case when payload->>'Account.CrmId' = '' then null else payload->>'Account.CrmId' end as crmid,
            case when payload->>'Account.Notes' = '' then null else payload->>'Account.Notes' end as notes,
            case when payload->>'Account.VAT' = '' then null else payload->>'Account.VAT' end as vat,
            case when payload->>'Account.Status' = '' then null else payload->>'Account.Status' end as status,
            case when payload->>'Account.AutoPay' = '' then null else (payload->>'Account.AutoPay')::boolean end as autopay,
            case when payload->>'Account.Balance' = '' then null else (payload->>'Account.Balance')::numeric end as balance,
            case when payload->>'Account.Currency' = '' then null else payload->>'Account.Currency' end as currency,
            case when payload->>'Account.ParentId' = '' then null else payload->>'Account.ParentId' end as parentid,
            case when payload->>'Account.is_deleted' = '' then null else (payload->>'Account.is_deleted')::boolean end as is_deleted,
            case when payload->>'Account.CreatedById' = '' then null else payload->>'Account.CreatedById' end as createdbyid,
            case when payload->>'Account.CreatedDate' = '' then null else to_timestamp((payload->>'Account.CreatedDate')::text,'YYYY-MM-DD HH24:MI:SSTZHTZM') end as createddate,
            case when payload->>'Account.PaymentTerm' = '' then null else payload->>'Account.PaymentTerm' end as paymentterm,
            case when payload->>'Account.UpdatedById' = '' then null else payload->>'Account.UpdatedById' end as updatedbyid,
            case when payload->>'Account.BillCycleDay' = '' then null else (payload->>'Account.BillCycleDay')::integer end as billcycleday,
            case when payload->>'Account.SalesRepName' = '' then null else payload->>'Account.SalesRepName' end as salesrepname,
            case when payload->>'Account.AccountNumber' = '' then null else payload->>'Account.AccountNumber' end as accountnumber,
            case when payload->>'Account.CreditBalance' = '' then null else (payload->>'Account.CreditBalance')::numeric end as creditbalance,
            case when payload->>'Account.SequenceSetId' = '' then null else payload->>'Account.SequenceSetId' end as sequencesettid,
            case when payload->>'Account.PaymentGateway' = '' then null else payload->>'Account.PaymentGateway' end as paymentgateway,
            case when payload->>'Account.TaxCompanyCode' = '' then null else payload->>'Account.TaxCompanyCode' end as taxcompanycode,
            case when payload->>'Account.LastInvoiceDate' = '' then null else (payload->>'Account.LastInvoiceDate')::timestamp at time zone 'America/Toronto' end as lastinvoicedate,
            case when payload->>'Account.TaxExemptStatus' = '' then null else payload->>'Account.TaxExemptStatus' end as taxexemptstatus,
            case when payload->>'Account.AllowInvoiceEdit' = '' then null else (payload->>'Account.AllowInvoiceEdit')::boolean end as allowinvoiceedit,
            case when payload->>'Account.BcdSettingOption' = '' then null else (payload->>'Account.BcdSettingOption') end as bcdsettingoption,
            case when payload->>'Account.UnappliedBalance' = '' then null else (payload->>'Account.UnappliedBalance')::numeric end as unappliedbalance,
            case when payload->>'Account.InvoiceTemplateId' = '' then null else payload->>'Account.InvoiceTemplateId' end as invoicetemplateid,
            case when payload->>'Account.PurchaseOrderNumber' = '' then null else payload->>'Account.PurchaseOrderNumber' end as purchaseordernumber,
            case when payload->>'Account.TotalInvoiceBalance' = '' then null else (payload->>'Account.TotalInvoiceBalance')::numeric end as totalinvoicebalance,
            case when payload->>'Account.TaxExemptDescription' = '' then null else payload->>'Account.TaxExemptDescription' end as taxexemptdescription,
            case when payload->>'Account.TotalDebitMemoBalance' = '' then null else (payload->>'Account.TotalDebitMemoBalance')::numeric end as totaldebitmemobalance,
            case when payload->>'Account.CommunicationProfileId' = '' then null else payload->>'Account.CommunicationProfileId' end as communicationprofileid,
            case when payload->>'Account.CustomerServiceRepName' = '' then null else payload->>'Account.CustomerServiceRepName' end as customerservicerepname,
            case when payload->>'Account.TaxExemptCertificateID' = '' then null else payload->>'Account.TaxExemptCertificateID' end as taxexemptcertificateid,
            case when payload->>'Account.TaxExemptEffectiveDate' = '' then null else to_timestamp(payload->>'Account.TaxExemptEffectiveDate','YYYY-MM-DD HH24:MI:SSTZHTZM') end as taxexempteffectivedate,
            case when payload->>'Account.TaxExemptEntityUseCode' = '' then null else payload->>'Account.TaxExemptEntityUseCode' end as taxexemptentityusecode,
            case when payload->>'Account.AdditionalEmailAddresses' = '' then null else payload->>'Account.AdditionalEmailAddresses' end as additionalemailaddresses,
            case when payload->>'Account.TaxExemptCertificateType' = '' then null else payload->>'Account.TaxExemptCertificateType' end as taxexemptcertificatetype,
            case when payload->>'Account.internalAccountNumber__c' = '' then null else (payload->>'Account.internalAccountNumber__c')::integer end as internalaccountnumber__c,
            case when payload->>'Account.InvoiceDeliveryPrefsEmail' = '' then null else (payload->>'Account.InvoiceDeliveryPrefsEmail')::boolean end as invoicedeliveryprefsemail,
            case when payload->>'Account.InvoiceDeliveryPrefsPrint' = '' then null else (payload->>'Account.InvoiceDeliveryPrefsPrint')::boolean end as invoicedeliveryprefsprint,
            case when payload->>'Account.UnappliedCreditMemoAmount' = '' then null else (payload->>'Account.UnappliedCreditMemoAmount')::numeric end as unappliedcreditmemoamount,
            case when payload->>'Account.TaxExemptIssuingJurisdiction' = '' then null else payload->>'Account.TaxExemptIssuingJurisdiction' end as taxexemptissuingjurisdiction,
            case when payload->>'Account.contactLanguagePreference__c' = '' then null else payload->>'Account.contactLanguagePreference__c' end as contactlaanguagepreference__c
        from
            zuora.account;
    ''',
    '''
        create or replace view zuora.v_invoiceitem as
        select
            id,
            updateddate,
            case when payload->>'InvoiceItem.SKU' = '' then null else payload->>'InvoiceItem.SKU' end as sku,
            case when payload->>'InvoiceItem.UOM' = '' then null else payload->>'InvoiceItem.UOM' end as uom,
            case when payload->>'InvoiceItem.Balance' = '' then null else (payload->>'InvoiceItem.Balance')::numeric end as balance,
            case when payload->>'InvoiceItem.TaxCode' = '' then null else payload->>'InvoiceItem.TaxCode' end as taxcode,
            case when payload->>'InvoiceItem.TaxMode' = '' then null else payload->>'InvoiceItem.TaxMode' end as taxmode,
            case when payload->>'InvoiceItem.Quantity' = '' then null else (payload->>'InvoiceItem.Quantity')::integer end as quantity,
            case when payload->>'InvoiceItem.TaxAmount' = '' then null else (payload->>'InvoiceItem.TaxAmount')::numeric end as taxamount,
            case when payload->>'InvoiceItem.UnitPrice' = '' then null else (payload->>'InvoiceItem.UnitPrice')::numeric end as unitprice,
            case when payload->>'InvoiceItem.ChargeDate' = '' then null else to_timestamp(payload->>'InvoiceItem.ChargeDate','YYYY-MM-DD HH24:MI:SSTZHTZM') end as chargedate,
            case when payload->>'InvoiceItem.ChargeName' = '' then null else payload->>'InvoiceItem.ChargeName' end as chargename,
            case when payload->>'InvoiceItem.is_deleted' = '' then null else (payload->>'InvoiceItem.is_deleted')::boolean end as is_deleted,
            case when payload->>'InvoiceItem.CreatedById' = '' then null else payload->>'InvoiceItem.CreatedById' end as createdbyid,
            case when payload->>'InvoiceItem.CreatedDate' = '' then null else to_timestamp(payload->>'InvoiceItem.CreatedDate','YYYY-MM-DD HH24:MI:SSTZHTZM') end as createddate,
            case when payload->>'InvoiceItem.UpdatedById' = '' then null else payload->>'InvoiceItem.UpdatedById' end as updatedbyid,
            case when payload->>'InvoiceItem.ChargeAmount' = '' then null else (payload->>'InvoiceItem.ChargeAmount')::numeric end as chargeamount,
            case when payload->>'InvoiceItem.AccountingCode' = '' then null else payload->>'InvoiceItem.AccountingCode' end as accountingcode,
            case when payload->>'InvoiceItem.ProcessingType' = '' then null else (payload->>'InvoiceItem.ProcessingType')::integer end as processingtype,
            case when payload->>'InvoiceItem.ServiceEndDate' = '' then null else (payload->>'InvoiceItem.ServiceEndDate')::timestamp at time zone 'America/Toronto' end as serviceenddate,
            case when payload->>'InvoiceItem.SubscriptionId' = '' then null else payload->>'InvoiceItem.SubscriptionId' end as subscriptionid,
            case when payload->>'InvoiceItem.RevRecStartDate' = '' then null else (payload->>'InvoiceItem.RevRecStartDate')::timestamp at time zone 'America/Toronto' end as revrecstartdate,
            case when payload->>'InvoiceItem.TaxExemptAmount' = '' then null else (payload->>'InvoiceItem.TaxExemptAmount')::numeric end as taxexemptamount,
            case when payload->>'InvoiceItem.ServiceStartDate' = '' then null else (payload->>'InvoiceItem.ServiceStartDate')::timestamp at time zone 'America/Toronto' end as servicestartdate,
            case when payload->>'InvoiceItem.AppliedToInvoiceItemId' = '' then null else payload->>'InvoiceItem.AppliedToInvoiceItemId' end as appliedtoinvoiceitemid,
            case when payload->>'Invoice.Id' = '' then null else payload->>'Invoice.Id' end as invoiceid
        from
            zuora.invoiceitem;
    ''',
    '''
        create or replace view zuora.v_invoice as
        select
            id,
            updateddate,
            case when payload->>'Invoice.Amount' = '' then null else (payload->>'Invoice.Amount')::numeric end as amount,
            case when payload->>'Invoice.Source' = '' then null else payload->>'Invoice.Source' end as source,
            case when payload->>'Invoice.Status' = '' then null else payload->>'Invoice.Status' end as status,
            case when payload->>'Invoice.AutoPay' = '' then null else (payload->>'Invoice.AutoPay')::boolean end as autopay,
            case when payload->>'Invoice.Balance' = '' then null else (payload->>'Invoice.Balance')::numeric end as balance,
            case when payload->>'Invoice.DueDate' = '' then null else (payload->>'Invoice.DueDate')::timestamp at time zone 'America/Toronto' end as duedate,
            case when payload->>'Invoice.Comments' = '' then null else payload->>'Invoice.Comments' end as comments,
            case when payload->>'Invoice.PostedBy' = '' then null else payload->>'Invoice.PostedBy' end as postedby,
            case when payload->>'Invoice.Reversed' = '' then null else (payload->>'Invoice.Reversed')::boolean end as reversed,
            case when payload->>'Invoice.SourceId' = '' then null else payload->>'Invoice.SourceId' end as sourceid,
            case when payload->>'Invoice.TaxAmount' = '' then null else (payload->>'Invoice.TaxAmount')::numeric end as taxamount,
            case when payload->>'Invoice.PostedDate' = '' then null else to_timestamp(payload->>'Invoice.PostedDate','YYYY-MM-DD HH24:MI:SSTZHTZM') end as posteddate,
            case when payload->>'Invoice.TargetDate' = '' then null else (payload->>'Invoice.TargetDate')::timestamp at time zone 'America/Toronto' end as targetdate,
            case when payload->>'Invoice.is_deleted' = '' then null else (payload->>'Invoice.is_deleted')::boolean end as is_deleted,
            case when payload->>'Invoice.CreatedById' = '' then null else payload->>'Invoice.CreatedById' end as createdbyid,
            case when payload->>'Invoice.CreatedDate' = '' then null else to_timestamp(payload->>'Invoice.CreatedDate','YYYY-MM-DD HH24:MI:SSTZHTZM') end as createddate,
            case when payload->>'Invoice.InvoiceDate' = '' then null else (payload->>'Invoice.InvoiceDate')::timestamp at time zone 'America/Toronto' end as invoicedate,
            case when payload->>'Invoice.UpdatedById' = '' then null else payload->>'Invoice.UpdatedById' end as updatedbyid,
            case when payload->>'Invoice.RefundAmount' = '' then null else (payload->>'Invoice.RefundAmount')::numeric end as refundamount,
            case when payload->>'Invoice.IncludesUsage' = '' then null else (payload->>'Invoice.IncludesUsage')::boolean end as includesusage,
            case when payload->>'Invoice.InvoiceNumber' = '' then null else (payload->>'Invoice.InvoiceNumber') end as invoicenumber,
            case when payload->>'Invoice.PaymentAmount' = '' then null else (payload->>'Invoice.PaymentAmount')::numeric end as paymentamount,
            case when payload->>'Invoice.IncludesOneTime' = '' then null else (payload->>'Invoice.IncludesOneTime')::boolean end as includesonetime,
            case when payload->>'Invoice.TaxExemptAmount' = '' then null else (payload->>'Invoice.TaxExemptAmount')::numeric end as taxexemptamount,
            case when payload->>'Invoice.AdjustmentAmount' = '' then null else (payload->>'Invoice.AdjustmentAmount')::numeric end as adjustmentamount,
            case when payload->>'Invoice.AmountWithoutTax' = '' then null else (payload->>'Invoice.AmountWithoutTax')::numeric end as amountwithouttax,
            case when payload->>'Invoice.IncludesRecurring' = '' then null else (payload->>'Invoice.IncludesRecurring')::boolean end as includesrecurring,
            case when payload->>'Invoice.LastEmailSentDate' = '' then null else to_timestamp(payload->>'Invoice.LastEmailSentDate','YYYY-MM-DD HH24:MI:SSTZHTZM') end as lastemailsentdate,
            case when payload->>'Invoice.legacyInvoiceNumber__c' = '' then null else payload->>'Invoice.legacyInvoiceNumber__c' end as legacyinvoicenumber__c,
            case when payload->>'Invoice.TransferredToAccounting' = '' then null else (payload->>'Invoice.TransferredToAccounting')::boolean end as transferredtoaccounting,
            case when payload->>'Invoice.CreditBalanceAdjustmentAmount' = '' then null else (payload->>'Invoice.CreditBalanceAdjustmentAmount')::numeric end as creditbalanceadjustmentamount
        from
            zuora.invoice;
    ''',
    '''
        create or replace view zuora.v_subscription as
        select
            id,
            updateddate,
            case when payload->>'Subscription.Name' = '' then null else payload->>'Subscription.Name' end as name,
            case when payload->>'Subscription.Notes' = '' then null else payload->>'Subscription.Notes' end as notes,
            case when payload->>'Subscription.Status' = '' then null else payload->>'Subscription.Status' end as status,
            case when payload->>'Subscription.Version' = '' then null else (payload->>'Subscription.Version')::integer end as version,
            case when payload->>'Subscription.TermType' = '' then null else payload->>'Subscription.TermType' end as termtype,
            case when payload->>'Subscription.AutoRenew' = '' then null else (payload->>'Subscription.AutoRenew')::boolean end as autorenew,
            case when payload->>'Subscription.OriginalId' = '' then null else payload->>'Subscription.OriginalId' end as originalid,
            case when payload->>'Subscription.is_deleted' = '' then null else (payload->>'Subscription.is_deleted')::boolean end as is_deleted,
            case when payload->>'Subscription.CreatedById' = '' then null else payload->>'Subscription.CreatedById' end as createdbyid,
            case when payload->>'Subscription.CreatedDate' = '' then null else to_timestamp(payload->>'Subscription.CreatedDate','YYYY-MM-DD HH24:MI:SSTZHTZM') end as createddate,
            case when payload->>'Subscription.CurrentTerm' = '' then null else (payload->>'Subscription.CurrentTerm')::integer end as currentterm,
            case when payload->>'Subscription.InitialTerm' = '' then null else (payload->>'Subscription.InitialTerm')::integer end as initialterm,
            case when payload->>'Subscription.RenewalTerm' = '' then null else (payload->>'Subscription.RenewalTerm')::integer end as renewalterm,
            case when payload->>'Subscription.TermEndDate' = '' then null else (payload->>'Subscription.TermEndDate')::timestamp at time zone 'America/Toronto' end as termenddate,
            case when payload->>'Subscription.UpdatedById' = '' then null else payload->>'Subscription.UpdatedById' end as updatedbyid,
            case when payload->>'Subscription.CancelledDate' = '' then null else (payload->>'Subscription.CancelledDate')::timestamp at time zone 'America/Toronto' end as cancelleddate,
            case when payload->>'Subscription.QuoteType__QT' = '' then null else payload->>'Subscription.QuoteType__QT' end as quotetype__qt,
            case when payload->>'Subscription.TermStartDate' = '' then null else (payload->>'Subscription.TermStartDate')::timestamp at time zone 'America/Toronto' end as termstartdate,
            case when payload->>'Subscription.InvoiceOwnerId' = '' then null else payload->>'Subscription.InvoiceOwnerId' end as invoiceownerid,
            case when payload->>'Subscription.RenewalSetting' = '' then null else payload->>'Subscription.RenewalSetting' end as renewalsetting,
            case when payload->>'Subscription.QuoteNumber__QT' = '' then null else payload->>'Subscription.QuoteNumber__QT' end as quotenumber__qt,
            case when payload->>'Subscription.CreatorAccountId' = '' then null else payload->>'Subscription.CreatorAccountId' end as creatoraccountid,
            case when payload->>'Subscription.IsInvoiceSeparate' = '' then null else (payload->>'Subscription.IsInvoiceSeparate')::boolean end as isinvoiceseparate,
            case when payload->>'Subscription.CpqBundleJsonId__QT' = '' then null else payload->>'Subscription.CpqBundleJsonId__QT' end as cpqbundlejsonid__qt,
            case when payload->>'Subscription.OpportunityName__QT' = '' then null else payload->>'Subscription.OpportunityName__QT' end as opportunityname__qt,
            case when payload->>'Subscription.OriginalCreatedDate' = '' then null else to_timestamp(payload->>'Subscription.OriginalCreatedDate','YYYY-MM-DD HH24:MI:SSTZHTZM') end as originalcreateddate,
            case when payload->>'Subscription.SubscriptionEndDate' = '' then null else (payload->>'Subscription.SubscriptionEndDate')::timestamp at time zone 'America/Toronto' end as subscriptionenddate,
            case when payload->>'Subscription.ContractEffectiveDate' = '' then null else (payload->>'Subscription.ContractEffectiveDate')::timestamp at time zone 'America/Toronto' end as contracteffectivedate,
            case when payload->>'Subscription.CreatorInvoiceOwnerId' = '' then null else payload->>'Subscription.CreatorInvoiceOwnerId' end as creatorinvoiceownerid,
            case when payload->>'Subscription.CurrentTermPeriodType' = '' then null else payload->>'Subscription.CurrentTermPeriodType' end as currenttermperiodtype,
            case when payload->>'Subscription.InitialTermPeriodType' = '' then null else payload->>'Subscription.InitialTermPeriodType' end as initialtermperiodtype,
            case when payload->>'Subscription.QuoteBusinessType__QT' = '' then null else payload->>'Subscription.QuoteBusinessType__QT' end as quotebusinesstype__qt,
            case when payload->>'Subscription.RenewalTermPeriodType' = '' then null else payload->>'Subscription.RenewalTermPeriodType' end as renewaltermperiodtype,
            case when payload->>'Subscription.ServiceActivationDate' = '' then null else (payload->>'Subscription.ServiceActivationDate')::timestamp at time zone 'America/Toronto' end as serviceactivationdate,
            case when payload->>'Subscription.SubscriptionStartDate' = '' then null else (payload->>'Subscription.SubscriptionStartDate')::timestamp at time zone 'America/Toronto' end as subscriptionstartdate,
            case when payload->>'Subscription.ContractAcceptanceDate' = '' then null else (payload->>'Subscription.ContractAcceptanceDate')::timestamp at time zone 'America/Toronto' end as contractacceptancedate,
            case when payload->>'Subscription.PreviousSubscriptionId' = '' then null else payload->>'Subscription.PreviousSubscriptionId' end as previoussubscriptionid,
            case when payload->>'Subscription.OpportunityCloseDate__QT' = '' then null else payload->>'Subscription.OpportunityCloseDate__QT' end as opportunitycloseddate__qt
        from
            zuora.subscription;
    ''',
    '''
        create or replace view zuora.v_creditmemo as
        select
            id,
            updateddate,
            case when payload->>'CreditMemo.Source' = '' then null else payload->>'CreditMemo.Source' end as source,
            case when payload->>'CreditMemo.Status' = '' then null else payload->>'CreditMemo.Status' end as status,
            case when payload->>'CreditMemo.Balance' = '' then null else (payload->>'CreditMemo.Balance')::numeric end as balance,
            case when payload->>'CreditMemo.Comments' = '' then null else payload->>'CreditMemo.Comments' end as comments,
            case when payload->>'CreditMemo.MemoDate' = '' then null else (payload->>'CreditMemo.MemoDate')::timestamp at time zone 'America/Toronto' end as memodate,
            case when payload->>'CreditMemo.PostedOn' = '' then null else to_timestamp(payload->>'CreditMemo.PostedOn','YYYY-MM-DD HH24:MI:SSTZHTZM') end as postedon,
            case when payload->>'CreditMemo.SourceId' = '' then null else payload->>'CreditMemo.SourceId' end as sourceid,
            case when payload->>'CreditMemo.TaxAmount' = '' then null else (payload->>'CreditMemo.TaxAmount')::numeric end as taxamount,
            case when payload->>'CreditMemo.MemoNumber' = '' then null else payload->>'CreditMemo.MemoNumber' end as memonumber,
            case when payload->>'CreditMemo.PostedById' = '' then null else payload->>'CreditMemo.PostedById' end as postedbyid,
            case when payload->>'CreditMemo.ReasonCode' = '' then null else payload->>'CreditMemo.ReasonCode' end as reasoncode,
            case when payload->>'CreditMemo.TargetDate' = '' then null else (payload->>'CreditMemo.TargetDate')::timestamp at time zone 'America/Toronto' end as targetdate,
            case when payload->>'CreditMemo.is_deleted' = '' then null else (payload->>'CreditMemo.is_deleted')::boolean end as is_deleted,
            case when payload->>'CreditMemo.CancelledOn' = '' then null else to_timestamp(payload->>'CreditMemo.CancelledOn','YYYY-MM-DD HH24:MI:SSTZHTZM') end as cancelledon,
            case when payload->>'CreditMemo.CreatedById' = '' then null else payload->>'CreditMemo.CreatedById' end as createdbyid,
            case when payload->>'CreditMemo.CreatedDate' = '' then null else to_timestamp(payload->>'CreditMemo.CreatedDate','YYYY-MM-DD HH24:MI:SSTZHTZM') end createddate,
            case when payload->>'CreditMemo.TotalAmount' = '' then null else (payload->>'CreditMemo.TotalAmount')::numeric end as totalamount,
            case when payload->>'CreditMemo.UpdatedById' = '' then null else payload->>'CreditMemo.UpdatedById' end as updatedbyid,
            case when payload->>'CreditMemo.RefundAmount' = '' then null else (payload->>'CreditMemo.RefundAmount')::numeric end as refundamount,
            case when payload->>'CreditMemo.AppliedAmount' = '' then null else (payload->>'CreditMemo.AppliedAmount')::numeric end as appliedamount,
            case when payload->>'CreditMemo.CancelledById' = '' then null else (payload->>'CreditMemo.CancelledById') end as cancelledbyid,
            case when payload->>'CreditMemo.DiscountAmount' = '' then null else (payload->>'CreditMemo.DiscountAmount')::numeric end as discountamount,
            case when payload->>'CreditMemo.ExchangeRateDate' = '' then null else (payload->>'CreditMemo.ExchangeRateDate')::timestamp at time zone 'America/Toronto' end as exchangeratedate,
            case when payload->>'CreditMemo.TotalTaxExemptAmount' = '' then null else (payload->>'CreditMemo.TotalTaxExemptAmount')::numeric end as totaltaxexemptamount,
            case when payload->>'CreditMemo.TotalAmountWithoutTax' = '' then null else (payload->>'CreditMemo.TotalAmountWithoutTax')::numeric end as totalamountwithouttax,
            case when payload->>'CreditMemo.TransferredToAccounting' = '' then null else (payload->>'CreditMemo.TransferredToAccounting')::boolean end as transeferredtoaccounting,
            case when payload->>'CreditMemo.legacyCreditMemoNumber__c' = '' then null else (payload->>'CreditMemo.legacyCreditMemoNumber__c') end as legacycreditmemonumber__c
        from
            zuora.creditmemo;
    ''',
    '''
        create or replace view zuora.v_billings_monthly as
        select
            d.accountnumber as master_business_id,
            c.id as subscriptionid,
            date_trunc('month', a.servicestartdate) as date,
            a.accountingcode,
            a.chargename,
            a.chargeamount,
            a.quantity
        from
            zuora.v_invoiceitem a
        join
            zuora.v_invoice b on b.id = a.invoiceid
        left join
            zuora.v_subscription c
        on
            c.id = a.subscriptionid
        left join
            zuora.v_account d
        on
            d.id = c.creatoraccountid
        where
            a.chargeamount <> 0
        and
            b.status = 'Posted'
        and
            b.reversed = false;
    ''',
    '''
        create or replace view zuora.v_saas_revenue_monthly as
        with subscriptions as (
        select
            g.creatoraccountid as accountid,
            a.subscriptionid,
            a.chargename,
            date_trunc('month',a.servicestartdate) as servicestartdate,
            a.chargeamount,
            coalesce(c.chargeamount,0) as discount,
            coalesce(d.chargeamount,0) as dsp_discount,
            coalesce(e.chargeamount,0) as held_billing_discount,
            coalesce(f.chargeamount,0) as saas_discount
        from
            zuora.v_invoiceitem a
        inner join zuora.v_invoice b on b.id = a.invoiceid
        left join zuora.v_subscription g on g.id = a.subscriptionid
        left join zuora.v_invoiceitem c on c.appliedtoinvoiceitemid = a.id and c.chargename = 'Discount'
        left join zuora.v_invoiceitem d on d.appliedtoinvoiceitemid = a.id and d.chargename = 'DSP Discount'
        left join zuora.v_invoiceitem e on e.appliedtoinvoiceitemid = a.id and e.chargename = 'Held Billing Discount'
        left join zuora.v_invoiceitem f on f.appliedtoinvoiceitemid = a.id and f.chargename = 'SaaS Discount'
        where
            b.status = 'Posted'
        and
            b.reversed = false
        and
            a.chargename in ('AutoVerify Reviews','AVR Suite -- Proration','AVR Suite'))
        select
            accountid,
            subscriptionid,
            servicestartdate,
            sum(case when chargename = 'AVR Suite' then chargeamount else 0 end) as avr_suite_price,
            sum(case when chargename = 'AutoVerify Reviews' then chargeamount else 0 end) as av_reviews_price,
            sum(case when chargename = 'AVR Suite -- Proration' then chargeamount else 0 end) as avr_suite_proration_price,
            sum(case when chargename = 'AVR Suite' then discount else 0 end) as avr_suite_discount,
            sum(case when chargename = 'AVR Suite -- Proration' then discount else 0 end) as avr_suite_proration_discount,
            sum(case when chargename = 'AutoVerify Reviews' then discount else 0 end) as av_reviews_discount,
            sum(case when chargename = 'AVR Suite' then dsp_discount else 0 end) as avr_suite_dsp_discount,
            sum(case when chargename = 'AVR Suite -- Proration' then dsp_discount else 0 end) as avr_suite_proration_dsp_discount,
            sum(case when chargename = 'AutoVerify Reviews' then dsp_discount else 0 end) as av_reviews_dsp_discount,
            sum(case when chargename = 'AVR Suite' then saas_discount else 0 end) as avr_suite_saas_discount,
            sum(case when chargename = 'AVR Suite -- Proration' then saas_discount else 0 end) as avr_suite_proration_saas_discount,
            sum(case when chargename = 'AutoVerify Reviews' then saas_discount else 0 end) as av_reviews_saas_discount,
            sum(case when chargename = 'AVR Suite' then held_billing_discount else 0 end) as avr_suite_held_billing_discount,
            sum(case when chargename = 'AVR Suite -- Proration' then held_billing_discount else 0 end) as avr_suite_proration_held_billing_discount,
            sum(case when chargename = 'AutoVerify Reviews' then held_billing_discount else 0 end) as av_reviews_held_billing_discount
        from subscriptions
        group by 1,2,3;
    ''',
    '''
        create or replace view autoverify.v_sda_master_businesses as
        select
            id,
            created_at,
            updated_at,
            case when payload->>'name' != '' then payload->>'name' else null end as name,
            case when payload->>'website_url' != '' then payload->>'website_url' else null end as website_url,
            case when payload->>'phone_number' != '' then payload->>'phone_number' else null end as phone_number,
            case when payload->>'address_line_1' != '' then payload->>'address_line_1' else null end as address_line_1,
            case when payload->>'address_line_2' != '' then payload->>'address_line_2' else null end as address_line2,
            case when payload->>'city' != '' then payload->>'city' else null end as city,
            case when payload->>'postal_code' != '' then translate(payload->>'postal_code',' ','') else null end as postal_code,
            case when payload->>'country' != '' then payload->>'country' else null end as country,
            case when payload->>'province' != '' then payload->>'province' else null end as province,
            case when payload->>'subscriptions' != '' then payload->>'subscriptions' else null end as subscriptions,
            case when payload->>'category' != '' then payload->>'category' else null end as category,
            case when payload->>'crm' != '' then payload->>'crm' else null end as crm,
            case when payload->>'crm_id' != '' then payload->>'crm_id' else null end as crm_id,
            case when payload->>'belongs_to_dealership_group' != '' then (payload->>'belongs_to_dealership_group')::int4 else null end as belongs_to_dealership_group,
            case when payload->>'contact_language_pref' != '' then payload->>'contact_language_pref' else null end as contact_language_pref,
            case when payload->>'parent_business_id' != '' then (payload->>'parent_business_id')::uuid else null end as parent_business_id,
            case when payload->>'zuora_account_id' != '' then payload->>'zuora_account_id' else null end as zuora_account_id,
            case when payload->>'account_number' != '' then payload->>'account_number' else null end as account_number,
            case when payload->>'partner_name' != '' then payload->>'partner_name' else null end as partner_name,
            case when payload->>'route_one_integration_id' != '' then payload->>'route_one_integration_id' else null end as route_one_integration_id,
            case when payload->>'spincar_dealer_id' != '' then payload->>'spincar_dealer_id' else null end as spincar_dealer_id
        from
            autoverify.sda_master_businesses;
    ''',
    '''
        CREATE OR REPLACE FUNCTION autoverify.lead_content(is_insurance integer, is_trade integer, is_credit_partial integer, is_credit_verified integer, is_credit_finance integer, is_ecom integer, is_test_drive integer)
             RETURNS text[]
             LANGUAGE plpgsql
            AS $function$
            declare
                    result text[];
                begin
                    if
                        is_credit_finance = 1
                    then
                        result := array_append(result,'finance');
                    end if;

                    if
                        is_credit_finance = 0 and (is_credit_verified = 1 or is_credit_partial = 1)
                    then
                        result := array_append(result,'credit');
                    end if;

                    if
                        is_test_drive = 1
                    then
                        result := array_append(result,'testdrive');
                    end if;

                    if
                        is_trade = 1
                    then
                        result := array_append(result,'trade');
                    end if;

                    if
                        is_insurance = 1
                    then
                        result := array_append(result,'insurance');
                    end if;

                    if
                        is_ecom = 1
                    then
                        result := array_append(result,'payments');
                    end if;

                    if
                        result is null
                    then
                        result := array_append(result,'unclassified');
                    end if;

                return result;
            end;
            $function$;
    ''',
    '''
        create or replace view zuora.v_order as
        select
            id,
            updateddate,
            case when payload->>'Order.OrderDate' = '' then null else (payload->>'Order.OrderDate')::timestamp at time zone 'America/Toronto' end as orderdate,
            case when payload->>'Order.is_deleted' = '' then null else (payload->>'Order.is_deleted')::boolean end as is_deleted,
            case when payload->>'Order.CreatedById' = '' then null else payload->>'Order.CreatedById' end as createdbyid,
            case when payload->>'Order.CreatedDate' = '' then null else to_timestamp(payload->>'Order.CreatedDate','YYYY-MM-DD HH24:MI:SSTZHTZM') end as createddate,
            case when payload->>'Order.Description' = '' then null else payload->>'Order.Description' end as description,
            case when payload->>'Order.OrderNumber' = '' then null else payload->>'Order.OrderNumber' end as ordernumber,
            case when payload->>'Order.UpdatedById' = '' then null else payload->>'Order.UpdatedById' end as updatedbyid,
            case when payload->>'Account.Id' = '' then null else payload->>'Account.Id' end as accountid
        from
            zuora.order;
    ''',
    '''
        create or replace view zuora.v_contact as
        select
            id,
            updateddate,
            case when payload->>'Contact.Fax' = '' then null else (payload->>'Contact.Fax')::text end as fax,
            case when payload->>'Contact.City' = '' then null else (payload->>'Contact.City')::text end as city,
            case when payload->>'Contact.State' = '' then null else (payload->>'Contact.State')::text end as state,
            case when payload->>'Contact.County' = '' then null else (payload->>'Contact.County')::text end as county,
            case when payload->>'Contact.Country' = '' then null else (payload->>'Contact.Country')::text end as country,
            case when payload->>'Contact.Address1' = '' then null else (payload->>'Contact.Address1')::text end as address1,
            case when payload->>'Contact.Address2' = '' then null else (payload->>'Contact.Address2')::text end as address2,
            case when payload->>'Contact.LastName' = '' then null else (payload->>'Contact.LastName')::text end as lastname,
            case when payload->>'Contact.NickName' = '' then null else (payload->>'Contact.NickName')::text end as nickname,
            case when payload->>'Contact.AccountId' = '' then null else (payload->>'Contact.AccountId')::text end as accountid,
            case when payload->>'Contact.FirstName' = '' then null else (payload->>'Contact.FirstName')::text end as firstname,
            case when payload->>'Contact.HomePhone' = '' then null else (payload->>'Contact.HomePhone')::text end as homephone,
            case when payload->>'Contact.TaxRegion' = '' then null else (payload->>'Contact.TaxRegion')::text end as taxregion,
            case when payload->>'Contact.WorkEmail' = '' then null else (payload->>'Contact.WorkEmail')::text end as workemail,
            case when payload->>'Contact.WorkPhone' = '' then null else (payload->>'Contact.WorkPhone')::text end as workphone,
            case when payload->>'Contact.OtherPhone' = '' then null else (payload->>'Contact.OtherPhone')::text end as otherphone,
            case when payload->>'Contact.PostalCode' = '' then null else replace((payload->>'Contact.PostalCode')::text,' ','') end as postalcode,
            case when payload->>'Contact.is_deleted' = '' then null else (payload->>'Contact.is_deleted')::boolean end as is_deleted,
            case when payload->>'Contact.CreatedById' = '' then null else (payload->>'Contact.CreatedById')::text end as createdbyid,
            case when payload->>'Contact.CreatedDate' = '' then null else to_timestamp(payload->>'Contact.CreatedDate','YYYY-MM-DD HH24:MI:SSTZHTZM') end as createddate,
            case when payload->>'Contact.Description' = '' then null else (payload->>'Contact.Description')::text end as description,
            case when payload->>'Contact.MobilePhone' = '' then null else (payload->>'Contact.MobilePhone')::text end as mobilephone,
            case when payload->>'Contact.UpdatedById' = '' then null else (payload->>'Contact.UpdatedById')::text end as updatedbyid,
            case when payload->>'Contact.PersonalEmail' = '' then null else (payload->>'Contact.PersonalEmail')::text end as personalemail,
            case when payload->>'Contact.OtherPhoneType' = '' then null else (payload->>'Contact.OtherPhoneType')::text end as otherphonetype
        from
            zuora.contact;
    ''',
    '''
        create table if not exists sage.customer_info
        (
            id integer primary key,
            payload jsonb
        );
    ''',
    '''
        create table if not exists sage.sales
        (
            id integer primary key,
            payload jsonb
        );
    ''',
    '''
        create or replace view autoverify.v_mpm_lead_content as
        select
            a.id,
            (b.payload->>'master_business_id')::uuid as master_business_id,
            a.created_at,
            a.payload->>'device' as device,
            autoverify.lead_content((a.payload->>'is_insurance')::int, (a.payload->>'is_trade')::int, (a.payload->>'is_credit_partial')::int, (a.payload->>'is_credit_verified')::int, (a.payload->>'is_credit_finance')::int, (a.payload->>'is_ecom')::int, (a.payload->>'is_test_drive')::int)
        from
            autoverify.mpm_leads a
        left join autoverify.mpm_integration_settings b on b.id = (a.payload->>'integration_settings_id')::uuid;
    ''',
    '''
        create or replace view autoverify.v_active_master_businesses as
        select
            distinct((payload->>'master_business_id')::uuid) as master_business_id
        from
            autoverify.mpm_integration_settings
        where
            payload->>'status' = 'active';
    ''',
    '''
        create or replace view autoverify.v_mpm_integration_settings as
        select
            id,
            (payload->>'master_business_id')::uuid as master_business_id,
            payload->>'status' as status
        from
            autoverify.mpm_integration_settings;
    ''',
    '''
        create or replace view redash.v_avr_trade_leads_only as
        with lc as
        (
            select distinct on (master_business_id,lead_content)
            master_business_id,
            created_at,
            lead_content
            from autoverify.v_mpm_lead_content
            where master_business_id in (select master_business_id from autoverify.v_active_master_businesses)
            order by master_business_id,lead_content, created_at desc
        )
        select
            name,
            website_url,
            master_business_id,
            crm_id,
            subscriptions,
            last_trade_lead
        from
        (
        select
            a.*,
            (case when a.last_credit_lead is not null then 1 else 0 end +
            case when a.last_trade_lead is not null then 1 else 0 end +
            case when a.last_testdrive_lead is not null then 1 else 0 end +
            case when a.last_insurance_lead is not null then 1 else 0 end +
            case when a.last_payments_lead is not null then 1 else 0 end)
            as lead_types
        from
        (
        select
            a.name,
            a.website_url,
            a.id as master_business_id,
            a.crm_id,
            a.subscriptions,
            max(c.created_at) as last_credit_lead,
            max(d.created_at) as last_trade_lead,
            max(e.created_at) as last_testdrive_lead,
            max(f.created_at) as last_insurance_lead,
            max(g.created_at) as last_payments_lead,
            json_array_length(a.subscriptions::json) as subscriptions_count
        from
            autoverify.v_sda_master_businesses a
        left join lc c on c.master_business_id = a.id and ('credit' = ANY(c.lead_content) or 'finance' = ANY(c.lead_content))
        left join lc d on d.master_business_id = a.id and 'trade' = ANY(d.lead_content)
        left join lc e on e.master_business_id = a.id and 'testdrive' = ANY(e.lead_content)
        left join lc f on f.master_business_id = a.id and 'insurance' = ANY(f.lead_content)
        left join lc g on g.master_business_id = a.id and 'payments' = ANY(g.lead_content)
        where
            a.id in (select master_business_id from autoverify.v_active_master_businesses)
        and
            a.subscriptions like '%avr%'
        group by 1,2,3,4,5,11
        ) a
        ) a
        where last_trade_lead is not null and lead_types = 1;
    ''',
    '''
        create or replace view redash.v_avr_credit_leads_only as
        with lc as
        (
            select distinct on (master_business_id,lead_content)
            master_business_id,
            created_at,
            lead_content
            from autoverify.v_mpm_lead_content
            where master_business_id in (select master_business_id from autoverify.v_active_master_businesses)
            order by master_business_id,lead_content, created_at desc
        )
        select
            name,
            website_url,
            master_business_id,
            crm_id,
            subscriptions,
            last_credit_lead
        from
        (
        select
            a.*,
            (case when a.last_credit_lead is not null then 1 else 0 end +
            case when a.last_trade_lead is not null then 1 else 0 end +
            case when a.last_testdrive_lead is not null then 1 else 0 end +
            case when a.last_insurance_lead is not null then 1 else 0 end +
            case when a.last_payments_lead is not null then 1 else 0 end)
            as lead_types
        from
        (
        select
            a.name,
            a.website_url,
            a.id as master_business_id,
            a.crm_id,
            a.subscriptions,
            max(c.created_at) as last_credit_lead,
            max(d.created_at) as last_trade_lead,
            max(e.created_at) as last_testdrive_lead,
            max(f.created_at) as last_insurance_lead,
            max(g.created_at) as last_payments_lead,
            json_array_length(a.subscriptions::json) as subscriptions_count
        from
            autoverify.v_sda_master_businesses a
        left join lc c on c.master_business_id = a.id and ('credit' = ANY(c.lead_content) or 'finance' = ANY(c.lead_content))
        left join lc d on d.master_business_id = a.id and 'trade' = ANY(d.lead_content)
        left join lc e on e.master_business_id = a.id and 'testdrive' = ANY(e.lead_content)
        left join lc f on f.master_business_id = a.id and 'insurance' = ANY(f.lead_content)
        left join lc g on g.master_business_id = a.id and 'payments' = ANY(g.lead_content)
        where
            a.id in (select master_business_id from autoverify.v_active_master_businesses)
        and
            a.subscriptions like '%avr%'
        group by 1,2,3,4,5,11
        ) a
        ) a
        where last_credit_lead is not null and lead_types = 1;
    ''',
    '''
        create or replace view autoverify.v_mpm_leads as
        select
            id,
            case when payload->>'status' = '' then null else payload->>'status' end as status,
            created_at,
            updated_at,
            case when payload->>'customer_first_name' = '' then null else payload->>'customer_first_name' end as customer_first_name,
            case when payload->>'customer_last_name' = '' then null else payload->>'customer_last_name' end as customer_last_name,
            case when payload->>'customer_email' = '' then null else payload->>'customer_email' end as customer_email,
            case when payload->>'customer_phone' = '' then null else payload->>'customer_phone' end as customer_phone,
            case when payload->>'customer_language' = '' then null else payload->>'customer_language' end as customer_language,
            case when payload->>'customer_mobile_phone' = '' then null else payload->>'customer_mobile_phone' end as customer_mobile_phone,
            case when payload->>'customer_payload' = '' then null else (payload->>'customer_payload')::json end as customer_payload,
            case when payload->>'customer_address_line_1' = '' then null else payload->>'customer_address_line_1' end as customer_address_line_1,
            case when payload->>'customer_address_line_2' = '' then null else payload->>'customer_address_line_2' end as customer_address_line_2,
            case when payload->>'customer_city' = '' then null else payload->>'customer_city' end as customer_city,
            case when payload->>'customer_postal_code' = '' then null else translate(payload->>'customer_postal_code',' ','') end as customer_postal_code,
            case when payload->>'customer_country' = '' then null else payload->>'customer_country' end as customer_country,
            case when payload->>'customer_province' = '' then null else payload->>'customer_province' end as customer_province,
            case when payload->>'trade_high_value' = '' then null else (payload->>'trade_high_value')::double precision/100 end as trade_high_value,
            case when payload->>'trade_low_value' = '' then null else (payload->>'trade_low_value')::double precision/100 end as trade_low_value,
            case when payload->>'trade_deductions' = '' then null else payload->>'trade_deductions' end as trade_deductions,
            case when payload->>'trade_customer_referral_url' = '' then null else payload->>'trade_customer_referral_url' end as trade_customer_referral_url,
            case when payload->>'trade_vehicle_year' = '' then null else (payload->>'trade_vehicle_year')::int end as trade_vehicle_year,
            case when payload->>'trade_vehicle_make' = '' then null else payload->>'trade_vehicle_make' end as trade_vehicle_make,
            case when payload->>'trade_vehicle_model' = '' then null else payload->>'trade_vehicle_model' end as trade_vehicle_model,
            case when payload->>'trade_vehicle_style' = '' then null else payload->>'trade_vehicle_style' end as trade_vehicle_style,
            case when payload->>'trade_vehicle_trim' = '' then null else payload->>'trade_vehicle_trim' end as trade_vehicle_trim,
            case when payload->>'trade_vehicle_mileage' = '' then null else (payload->>'trade_vehicle_mileage')::double precision end as trade_vehicle_mileage,
            case when payload->>'trade_vehicle_vin' = '' then null else payload->>'trade_vehicle_vin' end as trade_vehicle_vin,
            case when payload->>'insurance_quote_id' = '' then null else (payload->>'insurance_quote_id')::uuid end as insurance_quote_id,
            case when payload->>'insurance_referrer_url' = '' then null else payload->>'insurance_referrer_url' end as insurance_referrer_url,
            case when payload->>'insurance_quote_payload' = '' then null else (payload->>'insurance_quote_payload')::json end as insurance_quote_payload,
            case when payload->>'insurance_purchase_price' = '' then null else (payload->>'insurance_purchase_price')::double precision/100 end as insurance_purchase_price,
            case when payload->>'insurance_vehicle_year' = '' then null else (payload->>'insurance_vehicle_year')::int end as insurance_vehicle_year,
            case when payload->>'insurance_vehicle_make' = '' then null else payload->>'insurance_vehicle_make' end as insurance_vehicle_make,
            case when payload->>'insurance_vehicle_model' = '' then null else payload->>'insurance_vehicle_model' end as insurance_vehicle_model,
            case when payload->>'insurance_vehicle_trim' = '' then null else payload->>'insurance_vehicle_trim' end as insurance_vehicle_trim,
            case when payload->>'insurance_vehicle_style' = '' then null else payload->>'insurance_vehicle_style' end as insurance_vehicle_style,
            case when payload->>'insurance_vehicle_mileage' = '' then null else (payload->>'insurance_vehicle_mileage')::double precision end as insurance_vehicle_mileage,
            case when payload->>'insurance_vehicle_vin' = '' then null else payload->>'insurance_vehicle_vin' end as insurance_vehicle_vin,
            case when payload->>'customer_birthdate' = '' then null else to_timestamp(payload->>'customer_birthdate','YYYY-MM-DD HH24:MI:SS') end as customer_birthdate,
            case when payload->>'customer_tracking_id' = '' then null else payload->>'customer_tracking_id' end as customer_tracking_id,
            case when payload->>'credit_rating' = '' then null else (payload->>'credit_rating')::int end as credit_rating,
            case when payload->>'credit_dealertrack_sent_at' = '' then null else to_timestamp(payload->>'credit_dealertrack_sent_at','YYYY-MM-DD HH24:MI:SS') end as credit_dealertrack_sent_at,
            case when payload->>'source_url' = '' then null else payload->>'source_url' end as source_url,
            case when payload->>'credit_vehicle_price' = '' then null else (payload->>'credit_vehicle_price')::double precision/100 end as credit_vehicle_price,
            case when payload->>'credit_trade_in_value' = '' then null else (payload->>'credit_trade_in_value')::double precision/100 end as credit_trade_in_value,
            case when payload->>'credit_down_payment' = '' then null else (payload->>'credit_down_payment')::double precision end as credit_down_payment,
            case when payload->>'credit_sales_tax' = '' then null else (payload->>'credit_sales_tax')::double precision end as credit_sales_tax,
            case when payload->>'credit_interest_rate' = '' then null else (payload->>'credit_interest_rate')::double precision end as credit_interest_rate,
            case when payload->>'credit_loan_amount' = '' then null else (payload->>'credit_loan_amount')::double precision end as credit_loan_amount,
            case when payload->>'language_code' = '' then null else payload->>'language_code' end as language_code,
            case when payload->>'integration_settings_id' = '' then null else (payload->>'integration_settings_id')::uuid end as integration_settings_id,
            case when payload->>'credit_creditor_name' = '' then null else payload->>'credit_creditor_name' end as credit_creditor_name,
            case when payload->>'credit_payload' = '' then null else (payload->>'credit_payload')::json end as credit_payload,
            case when payload->>'trade_low_list' = '' then null else (payload->>'trade_low_list')::float/100 end as trade_low_list,
            case when payload->>'trade_high_list' = '' then null else (payload->>'trade_high_list')::float/100 end as trade_high_list,
            case when payload->>'trade_list_count' = '' then null else (payload->>'trade_list_count')::int end as trade_list_count,
            case when payload->>'trade_aged_discount' = '' then null else (payload->>'trade_aged_discount')::double precision/100 end as trade_aged_discount,
            case when payload->>'trade_reconditioning' = '' then null else (payload->>'trade_reconditioning')::double precision/100 end as trade_reconditioning,
            case when payload->>'trade_advertising' = '' then null else (payload->>'trade_advertising')::double precision/100 end as trade_advertising,
            case when payload->>'trade_overhead' = '' then null else (payload->>'trade_overhead')::double precision/100 end as trade_overhead,
            case when payload->>'trade_dealer_profit' = '' then null else (payload->>'trade_dealer_profit')::double precision/100 end as trade_dealer_profit,
            case when payload->>'trade_in_source' = '' then null else payload->>'trade_in_source' end as trade_in_source,
            case when payload->>'route_list' = '' then null else (payload->>'route_list')::json end as route_list,
            case when payload->>'experiment' = '' then null else (payload->>'experiment')::json end as experiment,
            case when payload->>'credit_payment_frequency' = '' then null else (payload->>'credit_payment_frequency')::int end as credit_payment_frequency,
            case when payload->>'credit_trade_in_owing' = '' then null else (payload->>'credit_trade_in_owing')::double precision end as credit_trade_in_owing,
            case when payload->>'credit_payment_amount' = '' then null else (payload->>'credit_payment_amount')::double precision end as credit_payment_amount,
            case when payload->>'credit_term_in_months' = '' then null else (payload->>'credit_term_in_months')::int end as credit_term_in_months,
            case when payload->>'credit_regional_rating' = '' then null else (payload->>'credit_regional_rating')::int end as credit_regional_rating,
            case when payload->>'ecom_reserved' = '' then null else (payload->>'ecom_reserved')::int end as ecom_reserved,
            case when payload->>'testdrive_requested_date' = '' then null else to_timestamp(payload->>'testdrive_requested_date','YYYY-MM-DD HH24:MI:SS') end as testdrive_requested_date,
            case when payload->>'testdrive_time_of_day_preference' = '' then null else payload->>'testdrive_time_of_day_preference' end as testdrive_time_of_day_preference,
            case when payload->>'testdrive_language' = '' then null else payload->>'testdrive_language' end as testdrive_language,
            case when payload->>'testdrive_gender_preference' = '' then null else payload->>'testdrive_gender_preference' end as testdrive_gender_preference,
            case when payload->>'testdrive_technology_interests' = '' then null else (payload->>'testdrive_technology_interests')::json end as testdrive_technology_interests,
            case when payload->>'testdrive_route_name' = '' then null else payload->>'testdrive_route_name' end as testdrive_route_name,
            case when payload->>'testdrive_beverage' = '' then null else payload->>'testdrive_beverage' end as testdrive_beverage,
            case when payload->>'testdrive_comments' = '' then null else payload->>'testdrive_comments' end as testdrive_comments,
            case when payload->>'oem_of_interest' = '' then null else payload->>'oem_of_interest' end as oem_of_interest,
            case when payload->>'billing_key' = '' then null else payload->>'billing_key' end as billing_key,
            case when payload->>'spincar_lead_report_url' = '' then null else payload->>'spincar_lead_report_url' end as spincar_lead_report_url,
            case when payload->>'is_insurance' = '' then null else (payload->>'is_insurance')::int end as is_insurance,
            case when payload->>'is_trade' = '' then null else (payload->>'is_trade')::int end as is_trade,
            case when payload->>'is_credit_partial' = '' then null else (payload->>'is_credit_partial')::int end as is_credit_partial,
            case when payload->>'is_credit_verified' = '' then null else (payload->>'is_credit_verified')::int end as is_credit_verified,
            case when payload->>'is_credit_finance' = '' then null else (payload->>'is_credit_finance')::int end as is_credit_finance,
            case when payload->>'is_ecom' = '' then null else (payload->>'is_ecom')::int end as is_ecom,
            case when payload->>'is_spotlight' = '' then null else (payload->>'is_spotlight')::int end as is_spotlight,
            case when payload->>'device' = '' then null else payload->>'device' end as device,
            case when payload->>'is_credit_regional' = '' then null else (payload->>'is_credit_regional')::int end as is_credit_regional,
            case when payload->>'is_test_drive' = '' then null else (payload->>'is_test_drive')::int end as is_test_drive,
            case when payload->>'subType' = '' then null else (payload->>'subType')::text end as sub_type,
            case when payload->>'is_accident_check' = '' then null else (payload->>'is_accident_check')::int end as is_accident_check
        FROM
            autoverify.mpm_leads;
    ''',
    '''
        CREATE TABLE IF NOT EXISTS s3.authenticom_sales_data
        (
            s3_id bigint REFERENCES s3.scanned_files(id) ON DELETE CASCADE,
            payload jsonb
        );
        CREATE INDEX IF NOT EXISTS authenticom_sales_data_s3_id_idx
        ON s3.authenticom_sales_data (s3_id);
    ''',
    '''
        create or replace view s3.v_authenticom_sales_data as
        select
            case when payload->>'Cost' = '' then null else (payload->>'Cost')::double precision end as cost,
            case when payload->>'MSRP' = '' then null else (payload->>'MSRP')::double precision end as msrp,
            case when payload->>'Term' = '' then null else (payload->>'Term')::integer end as term,
            case when payload->>'BankName' = '' then null else (payload->>'BankName') end as bankname,
            case when payload->>'DealType' = '' then null else (payload->>'DealType') end as dealtype,
            case when payload->>'SaleType' = '' then null else (payload->>'SaleType') end as saletype,
            case when payload->>'BackGross' = '' then null else (payload->>'BackGross')::double precision end as backgross,
            case when payload->>'CashPrice' = '' then null else (payload->>'CashPrice')::double precision end as cashprice,
            case when payload->>'EntryDate' = '' then null else to_timestamp(payload->>'EntryDate','MM/DD/YYYY') end as entrydate,
            case when payload->>'Language' = '' then null else (payload->>'Language') end as language,
            case when payload->>'ListPrice' = '' then null else (payload->>'ListPrice')::double precision end as listprice,
            case when payload->>'LeaseTerm' = '' then null else (payload->>'LeaseTerm')::integer end as leaseterm,
            case when payload->>'TrimLevel' = '' then null else (payload->>'TrimLevel') end as trimlevel,
            case when payload->>'ACDealerID' = '' then null else (payload->>'ACDealerID') end as acdealerid,
            case when payload->>'DealNumber' = '' then null else (payload->>'DealNumber') end as dealnumber,
            case when payload->>'DealStatus' = '' then null else (payload->>'DealStatus') end as dealstatus,
            case when payload->>'FrontGross' = '' then null else (payload->>'FrontGross')::double precision end as frontgross,
            case when payload->>'StatusDate' = '' then null else to_timestamp(payload->>'StatusDate','MM/DD/YYYY') end as statusdate,
            case when payload->>'VehicleVIN' = '' then null else (payload->>'VehicleVIN') end as vehiclevin,
            case when payload->>'CoBuyerCell' = '' then null else (payload->>'CoBuyerCell') end as cobuyercell,
            case when payload->>'CoBuyerCity' = '' then null else (payload->>'CoBuyerCity') end as cobuyercity,
            case when payload->>'CoBuyerName' = '' then null else (payload->>'CoBuyerName') end as cobuyername,
            case when payload->>'CustomerZip' = '' then null else (payload->>'CustomerZip') end as customerzip,
            case when payload->>'DownPayment' = '' then null else (payload->>'DownPayment')::double precision end as downpayment,
            case when payload->>'VehicleMake' = '' then null else (payload->>'VehicleMake') end as vehiclemake,
            case when payload->>'VehicleType' = '' then null else (payload->>'VehicleType') end as vehicletype,
            case when payload->>'VehicleYear' = '' then null else (payload->>'VehicleYear')::integer end as vehicleyear,
            case when payload->>'CoBuyerEmail' = '' then null else (payload->>'CoBuyerEmail') end as cobuyeremail,
            case when payload->>'CoBuyerState' = '' then null else (payload->>'CoBuyerState') end as cobuyerstate,
            case when payload->>'ContractDate' = '' then null else to_timestamp(payload->>'ContractDate','MM/DD/YYYY') end as contractdate,
            case when payload->>'CustomerCity' = '' then null else (payload->>'CustomerCity') end as customercity,
            case when payload->>'CustomerName' = '' then null else (payload->>'CustomerName') end as customername,
            case when payload->>'DealBookDate' = '' then null else to_timestamp(payload->>'DealBookDate','MM/DD/YYYY') end as dealbookdate,
            case when payload->>'RealBookDate' = '' then null else to_timestamp(payload->>'RealBookDate','MM/DD/YYYY') end as realbookdate,
            case when payload->>'VehicleModel' = '' then null else (payload->>'VehicleModel') end as vehiclemodel,
            case when payload->>'CustomerEmail' = '' then null else (payload->>'CustomerEmail') end as CustomerEmail,
            case when payload->>'CustomerState' = '' then null else (payload->>'CustomerState') end as CustomerState,
            case when payload->>'InvoiceAmount' = '' then null else (payload->>'InvoiceAmount')::double precision end as InvoiceAmount,
            case when payload->>'RetailPayment' = '' then null else (payload->>'RetailPayment')::double precision end as RetailPayment,
            case when payload->>'TradeIn_1_VIN' = '' then null else (payload->>'TradeIn_1_VIN') end as TradeIn_1_VIN,
            case when payload->>'TradeIn_2_VIN' = '' then null else (payload->>'TradeIn_2_VIN') end as TradeIn_2_VIN,
            case when payload->>'AccountingDate' = '' then null else to_timestamp(payload->>'AccountingDate','MM/DD/YYYY') end as AccountingDate,
            case when payload->>'AmountFinanced' = '' then null else (payload->>'AmountFinanced')::double precision end as AmountFinanced,
            case when payload->>'CoBuyerAddress' = '' then null else (payload->>'CoBuyerAddress') end as CoBuyerAddress,
            case
                when payload->>'ClientDealerID' = '' then null
                when payload->>'ClientDealerID' = 'd6a8da09-ed94-4828-8ba4-4b35e99c4ab0' then 'bfd98837-6f44-46c2-a465-4425b5a4c67a'
                else (payload->>'ClientDealerID')
            end as ClientDealerID,
            case when payload->>'CoBuyerCountry' = '' then null else (payload->>'CoBuyerCountry') end as CoBuyerCountry,
            case when payload->>'CoBuyerCustNum' = '' then null else (payload->>'CoBuyerCustNum') end as CoBuyerCustNum,
            case when payload->>'CustomerCounty' = '' then null else (payload->>'CustomerCounty') end as CustomerCounty,
            case when payload->>'CustomerNumber' = '' then null else (payload->>'CustomerNumber')::text end as CustomerNumber,
            case when payload->>'NetTradeAmount' = '' then null else (payload->>'NetTradeAmount')::double precision end as NetTradeAmount,
            case when payload->>'TradeIn_1_Make' = '' then null else (payload->>'TradeIn_1_Make') end as TradeIn_1_Make,
            case when payload->>'TradeIn_1_Year' = '' then null else (payload->>'TradeIn_1_Year')::integer end as TradeIn_1_Year,
            case when payload->>'TradeIn_2_Make' = '' then null else (payload->>'TradeIn_2_Make') end as TradeIn_2_Make,
            case when payload->>'TradeIn_2_Year' = '' then null else (payload->>'TradeIn_2_Year')::integer end as TradeIn_2_Year,
            case when payload->>'VehicleMileage' = '' then null else (payload->>'VehicleMileage')::integer end as VehicleMileage,
            case when payload->>'CoBuyerLastName' = '' then null else (payload->>'CoBuyerLastName') end as CoBuyerLastName,
            case when payload->>'CustomerAddress' = '' then null else (payload->>'CustomerAddress') end as CustomerAddress,
            case when payload->>'GrossProfitSale' = '' then null else (payload->>'GrossProfitSale')::double precision end as GrossProfitSale,
            case when payload->>'Salesman_1_Name' = '' then null else (payload->>'Salesman_1_Name') end as Salesman_1_Name,
            case when payload->>'TradeIn_1_Gross' = '' then null else (payload->>'TradeIn_1_Gross')::double precision end as TradeIn_1_Gross,
            case when payload->>'TradeIn_1_Model' = '' then null else (payload->>'TradeIn_1_Model') end as TradeIn_1_Model,
            case when payload->>'TradeIn_2_Gross' = '' then null else (payload->>'TradeIn_2_Gross')::double precision end as TradeIn_2_Gross,
            case when payload->>'TradeIn_2_Model' = '' then null else (payload->>'TradeIn_2_Model') end as TradeIn_2_Model,
            case when payload->>'CoBuyerFirstName' = '' then null else (payload->>'CoBuyerFirstName') end as CoBuyerFirstName,
            case when payload->>'CoBuyerHomePhone' = '' then null else (payload->>'CoBuyerHomePhone') end as CoBuyerHomePhone,
            case when payload->>'CoBuyerWorkPhone' = '' then null else (payload->>'CoBuyerWorkPhone') end as CoBuyerWorkPhone,
            case when payload->>'CustomerLastName' = '' then null else (payload->>'CustomerLastName') end as CustomerLastName,
            case when payload->>'SalesManagerName' = '' then null else (payload->>'SalesManagerName') end as SalesManagerName,
            case when payload->>'TradeIn_1_Payoff' = '' then null else (payload->>'TradeIn_1_Payoff')::double precision end as TradeIn_1_Payoff,
            case when payload->>'TradeIn_2_Payoff' = '' then null else (payload->>'TradeIn_2_Payoff')::double precision end as TradeIn_2_Payoff,
            case when payload->>'CustomerBirthDate' = '' then null else to_timestamp(payload->>'CustomerBirthDate','MM/DD/YYYY') end as CustomerBirthDate,
            case when payload->>'CustomerCellPhone' = '' then null else (payload->>'CustomerCellPhone') end as CustomerCellPhone,
            case when payload->>'CustomerFirstName' = '' then null else (payload->>'CustomerFirstName') end as CustomerFirstName,
            case when payload->>'CustomerHomePhone' = '' then null else (payload->>'CustomerHomePhone') end as CustomerHomePhone,
            case when payload->>'CustomerWorkPhone' = '' then null else (payload->>'CustomerWorkPhone') end as CustomerWorkPhone
        from
            s3.authenticom_sales_data;
    ''',
    '''
        create or replace view autoverify.v_accident_check_reports as
        select
            id,
            created_at,
            case when payload->>'vin' = '' then null else payload->>'vin' end as vin,
            case when payload->>'status' = '' then null else payload->>'status' end as status,
            case when payload->>'batch_id' = '' then null else (payload->>'batch_id') end as batch_id,
            case when payload->>'expired_on' = '' then null else to_timestamp(payload->>'expired_on','YYYY-MM-DD HH24:MI:SS') end as expired_on,
            case when payload->>'data_source' = '' then null else (payload->>'data_source')::integer end as data_source,
            case when payload->>'is_archived' = '' then null else (payload->>'is_archived')::integer end as is_archived,
            case when payload->>'vin_pattern' = '' then null else (payload->>'vin_pattern') end as vin_pattern,
            case when payload->>'disclosure_id' = '' then null else (payload->>'disclosure_id')::integer end as disclosure_id,
            case when payload->>'accident_reported' = '' then null else payload->>'accident_reported' end as accident_reported,
            case when payload->>'master_business_id' = '' then null else (payload->>'master_business_id') end as master_business_id,
            case when payload->>'accident_damage_level' = '' then null else payload->>'accident_damage_level' end as accident_damage_level,
            case when payload->>'accident_repair_amount' = '' then null else (payload->>'accident_repair_amount')::integer end as accident_repair_amount
        from
            autoverify.accident_check_reports;
    ''',
    '''
        create materialized view if not exists autoverify.m_mpm_lead_details as
        select
            c.id as master_business_id,
            a.*,
            autoverify.lead_content(a.is_insurance, a.is_trade, a.is_credit_partial, a.is_credit_verified, a.is_credit_finance, a.is_ecom, a.is_test_drive) as lead_content
        from
            autoverify.v_mpm_leads a
        left join autoverify.v_mpm_integration_settings  b on b.id = a.integration_settings_id
        left join autoverify.sda_master_businesses c on c.id = b.master_business_id;
        CREATE UNIQUE INDEX IF NOT EXISTS m_mpm_lead_details_id_idx ON autoverify.m_mpm_lead_details (id);
    ''',
    '''
        create or replace view autoverify.v_tradesii_leads as
        select
            id,
            created_at,
            case when payload->>'type' = '' then null else (payload->>'type')::int end as type,
            case when payload->>'status' = '' then null else (payload->>'status')::int end as status,
            case when payload->>'payload' = '' then null else (payload->>'payload')::json end as payload,
            case when payload->>'customer_ip' = '' then null else (payload->>'customer_ip')::text end as customer_ip,
            case when payload->>'vehicle_vin' = '' then null else (payload->>'vehicle_vin')::text end as vehicle_vin,
            case when payload->>'vehicle_make' = '' then null else (payload->>'vehicle_make')::text end as vehicle_make,
            case when payload->>'vehicle_trim' = '' then null else (payload->>'vehicle_trim')::text end as vehicle_trim,
            case when payload->>'vehicle_year' = '' then null else (payload->>'vehicle_year')::int end as vehicle_year,
            case when payload->>'customer_name' = '' then null else (payload->>'customer_name')::text end as customer_name,
            case when payload->>'vehicle_model' = '' then null else (payload->>'vehicle_model')::text end as vehicle_model,
            case when payload->>'vehicle_style' = '' then null else (payload->>'vehicle_style')::text end as vehicle_style,
            case when payload->>'vehicle_prices' = '' then null else replace(payload->>'vehicle_prices','"\','')::json end as vehicle_prices,
            case when payload->>'vehicle_mileage' = '' then null else (payload->>'vehicle_mileage')::double precision end as vehicle_mileage,
            case when payload->>'tradesii_report_id' = '' then null else (payload->>'tradesii_report_id')::uuid end as tradesii_report_id,
            case when payload->>'vehicle_deductions' = '' then null else replace(payload->>'vehicle_deductions','"\','')::json end as vehicle_deductions,
            case when payload->>'customer_postal_code' = '' then null else replace((payload->>'customer_postal_code')::text,' ','') end as customer_postal_code,
            case when payload->>'customer_phone_number' = '' then null else (payload->>'customer_phone_number')::text end as customer_phone_number,
            case when payload->>'customer_referrer_url' = '' then null else (payload->>'customer_referrer_url')::text end as customer_referrer_url,
            case when payload->>'customer_email_address' = '' then null else (payload->>'customer_email_address')::text end as customer_email_address,
            case when payload->>'tradesii_business_profile_id' = '' then null else (payload->>'tradesii_business_profile_id')::uuid end as tradesii_business_profile_id
        from
            autoverify.tradesii_leads;
    ''',
    '''
        create or replace view autoverify.v_tradesii_businesses as
        select
            id,
            created_at,
            updated_at,
            case when payload->>'city' = '' then null else (payload->>'city')::text end as city,
            case when payload->>'name' = '' then null else (payload->>'name')::text end as name,
            case when payload->>'status' = '' then null else (payload->>'status')::int end as status,
            case when payload->>'crm_type' = '' then null else (payload->>'crm_type')::int end as crm_type,
            case when payload->>'postal_code' = '' then null else replace((payload->>'postal_code')::text,' ','') end as postal_code,
            case when payload->>'website_url' = '' then null else (payload->>'website_url')::text end as website_url,
            case when payload->>'country_code' = '' then null else (payload->>'country_code')::text end as country_code,
            case when payload->>'phone_number' = '' then null else (payload->>'phone_number')::json end as phone_number,
            case when payload->>'province_code' = '' then null else (payload->>'province_code')::text end as province_code,
            case when payload->>'address_line_1' = '' then null else (payload->>'address_line_1')::text end as address_line_1,
            case when payload->>'address_line_2' = '' then null else (payload->>'address_line_2')::text end as address_line_2,
            case when payload->>'crm_account_id' = '' then null else (payload->>'crm_account_id')::text end as crm_account_id,
            case when payload->>'master_business_id' = '' then null else (payload->>'master_business_id')::uuid end as master_business_id,
            case when payload->>'stripe_customer_id' = '' then null else (payload->>'stripe_customer_id')::text end as stripe_customer_id
        from
            autoverify.tradesii_businesses;
    ''',
    '''
        create or replace view autoverify.v_tradesii_business_profiles as
        select
            id,
            created_at,
            case when payload->>'type' = '' then null else (payload->>'type')::int end as type,
            case when payload->>'gtm_id' = '' then null else (payload->>'gtm_id')::text end as gtm_id,
            case when payload->>'status' = '' then null else (payload->>'status')::int end as status,
            case when payload->>'api_key' = '' then null else (payload->>'api_key')::uuid end as api_key,
            case when payload->>'language' = '' then null else (payload->>'language')::text end as language,
            case when payload->>'external_id' = '' then null else (payload->>'external_id')::text end as external_id,
            case when payload->>'integration_name' = '' then null else (payload->>'integration_name')::text end as integration_name,
            case when payload->>'business_crm_type' = '' then null else (payload->>'business_crm_type')::text end as business_crm_type,
            case when payload->>'credsii_integration' = '' then null else (payload->>'credsii_integration')::int end as credsii_integration,
            case when payload->>'crm_recipient_emails' = '' then null else (payload->>'crm_recipient_emails')::json end as crm_recipient_emails,
            case when payload->>'tradesii_business_id' = '' then null else (payload->>'tradesii_business_id')::uuid end as tradesii_business_id,
            case when payload->>'lead_recipient_emails' = '' then null else (payload->>'lead_recipient_emails')::json end as lead_recipient_emails,
            case when payload->>'phone_number_verification' = '' then null else (payload->>'phone_number_verification')::int end as phone_number_verification,
            case when payload->>'regional_price_adjustment' = '' then null else (payload->>'regional_price_adjustment')::double precision end as regional_price_adjustment
        from
            autoverify.tradesii_business_profiles;
    ''',
    '''
        create or replace view autoverify.v_credsii_leads as
        SELECT
            id,
            created_at,
            updated_at,
            case when payload->>'city' = '' then null else (payload->>'city')::text end as city,
            case when payload->>'country' = '' then null else (payload->>'country')::text end as country,
            case when payload->>'province' = '' then null else (payload->>'province')::text end as province,
            case when payload->>'birthdate' = '' then null else to_timestamp(payload->>'birthdate','YYYY-MM-DD')::text end as birthdate,
            case when payload->>'last_name' = '' then null else (payload->>'last_name')::text end as last_name,
            case when payload->>'lead_type' = '' then null else (payload->>'lead_type')::int end as lead_type,
            case when payload->>'report_id' = '' then null else (payload->>'report_id')::uuid end as report_id,
            case when payload->>'first_name' = '' then null else (payload->>'first_name')::text end as first_name,
            case when payload->>'billing_key' = '' then null else (payload->>'billing_key')::text end as billing_key,
            case when payload->>'lead_status' = '' then null else (payload->>'lead_status')::int end as lead_status,
            case when payload->>'postal_code' = '' then null else replace((payload->>'postal_code')::text,' ','') end as postal_code,
            case when payload->>'tracking_id' = '' then null else (payload->>'tracking_id')::text end as tracking_id,
            case when payload->>'meta_monthly' = '' then null else (payload->>'meta_monthly')::double precision end as meta_monthly,
            case when payload->>'phone_number' = '' then null else (payload->>'phone_number')::json end as phone_number,
            case when payload->>'credit_rating' = '' then null else (payload->>'credit_rating')::text end as credit_rating,
            case when payload->>'email_address' = '' then null else (payload->>'email_address')::text end as email_address,
            case when payload->>'address_line_1' = '' then null else (payload->>'address_line_1')::text end as address_line_1,
            case when payload->>'address_line_2' = '' then null else (payload->>'address_line_2')::text end as address_line_2,
            case when payload->>'financial_form' = '' then null else (payload->>'financial_form')::text end as financial_form,
            case when payload->>'meta_sales_tax' = '' then null else (payload->>'meta_sales_tax')::double precision end as meta_sales_tax,
            case when payload->>'conversation_id' = '' then null else (payload->>'conversation_id')::text end as conversation_id,
            case when payload->>'meta_source_url' = '' then null else (payload->>'meta_source_url')::text end as meta_source_url,
            case when payload->>'meta_loan_amount' = '' then null else (payload->>'meta_loan_amount')::double precision end as meta_loan_amount,
            case when payload->>'route_one_result' = '' then null else (payload->>'route_one_result')::text end as route_one_result,
            case when payload->>'routeone_sent_at' = '' then null else (payload->>'routeone_sent_at')::text end as routeone_sent_at,
            case when payload->>'meta_down_payment' = '' then null else (payload->>'meta_down_payment')::double precision end as meta_down_payment,
            case when payload->>'meta_loan_balance' = '' then null else (payload->>'meta_loan_balance')::double precision end as meta_loan_balance,
            case when payload->>'meta_interest_rate' = '' then null else (payload->>'meta_interest_rate')::double precision end as meta_interest_rate,
            case when payload->>'meta_loan_duration' = '' then null else (payload->>'meta_loan_duration')::integer end as meta_loan_duration,
            case when payload->>'meta_vehicle_price' = '' then null else (payload->>'meta_vehicle_price')::double precision end as meta_vehicle_price,
            case when payload->>'dealertrack_sent_at' = '' then null else to_timestamp((payload->>'dealertrack_sent_at')::text,'YYYY-MM-DD HH24:MI:SS') end as dealertrack_sent_at,
            case when payload->>'meta_trade_in_value' = '' then null else (payload->>'meta_trade_in_value')::double precision end as meta_trade_in_value,
            case when payload->>'financial_form_stored_at' = '' then null else to_timestamp((payload->>'financial_form_stored_at')::text,'YYYY-MM-DD HH24:MI:SS') end as financial_form_stored_at
        FROM
            autoverify.credsii_leads;
    ''',
    '''
        create or replace view autoverify.v_credsii_businesses as
        SELECT
            id,
            created_at,
            updated_at,
            case when payload->>'city' = '' then null else (payload->>'city')::text end as city,
            case when payload->>'name' = '' then null else (payload->>'name')::text end as name,
            case when payload->>'status' = '' then null else (payload->>'status')::int end as status,
            case when payload->>'country' = '' then null else (payload->>'country')::text end as country,
            case when payload->>'crm_type' = '' then null else (payload->>'crm_type')::int end as crm_type,
            case when payload->>'province' = '' then null else (payload->>'province')::text end as province,
            case when payload->>'postal_code' = '' then null else replace((payload->>'postal_code')::text,' ','') end as postal_code,
            case when payload->>'website_url' = '' then null else (payload->>'website_url')::text end as website_url,
            case when payload->>'phone_number' = '' then null else (payload->>'phone_number')::json end as phone_number,
            case when payload->>'address_line_1' = '' then null else (payload->>'address_line_1')::text end as address_line_1,
            case when payload->>'address_line_2' = '' then null else (payload->>'address_line_2')::text end as address_line_2,
            case when payload->>'crm_account_id' = '' then null else (payload->>'crm_account_id')::text end as crm_account_id,
            case when payload->>'master_business_id' = '' then null else (payload->>'master_business_id')::uuid end as master_business_id,
            case when payload->>'stripe_customer_id' = '' then null else (payload->>'stripe_customer_id')::text end as stripe_customer_id
        FROM
            autoverify.credsii_businesses;
    ''',
    '''
        create or replace view autoverify.v_credsii_business_profiles as
    SELECT
        id,
        created_at,
        updated_at,
        case when payload->>'crm' = '' then null else (payload->>'crm')::int end as crm,
        case when payload->>'type' = '' then null else (payload->>'type')::int end as type,
        case when payload->>'banks' = '' then null else (payload->>'banks')::json end as banks,
        case when payload->>'status' = '' then null else (payload->>'status')::int end as status,
        case when payload->>'api_key' = '' then null else (payload->>'api_key')::uuid end as api_key,
        case when payload->>'buttons' = '' then null else (payload->>'buttons')::json end as buttons,
        case when payload->>'language' = '' then null else (payload->>'language')::text end as language,
        case when payload->>'live_date' = '' then null else to_timestamp((payload->>'live_date')::text,'YYYY-MM-DD HH24:MI:SS') end as live_date,
        case when payload->>'fax_number' = '' then null else (payload->>'fax_number')::text end as fax_number,
        case when payload->>'business_id' = '' then null else (payload->>'business_id')::uuid end as business_id,
        case when payload->>'external_id' = '' then null else (payload->>'external_id')::text end as external_id,
        case when payload->>'route_one_key' = '' then null else (payload->>'route_one_key')::text end as route_one_key,
        case when payload->>'dealertrack_id' = '' then null else (payload->>'dealertrack_id')::text end as dealertrack_id,
        case when payload->>'finance_emails' = '' then null else (payload->>'finance_emails')::json end as finance_emails,
        case when payload->>'interest_rates' = '' then null else (payload->>'interest_rates')::json end as interest_rates,
        case when payload->>'integration_name' = '' then null else (payload->>'integration_name')::text end as integration_name,
        case when payload->>'crm_recipient_emails' = '' then null else (payload->>'crm_recipient_emails')::json end as crm_recipient_emails,
        case when payload->>'lead_recipient_emails' = '' then null else (payload->>'lead_recipient_emails')::json end as lead_recipient_emails,
        case when payload->>'dealertrack_confirm_id' = '' then null else (payload->>'dealertrack_confirm_id')::text end as dealertrack_confirm_id
    FROM
        autoverify.credsii_business_profiles;
    ''',
    '''
        create or replace view autoverify.v_credsii_reports as
        SELECT
            id,
            created_at,
            updated_at,
            case when payload->>'city' = '' then null else (payload->>'city')::text end as city,
            case when payload->>'status' = '' then null else (payload->>'status')::int end as status,
            case when payload->>'country' = '' then null else (payload->>'country')::text end as country,
            case when payload->>'language' = '' then null else (payload->>'language')::text end as language,
            case when payload->>'province' = '' then null else (payload->>'province')::text end as province,
            case when payload->>'verified' = '' then null else (payload->>'verified')::int end as verified,
            case when payload->>'birthdate' = '' then null else to_timestamp((payload->>'birthdate')::text,'YYYY-MM-DD HH24:MI:SS') end as birthdate,
            case when payload->>'last_name' = '' then null else (payload->>'last_name')::text end as last_name,
            case when payload->>'first_name' = '' then null else (payload->>'first_name')::text end as first_name,
            case when payload->>'profile_id' = '' then null else (payload->>'profile_id')::uuid end as profile_id,
            case when payload->>'source_url' = '' then null else (payload->>'source_url')::text end as source_url,
            case when payload->>'postal_code' = '' then null else replace((payload->>'postal_code')::text,' ','') end as postal_code,
            case when payload->>'tracking_id' = '' then null else (payload->>'tracking_id')::text end as tracking_id,
            case when payload->>'phone_number' = '' then null else (payload->>'phone_number')::json end as phone_number,
            case when payload->>'credit_rating' = '' then null else (payload->>'credit_rating')::int end as credit_rating,
            case when payload->>'email_address' = '' then null else (payload->>'email_address')::text end as email_address,
            case when payload->>'address_line_1' = '' then null else (payload->>'address_line_1')::text end as address_line_1,
            case when payload->>'address_line_2' = '' then null else (payload->>'address_line_2')::text end as address_line_2,
            case when payload->>'tradesii_report_id' = '' then null else (payload->>'tradesii_report_id')::uuid end as tradesii_report_id
        FROM
            autoverify.credsii_reports;
    ''',
    '''
        create or replace view autoverify.v_insuresii_leads as
        select
            id,
            created_at,
            updated_at,
            case when payload->>'vin' = '' then null else (payload->>'vin')::text end as vin,
            case when payload->>'city' = '' then null else (payload->>'city')::text end as city,
            case when payload->>'make' = '' then null else (payload->>'make')::text end as make,
            case when payload->>'trim' = '' then null else (payload->>'trim')::text end as trim,
            case when payload->>'year' = '' then null else (payload->>'year')::int end as year,
            case when payload->>'email' = '' then null else (payload->>'email')::text end as email,
            case when payload->>'model' = '' then null else (payload->>'model')::text end as model,
            case when payload->>'phone' = '' then null else (payload->>'phone')::text end as phone,
            case when payload->>'style' = '' then null else (payload->>'style')::text end as style,
            case when payload->>'country' = '' then null else (payload->>'country')::text end as country,
            case when payload->>'mileage' = '' then null else (payload->>'mileage')::double precision end as mileage,
            case when payload->>'payload' = '' then null else (payload->>'payload')::json end as payload,
            case when payload->>'language' = '' then null else (payload->>'language')::text end as language,
            case when payload->>'province' = '' then null else (payload->>'province')::text end as province,
            case when payload->>'quote_id' = '' then null else (payload->>'quote_id')::uuid end as quote_id,
            case when payload->>'last_name' = '' then null else (payload->>'last_name')::text end as last_name,
            case when payload->>'lead_type' = '' then null else (payload->>'lead_type')::text end as lead_type,
            case when payload->>'first_name' = '' then null else (payload->>'first_name')::text end as first_name,
            case when payload->>'profile_id' = '' then null else (payload->>'profile_id')::uuid end as profile_id,
            case when payload->>'lead_status' = '' then null else (payload->>'lead_status')::text end as lead_status,
            case when payload->>'postal_code' = '' then null else replace((payload->>'postal_code')::text,' ','') end as postal_code,
            case when payload->>'mobile_phone' = '' then null else (payload->>'mobile_phone')::text end as mobile_phone,
            case when payload->>'referrer_url' = '' then null else (payload->>'referrer_url')::text end as referrer_url,
            case when payload->>'quote_payload' = '' then null else (payload->>'quote_payload')::json end as quote_payload,
            case when payload->>'address_line_1' = '' then null else (payload->>'address_line_1')::text end as address_line_1,
            case when payload->>'address_line_2' = '' then null else (payload->>'address_line_2')::text end as address_line_2,
            case when payload->>'purchase_price' = '' then null else (payload->>'purchase_price')::double precision end as purchase_price
        from
            autoverify.insuresii_leads;
    ''',
    '''
        create or replace view autoverify.v_insuresii_business_profiles as
        SELECT
            id,
            case when payload->>'type' = '' then null else (payload->>'type')::text end as type,
            case when payload->>'gtm_id' = '' then null else (payload->>'gtm_id')::text end as gtm_id,
            case when payload->>'status' = '' then null else (payload->>'status')::text end as status,
            case when payload->>'api_key' = '' then null else (payload->>'api_key')::text end as api_key,
            case when payload->>'crm_type' = '' then null else (payload->>'crm_type')::text end as crm_type,
            case when payload->>'language' = '' then null else (payload->>'language')::text end as language,
            case when payload->>'live_date' = '' then null else to_timestamp((payload->>'live_date')::text,'YYYY-MM-DD HH24:MI:SS') end as live_date,
            case when payload->>'business_id' = '' then null else (payload->>'business_id')::uuid end as business_id,
            case when payload->>'external_id' = '' then null else (payload->>'external_id')::text end as external_id,
            case when payload->>'crm_recipient_emails' = '' then null else (payload->>'crm_recipient_emails')::json end as crm_recipient_emails,
            case when payload->>'lead_recipient_emails' = '' then null else (payload->>'lead_recipient_emails')::json end as lead_recipient_emails,
            case when payload->>'requires_mobile_verification' = '' then null else (payload->>'requires_mobile_verification')::int end as requires_mobile_verification
        FROM
            autoverify.insuresii_business_profiles;
    ''',
    '''
        create table if not exists autoverify.dashboard_lead_content
        (
            id uuid PRIMARY KEY,
            master_business_id uuid not null,
            integration_settings_id uuid,
            created_at timestamptz,
            device text,
            lead_content text [] CHECK (cardinality(lead_content) > 0)
        );
    ''',
    '''
        CREATE OR REPLACE VIEW autoverify.v_mpm_lead_details
        AS SELECT
            b.master_business_id,
            a.id,
            a.status,
            a.created_at,
            a.updated_at,
            a.customer_first_name,
            a.customer_last_name,
            a.customer_email,
            a.customer_phone,
            a.customer_language,
            a.customer_mobile_phone,
            a.customer_payload,
            a.customer_address_line_1,
            a.customer_address_line_2,
            a.customer_city,
            a.customer_postal_code,
            a.customer_country,
            a.customer_province,
            a.trade_high_value,
            a.trade_low_value,
            a.trade_deductions,
            a.trade_customer_referral_url,
            a.trade_vehicle_year,
            a.trade_vehicle_make,
            a.trade_vehicle_model,
            a.trade_vehicle_style,
            a.trade_vehicle_trim,
            a.trade_vehicle_mileage,
            a.trade_vehicle_vin,
            a.insurance_quote_id,
            a.insurance_referrer_url,
            a.insurance_quote_payload,
            a.insurance_purchase_price,
            a.insurance_vehicle_year,
            a.insurance_vehicle_make,
            a.insurance_vehicle_model,
            a.insurance_vehicle_trim,
            a.insurance_vehicle_style,
            a.insurance_vehicle_mileage,
            a.insurance_vehicle_vin,
            a.customer_birthdate,
            a.customer_tracking_id,
            a.credit_rating,
            a.credit_dealertrack_sent_at,
            a.source_url,
            a.credit_vehicle_price,
            a.credit_trade_in_value,
            a.credit_down_payment,
            a.credit_sales_tax,
            a.credit_interest_rate,
            a.credit_loan_amount,
            a.language_code,
            a.integration_settings_id,
            a.credit_creditor_name,
            a.credit_payload,
            a.trade_low_list,
            a.trade_high_list,
            a.trade_list_count,
            a.trade_aged_discount,
            a.trade_reconditioning,
            a.trade_advertising,
            a.trade_overhead,
            a.trade_dealer_profit,
            a.trade_in_source,
            a.route_list,
            a.experiment,
            a.credit_payment_frequency,
            a.credit_trade_in_owing,
            a.credit_payment_amount,
            a.credit_term_in_months,
            a.credit_regional_rating,
            a.ecom_reserved,
            a.testdrive_requested_date,
            a.testdrive_time_of_day_preference,
            a.testdrive_language,
            a.testdrive_gender_preference,
            a.testdrive_technology_interests,
            a.testdrive_route_name,
            a.testdrive_beverage,
            a.testdrive_comments,
            a.oem_of_interest,
            a.billing_key,
            a.spincar_lead_report_url,
            a.is_insurance,
            a.is_trade,
            a.is_credit_partial,
            a.is_credit_verified,
            a.is_credit_finance,
            a.is_ecom,
            a.is_spotlight,
            a.device,
            a.is_credit_regional,
            a.is_test_drive,
            autoverify.lead_content(a.is_insurance, a.is_trade, a.is_credit_partial, a.is_credit_verified, a.is_credit_finance, a.is_ecom, a.is_test_drive, a.is_accident_check, a.is_spotlight, a.sub_type) AS lead_content,
            a.sub_type,
            a.is_accident_check
        FROM autoverify.v_mpm_leads a
        LEFT JOIN autoverify.v_mpm_integration_settings b ON b.id = a.integration_settings_id;
    ''',
    '''
        CREATE TABLE IF NOT EXISTS autoverify.mpm_lead_details
        (
            master_business_id uuid NULL,
            id uuid NULL,
            status text NULL,
            created_at timestamptz NULL,
            updated_at timestamptz NULL,
            customer_first_name text NULL,
            customer_last_name text NULL,
            customer_email text NULL,
            customer_phone text NULL,
            customer_language text NULL,
            customer_mobile_phone text NULL,
            customer_payload json NULL,
            customer_address_line_1 text NULL,
            customer_address_line_2 text NULL,
            customer_city text NULL,
            customer_postal_code text NULL,
            customer_country text NULL,
            customer_province text NULL,
            trade_high_value float8 NULL,
            trade_low_value float8 NULL,
            trade_deductions text NULL,
            trade_customer_referral_url text NULL,
            trade_vehicle_year int4 NULL,
            trade_vehicle_make text NULL,
            trade_vehicle_model text NULL,
            trade_vehicle_style text NULL,
            trade_vehicle_trim text NULL,
            trade_vehicle_mileage float8 NULL,
            trade_vehicle_vin text NULL,
            insurance_quote_id uuid NULL,
            insurance_referrer_url text NULL,
            insurance_quote_payload json NULL,
            insurance_purchase_price float8 NULL,
            insurance_vehicle_year int4 NULL,
            insurance_vehicle_make text NULL,
            insurance_vehicle_model text NULL,
            insurance_vehicle_trim text NULL,
            insurance_vehicle_style text NULL,
            insurance_vehicle_mileage float8 NULL,
            insurance_vehicle_vin text NULL,
            customer_birthdate timestamptz NULL,
            customer_tracking_id text NULL,
            credit_rating int4 NULL,
            credit_dealertrack_sent_at timestamptz NULL,
            source_url text NULL,
            credit_vehicle_price float8 NULL,
            credit_trade_in_value float8 NULL,
            credit_down_payment float8 NULL,
            credit_sales_tax float8 NULL,
            credit_interest_rate float8 NULL,
            credit_loan_amount float8 NULL,
            language_code text NULL,
            integration_settings_id uuid NULL,
            credit_creditor_name text NULL,
            credit_payload json NULL,
            trade_low_list float8 NULL,
            trade_high_list float8 NULL,
            trade_list_count int4 NULL,
            trade_aged_discount float8 NULL,
            trade_reconditioning float8 NULL,
            trade_advertising float8 NULL,
            trade_overhead float8 NULL,
            trade_dealer_profit float8 NULL,
            trade_in_source text NULL,
            route_list json NULL,
            experiment json NULL,
            credit_payment_frequency int4 NULL,
            credit_trade_in_owing float8 NULL,
            credit_payment_amount float8 NULL,
            credit_term_in_months int4 NULL,
            credit_regional_rating int4 NULL,
            ecom_reserved int4 NULL,
            testdrive_requested_date timestamptz NULL,
            testdrive_time_of_day_preference text NULL,
            testdrive_language text NULL,
            testdrive_gender_preference text NULL,
            testdrive_technology_interests json NULL,
            testdrive_route_name text NULL,
            testdrive_beverage text NULL,
            testdrive_comments text NULL,
            oem_of_interest text NULL,
            billing_key text NULL,
            spincar_lead_report_url text NULL,
            is_insurance int4 NULL,
            is_trade int4 NULL,
            is_credit_partial int4 NULL,
            is_credit_verified int4 NULL,
            is_credit_finance int4 NULL,
            is_ecom int4 NULL,
            is_spotlight int4 NULL,
            device text NULL,
            is_credit_regional int4 NULL,
            is_test_drive int4 NULL,
            lead_content text NULL,
            sub_type text null,
            is_accident_check int4 null
        );
        CREATE INDEX IF NOT EXISTS mpm_lead_details_created_at_idx ON autoverify.mpm_lead_details USING btree (created_at);
        CREATE UNIQUE INDEX IF NOT EXISTS mpm_lead_details_id_idx ON autoverify.mpm_lead_details USING btree (id);
        CREATE INDEX IF NOT EXISTS mpm_lead_details_master_business_id_idx ON autoverify.mpm_lead_details USING btree (master_business_id);
    '''
    ,
    '''
        create materialized view if not exists public.m_lifetime_mixed_leads as
        SELECT
            master_business_id,
            array_to_string(lead_content, '-'::text) AS mixed_lead_type,
            count(*) as leads
        FROM
            autoverify.dashboard_lead_content
        GROUP BY
            1,2;
        CREATE unique INDEX IF NOT EXISTS  m_lifetime_mixed_leads_unq_idx ON  public.m_lifetime_mixed_leads (master_business_id,mixed_lead_type);
    ''',
    '''
        create materialized view if not exists public.m_lifetime_mixed_leads_device as
        SELECT
            master_business_id,
            array_to_string(lead_content, '-'::text) AS mixed_lead_type,
            device,
            count(*) as leads
        FROM
            autoverify.dashboard_lead_content
        GROUP BY
            1,2,3;
        CREATE unique INDEX IF NOT EXISTS m_lifetime_mixed_leads_device_unq_idx ON  public.m_lifetime_mixed_leads_device (master_business_id,mixed_lead_type,device);
    ''',
    '''
        CREATE TABLE if not exists autoverify.roi_arguments
        (
            "version" int4 NOT NULL,
            vehicle_sale_profit double precision NOT NULL,
            insurance_lead_conversion_probability double precision NOT NULL,
            trade_lead_conversion_probability double precision NOT NULL,
            credit_lead_conversion_probability double precision NOT NULL,
            finance_lead_conversion_probability double precision NOT NULL,
            payments_lead_conversion_probability double precision NULL,
            testdrive_lead_conversion_probability double precision NULL,
            spotlight_lead_conversion_probability double precision null,
            accidentcheck_lead_conversion_probability double precision null,
            high_quality_trade_in_probability double precision NOT NULL,
            trade_with_credit_lead_probability double precision NOT NULL,
            trade_with_insurance_lead_probability double precision NOT NULL,
            trade_with_finance_lead_probability double precision NOT NULL,
            trade_with_payments_lead_probability double precision NULL,
            trade_with_testdrive_lead_probability double precision NULL,
            trade_with_spotlight_lead_probability double precision null,
            trade_with_accidentcheck_lead_probability double precision null,
            CONSTRAINT roi_arguments_pkey PRIMARY KEY (version)
        );
    ''',
    '''
        CREATE TABLE if not exists autoverify.roi_arguments_version_history (
            "version" int4 NULL,
            "date" timestamptz NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT fk_roi_arguments
              FOREIGN KEY(version)
              REFERENCES autoverify.roi_arguments(version)
        );
    ''',
    '''
        create materialized view if not exists public.m_lifetime_master_leads as
        SELECT
            master_business_id,
            lead_content[1] AS master_lead_type,
            count(*)::numeric as leads
        FROM
            autoverify.dashboard_lead_content
        GROUP BY
            1,2;
        CREATE unique INDEX IF NOT EXISTS  m_lifetime_master_leads_unq_idx ON  public.m_lifetime_master_leads (master_business_id,master_lead_type);
    ''',
    '''
        create materialized view if not exists public.m_lifetime_master_leads_device as
        SELECT
            master_business_id,
            lead_content[1] AS master_lead_type,
            device,
            count(*)::numeric as leads
        FROM
            autoverify.dashboard_lead_content
        GROUP BY
            1,2,3;
        CREATE unique INDEX IF NOT EXISTS  m_lifetime_master_leads_device_unq_idx ON  public.m_lifetime_master_leads_device (master_business_id,device,master_lead_type);
    ''',
    '''
        create materialized view if not exists public.m_master_leads_daily as
        SELECT
            master_business_id,
            date_trunc('day',created_at) as date,
            lead_content[1] AS master_lead_type,
            count(*)::numeric as leads
        FROM
            autoverify.dashboard_lead_content
        GROUP BY
            1,2,3;
        CREATE unique INDEX IF NOT EXISTS  m_master_leads_daily_unq_idx ON  public.m_master_leads_daily (master_business_id,date,master_lead_type);
    ''',
    '''
        create materialized view if not exists public.m_master_leads_daily_device as
        SELECT
            master_business_id,
            date_trunc('day',created_at) as date,
            lead_content[1] AS master_lead_type,
            device,
            count(*)::numeric as leads
        FROM
            autoverify.dashboard_lead_content
        GROUP BY
            1,2,3,4;
        CREATE unique INDEX IF NOT EXISTS  m_master_leads_daily_device_unq_idx ON  public.m_master_leads_daily_device (master_business_id,date,device,master_lead_type);
    ''',
    '''
        create materialized view if not exists public.m_master_leads_monthly as
        SELECT
            master_business_id,
            date_trunc('month',created_at) as date,
            lead_content[1] AS master_lead_type,
            count(*)::numeric as leads
        FROM
            autoverify.dashboard_lead_content
        GROUP BY
            1,2,3;
        CREATE unique INDEX IF NOT EXISTS  m_master_leads_montly_unq_idx ON  public.m_master_leads_monthly (master_business_id,date,master_lead_type);
    ''',
    '''
        create materialized view if not exists public.m_master_leads_monthly_device as
        SELECT
            master_business_id,
            date_trunc('month',created_at) as date,
            lead_content[1] AS master_lead_type,
            device,
            count(*)::numeric as leads
        FROM
            autoverify.dashboard_lead_content
        GROUP BY
            1,2,3,4;
        CREATE unique INDEX IF NOT EXISTS  m_master_leads_montly_device_unq_idx ON  public.m_master_leads_monthly_device (master_business_id,date,device,master_lead_type);
    ''',
    '''
        create materialized view if not exists public.m_master_leads_weekly as
        SELECT
            master_business_id,
            date_trunc('week',created_at) as date,
            lead_content[1] AS master_lead_type,
            count(*)::numeric as leads
        FROM
            autoverify.dashboard_lead_content
        GROUP BY
            1,2,3;
        CREATE unique INDEX IF NOT EXISTS  m_master_leads_weekly_unq_idx ON  public.m_master_leads_weekly (master_business_id,date,master_lead_type);
    ''',
    '''
        create materialized view if not exists public.m_master_leads_weekly_device as
        SELECT
            master_business_id,
            date_trunc('week',created_at) as date,
            lead_content[1] AS master_lead_type,
            device,
            count(*)::numeric as leads
        FROM
            autoverify.dashboard_lead_content
        GROUP BY
            1,2,3,4;
        CREATE unique INDEX IF NOT EXISTS  m_master_leads_weekly_device_unq_idx ON  public.m_master_leads_weekly_device (master_business_id,date,device,master_lead_type);
    ''',
    '''
        create materialized view if not exists public.m_master_leads_yearly as
        SELECT
            master_business_id,
            date_trunc('year',created_at) as date,
            lead_content[1] AS master_lead_type,
            count(*)::numeric as leads
        FROM
            autoverify.dashboard_lead_content
        GROUP BY
            1,2,3;
        CREATE unique INDEX IF NOT EXISTS  m_master_leads_yearly_unq_idx ON  public.m_master_leads_yearly (master_business_id,date,master_lead_type);
    ''',
    '''
        create materialized view if not exists public.m_master_leads_yearly_device as
        SELECT
            master_business_id,
            date_trunc('year',created_at) as date,
            lead_content[1] AS master_lead_type,
            device,
            count(*)::numeric as leads
        FROM
            autoverify.dashboard_lead_content
        GROUP BY
            1,2,3,4;
        CREATE unique INDEX IF NOT EXISTS  m_master_leads_yearly_device_unq_idx ON  public.m_master_leads_yearly_device (master_business_id,date,device,master_lead_type);
    ''',
    '''
        create materialized view if not exists public.m_mixed_leads_monthly as
        SELECT
            master_business_id,
            date_trunc('month',created_at) as date,
            array_to_string(lead_content, '-'::text) AS mixed_lead_type,
            count(*)::numeric as leads
        FROM
            autoverify.dashboard_lead_content
        GROUP BY
            1,2,3;
        CREATE unique INDEX IF NOT EXISTS  m_mixed_leads_monthly_unq_idx ON  public.m_mixed_leads_monthly (master_business_id,date,mixed_lead_type);
    ''',
    '''
        create materialized view if not exists public.m_mixed_leads_monthly_device as
        SELECT
            master_business_id,
            date_trunc('month',created_at) as date,
            array_to_string(lead_content, '-'::text) AS mixed_lead_type,
            device,
            count(*)::numeric as leads
        FROM
            autoverify.dashboard_lead_content
        GROUP BY
            1,2,3,4;
        CREATE unique INDEX IF NOT EXISTS  m_mixed_leads_monthly_device_unq_idx ON  public.m_mixed_leads_monthly_device (master_business_id,date,device,mixed_lead_type);
    ''',
    '''
        create materialized view if not exists public.m_mixed_leads_yearly_device as
        SELECT
            master_business_id,
            date_trunc('year',created_at) as date,
            array_to_string(lead_content, '-'::text) AS mixed_lead_type,
            device,
            count(*)::numeric as leads
        FROM
            autoverify.dashboard_lead_content
        GROUP BY
            1,2,3,4;
        CREATE unique INDEX IF NOT EXISTS  m_mixed_leads_yearly_device_unq_idx ON  public.m_mixed_leads_yearly_device (master_business_id,date,device,mixed_lead_type);
    ''',
    '''
        create materialized view if not exists public.m_mixed_leads_yearly as
        SELECT
            master_business_id,
            date_trunc('year',created_at) as date,
            array_to_string(lead_content, '-'::text) AS mixed_lead_type,
            count(*)::numeric as leads
        FROM
            autoverify.dashboard_lead_content
        GROUP BY
            1,2,3;
        CREATE unique INDEX IF NOT EXISTS  m_mixed_leads_yearly_unq_idx ON  public.m_mixed_leads_yearly (master_business_id,date,mixed_lead_type);
    ''',
    '''
        CREATE OR REPLACE VIEW autoverify.v_master_lead_type_estimated_value as
        SELECT
            version as version,
            'trade' as master_lead_type,
            vehicle_sale_profit * (trade_lead_conversion_probability + trade_lead_conversion_probability * high_quality_trade_in_probability) as value,
            trade_lead_conversion_probability as conversion_rate
        from
            autoverify.roi_arguments
        where
            version = (select version from autoverify.roi_arguments_version_history order by autoverify.roi_arguments_version_history."date" desc limit 1)
        union
        SELECT
            version as version,
            'credit' as master_lead_type,
            vehicle_sale_profit * (credit_lead_conversion_probability + credit_lead_conversion_probability * high_quality_trade_in_probability * trade_with_credit_lead_probability) as value,
            credit_lead_conversion_probability as conversion_rate
        from
            autoverify.roi_arguments
        where
            version = (select version from autoverify.roi_arguments_version_history order by autoverify.roi_arguments_version_history."date" desc limit 1)
        union
        SELECT
            version as version,
            'insurance' as master_lead_type,
            vehicle_sale_profit * (insurance_lead_conversion_probability + insurance_lead_conversion_probability * high_quality_trade_in_probability * trade_with_insurance_lead_probability) as value,
            insurance_lead_conversion_probability as conversion_rate
        from
            autoverify.roi_arguments
        where
            version = (select version from autoverify.roi_arguments_version_history order by autoverify.roi_arguments_version_history."date" desc limit 1)
        union
        SELECT
            version as version,
            'finance' as master_lead_type,
            vehicle_sale_profit * (finance_lead_conversion_probability + finance_lead_conversion_probability * high_quality_trade_in_probability * trade_with_finance_lead_probability) as value,
            finance_lead_conversion_probability as conversion_rate
        from
            autoverify.roi_arguments
        where
            version = (select version from autoverify.roi_arguments_version_history order by autoverify.roi_arguments_version_history."date" desc limit 1)
        union
        SELECT
            version as version,
            'payments' as master_lead_type,
            vehicle_sale_profit * (payments_lead_conversion_probability + payments_lead_conversion_probability * high_quality_trade_in_probability * trade_with_payments_lead_probability) as value,
            payments_lead_conversion_probability as conversion_rate
        from
            autoverify.roi_arguments
        where
            version = (select version from autoverify.roi_arguments_version_history order by autoverify.roi_arguments_version_history."date" desc limit 1)
        union
        SELECT
            version as version,
            'testdrive' as master_lead_type,
            vehicle_sale_profit * (testdrive_lead_conversion_probability + testdrive_lead_conversion_probability * high_quality_trade_in_probability * trade_with_testdrive_lead_probability) as value,
            testdrive_lead_conversion_probability as conversion_rate
        from
            autoverify.roi_arguments
        where
            version = (select version from autoverify.roi_arguments_version_history order by autoverify.roi_arguments_version_history."date" desc limit 1)
        union
        SELECT
            version as version,
            'spotlight' as master_lead_type,
            vehicle_sale_profit * (spotlight_lead_conversion_probability + spotlight_lead_conversion_probability * high_quality_trade_in_probability * trade_with_spotlight_lead_probability) as value,
            spotlight_lead_conversion_probability as conversion_rate
        from
            autoverify.roi_arguments
        where
            version = (select version from autoverify.roi_arguments_version_history order by autoverify.roi_arguments_version_history."date" desc limit 1);
    ''',
    '''
        create materialized view if not exists public.m_estimated_revenue_quarterly as
        select
            a.master_business_id,
            a.date,
            sum(a.leads * coalesce(b.value,0)) as estimated_revenue
        from
            (select
            master_business_id,
            date_trunc('quarter',created_at) as date,
            lead_content[1] as master_lead_type,
            count(*) as leads
        from
            autoverify.dashboard_lead_content
        group by 1,2,3) a
        left join autoverify.v_master_lead_type_estimated_value b on b.master_lead_type = a.master_lead_type
        group by 1,2;
        CREATE unique INDEX IF NOT EXISTS m_estimated_revenue_quarterly_unq_idx ON  public.m_estimated_revenue_quarterly (master_business_id,date);
    ''',
    '''
        create materialized view if not exists public.m_estimated_revenue_yearly as
        select
            a.master_business_id,
            a.date,
            sum(a.leads * coalesce(b.value,0)) as estimated_revenue
        from
            (select
            master_business_id,
            date_trunc('year',created_at) as date,
            lead_content[1] as master_lead_type,
            count(*) as leads
        from
            autoverify.dashboard_lead_content
        group by 1,2,3) a
        left join autoverify.v_master_lead_type_estimated_value b on b.master_lead_type = a.master_lead_type
        group by 1,2;
        CREATE unique INDEX IF NOT EXISTS m_estimated_revenue_yearly_unq_idx ON  public.m_estimated_revenue_yearly (master_business_id,date);
    ''',
    '''
        create materialized view if not exists public.m_lifetime_estimated_revenue as
        select
            a.master_business_id,
            sum(a.leads * coalesce(b.value,0)) as estimated_revenue
        from
            (select
            master_business_id,
            lead_content[1] as master_lead_type,
            count(*) as leads
        from
            autoverify.dashboard_lead_content
        group by 1,2) a
        left join autoverify.v_master_lead_type_estimated_value b on b.master_lead_type = a.master_lead_type
        group by 1;
        CREATE unique INDEX IF NOT EXISTS m_lifetime_estimated_revenue_unq_idx ON  public.m_lifetime_estimated_revenue (master_business_id);
    ''',
    '''
        create materialized view if not exists public.m_estimated_vehicles_sold_monthly as
        select
            a.master_business_id,
            a.date,
            sum(a.leads * b.conversion_rate) as estimated_vehicles_sold
        from
        (
            select
                master_business_id,
                date_trunc('month',created_at) as date,
                lead_content[1] as master_lead_type,
                count(*) as leads
            from
                autoverify.dashboard_lead_content
            group by 1,2,3
        ) a
        left join autoverify.v_master_lead_type_estimated_value b on b.master_lead_type = a.master_lead_type
        group by 1,2;
        CREATE unique INDEX IF NOT EXISTS m_estimated_vehicles_sold_monthly_unq_idx ON  public.m_estimated_vehicles_sold_monthly (master_business_id,date);
    ''',
    '''
        create materialized view if not exists public.m_lifetime_estimated_vehicles_sold as
        select
            a.master_business_id,
            sum(a.leads * b.conversion_rate) as estimated_vehicles_sold
        from
        (
            select
                master_business_id,
                lead_content[1] as master_lead_type,
                count(*) as leads
            from
                autoverify.dashboard_lead_content
            group by 1,2
        ) a
        left join autoverify.v_master_lead_type_estimated_value b on b.master_lead_type = a.master_lead_type
        group by 1;
        CREATE unique INDEX IF NOT EXISTS m_lifetime_estimated_vehicles_sold_unq_idx ON  public.m_lifetime_estimated_vehicles_sold (master_business_id);
    ''',
    '''
        create or replace view operations.v_alerts as
        select
            a.*,
            now() - a.last_run as time_since_last_run,
            case
                when now() - a.last_run > frequency
                then 1
                else 0
            end as is_overdue,
            case
                when status is not null and status != 'success'
                then 1
                else 0
            end as bad_status
        from
            operations.scheduler a;
    ''',
    '''
        create table if not exists autoverify.spotlight_parameters
        (
            version integer not null,
            spotlight text not null,
            region text not null,
            payload jsonb not null,
            primary key (version,spotlight,region)
        );
        insert into autoverify.spotlight_parameters values (1,'low mileage','CA','{"age":"90 days","vehicle_grouping":"make","minimum_milege":"500","minimum_vehicles":"100"}')
        on conflict (version,spotlight,region) do update set payload = excluded.payload;
    ''',
    '''
        create table if not exists operations.sessions
        (
            id BIGSERIAL NOT NULL,
            schema text not null,
            script text not null,
	        payload jsonb NOT NULL,
	        validated boolean default false,
	        CONSTRAINT versions_pk PRIMARY KEY (id)
        );
    ''',
    '''
        create table if not exists public.spotlight_low_mileage_ca
        (
            session_id bigint REFERENCES operations.sessions(id) ON DELETE CASCADE,
            vin_pattern text not null,
            region text not null,
            mileage int not null,
            CONSTRAINT session_id_vin_pattern_region_pk PRIMARY KEY (session_id,vin_pattern,region)
        );
    ''',
    '''
        create table if not exists public.spotlight_low_mileage_us
        (
            session_id bigint REFERENCES operations.sessions(id) ON DELETE CASCADE,
            vin_pattern text not null,
            region text not null,
            mileage int not null,
            CONSTRAINT spotlight_low_mileage_us_session_id_vin_pattern_region_pk PRIMARY KEY (session_id,vin_pattern,region)
        );
    '''
]

with postgreshandler.get_analytics_connection() as connection:
    for query in queries:
        print(query)
        if len(query) > 0:
            with connection.cursor() as cursor:
                cursor.execute(query)