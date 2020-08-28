import sys
sys.path.insert(0, '../..')
import boto3
import src.settings
import io
import postgreshandler
import json
import os
import datetime
import psycopg2

queries = [
    '''
        CREATE SCHEMA IF NOT EXISTS autoverify;
    ''',
    '''
        CREATE SCHEMA IF NOT EXISTS operations;
    ''',
    '''
        CREATE SCHEMA IF NOT EXISTS utility;
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
        VALUES ('autoverify','accident_check_reports','2020-01-01','30 minutes') 
        ON CONFLICT ON CONSTRAINT scheduler_pk 
        DO NOTHING;
    ''',
    '''
        INSERT INTO operations.scheduler (schema,script,start_date,frequency) 
        VALUES ('public','accident_check_vins_checked_lifetime','2020-01-01 00:30:00','1 hour') 
        ON CONFLICT ON CONSTRAINT scheduler_pk 
        DO NOTHING;
    ''',
    '''
        INSERT INTO operations.scheduler (schema,script,start_date,frequency) 
        VALUES ('public','accident_check_vins_clean_lifetime','2020-01-01 00:30:00','1 hour') 
        ON CONFLICT ON CONSTRAINT scheduler_pk 
        DO NOTHING;
    ''',
    '''
        INSERT INTO operations.scheduler (schema,script,start_date,frequency) 
        VALUES ('public','accident_check_vins_reported_lifetime','2020-01-01 00:30:00','1 hour') 
        ON CONFLICT ON CONSTRAINT scheduler_pk 
        DO NOTHING;
    ''',
    '''
        INSERT INTO operations.scheduler (schema,script,start_date,frequency) 
        VALUES ('public','accident_check_vins_proportion_reported_lifetime','2020-01-01 00:30:00','1 hour') 
        ON CONFLICT ON CONSTRAINT scheduler_pk 
        DO NOTHING;
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
        SELECT 
            blocked_locks.pid AS blocked_pid,
            blocked_activity.usename AS blocked_user,
            blocking_locks.pid AS blocking_pid,
            blocking_activity.usename AS blocking_user,
            blocked_activity.query AS blocked_statement,
            blocking_activity.query AS current_statement_in_blocking_process
        FROM 
            pg_locks blocked_locks
        JOIN 
            pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
        JOIN 
            pg_locks blocking_locks ON blocking_locks.locktype = blocked_locks.locktype AND NOT blocking_locks.database IS DISTINCT FROM blocked_locks.database AND NOT blocking_locks.relation IS DISTINCT FROM blocked_locks.relation AND NOT blocking_locks.page IS DISTINCT FROM blocked_locks.page AND NOT blocking_locks.tuple IS DISTINCT FROM blocked_locks.tuple AND NOT blocking_locks.virtualxid IS DISTINCT FROM blocked_locks.virtualxid AND NOT blocking_locks.transactionid IS DISTINCT FROM blocked_locks.transactionid AND NOT blocking_locks.classid IS DISTINCT FROM blocked_locks.classid AND NOT blocking_locks.objid IS DISTINCT FROM blocked_locks.objid AND NOT blocking_locks.objsubid IS DISTINCT FROM blocked_locks.objsubid AND blocking_locks.pid <> blocked_locks.pid
        JOIN 
            pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
        WHERE NOT 
            blocked_locks.granted;
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
        create table if not exists public.accident_check_vins_checked_lifetime
        (
            master_business_id text primary key,
            vins integer
        );
    ''',
    '''
        create table if not exists public.accident_check_vins_clean_lifetime
        (
            master_business_id text primary key,
            vins integer
        );
    ''',
    '''
        create table if not exists public.accident_check_vins_reported_lifetime
        (
            master_business_id text primary key,
            vins integer
        );
    ''',
    '''
        create table if not exists public.accident_check_vins_proportion_reported_lifetime
        (
            master_business_id text primary key,
            vins integer
        );
    '''
]

with postgreshandler.get_dashboard_connection() as connection:
    for query in queries:
        print(query)
        if len(query) > 0:
            with connection.cursor() as cursor:
                cursor.execute(query)