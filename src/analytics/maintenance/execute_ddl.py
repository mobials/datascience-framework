import sys
sys.path.insert(0,'../..')
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
        CREATE TABLE IF NOT EXISTS autoverify.mpm_leads
        (
            id uuid primary key,
            created_at timestamptz not null,
            updated_at timestamptz not null,
            payload jsonb not null
        );
        ALTER TABLE autoverify.mpm_leads 
        SET (autovacuum_vacuum_scale_factor = 0, autovacuum_vacuum_threshold = 10000);
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
            constraint avr_widget_impressions_ip_daily_pk primary key (master_business_id,date,impressions)
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
            constraint avr_widget_impressions_ip_weekly_pk primary key (master_business_id,date,impressions)
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
            constraint avr_widget_impressions_ip_monthly_pk primary key (master_business_id,date,impressions)
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
            constraint avr_unique_impressions_ip_quarterly_pk primary key (master_business_id,date,impressions)
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
            a.chargeamount
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
        CREATE OR REPLACE FUNCTION autoverify.lead_content(is_insurance integer, is_trade integer, is_credit_partial integer, is_credit_verified integer, is_credit_finance integer, is_ecom integer, is_testdrive integer)
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
                        is_testdrive = 1
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
                        is_testdrive = 1
                    then 
                        result := array_append(result,'testdrive');
                    end if;
            
                    if 
                        result is null
                    then 
                        raise exception 'Unhandled lead content. is_insurance = %; is_trade = %, is_credit_partial = %; is_credit_verified = %; is_credit_finance = %; is_ecom = %; is_testdrive = %',$1,$2,$3,$4,$5,$6,$7;
                    end if;
                
                return result;
            end;
            $function$;
    ''',
    '''
        create or replace view autoverify.v_mpm_lead_details as  
        select 
            id,
            (payload->>'is_insurance')::integer as is_insurance,
            (payload->>'is_trade')::integer as is_trade,
            (payload->>'is_credit_partial')::integer as is_credit_partial,
            (payload->>'is_credit_verified')::integer as is_credit_verified,
            (payload->>'is_credit_finance')::integer as is_credit_finance,
            (payload->>'is_ecom')::integer as is_ecom,
            (payload->>'is_spotlight')::integer as is_spotlight,
            (payload->>'is_credit_regional')::integer as is_credit_regional,
            (payload->>'is_test_drive')::integer as is_test_drive
         from 
            autoverify.mpm_leads;
    '''
]

with postgreshandler.get_analytics_connection() as connection:
    for query in queries:
        print(query)
        if len(query) > 0:
            with connection.cursor() as cursor:
                cursor.execute(query)
