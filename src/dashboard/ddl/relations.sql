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

CREATE TABLE IF NOT EXISTS zuora_credit_memo_posted (
    s3_id bigint REFERENCES s3(id) ON DELETE CASCADE,
    payload jsonb
);

CREATE UNIQUE INDEX IF NOT EXISTS zuora_credit_memo_posted_unq_idx ON zuora_credit_memo_posted(((payload->>'event_id')::uuid));

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

CREATE OR REPLACE VIEW v_zuora_invoice_item_created_keys AS
    SELECT DISTINCT(field) AS keys
    FROM
    (
        SELECT
	        jsonb_object_keys(payload) AS field
        FROM
             public.zuora_invoice_item_created
    ) a;

CREATE OR REPLACE VIEW v_zuora_credit_memo_posted_keys AS
    SELECT DISTINCT(field) AS keys
    FROM
    (
        SELECT
	        jsonb_object_keys(payload) AS field
        FROM
             public.zuora_credit_memo_posted
    ) a;

CREATE OR REPLACE VIEW v_zoura_invoice_item_created AS
    SELECT
        s3_id,
        (payload->>'event_id')::uuid as event_id,
        payload->>'event_name' as event_name,
        to_timestamp(payload->>'happened_at','YYYY-MM-DD HH24:MI:SS') as happened_at,
        to_timestamp(payload->>'Invoice.CreatedDate','YYYY-MM-DD HH24:MI:SS') as invoice_created_date,
        payload->>'Invoice.Id' as invoice_id,
        (payload->>'Account.AccountNumber') as account_account_number,
        to_timestamp(payload->>'Invoice.InvoiceDate','YYYY-MM-DD') as invoice_invoice_date,
        payload->>'InvoiceItem.AccountingCode' as invoice_item_accounting_code,
        case when payload->>'InvoiceItem.AppliedToInvoiceItemId' = '' then null else payload->>'InvoiceItem.AppliedToInvoiceItemId' end as invoice_item_applied_to_invoice_item_id,
        (payload->>'InvoiceItem.Balance')::numeric as invoice_item_balance,
        (payload->>'InvoiceItem.ChargeAmount')::numeric as invoice_item_charge_amount,
        to_timestamp(payload->>'InvoiceItem.ChargeDate','YYYY-MM-DD HH24:MI:SS') as invoice_item_charge_date,
        payload->>'InvoiceItem.ChargeName' as invoice_item_charge_name,
        payload->>'InvoiceItem.CreatedById' as invoice_item_created_by_id,
        to_timestamp(payload->>'InvoiceItem.CreatedDate','YYYY-MM-DD HH24:MI:SS') as invoice_item_created_date,
        payload->>'InvoiceItem.Id' as invoice_item_id,
        (payload->>'InvoiceItem.ProcessingType')::int4 as invoice_item_processing_type,
        (payload->>'InvoiceItem.Quantity')::int4 as invoice_item_quantity,
        case when payload->>'InvoiceItem.RevRecStartDate' = '' then null else to_timestamp(payload->>'InvoiceItem.RevRecStartDate','YYYY-MM-DD') end as invoice_item_rev_rec_start_date,
        to_timestamp(payload->>'InvoiceItem.ServiceEndDate','YYYY-MM-DD') as invoice_item_service_end_date,
        to_timestamp(payload->>'InvoiceItem.ServiceStartDate','YYYY-MM-DD') as service_start_date,
        payload->>'InvoiceItem.SKU' as invoice_item_sku,
        payload->>'InvoiceItem.SubscriptionId' as invoice_item_subscription_id,
        (payload->>'InvoiceItem.TaxAmount')::numeric as invoice_item_tax_amount,
        payload->>'InvoiceItem.TaxCode' as invoice_item_tax_code,
        (payload->>'InvoiceItem.TaxExemptAmount')::numeric as invoice_item_tax_exempt_amount,
        payload->>'InvoiceItem.TaxMode' as invoice_item_tax_mode,
        (payload->>'InvoiceItem.UnitPrice')::numeric as invoice_item_unit_price,
        case when payload->>'InvoiceItem.UOM' = '' then null else payload->>'InvoiceItem.UOM' end as invoice_item_uom,
        payload->>'InvoiceItem.UpdatedById' as invoice_item_updated_by_id,
        to_timestamp(payload->>'InvoiceItem.UpdatedDate','YYYY-MM-DD HH24:MI:SS') as invoice_item_updated_date,
        case when payload->>'Invoice.PostedDate' = '' then null else to_timestamp(payload->>'Invoice.PostedDate','YYYY-MM-DD HH24:MI:SS') end as invoice_posted_date,
        payload->>'ProductRatePlanCharge.ChargeType' as product_rate_plan_charge_charge_type,
        case when payload->>'ProductRatePlanCharge.UOM' = '' then null else payload->>'ProductRatePlanCharge.UOM' end as product_rate_plan_charge_uom,
        (payload->>'Subscription.CreatorAccountId') as subscription_creator_account_id
    FROM
        zuora_invoice_item_created;

create or replace view v_zuora_credit_memo_posted as
    select
        s3_id,
        (payload->>'event_id')::uuid as event_id,
        payload->>'event_name' as event_name,
        to_timestamp(payload->>'happened_at','YYYY-MM-DD HH24:MI:SS') as happened_at,
        (payload->>'CreditMemo.AppliedAmount')::numeric as credit_memo_applied_amount,
        (payload->>'CreditMemo.Balance')::numeric as credit_memo_balance,
        case when payload->>'CreditMemo.CancelledById' = '' then null else payload->>'CreditMemo.CancelledById' end as credit_memo_cancelled_by_id,
        case when payload->>'CreditMemo.CancelledOn' = '' then null else payload->>'CreditMemo.CancelledOn' end  as credit_memo_cancelled_on,
        case when payload->>'CreditMemo.Comments' = '' then null else payload->>'CreditMemo.Comments' end as credit_memo_comments,
        payload->>'CreditMemo.CreatedById' as credit_memo_created_by_id,
        to_timestamp(payload->>'CreditMemo.CreatedDate','YYYY-MM-DD HH24:MI:SS') as credit_memo_created_date,
        (payload->>'CreditMemo.DiscountAmount')::numeric as credit_memo_discount_amount,
        to_timestamp(payload->>'CreditMemo.ExchangeRateDate','YYYY-MM-DD') as credit_memo_exchange_rate_date,
        payload->>'CreditMemo.Id' as credit_memo_id,
        payload->>'CreditMemo.legacyCreditMemoNumber__c' as credit_memo_legacy_credit_memo_number,
        to_timestamp(payload->>'CreditMemo.MemoDate','YYYY-MM-DD') as credit_memo_memo_date,
        payload->>'CreditMemo.MemoNumber' as credit_memo_memo_number,
        payload->>'CreditMemo.PostedById' as credit_memo_posted_by_id,
        to_timestamp(payload->>'CreditMemo.PostedOn','YYYY-MM-DD HH24:MI:SS') as credit_memo_posted_on,
        payload->>'CreditMemo.ReasonCode' as credit_memo_reason_code,
        (payload->>'CreditMemo.RefundAmount')::numeric as credit_memo_refund_amount,
        payload->>'CreditMemo.Source' as credit_memo_source,
        case when payload->>'CreditMemo.SourceId' = '' then null else payload->>'CreditMemo.SourceId' end as credit_memo_source_id,
        payload->>'CreditMemo.Status' as credit_memo_status,
        case when payload->>'CreditMemo.TargetDate' = '' then null else to_timestamp(payload->>'CreditMemo.TargetDate','YYYY-MM-DD') end as credit_memo_target_date,
        (payload->>'CreditMemo.TaxAmount')::numeric as credit_memo_tax_amount,
        payload->>'CreditMemo.TaxMessage' as credit_memo_tax_message,
        case when payload->>'CreditMemo.TaxStatus' = '' then null else payload->>'CreditMemo.TaxStatus' end as credit_memo_tax_status,
        (payload->>'CreditMemo.TotalAmount')::numeric as credit_memo_total_amount,
        (payload->>'CreditMemo.TotalAmountWithoutTax')::numeric as credit_memo_total_amount_without_tax,
        (payload->>'CreditMemo.TotalTaxExemptAmount')::numeric as credit_memo_total_tax_exempt_amount,
        case when payload->>'CreditMemo.TransferredToAccounting' = '' then null else payload->>'CreditMemo.TransferredToAccounting' end as credit_memo_transferred_to_accounting,
        payload->>'CreditMemo.UpdatedById' as credit_memo_updated_by_id,
        case when payload->>'CreditMemo.UpdatedDate' = '' then null else to_timestamp(payload->>'CreditMemo.UpdatedDate','YYYY-MM-DD HH24:MI:SS') end as credit_memo_updated_date
    from
        zuora_credit_memo_posted;

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

INSERT INTO scheduler (script,start_date,frequency) VALUES ('zuora_credit_memo_posted','2020-05-23','1 day') ON CONFLICT ON CONSTRAINT scheduler_pk DO NOTHING;
INSERT INTO scheduler (script,start_date,frequency) VALUES ('zuora_invoice_item_created','2020-05-23','1 day') ON CONFLICT ON CONSTRAINT scheduler_pk DO NOTHING;