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
CREATE UNIQUE INDEX IF NOT EXISTS tradesii_leads_lead_id_unq_idx ON tradesii_leads(((payload->'lead'->>'id')::uuid));
CREATE UNIQUE INDEX IF NOT EXISTS tradesii_leads_event_id_unq_idx ON tradesii_leads(((payload->>'event_id')::uuid));

CREATE TABLE IF NOT EXISTS credsii_leads (
    s3_id bigint REFERENCES s3(id) ON DELETE CASCADE,
    payload jsonb
);
CREATE UNIQUE INDEX IF NOT EXISTS credsii_leads_lead_id_unq_idx ON credsii_leads(((payload->'lead'->>'id')::uuid));
CREATE UNIQUE INDEX IF NOT EXISTS credsii_leads_event_id_unq_idx ON credsii_leads(((payload->>'event_id')::uuid));

CREATE TABLE IF NOT EXISTS insuresii_leads (
    s3_id bigint REFERENCES s3(id) ON DELETE CASCADE,
    payload jsonb
);

CREATE UNIQUE INDEX IF NOT EXISTS insuresii_leads_event_id_unq_idx ON insuresii_leads(((payload->>'event_id')::uuid));
CREATE UNIQUE INDEX IF NOT EXISTS insuresii_leads_lead_id_unq_idx ON insuresii_leads(((payload->'lead'->>'id')::uuid));

CREATE TABLE IF NOT EXISTS reservesii_reservations (
    s3_id bigint REFERENCES s3(id) ON DELETE CASCADE,
    payload jsonb
);

CREATE UNIQUE INDEX IF NOT EXISTS reservesii_reservations_event_id_unq_idx ON reservesii_reservations(((payload->>'event_id')::uuid));

CREATE TABLE IF NOT EXISTS marketplace_leads (
    s3_id bigint REFERENCES s3(id) ON DELETE CASCADE,
    payload jsonb
);

CREATE UNIQUE INDEX IF NOT EXISTS marketplace_leads_event_id_unq_idx ON marketplace_leads(((payload->>'event_id')::uuid));
CREATE UNIQUE INDEX IF NOT EXISTS marketplace_leads_lead_id_unq_idx ON marketplace_leads(((payload->'lead'->>'id')::uuid));



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

CREATE OR REPLACE VIEW v_credsii_leads AS
    SELECT
        payload->>'event_id' AS event_id,
        to_timestamp(payload->>'happened_at','YYYY-MM-DD HH24:MI:SS') AS happened_at,
        payload->>'master_business_id' AS master_business_id,
        payload->'lead'->>'id' AS id,
        to_timestamp(payload->'lead'->>'createdAt','YYYY-MM-DD HH24:MI:SS') AS created_at,
        payload->'lead'->'person'->'email'->>'email' AS email,
        payload->'lead'->'person'->>'firstName'AS first_name,
        payload->'lead'->'person'->>'lastName'AS last_name,
        payload->'lead'->'person'->'phoneNumber'->>'phone_number' AS phone_number,
        payload->'lead'->'address'->>'city' AS city,
        replace(payload->'lead'->'address'->>'postal_code',' ','') AS postal_code,
        payload->'lead'->'address'->>'province_id' AS province_id,
        payload->'lead'->'address'->>'address_line_1' AS address_line_1,
        payload->'lead'->'address'->>'address_line_2' AS address_line_2,
        payload->'lead'->>'leadStatus' AS lead_status,
        payload->'lead'->'billingKey' AS billing_key,
        payload->'lead'->'creditRating' AS credit_rating,
        payload->'lead'->'conversationId' AS conversation_id,
        payload->'lead'->'routeOneResult' AS route_one_result,
        payload->'lead'->'routeOneSentAt' AS route_one_sent_at,
        payload->'lead'->'dealertrackSentAt' AS dealer_track_sent_at,
        payload->'lead'->'financialFormStoredAt' AS financial_form_stored_at
    FROM
        credsii_leads;

CREATE OR REPLACE VIEW v_tradesii_leads AS
    SELECT
        payload->>'event_id' AS event_id,
        to_timestamp(payload->>'happened_at','YYYY-MM-DD HH24:MI:SS') AS happened_at,
        payload->>'mbid' AS master_business_id,
        payload->'lead'->>'id' AS id,
        to_timestamp(payload->'lead'->'createdAt'->>'date','YYYY-MM-DD HH24:MI:SS') AS created_at,
        payload->'lead'->'customer'->>'email_address' AS email_address,
        payload->'lead'->'customer'->>'name' AS name,
        payload->'lead'->'customer'->>'phone_number' AS phone_number,
        replace(payload->'lead'->'customer'->>'postal_code',' ','') AS postal_code,
        payload->'lead'->'customer'->>'ip' AS ip,
        payload->'lead'->'customer'->>'referrer_url' AS referrer_url,
        payload->'lead'->>'businessProfileId' AS business_profile_id,
        payload->'lead'->>'reportId' AS report_id,
        payload->'lead'->'vehicle'->>'vin' AS vin,
        (payload->'lead'->'vehicle'->>'year')::int AS year,
        payload->'lead'->'vehicle'->>'make' AS make,
        payload->'lead'->'vehicle'->>'model' AS model,
        payload->'lead'->'vehicle'->>'trim' AS trim,
        payload->'lead'->'vehicle'->>'style' as style,
        (payload->'lead'->'vehicle'->>'mileage')::double precision AS mileage,
        (payload->'lead'->'vehicle'->>'trade_in_low')::double precision/100.0 AS trade_in_low,
        (payload->'lead'->'vehicle'->>'trade_in_high')::double precision/100.0 AS trade_in_high
    FROM
        tradesii_leads;

CREATE OR REPLACE VIEW v_insuresii_leads AS
    SELECT
        payload->>'event_id' AS event_id,
        to_timestamp(payload->>'happened_at','YYYY-MM-DD HH24:MI:SS') AS happened_at,
        payload->>'master_business_id' AS master_business_id,
        payload->'lead'->>'id' AS id,
        payload->'lead'->>'quoteId' AS quote_id,
        payload->'lead'->'vehicle'->>'vin' AS vin,
        (payload->'lead'->'vehicle'->>'year')::int AS year,
        payload->'lead'->'vehicle'->>'make' AS make,
        payload->'lead'->'vehicle'->>'model' AS model,
        payload->'lead'->'vehicle'->>'trim' AS trim,
        payload->'lead'->'vehicle'->>'style' as style,
        (payload->'lead'->'vehicle'->>'mileage')::double precision AS mileage,
        to_timestamp(payload->'lead'->>'createdAt','YYYY-MM-DD HH24:MI:SS') AS created_at,
        payload->'lead'->'customer'->>'email' AS email,
        payload->'lead'->'customer'->>'firstName' AS first_name,
        payload->'lead'->'customer'->>'lastName' AS last_name,
        payload->'lead'->'customer'->>'phone' AS phone,
        payload->'lead'->'customer'->>'mobilePhone' AS mobile_phone,
        replace(payload->'lead'->'customer'->'address'->>'postal_code',' ','') AS postal_code,
        payload->'lead'->'customer'->'address'->>'city' AS city,
        payload->'lead'->'customer'->'address'->>'province_id' AS province_id,
        payload->'lead'->'customer'->>'country_code' AS country_code,
        payload->'lead'->'customer'->>'address_line_1' AS address_line_1,
        payload->'lead'->'customer'->>'address_line_2' AS address_line_2,
        payload->'lead'->'customer'->'payload'->>'gender' AS gender,
        payload->'lead'->'customer'->'payload'->>'kmToWork' AS km_to_work,
        payload->'lead'->'customer'->'payload'->>'dateOfBirth' AS date_of_birth,
        payload->'lead'->'customer'->'payload'->>'winterTires' AS winter_tires,
        payload->'lead'->'customer'->'payload'->>'maritalStatus' AS marital_status,
        payload->'lead'->'customer'->'payload'->>'currentLicense' AS current_license,
        payload->'lead'->>'profileId' as profile_id,
        to_timestamp(payload->'lead'->>'updatedAt','YYYY-MM-DD HH24:MI:SS') as updated_at,
        payload->'lead'->>'leadStatus' as lead_status,
        payload->'lead'->>'referenceId' as reference_id,
        payload->'lead'->>'referrerUrl' as referrer_url,
        payload->'lead'->'quotePayload' as quote_payload
    FROM
        insuresii_leads;

CREATE OR REPLACE VIEW v_marketplace_leads AS
    SELECT
        payload->>'event_id' AS event_id,
        to_timestamp(payload->>'happened_at','YYYY-MM-DD HH24:MI:SS') AS happened_at,
        payload->>'mbid' AS master_business_id,
        payload->'lead'->>'id' AS id,
        payload->'lead'->'contact'->>'firstName' as first_name,
        payload->'lead'->'contact'->>'lastName' as last_name,
        payload->'lead'->'contact'->>'email' as email,
        payload->'lead'->'contact'->>'mobilePhone' as mobile_phone,
        payload->'lead'->'contact'->>'language' as language,
        payload->'lead'->'contact'->'address'->>'address_line_1' as address_line_1,
        payload->'lead'->'contact'->'address'->>'address_line_2' as address_line_2,
        payload->'lead'->'contact'->'address'->>'city' as city,
        payload->'lead'->'contact'->'address'->>'postal_code' as postal_code,
        payload->'lead'->'contact'->'address'->>'province_id' as province_id,
        payload->'lead'->'contact'->'address'->>'country_code' as country_code,
        payload->'lead'->>'shopperId' as shopper_id,
        payload->'lead'->>'creditRating' as credit_rating,
        payload->'lead'->'tradeLeadInfo'->'vehicle'->>'vin' as vin,
        (payload->'lead'->'tradeLeadInfo'->'vehicle'->>'year')::int as year,
        payload->'lead'->'tradeLeadInfo'->'vehicle'->>'make' as make,
        payload->'lead'->'tradeLeadInfo'->'vehicle'->>'model' as model,
        payload->'lead'->'tradeLeadInfo'->'vehicle'->>'trim' as trim,
        payload->'lead'->'tradeLeadInfo'->'vehicle'->>'style' as style,
        (payload->'lead'->'tradeLeadInfo'->'vehicle'->>'mileage')::double precision as mileage,
        payload->'lead'->'tradeLeadInfo'->'vehicle'->>'status' as status,
        (payload->'lead'->'tradeLeadInfo'->>'high_value')::double precision/100.0 as high_value,
        (payload->'lead'->'tradeLeadInfo'->>'low_value')::double precision/100.0 as low_value,
        payload->'lead'->'tradeLeadInfo'->>'selling_vehicle_only' as selling_vehicle_only,
        (payload->'lead'->'tradeLeadInfo'->>'tax_rate')::double precision as tax_rate,
        (payload->'lead'->'purchaseInformation'->>'monthlyBudget')::double precision/100.0 as monthly_budget,
        (payload->'lead'->'purchaseInformation'->>'purchasePeriod')::int purchase_period,
        payload->'lead'->'purchaseInformation'->>'desiredVehicleType' as desired_vehicle_type,
        payload->'lead'->'purchaseInformation'->>'desiredVehicleCondition' as desired_vehicle_condition,
        payload->'lead'->'purchaseInformation'->>'considerDeferredPayments' as consider_deferred_payments,
        payload->'lead'->>'creditRatingProvided' as credit_rating_provided,
        payload->'lead'->>'regionalCreditRating' as regional_credit_rating,
        payload->'lead'->>'shopperReceivedTradeReport' as shopper_received_trade_report,
        payload->'lead'->>'shopperReceivedCreditReport' as shopper_received_credit_report,
        payload->'lead'->>'financeApplicationWasSubmitted' as finance_application_was_submitted
    FROM
        marketplace_leads;

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
        to_timestamp(payload->>'InvoiceItem.ServiceStartDate','YYYY-MM-DD') as invoice_item_service_start_date,
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
        case when payload->>'CreditMemo.legacyCreditMemoNumber__c' = '' then null else payload->>'CreditMemo.legacyCreditMemoNumber__c' end as credit_memo_legacy_credit_memo_number,
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
        case when payload->>'CreditMemo.UpdatedDate' = '' then null else to_timestamp(payload->>'CreditMemo.UpdatedDate','YYYY-MM-DD HH24:MI:SS') end as credit_memo_updated_date,
        case when payload->>'Invoice.Id' = '' then null else payload->>'Invoice.Id' end as invoice_id
    from
        zuora_credit_memo_posted;


CREATE TABLE IF NOT EXISTS sda_audit_log
(
    s3_id bigint REFERENCES s3(id) ON DELETE CASCADE,
    id uuid PRIMARY KEY,
    user_id uuid,
    happened_at timestamptz,
    end_point text,
    request_method text,
    response_code int4,
    request_payload jsonb,
    response_payload jsonb
);