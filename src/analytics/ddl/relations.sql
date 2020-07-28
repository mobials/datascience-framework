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

CREATE TABLE IF NOT EXISTS integration_widget_impressions
(
    s3_id bigint REFERENCES s3(id) ON DELETE CASCADE,
    date timestamptz,
    master_business_id uuid,
    widget_id uuid,
    ip_address text,
    product text,
    device_type text,
    referrer_url text,
    CONSTRAINT integration_widget_impressions_pk PRIMARY KEY (ip_address,date,product,widget_id,master_business_id,device_type)
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
CREATE INDEX IF NOT EXISTS tradesii_leads_s3_id_idx ON public.tradesii_leads (s3_id);

CREATE TABLE IF NOT EXISTS credsii_leads (
    s3_id bigint REFERENCES s3(id) ON DELETE CASCADE,
    payload jsonb
);
CREATE UNIQUE INDEX IF NOT EXISTS credsii_leads_lead_id_unq_idx ON credsii_leads(((payload->'lead'->>'id')::uuid));

CREATE TABLE IF NOT EXISTS insuresii_leads (
    s3_id bigint REFERENCES s3(id) ON DELETE CASCADE,
    payload jsonb
);

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

CREATE OR REPLACE VIEW v_credsii_leads AS
    SELECT
        (payload->>'event_id')::uuid AS event_id,
        to_timestamp(payload->>'happened_at','YYYY-MM-DD HH24:MI:SS') AS happened_at,
        (payload->>'master_business_id')::uuid AS master_business_id,
        (payload->'lead'->>'id')::uuid AS id,
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
        payload->'lead'->>'routeOneSentAt' AS route_one_sent_at,
        case when payload->'lead'->>'dealertrackSentAt' != '' then to_timestamp(payload->'lead'->'dealertrackSentAt'->>'date','YYYY-MM-DD HH24:MI:SS') else null end AS dealer_track_sent_at,
        case when payload->'lead'->>'financialFormStoredAt' != '' then to_timestamp(payload->'lead'->'financialFormStoredAt'->>'date','YYYY-MM-DD HH24:MI:SS') else null end as financial_form_stored_at
    FROM
        credsii_leads;

CREATE OR REPLACE VIEW v_tradesii_leads AS
    SELECT
        (payload->>'event_id')::uuid AS event_id,
        to_timestamp(payload->>'happened_at','YYYY-MM-DD HH24:MI:SS') AS happened_at,
        case when payload->>'mbid' != '' then (payload->>'mbid')::uuid else null end AS master_business_id,
        (payload->'lead'->>'id')::uuid AS id,
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
        (payload->>'event_id')::uuid AS event_id,
        to_timestamp(payload->>'happened_at','YYYY-MM-DD HH24:MI:SS') AS happened_at,
        (payload->>'master_business_id')::uuid AS master_business_id,
        (payload->'lead'->>'id')::uuid AS id,
        (payload->'lead'->>'quoteId')::uuid AS quote_id,
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
        (payload->'lead'->>'profileId')::uuid as profile_id,
        to_timestamp(payload->'lead'->>'updatedAt','YYYY-MM-DD HH24:MI:SS') as updated_at,
        payload->'lead'->>'leadStatus' as lead_status,
        (payload->'lead'->>'referenceId')::uuid as reference_id,
        payload->'lead'->>'referrerUrl' as referrer_url,
        payload->'lead'->'quotePayload' as quote_payload
    FROM
        insuresii_leads;

CREATE OR REPLACE VIEW v_marketplace_leads AS
	SELECT
        (payload->>'event_id')::uuid AS event_id,
        to_timestamp(payload->>'happened_at','YYYY-MM-DD HH24:MI:SSTZH:TZM') AS happened_at,
        (payload->>'mbid')::uuid AS master_business_id,
        to_timestamp(payload->>'createdAt','YYYY-MM-DD HH24:MI:SSTZH:TZM') as created_at,
        (payload->'lead'->>'id')::uuid AS id,
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
        (payload->'lead'->'contact'->'isEmailVerified')::boolean as is_email_verified,
        (payload->'lead'->'contact'->'isMobilePhoneVerified')::boolean as is_mobile_phone_verified,
        (payload->'lead'->>'shopperId')::uuid as shopper_id,
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
        (payload->'lead'->'tradeLeadInfo'->>'selling_vehicle_only')::boolean as selling_vehicle_only,
        (payload->'lead'->'tradeLeadInfo'->>'tax_rate')::double precision as tax_rate,
        (payload->'lead'->'purchaseInformation'->>'monthlyBudget')::double precision/100.0 as monthly_budget,
        (payload->'lead'->'purchaseInformation'->>'purchasePeriod')::int purchase_period,
        payload->'lead'->'purchaseInformation'->>'desiredVehicleType' as desired_vehicle_type,
        payload->'lead'->'purchaseInformation'->>'desiredVehicleCondition' as desired_vehicle_condition,
        (payload->'lead'->'purchaseInformation'->>'considerDeferredPayments')::boolean as consider_deferred_payments,
        (payload->'lead'->>'creditRatingProvided')::boolean as credit_rating_provided,
        payload->'lead'->>'regionalCreditRating' as regional_credit_rating,
        (payload->'lead'->>'shopperReceivedTradeReport')::boolean as shopper_received_trade_report,
        (payload->'lead'->>'shopperReceivedCreditReport')::boolean as shopper_received_credit_report,
        (payload->'lead'->>'financeApplicationWasSubmitted')::boolean as finance_application_was_submitted
    FROM
        marketplace_leads;

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

CREATE TABLE IF NOT EXISTS zuora_account
(
    id text primary key,
    updateddate timestamptz not null,
    payload jsonb
);

CREATE TABLE IF NOT EXISTS zuora_invoice
(
    id text primary key,
    updateddate timestamptz not null,
    payload jsonb
);

CREATE TABLE IF NOT EXISTS zuora_invoiceitem
(
    id text primary key,
    updateddate timestamptz not null,
    payload jsonb
);

CREATE TABLE IF NOT EXISTS zuora_creditmemo
(
    id text primary key,
    updateddate timestamptz not null,
    payload jsonb
);

CREATE TABLE IF NOT EXISTS zuora_order
(
    id text primary key,
    updateddate timestamptz not null,
    payload jsonb
);


CREATE TABLE IF NOT EXISTS zuora_subscription
(
    id text primary key,
    updateddate timestamptz not null,
    payload jsonb
);

create or replace view v_zuora_account as
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
		zuora_account;

create or replace view v_zuora_invoiceitem as
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
		case when payload->>'InvoiceItem.AppliedToInvoiceItemId' = '' then null else payload->>'InvoiceItem.AppliedToInvoiceItemId' end as appliedttoinvoiceitemid
	from
		zuora_invoiceitem;

create or replace view v_zuora_invoice as
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
		zuora_invoice;

create or replace view v_zuora_subscription as
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
		zuora_subscription;

create or replace view v_zuora_creditmemo as
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
        zuora_creditmemo;