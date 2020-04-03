CREATE TABLE IF NOT EXISTS s3 (
    id bigserial,
	file text NOT NULL,
	script text not null,
    date timestamptz not null,
	CONSTRAINT s3_unq_idx UNIQUE (file,script),
	CONSTRAINT s3_pk PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS zuora_invoice_item_created (
    s3_id bigint REFERENCES s3(id) ON DELETE CASCADE,
    event_id uuid,
    happened_at timestamptz,
    account_account_number uuid,
    invoice_id text,
    invoice_created_date timestamptz,
    product_rate_plan_charge_charge_type text,
    product_rate_plan_charge_uom text,
    invoice_item_id text,
    invoice_item_charge_amount numeric,
    invoice_item_created_date timestamptz,
    invoice_item_uom text,
    product_plan_charge_rate_charge_type text,
    subscription_creator_account_id text,
    CONSTRAINT zuora_invoice_item_created_event_id_unq_idx UNIQUE (event_id)
);

CREATE TABLE IF NOT EXISTS zuora_subscription_updated (
    s3_id bigint REFERENCES s3(id) ON DELETE CASCADE,
    event_id uuid,
    happened_at timestamptz,
    account_account_number uuid,
    account_status text,
    product_id text,
    product_name text,
    product_sku text,
    product_rate_plan_id text,
    product_rate_plan_name text,
    rate_plan_id text,
    rate_plan_name text,
    subscription_creator_account_id text,
    subscription_creator_invoice_owner_id text,
    subscription_invoice_owner_id text,
    subscription_is_invoice_separate boolean,
    subscription_original_id text,
    subscription_previous_subscription_id text,
    subscription_id text,
    subscription_status text,
    CONSTRAINT zuora_subscription_updated_event_id_unq_idx UNIQUE (event_id)
)