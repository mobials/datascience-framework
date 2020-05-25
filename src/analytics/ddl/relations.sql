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

