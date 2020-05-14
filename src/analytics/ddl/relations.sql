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

CREATE TABLE IF NOT EXISTS event_names_key_dates(
    s3_id bigint REFERENCES s3(id) ON DELETE CASCADE,
    event_name TEXT NOT NULL,
    first_date TIMESTAMPTZ NOT NULL,
    last_date TIMESTAMPTZ NOT NULL,
    CONSTRAINT event_name_key_dates_pk PRIMARY KEY (event_name)
);

CREATE INDEX IF NOT EXISTS avr_widget_impressions_s3_id_idx ON avr_widget_impressions (s3_id);
CREATE INDEX IF NOT EXISTS avr_widget_impressions_date_idx ON avr_widget_impressions (date);

CREATE TABLE IF NOT EXISTS authenticom_sales_data (
    s3_id bigint REFERENCES s3(id) ON DELETE CASCADE,
    payload jsonb
);

ALTER TABLE avr_widget_impressions ADD COLUMN IF NOT EXISTS referrer_url TEXT;