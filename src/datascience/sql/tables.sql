CREATE TABLE IF NOT EXISTS s3 (
	file text NOT NULL,
	script text not null,
    date timestamptz not null,
	CONSTRAINT s3_pk PRIMARY KEY (file,script)
);

CREATE TABLE IF NOT EXISTS avr_widget_impressions(
    event_id uuid NOT NULL,
    happened_at timestamptz,
    master_business_id uuid,
    integration_settings_id uuid,
    ip_address text,
    product text,
    device_type text,
    CONSTRAINT  avr_impressions_pk PRIMARY KEY  (event_id)
);

