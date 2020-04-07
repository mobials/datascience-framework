CREATE TABLE IF NOT EXISTS s3 (
	file text NOT NULL,
	script text not null,
    date timestamptz not null,
	CONSTRAINT s3_pk PRIMARY KEY (file,script)
);

CREATE TABLE cdc (
	status_date timestamptz NOT NULL,
	vin text NOT NULL,
	taxonomy_vin text not null,
	"year" int4 NULL,
	make text NULL,
	model text NULL,
	trim text NULL,
	body_type text NULL,
	vehicle_type text NULL,
	trim_orig text NULL,
	miles float8 NULL,
	price float8 NULL,
	CONSTRAINT cdc_pk PRIMARY KEY (vin)
);