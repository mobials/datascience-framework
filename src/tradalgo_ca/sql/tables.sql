CREATE TABLE IF NOT EXISTS s3 (
    id bigserial,
	file text NOT NULL,
	script text not null,
    date timestamptz not null,
	CONSTRAINT s3_unq_idx UNIQUE (file,script),
	CONSTRAINT s3_pk PRIMARY KEY (id)
);

CREATE TABLE cdc (
    s3_id bigint REFERENCES s3(id) ON DELETE CASCADE,
	status_date timestamptz NOT NULL,
	vin text NOT NULL,
	zip text NOT NULL,
	"year" int4 NULL,
	make text NULL,
	model text NULL,
	trim text NULL,
	body_type text NULL,
	vehicle_type text NULL,
	city text NULL,
	state text NULL,
	trim_orig text NULL,
	miles float8 NULL,
	price float8 NULL,
	CONSTRAINT cdc_pk PRIMARY KEY (status_date,vin,zip)
);