CREATE TABLE public.holidays (
	id serial4 NOT NULL,
	"uuid" uuid NOT NULL,
	"name" varchar(255) NOT NULL,
	"date" date NOT NULL,
	observed date NOT NULL,
	public bool NOT NULL,
	country bpchar(2) NOT NULL,
	weekday_date_name varchar(50) NULL,
	weekday_date_numeric bpchar(1) NULL,
	weekday_observed_name varchar(50) NULL,
	weekday_observed_numeric bpchar(1) NULL,
	CONSTRAINT holidays_pkey PRIMARY KEY (id),
	CONSTRAINT holidays_uuid_key UNIQUE (uuid)
);
