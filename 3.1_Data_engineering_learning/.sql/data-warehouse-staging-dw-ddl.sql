CREATE SCHEMA IF NOT EXISTS staging;

CREATE SCHEMA IF NOT EXISTS dw;

DROP TABLE IF EXISTS staging.stg_purchase_invoices CASCADE;

DROP TABLE IF EXISTS staging.stg_vendors CASCADE;

CREATE TABLE IF NOT EXISTS staging.stg_vendors (
    vendor_name varchar,
    vendor_site varchar,
    created_date timestamp DEFAULT now()
);

CREATE TABLE IF NOT EXISTS staging.stg_purchase_invoices (
    po_number varchar,
    po_line_number smallint,
    item_name varchar,
    item_payment_status varchar,
    po_agent_full_name varchar,
    invoice_number varchar,
    invoice_status varchar,
    invoice_created_date timestamp,
    invoice_received_date timestamp,
    invoice_cancelled_date timestamp,
    invoice_paid_date timestamp,
    invoice_currency_code varchar,
    payment_currency_code varchar,
    amount_invoiced int,
    amount_paid int,
    invoice_line_description varchar,
    ap_agent_full_name varchar,
    liability_flag boolean,
    vendor_name varchar,
    vendor_site_name varchar,
    dim_purchase_line_id int,
    dim_invoice_header_id int,
    dim_invoice_line_id int,
    dim_vendor_id int,
    dim_invoice_created_date_id int,
    dim_invoice_received_date_id int,
    dim_invoice_cancelled_date_id int,
    dim_invoice_paid_date_id int,
    created_date timestamp DEFAULT now()
);

DROP TABLE IF EXISTS dw.dim_dates CASCADE;

DROP TABLE IF EXISTS dw.dim_purchase_lines CASCADE;

DROP TABLE IF EXISTS dw.dim_vendors CASCADE;

DROP TABLE IF EXISTS dw.dim_invoice_headers CASCADE;

DROP TABLE IF EXISTS dw.dim_invoice_lines CASCADE;

DROP TABLE IF EXISTS dw.fact_purchase_invoices CASCADE;

CREATE TABLE IF NOT EXISTS dw.dim_dates (
    date_id serial PRIMARY KEY,
    full_date date unique,
    "date" smallint,
    "month" smallint,
    "month_name" varchar,
    "year" smallint,
    "quarter" smallint,
    day_of_week varchar
);

CREATE TABLE IF NOT EXISTS dw.dim_purchase_lines (
    purchase_line_id serial PRIMARY KEY,
    po_number varchar,
    po_line_number smallint,
    item_name varchar,
    item_payment_status varchar,
    agent_full_name varchar,
    UNIQUE (po_number, po_line_number)
);

CREATE TABLE IF NOT EXISTS dw.dim_vendors (
    vendor_id serial PRIMARY KEY,
    vendor_name varchar,
    vendor_site varchar,
    UNIQUE (vendor_name, vendor_site)
);

CREATE TABLE IF NOT EXISTS dw.dim_invoice_headers (
    invoice_header_id serial PRIMARY KEY,
    invoice_number varchar,
    status varchar,
    invoice_currency_code varchar,
    payment_currency_code varchar,
    agent_full_name varchar,
    liability_flag boolean,
    UNIQUE (invoice_number)
);

CREATE TABLE IF NOT EXISTS dw.dim_invoice_lines (
    invoice_line_id serial PRIMARY KEY,
    invoice_line_description varchar
);

CREATE TABLE IF NOT EXISTS dw.fact_purchase_invoices (
    purchase_invoice_id serial PRIMARY KEY,
    amount_invoiced int,
    amount_paid int,
    received_date_id int REFERENCES dw.dim_dates,
    created_date_id int REFERENCES dw.dim_dates,
    cancelled_date_id int REFERENCES dw.dim_dates,
    paid_date_id int REFERENCES dw.dim_dates,
    purchase_line_id int REFERENCES dw.dim_purchase_lines,
    vendor_id int REFERENCES dw.dim_vendors,
    invoice_header_id int REFERENCES dw.dim_invoice_headers,
    invoice_line_id int REFERENCES dw.dim_invoice_lines,
    UNIQUE (purchase_line_id)
);