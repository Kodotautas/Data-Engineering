CREATE SCHEMA IF NOT EXISTS finance_datamart;

CREATE OR REPLACE VIEW finance_datamart.dim_dates_v AS
SELECT
    *
FROM
    dw.dim_dates
WHERE
    year BETWEEN 2021 AND 2022;

CREATE OR REPLACE VIEW finance_datamart.dim_vendors_v AS
SELECT
    *
FROM
    dw.dim_vendors;

CREATE OR REPLACE VIEW finance_datamart.dim_purchase_lines_v AS
SELECT
    *
FROM
    dw.dim_purchase_lines;

CREATE OR REPLACE VIEW finance_datamart.dim_invoice_headers_v AS
SELECT
    *
FROM
    dw.dim_invoice_headers;

CREATE OR REPLACE VIEW finance_datamart.dim_invoice_lines_v AS
SELECT
    *
FROM
    dw.dim_invoice_lines;

CREATE OR REPLACE VIEW finance_datamart.fact_purchase_invoices_v AS
SELECT
    *
FROM
    dw.fact_purchase_invoices;

DO
$$BEGIN
IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'finance') THEN
    EXECUTE 'REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA finance_datamart FROM finance';
	EXECUTE 'REVOKE USAGE ON SCHEMA finance_datamart FROM finance';
	EXECUTE 'REVOKE CONNECT ON DATABASE postgres FROM finance';
END IF;
END$$;

DROP ROLE IF EXISTS finance;

CREATE USER finance WITH PASSWORD 'finance';

GRANT CONNECT ON DATABASE postgres TO finance;

GRANT USAGE ON SCHEMA finance_datamart TO finance;

GRANT SELECT ON ALL TABLES IN SCHEMA finance_datamart TO finance;