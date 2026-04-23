\set ON_ERROR_STOP on

BEGIN;

TRUNCATE TABLE retail.transactions, retail.products, retail.households;
TRUNCATE TABLE retail.stg_households;
TRUNCATE TABLE retail.stg_products;
TRUNCATE TABLE retail.stg_transactions;

\copy retail.stg_households (c1, c2, c3, c4, c5, c6, c7, c8, c9) FROM 'C:/Users/eliri/source/repos/CS5165_Final_Project/8451_The_Complete_Journey_2_Sample-2/400_households.csv' WITH (FORMAT csv, HEADER true)
\copy retail.stg_products (c1, c2, c3, c4, c5) FROM 'C:/Users/eliri/source/repos/CS5165_Final_Project/8451_The_Complete_Journey_2_Sample-2/400_products.csv' WITH (FORMAT csv, HEADER true)
\copy retail.stg_transactions (c1, c2, c3, c4, c5, c6, c7, c8, c9) FROM 'C:/Users/eliri/source/repos/CS5165_Final_Project/8451_The_Complete_Journey_2_Sample-2/400_transactions.csv' WITH (FORMAT csv, HEADER true)

INSERT INTO retail.households (
    hshd_num,
    loyalty_flag,
    age_range,
    marital,
    income_range,
    homeowner,
    hshd_composition,
    hh_size,
    children
)
SELECT
    trim(c1),
    NULLIF(trim(c2), ''),
    CASE WHEN lower(trim(c3)) = 'null' THEN NULL ELSE NULLIF(trim(c3), '') END,
    CASE WHEN lower(trim(c4)) = 'null' THEN NULL ELSE NULLIF(trim(c4), '') END,
    CASE WHEN lower(trim(c5)) = 'null' THEN NULL ELSE NULLIF(trim(c5), '') END,
    CASE WHEN lower(trim(c6)) = 'null' THEN NULL ELSE NULLIF(trim(c6), '') END,
    CASE WHEN lower(trim(c7)) = 'null' THEN NULL ELSE NULLIF(trim(c7), '') END,
    CASE WHEN lower(trim(c8)) = 'null' THEN NULL ELSE NULLIF(trim(c8), '') END,
    CASE WHEN lower(trim(c9)) = 'null' THEN NULL ELSE NULLIF(trim(c9), '') END
FROM retail.stg_households;

INSERT INTO retail.products (
    product_num,
    department,
    commodity,
    brand_ty,
    natural_organic_flag
)
SELECT
    trim(c1),
    NULLIF(trim(c2), ''),
    NULLIF(trim(c3), ''),
    NULLIF(trim(c4), ''),
    NULLIF(trim(c5), '')
FROM retail.stg_products;

INSERT INTO retail.transactions (
    basket_num,
    hshd_num,
    purchase_date,
    product_num,
    spend,
    units,
    store_r,
    week_num,
    year
)
SELECT
    trim(c1),
    trim(c2),
    to_date(trim(c3), 'DD-MON-YY'),
    trim(c4),
    NULLIF(trim(c5), '')::numeric(12,2),
    NULLIF(trim(c6), '')::numeric(12,3),
    NULLIF(trim(c7), ''),
    NULLIF(trim(c8), '')::smallint,
    NULLIF(trim(c9), '')::smallint
FROM retail.stg_transactions;

COMMIT;

CREATE INDEX IF NOT EXISTS idx_transactions_hshd_num ON retail.transactions(hshd_num);
CREATE INDEX IF NOT EXISTS idx_transactions_product_num ON retail.transactions(product_num);

ANALYZE retail.households;
ANALYZE retail.products;
ANALYZE retail.transactions;
