CREATE OR REFRESH STREAMING TABLE silver_customer (
    CONSTRAINT customer_id_must_exist
        EXPECT (KUNNR IS NOT NULL)
        ON VIOLATION DROP ROW,

    CONSTRAINT customer_id_valid_format
        EXPECT (KUNNR RLIKE '^CUST[0-9]{7}$')
        ON VIOLATION DROP ROW,

    CONSTRAINT customer_name_not_blank
        EXPECT (NAME1 IS NOT NULL),

    CONSTRAINT country_code_valid
        EXPECT (LAND1 RLIKE '^[A-Z]{2}$'),

    CONSTRAINT credit_limit_in_range
        EXPECT (CREDIT_LIMIT >= 0 AND CREDIT_LIMIT <= 1000000000)
)
COMMENT 'Cleaned customer master — types corrected, junk removed'
AS
SELECT
    TRIM(KUNNR)                                                          AS KUNNR,
    CASE WHEN TRIM(NAME1)  IN ('N/A','null','NULL','None','','#N/A')
         THEN NULL ELSE TRIM(NAME1)  END                                 AS NAME1,
    UPPER(TRIM(LAND1))                                                   AS LAND1,
    CASE WHEN TRIM(BRSCH)  IN ('N/A','null','NULL','None','','#N/A')
         THEN NULL ELSE TRIM(BRSCH)  END                                 AS BRSCH,
    TRIM(KTOKD)                                                          AS KTOKD,
    CAST(CREDIT_LIMIT AS DECIMAL(15,2))                                  AS CREDIT_LIMIT,
    TO_DATE(ERDAT)                                                       AS ERDAT,
    TO_DATE(AEDAT)                                                       AS AEDAT
FROM STREAM(LIVE.bronze_customer);


CREATE OR REFRESH STREAMING TABLE silver_material (
    CONSTRAINT material_id_must_exist
        EXPECT (MATNR IS NOT NULL)
        ON VIOLATION DROP ROW,

    CONSTRAINT material_id_valid_format
        EXPECT (MATNR RLIKE '^MAT[0-9]{8}$')
        ON VIOLATION DROP ROW,

    CONSTRAINT description_not_blank
        EXPECT (MAKTX IS NOT NULL),

    CONSTRAINT material_type_valid
        EXPECT (MTART IN ('FERT','HALB','ROH','HIBE','ERSA','VERP','DIEN')),

    CONSTRAINT price_not_negative
        EXPECT (STANDARD_PRICE >= 0)
)
COMMENT 'Cleaned material master — types corrected, junk removed'
AS
SELECT
    TRIM(MATNR)                                                          AS MATNR,
    CASE WHEN TRIM(MAKTX)  IN ('N/A','null','NULL','None','','#N/A')
         THEN NULL ELSE TRIM(MAKTX)  END                                 AS MAKTX,
    TRIM(MTART)                                                          AS MTART,
    TRIM(MATKL)                                                          AS MATKL,
    UPPER(TRIM(MEINS))                                                   AS MEINS,
    CAST(STANDARD_PRICE AS DECIMAL(13,2))                                AS STANDARD_PRICE,
    CAST(BRGEW          AS DECIMAL(13,3))                                AS BRGEW,
    TO_DATE(ERDAT)                                                       AS ERDAT,
    TO_DATE(AEDAT)                                                       AS AEDAT
FROM STREAM(LIVE.bronze_material);


CREATE OR REFRESH STREAMING TABLE silver_sales_order (
    CONSTRAINT order_key_must_exist
        EXPECT (VBELN IS NOT NULL AND POSNR IS NOT NULL)
        ON VIOLATION DROP ROW,

    CONSTRAINT customer_must_exist
        EXPECT (KUNNR IS NOT NULL)
        ON VIOLATION DROP ROW,

    CONSTRAINT material_must_exist
        EXPECT (MATNR IS NOT NULL)
        ON VIOLATION DROP ROW,

    CONSTRAINT amount_must_exist
        EXPECT (NETWR IS NOT NULL)
        ON VIOLATION DROP ROW,

    CONSTRAINT amount_in_valid_range
        EXPECT (NETWR >= 0 AND NETWR <= 10000000)
        ON VIOLATION DROP ROW,

    CONSTRAINT quantity_must_be_positive
        EXPECT (KWMENG > 0)
        ON VIOLATION DROP ROW,

    CONSTRAINT currency_is_valid
        EXPECT (WAERK IN ('SEK','EUR','USD','GBP','INR','JPY','AUD','AED','BRL','CAD','CHF','HKD','SGD')),

    CONSTRAINT order_date_in_range
        EXPECT (ERDAT >= '2015-01-01' AND ERDAT <= current_date()),

    CONSTRAINT discount_in_range
        EXPECT (DISCOUNT_PCT >= 0 AND DISCOUNT_PCT <= 100)
)
COMMENT 'Cleaned sales orders — invalid rows removed, types corrected'
AS
SELECT
    TRIM(VBELN)                                                          AS VBELN,
    TRIM(POSNR)                                                          AS POSNR,
    TRIM(AUART)                                                          AS AUART,
    CASE WHEN TRIM(KUNNR)  IN ('N/A','null','NULL','None','','#N/A')
         THEN NULL ELSE TRIM(KUNNR)  END                                 AS KUNNR,
    CASE WHEN TRIM(MATNR)  IN ('N/A','null','NULL','None','','#N/A')
         THEN NULL ELSE TRIM(MATNR)  END                                 AS MATNR,
    TRIM(WERKS)                                                          AS WERKS,
    TRIM(VKORG)                                                          AS VKORG,
    TRIM(BUKRS)                                                          AS BUKRS,
    UPPER(TRIM(WAERK))                                                   AS WAERK,
    CAST(KWMENG      AS DECIMAL(13,3))                                   AS KWMENG,
    CAST(NETWR       AS DECIMAL(15,2))                                   AS NETWR,
    CAST(NETPR       AS DECIMAL(13,2))                                   AS NETPR,
    CAST(DISCOUNT_PCT AS DECIMAL(5,2))                                   AS DISCOUNT_PCT,
    CAST(TAX_AMOUNT  AS DECIMAL(13,2))                                   AS TAX_AMOUNT,
    TO_DATE(ERDAT)                                                       AS ERDAT
FROM STREAM(LIVE.bronze_sales_order);