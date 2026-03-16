CREATE OR REFRESH MATERIALIZED VIEW gold_sales_summary
COMMENT 'Daily sales grain joined with customer and material attributes'
AS
SELECT
    so.VBELN,
    so.POSNR,
    so.ERDAT,

    so.KUNNR,
    c.NAME1                                                         AS CUSTOMER_NAME,
    c.LAND1                                                         AS COUNTRY,
    c.BRSCH                                                         AS INDUSTRY,
    c.KTOKD                                                         AS ACCOUNT_GROUP,
    c.CREDIT_LIMIT,

    so.MATNR,
    m.MAKTX                                                         AS MATERIAL_DESC,
    m.MTART                                                         AS MATERIAL_TYPE,
    m.MATKL                                                         AS MATERIAL_GROUP,
    m.MEINS                                                         AS UOM,
    m.STANDARD_PRICE                                                AS LIST_PRICE,

    so.AUART,
    so.WERKS,
    so.VKORG,
    so.BUKRS,
    so.WAERK,

    so.KWMENG,
    so.NETPR,
    ROUND(so.KWMENG * so.NETPR, 2)                                  AS GROSS_VALUE,
    so.DISCOUNT_PCT,
    ROUND((so.KWMENG * so.NETPR) * so.DISCOUNT_PCT / 100, 2)       AS DISCOUNT_AMOUNT,
    so.NETWR,
    so.TAX_AMOUNT,
    ROUND(so.NETWR + so.TAX_AMOUNT, 2)                              AS TOTAL_VALUE,
    ROUND(so.NETPR - m.STANDARD_PRICE, 2)                          AS PRICE_VS_LIST

FROM LIVE.silver_sales_order so
LEFT JOIN LIVE.silver_customer c ON so.KUNNR = c.KUNNR
LEFT JOIN LIVE.silver_material m ON so.MATNR = m.MATNR;
