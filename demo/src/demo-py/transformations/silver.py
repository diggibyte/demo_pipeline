from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
 
JUNK_VALUES = ("N/A", "null", "NULL", "None", "", "#N/A")
 
def clean_str(col_name: str):
    trimmed = F.trim(F.col(col_name))
    return F.when(trimmed.isin(*JUNK_VALUES), F.lit(None)).otherwise(trimmed)
 
@dp.table(
    name="silver_customer",
    comment="Cleaned customer master — types corrected, junk removed",
)
@dp.expect_or_drop("customer_id_must_exist",   "KUNNR IS NOT NULL")
@dp.expect_or_drop("customer_id_valid_format", "KUNNR RLIKE '^CUST[0-9]{7}$'")
@dp.expect("customer_name_not_blank",          "NAME1 IS NOT NULL")
@dp.expect("country_code_valid",               "LAND1 RLIKE '^[A-Z]{2}$'")
@dp.expect("credit_limit_in_range",            "CREDIT_LIMIT >= 0 AND CREDIT_LIMIT <= 1000000000")
def silver_customer():
    return (
        spark.readStream.table("bronze_customer")
        .select(
            F.trim(F.col("KUNNR"))                        .alias("KUNNR"),
            clean_str("NAME1")                            .alias("NAME1"),
            F.upper(F.trim(F.col("LAND1")))               .alias("LAND1"),
            clean_str("BRSCH")                            .alias("BRSCH"),
            F.trim(F.col("KTOKD"))                        .alias("KTOKD"),
            F.col("CREDIT_LIMIT").cast(DecimalType(15, 2)).alias("CREDIT_LIMIT"),
            F.to_date(F.col("ERDAT"))                     .alias("ERDAT"),
            F.to_date(F.col("AEDAT"))                     .alias("AEDAT"),
        )
    )
 
VALID_MATERIAL_TYPES = ["FERT", "HALB", "ROH", "HIBE", "ERSA", "VERP", "DIEN"]
 
@dp.table(
    name="silver_material",
    comment="Cleaned material master — types corrected, junk removed",
)
@dp.expect_or_drop("material_id_must_exist",   "MATNR IS NOT NULL")
@dp.expect_or_drop("material_id_valid_format", "MATNR RLIKE '^MAT[0-9]{8}$'")
@dp.expect("description_not_blank",            "MAKTX IS NOT NULL")
@dp.expect("material_type_valid",              f"MTART IN ({', '.join(repr(t) for t in VALID_MATERIAL_TYPES)})")
@dp.expect("price_not_negative",               "STANDARD_PRICE >= 0")
def silver_material():
    return (
        spark.readStream.table("bronze_material")
        .select(
            F.trim(F.col("MATNR"))                           .alias("MATNR"),
            clean_str("MAKTX")                               .alias("MAKTX"),
            F.trim(F.col("MTART"))                           .alias("MTART"),
            F.trim(F.col("MATKL"))                           .alias("MATKL"),
            F.upper(F.trim(F.col("MEINS")))                  .alias("MEINS"),
            F.col("STANDARD_PRICE").cast(DecimalType(13, 2)) .alias("STANDARD_PRICE"),
            F.col("BRGEW").cast(DecimalType(13, 3))          .alias("BRGEW"),
            F.to_date(F.col("ERDAT"))                        .alias("ERDAT"),
            F.to_date(F.col("AEDAT"))                        .alias("AEDAT"),
        )
    )
 
VALID_CURRENCIES = [
    "SEK", "EUR", "USD", "GBP", "INR", "JPY",
    "AUD", "AED", "BRL", "CAD", "CHF", "HKD", "SGD",
]
 
@dp.table(
    name="silver_sales_order",
    comment="Cleaned sales orders — invalid rows removed, types corrected",
)
@dp.expect_or_drop("order_key_must_exist",      "VBELN IS NOT NULL AND POSNR IS NOT NULL")
@dp.expect_or_drop("customer_must_exist",       "KUNNR IS NOT NULL")
@dp.expect_or_drop("material_must_exist",       "MATNR IS NOT NULL")
@dp.expect_or_drop("amount_must_exist",         "NETWR IS NOT NULL")
@dp.expect_or_drop("amount_in_valid_range",     "NETWR >= 0 AND NETWR <= 10000000")
@dp.expect_or_drop("quantity_must_be_positive", "KWMENG > 0")
@dp.expect("currency_is_valid",   f"WAERK IN ({', '.join(repr(c) for c in VALID_CURRENCIES)})")
@dp.expect("order_date_in_range", "ERDAT >= '2015-01-01' AND ERDAT <= current_date()")
@dp.expect("discount_in_range",   "DISCOUNT_PCT >= 0 AND DISCOUNT_PCT <= 100")
def silver_sales_order():
    return (
        spark.readStream.table("bronze_sales_order")
        .select(
            F.trim(F.col("VBELN"))                        .alias("VBELN"),
            F.trim(F.col("POSNR"))                        .alias("POSNR"),
            F.trim(F.col("AUART"))                        .alias("AUART"),
            clean_str("KUNNR")                            .alias("KUNNR"),
            clean_str("MATNR")                            .alias("MATNR"),
            F.trim(F.col("WERKS"))                        .alias("WERKS"),
            F.trim(F.col("VKORG"))                        .alias("VKORG"),
            F.trim(F.col("BUKRS"))                        .alias("BUKRS"),
            F.upper(F.trim(F.col("WAERK")))               .alias("WAERK"),
            F.col("KWMENG").cast(DecimalType(13, 3))      .alias("KWMENG"),
            F.col("NETWR").cast(DecimalType(15, 2))       .alias("NETWR"),
            F.col("NETPR").cast(DecimalType(13, 2))       .alias("NETPR"),
            F.col("DISCOUNT_PCT").cast(DecimalType(5, 2)) .alias("DISCOUNT_PCT"),
            F.col("TAX_AMOUNT").cast(DecimalType(13, 2))  .alias("TAX_AMOUNT"),
            F.to_date(F.col("ERDAT"))                     .alias("ERDAT"),
        )
    )