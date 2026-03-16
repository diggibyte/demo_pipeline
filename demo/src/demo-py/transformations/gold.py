from pyspark import pipelines as dp
from pyspark.sql import functions as F
 
 
@dp.materialized_view(
    name="gold_sales_summary",
    comment="Daily sales grain joined with customer and material attributes",
)
def gold_sales_summary():
    so   = spark.read.table("silver_sales_order") #Read the "silver" level tables from DLT
    cust = spark.read.table("silver_customer")
    mat  = spark.read.table("silver_material")
 
    return (
        so
        .join(
            cust.select(
                F.col("KUNNR"),
                F.col("NAME1")       .alias("CUSTOMER_NAME"),
                F.col("LAND1")       .alias("COUNTRY"),
                F.col("BRSCH")       .alias("INDUSTRY"),
                F.col("KTOKD")       .alias("ACCOUNT_GROUP"),
                F.col("CREDIT_LIMIT"),
            ),
            on="KUNNR",
            how="left",
        )
        .join(
            mat.select(
                F.col("MATNR"),
                F.col("MAKTX")          .alias("MATERIAL_DESC"),
                F.col("MTART")          .alias("MATERIAL_TYPE"),
                F.col("MATKL")          .alias("MATERIAL_GROUP"),
                F.col("MEINS")          .alias("UOM"),
                F.col("STANDARD_PRICE") .alias("LIST_PRICE"),
            ),
            on="MATNR",
            how="left",
        )
        .withColumn("GROSS_VALUE",      F.round(F.col("KWMENG") * F.col("NETPR"), 2))
        .withColumn("DISCOUNT_AMOUNT",  F.round(F.col("GROSS_VALUE") * F.col("DISCOUNT_PCT") / 100, 2))# Compute discount amount per line
        .withColumn("TOTAL_VALUE",      F.round(F.col("NETWR") + F.col("TAX_AMOUNT"), 2))# Compute total invoiced value per line
        .withColumn("PRICE_VS_LIST",    F.round(F.col("NETPR") - F.col("LIST_PRICE"), 2))# Calculate the difference between selling price and standard list price.
        .select(
            "VBELN", "POSNR", "ERDAT",
            "KUNNR", "CUSTOMER_NAME", "COUNTRY", "INDUSTRY", "ACCOUNT_GROUP", "CREDIT_LIMIT",
            "MATNR", "MATERIAL_DESC", "MATERIAL_TYPE", "MATERIAL_GROUP", "UOM", "LIST_PRICE",
            "AUART", "WERKS", "VKORG", "BUKRS", "WAERK",
            "KWMENG", "NETPR", "GROSS_VALUE",
            "DISCOUNT_PCT", "DISCOUNT_AMOUNT",
            "NETWR", "TAX_AMOUNT", "TOTAL_VALUE",
            "PRICE_VS_LIST",
        )
    )
 
 
 
 