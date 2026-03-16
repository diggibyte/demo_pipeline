from pyspark import pipelines as dp
from pyspark.sql import functions as F
 
BASE_PATH = "/Volumes/chalmers_demo/demo_data/sap_data"
 
BASE_OPTIONS = {
    "cloudFiles.format":            "csv",
    "header":                       "true",
    "inferSchema":                  "false",
    "rescuedDataColumn":            "_rescued_data",
    "pathGlobFilter":               "*.csv",
}
 
FACT_OPTIONS = {
    **BASE_OPTIONS,
    "cloudFiles.maxFilesPerTrigger": "5",
}
 
@dp.table(
    name="bronze_customer",
    comment="Raw customer master data — loaded as-is from CSV (single file)",
)
def bronze_customer():
    return (
        spark.readStream
            .format("cloudFiles")
            .options(**BASE_OPTIONS)
            .load(f"{BASE_PATH}/customer")
    )
 
@dp.table(
    name="bronze_material",
    comment="Raw material master data — loaded as-is from CSV (single file)",
)
def bronze_material():
    return (
        spark.readStream
            .format("cloudFiles")
            .options(**BASE_OPTIONS)
            .load(f"{BASE_PATH}/material")
    )
 
@dp.table(
    name="bronze_sales_order",
    comment=(
        "Raw sales order data — loaded as-is from CSV. "
        "Auto Loader ingests all part files in FACT_SALES_ORDER/ folder. "
        "New part files dropped into the folder are picked up automatically."
    ),
)
def bronze_sales_order():
    return (
        spark.readStream
            .format("cloudFiles")
            .options(**FACT_OPTIONS)
            .load(f"{BASE_PATH}/Sales")
    )
 
