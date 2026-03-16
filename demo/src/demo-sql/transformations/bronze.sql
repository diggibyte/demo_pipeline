CREATE OR REFRESH STREAMING TABLE bronze_customer
COMMENT 'Raw customer master data — loaded as-is from CSV'
AS SELECT * FROM STREAM read_files(
    '/Volumes/chalmers_demo/demo_data/sap_data/customer',
    format            => 'csv',
    header            => true,
    inferSchema       => false,
    rescuedDataColumn => '_rescued_data',
    pathGlobFilter    => '*.csv'
);

CREATE OR REFRESH STREAMING TABLE bronze_material
COMMENT 'Raw material master data — loaded as-is from CSV'
AS SELECT * FROM STREAM read_files(
    '/Volumes/chalmers_demo/demo_data/sap_data/material',
    format            => 'csv',
    header            => true,
    inferSchema       => false,
    rescuedDataColumn => '_rescued_data',
    pathGlobFilter    => '*.csv'
);

CREATE OR REFRESH STREAMING TABLE bronze_sales_order
COMMENT 'Raw sales order data — loaded as-is from CSV. Auto Loader ingests all part files. New files dropped into the folder are picked up automatically.'
AS SELECT * FROM STREAM read_files(
    '/Volumes/chalmers_demo/demo_data/sap_data/Sales',
    format                => 'csv',
    header                => true,
    inferSchema           => false,
    rescuedDataColumn     => '_rescued_data',
    pathGlobFilter        => '*.csv',
    maxFilesPerTrigger    => '5'
);
