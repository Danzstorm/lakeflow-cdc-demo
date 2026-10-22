-- Source table + Change Data Capture
CREATE TABLE dbo.customers (
    customer_id INT           NOT NULL PRIMARY KEY,
    name        NVARCHAR(100) NOT NULL,
    email       NVARCHAR(150) NULL,
    city        NVARCHAR(60)  NULL,
    status      NVARCHAR(20)  NOT NULL,
    updated_at  DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
);
GO
EXEC sys.sp_cdc_enable_db;
GO
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name   = N'customers',
    @role_name     = NULL;
GO
