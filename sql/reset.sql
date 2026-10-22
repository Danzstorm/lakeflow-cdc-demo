-- Reset the source to an empty, CDC-enabled table (disabling CDC discards the old change table)
EXEC sys.sp_cdc_disable_table @source_schema = N'dbo', @source_name = N'customers', @capture_instance = N'all';
GO
DELETE FROM dbo.customers;
GO
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'customers', @role_name = NULL;
GO
