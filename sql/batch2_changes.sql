-- Batch 2: 5 updates, 2 deletes, 3 inserts
UPDATE dbo.customers SET city = N'Bogota',   updated_at = SYSUTCDATETIME() WHERE customer_id = 1;
UPDATE dbo.customers SET city = N'Santiago', updated_at = SYSUTCDATETIME() WHERE customer_id = 2;
UPDATE dbo.customers SET status = N'inactive', updated_at = SYSUTCDATETIME() WHERE customer_id = 3;
UPDATE dbo.customers SET status = N'inactive', updated_at = SYSUTCDATETIME() WHERE customer_id = 4;
UPDATE dbo.customers SET city = N'Lima',    updated_at = SYSUTCDATETIME() WHERE customer_id = 5;
DELETE FROM dbo.customers WHERE customer_id IN (19, 20);
INSERT INTO dbo.customers (customer_id, name, email, city, status) VALUES
(21, N'Ximena Bravo',  N'ximena.bravo@example.com',  N'Lima',   N'active'),
(22, N'Yago Fuentes',  N'yago.fuentes@example.com',  N'Quito',  N'active'),
(23, N'Zoe Salazar',   N'zoe.salazar@example.com',   N'Bogota', N'active');
GO
