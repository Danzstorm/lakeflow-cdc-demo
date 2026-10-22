-- Batch 3: one row with an invalid email, dropped by the pipeline Expectation
INSERT INTO dbo.customers (customer_id, name, email, city, status) VALUES
(24, N'Cliente Invalido', N'correo-sin-arroba', N'Lima', N'active');
GO
