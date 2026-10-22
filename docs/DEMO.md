# Demo en vivo: CDC de Azure SQL a Databricks

Guía corta para ejecutar y explicar la demo. El detalle completo está en `GUIA.md`.

## Qué vas a mostrar

Cambias datos en Azure SQL, ejecutas un job en Databricks y ves cómo los cambios llegan a tres capas (bronze, silver, gold), con el historial de cada cliente y un dato inválido descartado.

## Antes de empezar (fuera de cámara)

1. Terminal en la carpeta `lakeflow-cdc-demo`: `bash scripts/reset.sh`. Deja todo en blanco.
2. Cliente T-SQL abierto y conectado a Azure SQL (Query editor del portal, base `cdcdemo`, usuario `sqladmin`). La contraseña está en `.env`; no la muestres.
3. Tres pestañas en el cliente T-SQL, una por lote, con el SQL de la sección "Para copiar y pegar".
4. Databricks abierto en dos pestañas: **Jobs & Pipelines** y **SQL Editor** (warehouse "Serverless Starter Warehouse").
5. Ejecuta una consulta cualquiera en cada lado para despertar la base y el warehouse.

## Qué archivos explicar

Sí, hay que explicarlos, pero a nivel de idea. Con una o dos frases por archivo basta; no leas el código línea por línea. Ábrelos en el editor justo cuando corre el job, que es cuando tienes dos minutos libres.

| Archivo | Cuándo | Qué decir |
|---------|--------|-----------|
| `sql/00_schema_cdc.sql` | Antes del lote 1 | "Esta es la tabla fuente. Con `sp_cdc_enable_table` SQL empieza a registrar cada insert, update y delete en una tabla de cambios." |
| `src/extract_cdc.py` | Mientras corre el job del lote 1 | "Este job lee solo los cambios nuevos de SQL, usando el último LSN como marca de agua, y los guarda como JSON en un Volume." |
| `src/pipeline/01_bronze.sql` | Después del lote 1 | "Auto Loader lee los archivos nuevos y los guarda tal cual en una Streaming Table. Es la capa bronze." |
| `src/pipeline/02_silver.sql` | Lote 2 y lote 3 | "Primero validamos con Expectations. Después `AUTO CDC INTO` aplica inserts, updates y deletes: en una tabla queda el estado actual (SCD1) y en otra el historial (SCD2)." |
| `src/pipeline/03_gold.sql` | Después del lote 2 | "Dos vistas materializadas con agregados listos para negocio." |
| `databricks.yml` | Al final | "Aquí está definido el pipeline y el job de dos tareas, todo como código." |

Los archivos `sql/batch*.sql` no se explican aparte: los pegas y comentas qué cambia en cada lote.

## La demo, lote por lote

Cada lote repite lo mismo:
1. Pegas el SQL del lote en el cliente T-SQL y lo ejecutas.
2. Esperas unos 15 segundos (CDC captura en segundo plano).
3. Databricks, **Jobs & Pipelines**, `customers_cdc_job`, **Run now**. Tarda cerca de 2 minutos.
4. Pegas las consultas de verificación en el SQL Editor.

### Lote 1: carga inicial

- Cambia en SQL: 20 clientes nuevos.
- Resultado esperado: bronze 20, current 20, history 20.
- Si quieres mostrar el CDC crudo antes de correr el job, usa la consulta T-SQL 1.

### Lote 2: updates, deletes e inserts

- Cambia en SQL: 5 updates (ciudad o estado), 2 borrados y 3 clientes nuevos.
- Resultado esperado: bronze 30, current 21, history 28.
- Muestra la consulta 2 (estado actual: los clientes 19 y 20 ya no están) y la 3 (historial: el cliente 1 tiene dos versiones).

### Lote 3: dato inválido

- Cambia en SQL: un cliente con email sin arroba.
- Resultado esperado: bronze 31, current 21. La fila entró a bronze y la Expectation la descartó.
- Muestra la consulta 4.

## Para copiar y pegar

### En el cliente T-SQL

**Lote 1** (carga inicial)

```sql
INSERT INTO dbo.customers (customer_id, name, email, city, status) VALUES
(1,  N'Ana Torres',    N'ana.torres@example.com',    N'Lima',       N'active'),
(2,  N'Luis Mendoza',  N'luis.mendoza@example.com',  N'Bogota',     N'active'),
(3,  N'Carla Ruiz',    N'carla.ruiz@example.com',    N'Quito',      N'active'),
(4,  N'Diego Vargas',  N'diego.vargas@example.com',  N'Santiago',   N'active'),
(5,  N'Elena Soto',    N'elena.soto@example.com',    N'Montevideo', N'active'),
(6,  N'Felipe Rojas',  N'felipe.rojas@example.com',  N'Lima',       N'active'),
(7,  N'Gabriela Paz',  N'gabriela.paz@example.com',  N'Bogota',     N'active'),
(8,  N'Hugo Salas',    N'hugo.salas@example.com',    N'Quito',      N'active'),
(9,  N'Irene Campos',  N'irene.campos@example.com',  N'Santiago',   N'active'),
(10, N'Jorge Leon',    N'jorge.leon@example.com',    N'Montevideo', N'active'),
(11, N'Karla Mora',    N'karla.mora@example.com',    N'Lima',       N'active'),
(12, N'Mateo Rios',    N'mateo.rios@example.com',    N'Bogota',     N'active'),
(13, N'Natalia Cruz',  N'natalia.cruz@example.com',  N'Quito',      N'active'),
(14, N'Oscar Pinto',   N'oscar.pinto@example.com',   N'Santiago',   N'active'),
(15, N'Paula Vega',    N'paula.vega@example.com',    N'Montevideo', N'active'),
(16, N'Ricardo Luna',  N'ricardo.luna@example.com',  N'Lima',       N'active'),
(17, N'Sofia Herrera', N'sofia.herrera@example.com', N'Bogota',     N'active'),
(18, N'Tomas Navarro', N'tomas.navarro@example.com', N'Quito',      N'active'),
(19, N'Valeria Ortiz', N'valeria.ortiz@example.com', N'Santiago',   N'active'),
(20, N'Walter Diaz',   N'walter.diaz@example.com',   N'Montevideo', N'active');
```

**Lote 2** (cambios)

```sql
UPDATE dbo.customers SET city = N'Bogota',   updated_at = SYSUTCDATETIME() WHERE customer_id = 1;
UPDATE dbo.customers SET city = N'Santiago', updated_at = SYSUTCDATETIME() WHERE customer_id = 2;
UPDATE dbo.customers SET status = N'inactive', updated_at = SYSUTCDATETIME() WHERE customer_id = 3;
UPDATE dbo.customers SET status = N'inactive', updated_at = SYSUTCDATETIME() WHERE customer_id = 4;
UPDATE dbo.customers SET city = N'Lima',    updated_at = SYSUTCDATETIME() WHERE customer_id = 5;
DELETE FROM dbo.customers WHERE customer_id IN (19, 20);
INSERT INTO dbo.customers (customer_id, name, email, city, status) VALUES
(21, N'Ximena Bravo', N'ximena.bravo@example.com', N'Lima',   N'active'),
(22, N'Yago Fuentes', N'yago.fuentes@example.com', N'Quito',  N'active'),
(23, N'Zoe Salazar',  N'zoe.salazar@example.com',  N'Bogota', N'active');
```

**Lote 3** (dato inválido)

```sql
INSERT INTO dbo.customers (customer_id, name, email, city, status) VALUES
(24, N'Cliente Invalido', N'correo-sin-arroba', N'Lima', N'active');
```

**Consulta T-SQL 1: cambios crudos que capturó CDC** (opcional, antes de correr el job)

```sql
SELECT __$operation AS op, customer_id, city, status
FROM cdc.dbo_customers_CT
ORDER BY __$start_lsn;
```

`op`: 1 = borrado, 2 = inserción, 3 = update (antes), 4 = update (después).

### En el SQL Editor de Databricks

**Conteos** (úsala después de cada lote)

```sql
SELECT
  (SELECT count(*) FROM dbw_lakeflow_cdc.cdc_demo.customers_bronze)  AS bronze,
  (SELECT count(*) FROM dbw_lakeflow_cdc.cdc_demo.customers_current) AS current_rows,
  (SELECT count(*) FROM dbw_lakeflow_cdc.cdc_demo.customers_history) AS history_rows;
```

**Consulta 2: estado actual** (después del lote 2)

```sql
SELECT customer_id, name, city, status
FROM dbw_lakeflow_cdc.cdc_demo.customers_current
WHERE customer_id IN (1, 2, 3, 4, 5, 19, 20, 21)
ORDER BY customer_id;
```

**Consulta 3: historial** (después del lote 2)

```sql
SELECT customer_id, city, status, __START_AT, __END_AT
FROM dbw_lakeflow_cdc.cdc_demo.customers_history
WHERE customer_id IN (1, 3, 19)
ORDER BY customer_id, __START_AT;
```

**Gold** (después del lote 1 y del 2, para comparar)

```sql
SELECT * FROM dbw_lakeflow_cdc.cdc_demo.customers_by_city ORDER BY city;
```

**Consulta 4: el dato inválido** (después del lote 3)

```sql
SELECT
  (SELECT count(*) FROM dbw_lakeflow_cdc.cdc_demo.customers_bronze  WHERE customer_id = 24) AS en_bronze,
  (SELECT count(*) FROM dbw_lakeflow_cdc.cdc_demo.customers_current WHERE customer_id = 24) AS en_current;
```

Resultado: 1 en bronze y 0 en current. Las métricas de la Expectation también se ven en la interfaz, en el grafo del pipeline.

## Si algo falla

| Síntoma | Qué hacer |
|---------|-----------|
| El job falla en `extract` con error de conexión | La base estaba pausada. Ejecuta cualquier consulta en el cliente T-SQL para despertarla y pulsa **Run now** otra vez |
| El job dice `No new changes.` | CDC aún no capturó. Espera unos segundos y pulsa **Run now** otra vez |
| Ejecutaste un lote dos veces | Los números ya no coincidirán. Termina la demo o ejecuta `bash scripts/reset.sh` fuera de cámara |

## Genie Code (opcional, manual)

En el editor del pipeline, pídele: "Explica qué hace este pipeline y cómo viaja un DELETE hasta customers_current." No lo probé; ensáyalo antes.
