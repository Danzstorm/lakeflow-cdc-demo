# Webinar: CDC de Azure SQL a Databricks con Lakeflow Spark Declarative Pipelines

Guía para presentar y reproducir en vivo una demo reducida, probada de punta a punta el 2026-10-20.

- **Parte I, guion del webinar:** qué decir, qué ejecutar y qué debería verse en cada bloque.
- **Parte II, construcción paso a paso:** cómo se armó todo, con el código completo, para explicar qué se hizo en cada parte o para rehacerlo desde cero.

Los bloques siguen los cinco puntos del temario: ingesta incremental, CDC, arquitectura Medallion, calidad de datos e IA, y orquestación. Si tu presentación (PPT) usa otro orden, mueve los bloques; son independientes salvo por los lotes, que deben ejecutarse en orden 1, 2, 3.

# Parte I. Guion del webinar

## Antes de empezar (30 minutos antes)

Todo se ejecuta desde la carpeta `lakeflow-cdc-demo`.

| Qué | Comando o acción | Esperado |
|-----|------------------|----------|
| Sesión de Azure | `az login` | Sesión activa |
| Acceso a Databricks | `DATABRICKS_HOST=<url> DATABRICKS_AUTH_TYPE=azure-cli databricks current-user me` | Tu usuario |
| Despertar la base | `bash scripts/s.sh "SELECT 1"` | Responde. Si estaba pausada, la primera conexión tarda |
| Despertar el warehouse | `python scripts/q.py "SELECT 1"` | Responde. El primer arranque tarda cerca de un minuto |
| Estado inicial en blanco | `bash scripts/reset.sh` (tarda unos minutos) | Sin tablas en `cdc_demo`, tabla SQL vacía |
| Pestañas del navegador | Workspace de Databricks: Jobs & Pipelines, y Catalog | Abiertas de antemano |

Dos terminales: una para `lote.sh` (que muestra el progreso del job) y otra para las consultas.

**No muestres el archivo `.env` en pantalla.** Contiene la contraseña de SQL. Los scripts `s.sh` y `lote.sh` la leen sin imprimirla.

## Modo manual (sin scripts, recomendado en vivo)

Los scripts `lote.sh`, `s.sh` y `q.py` son atajos. En vivo se hace todo a mano con dos pantallas, y el único script que se usa es `reset.sh`, antes de empezar y fuera de cámara.

**Pantalla 1: cliente T-SQL contra Azure SQL.** Dos opciones:
- Portal de Azure, base `cdcdemo`, **Query editor**, con login SQL (usuario `sqladmin`, contraseña en `.env`). Si pide autorizar tu IP, usa el botón que ofrece el portal.
- Extensión MSSQL de VS Code, con el servidor y las mismas credenciales.

Abre el cliente y conéctate antes del webinar, para no mostrar la contraseña. No probé el Query editor del portal desde la terminal; ensáyalo.

Deja preparadas tres pestañas, una por lote, con el SQL ya pegado: lote 1 en el Paso 3 de la Parte II, lotes 2 y 3 en el Paso 9.

**Pantalla 2: workspace de Databricks.**
- **Jobs & Pipelines:** job `customers_cdc_job` y pipeline `customers_cdc_pipeline`.
- **SQL Editor:** con el warehouse "Serverless Starter Warehouse", para las consultas de verificación.

**Ciclo de cada lote (cuatro acciones):**
1. **SQL:** pega el lote en el cliente T-SQL y ejecútalo. Aquí se explica qué cambia en la fuente.
2. **Espera cerca de 15 segundos.** CDC captura los cambios en segundo plano. Es un buen momento para mostrar la tabla `cdc.dbo_customers_CT`.
3. **Databricks:** Jobs & Pipelines, abre `customers_cdc_job` y pulsa **Run now**. Abre la ejecución para ver las dos tareas avanzar. Tarda cerca de 2 minutos y es el momento para explicar el código.
4. **Verifica** en el SQL Editor.

**Equivalencias con lo que aparece en el guion:**

| El guion dice | Haz esto |
|---------------|----------|
| `bash scripts/lote.sh N` | Acciones 1, 2 y 3 del ciclo |
| `python scripts/q.py "SELECT ..."` | Pega el texto SQL (lo que va entre comillas) en el SQL Editor de Databricks |
| `bash scripts/s.sh '...'` | Pega el texto T-SQL en el cliente T-SQL |
| `databricks fs ls ...landing/customers` | Catalog, `dbw_lakeflow_cdc`, `cdc_demo`, Volume `landing`, carpeta `customers` |
| `<pipeline_id>` en `event_log(...)` | El id aparece en la URL de la página del pipeline |

Los nombres exactos de menús y botones pueden variar un poco según la versión de la interfaz de Databricks. Ensáyalo una vez antes del webinar. Yo trabajé con la CLI, no con los clics, así que esos pasos no los verifiqué.

**Antes de salir al aire** (fuera de cámara): `bash scripts/reset.sh`. Deja el workspace sin tablas y la tabla SQL vacía.

## Mapa de tiempos (sugerido)

| Bloque | Minutos | Lote que se ejecuta |
|--------|---------|---------------------|
| 0. Qué es Lakeflow | 5 | – |
| 1. Ingesta incremental | 12 | Lote 1 |
| 2. CDC con AUTO CDC | 15 | Lote 2 |
| 3. Arquitectura Medallion | 8 | – |
| 4. Calidad de datos e IA | 12 | Lote 3 y Genie Code |
| 5. Orquestación | 8 | – |
| 6. Cierre | 5 | – |

Cada ejecución del job tarda cerca de 2 a 2,5 minutos. Lanza el lote al comienzo del bloque y explica el código mientras corre.

## Bloque 0. Qué es Lakeflow

**Qué decir**
- Lakeflow agrupa tres piezas para construir pipelines de datos en Databricks: **Lakeflow Connect** (conectores de ingesta), **Lakeflow Spark Declarative Pipelines** (transformaciones declarativas) y **Lakeflow Jobs** (orquestación).
- En esta demo usamos las dos últimas. La ingesta la hacemos a mano, con un job en Python, para que se vea cómo funciona el CDC por dentro. Con Lakeflow Connect existe un conector gestionado para SQL Server que haría ese tramo sin código.
- Idea central de lo declarativo: describes **qué** tabla quieres y Databricks se ocupa de dependencias, orden, checkpoints y reintentos.

**Qué se construyó:** todo el flujo de la sección "Qué se construye" (Parte II).

**Mostrar:** el diagrama de arquitectura de la Parte II.

## Bloque 1. Ingesta incremental (Auto Loader y Streaming Tables)

**Qué se hizo:** un job extractor lee los cambios nuevos de Azure SQL y los deja como archivos JSON en un Volume de Unity Catalog. Una Streaming Table los incorpora con Auto Loader (`customers_bronze`).

**Qué decir**
- *Incremental* significa que cada ejecución procesa solo lo nuevo. Hay dos niveles:
  1. El extractor guarda un **watermark** (el último LSN leído) en una tabla Delta y la próxima vez pide solo lo posterior.
  2. **Auto Loader** (`read_files` dentro de `STREAM`) recuerda qué archivos ya procesó y solo toma los nuevos.
- Una **Streaming Table** es una tabla que se alimenta de una fuente que crece. No reprocesa lo anterior.
- Por qué archivos en un Volume: desacopla la fuente del pipeline y deja una copia cruda auditable.

**Código a explicar (Parte II, pasos 5 y 6)**
- `src/extract_cdc.py`: la consulta con el filtro `__$start_lsn > watermark`, la conversión del LSN a texto hexadecimal y el watermark.
- `src/pipeline/01_bronze.sql`: `CREATE OR REFRESH STREAMING TABLE ... FROM STREAM read_files(...)`.

**En vivo**

1. Mostrar la tabla fuente y que está vacía:
   ```bash
   bash scripts/s.sh "SELECT COUNT(*) AS filas FROM dbo.customers"
   ```
2. Lanzar el lote 1 (inserta 20 clientes en SQL, espera a CDC y ejecuta el job):
   ```bash
   bash scripts/lote.sh 1
   ```
3. Mientras corre, explicar el extractor. Al terminar, el job imprime algo como `Wrote 20 change rows ... watermark -> 0x...`.
4. Ver los archivos que aterrizaron:
   ```bash
   DATABRICKS_HOST=<url> DATABRICKS_AUTH_TYPE=azure-cli databricks fs ls dbfs:/Volumes/dbw_lakeflow_cdc/cdc_demo/landing/customers
   ```
5. Ver bronze:
   ```bash
   python scripts/q.py "SELECT customer_id, op, city, source_file FROM dbw_lakeflow_cdc.cdc_demo.customers_bronze ORDER BY customer_id LIMIT 5"
   ```

**Resultado verificado:** bronze 20, current 20, history 20.

**Pregunta frecuente:** ¿qué pasa si ejecuto el job dos veces sin cambios? El extractor imprime `No new changes.` y no se crea contenido nuevo; el pipeline no procesa nada.

## Bloque 2. Change Data Capture con AUTO CDC (SCD tipo 1 y 2)

**Qué se hizo:** se habilitó CDC en Azure SQL y se aplican los cambios a dos tablas de silver con `AUTO CDC INTO`: una con el estado actual (SCD1) y otra con el historial (SCD2).

**Qué decir**
- **CDC en SQL:** SQL registra cada insert, update y delete en una tabla de cambios (`cdc.dbo_customers_CT`). Esa tabla trae la operación (`__$operation`: 1 borrado, 2 inserción, 3 y 4 imagen anterior y posterior de un update) y el LSN, que ordena los cambios.
- **El problema que resuelve AUTO CDC:** aplicar esos eventos a una tabla destino correctamente, incluso si llegan desordenados o repetidos. Sin él se escribe un `MERGE` a mano con lógica de deduplicación y orden.
- **Los cuatro ingredientes:** `KEYS` (qué identifica un registro), `SEQUENCE BY` (cómo se ordenan los eventos), `APPLY AS DELETE WHEN` (qué evento es un borrado) y `STORED AS SCD TYPE 1 | 2`.
- **SCD tipo 1:** una fila por cliente con el último valor. Lo anterior se sobrescribe. Un delete elimina la fila.
- **SCD tipo 2:** cada cambio crea una **versión** con `__START_AT` y `__END_AT`. `TRACK HISTORY ON city, status` limita el historial a esas dos columnas.
- Límite a mencionar: `SEQUENCE BY commit_time` puede empatar si el mismo cliente cambia dos veces en el mismo milisegundo. En producción se ordena con el LSN.

**Código a explicar:** `src/pipeline/02_silver.sql`, los dos bloques `CREATE FLOW ... AS AUTO CDC INTO`.

**En vivo**

1. Mostrar los cambios crudos que SQL capturó del lote 1:
   ```bash
   bash scripts/s.sh 'SELECT __$operation AS op, customer_id, city, status FROM cdc.dbo_customers_CT ORDER BY customer_id'
   ```
2. Mostrar el lote 2 antes de ejecutarlo (`sql/batch2_changes.sql`): 5 updates, 2 deletes y 3 inserts.
3. Ejecutarlo:
   ```bash
   bash scripts/lote.sh 2
   ```
4. Mostrar la tabla de cambios, ahora con updates (imágenes 3 y 4) y deletes (1):
   ```bash
   bash scripts/s.sh 'SELECT __$operation AS op, customer_id, city, status FROM cdc.dbo_customers_CT WHERE __$operation <> 2 ORDER BY customer_id'
   ```
5. Estado actual (SCD1):
   ```bash
   python scripts/q.py "SELECT customer_id, name, city, status FROM dbw_lakeflow_cdc.cdc_demo.customers_current WHERE customer_id IN (1,2,3,4,5,19,20,21) ORDER BY customer_id"
   ```
   Los clientes 19 y 20 ya no están; el 21 aparece.
6. Historial (SCD2):
   ```bash
   python scripts/q.py "SELECT customer_id, city, status, __START_AT, __END_AT FROM dbw_lakeflow_cdc.cdc_demo.customers_history WHERE customer_id IN (1,3,19) ORDER BY customer_id, __START_AT"
   ```
   El cliente 1 tiene dos versiones (Lima y luego Bogota). El 3 cambia de estado. El 19, borrado, queda con su versión cerrada por `__END_AT`.

**Resultado verificado:** bronze 30, current 21 (20 menos 2 borrados más 3 nuevos), history 28 (20 originales más 5 versiones por updates y 3 por inserts).

## Bloque 3. Arquitectura Medallion (Bronze, Silver, Gold)

**Qué se hizo:** tres capas dentro de un solo pipeline, más dos Materialized Views en gold.

| Capa | Tablas | Contenido |
|------|--------|-----------|
| Bronze | `customers_bronze` | Eventos CDC crudos, tal como llegaron, con archivo de origen |
| Silver | `customers_clean`, `customers_current`, `customers_history` | Datos tipados, validados y con CDC aplicado |
| Gold | `customers_by_city`, `customers_with_changes` | Agregados listos para negocio |

**Qué decir**
- Cada capa sube la calidad y baja el detalle. Bronze permite reprocesar; gold es lo que consume el negocio.
- En un pipeline declarativo no se programa el orden: Databricks lo deduce de las referencias (`STREAM(customers_bronze)`, `FROM customers_current`) y dibuja el grafo.
- Una **Materialized View** guarda el resultado de una consulta y la actualiza de forma incremental cuando puede.

**Código a explicar:** `src/pipeline/03_gold.sql`.

**En vivo**

1. En el navegador: Jobs & Pipelines, abrir `customers_cdc_pipeline` y mostrar el **grafo** con las capas.
2. Consultar gold:
   ```bash
   python scripts/q.py "SELECT * FROM dbw_lakeflow_cdc.cdc_demo.customers_by_city ORDER BY city"
   ```
   Tras el lote 1 son 4 clientes activos en cada una de las 5 ciudades. Tras el lote 2, los valores esperados son Lima 5, Bogota 5, Quito 4, Santiago 3 y Montevideo 2. Esos números salen de calcular el lote a mano; confírmalos al ensayar.
3. Clientes con más de una versión:
   ```bash
   python scripts/q.py "SELECT * FROM dbw_lakeflow_cdc.cdc_demo.customers_with_changes ORDER BY customer_id"
   ```

## Bloque 4. Calidad de datos e IA (Expectations y Genie Code)

**Qué se hizo:** dos Expectations en `customers_clean`, que descartan filas inválidas antes de que lleguen al CDC.

**Qué decir**
- Una **Expectation** es una regla declarativa de calidad. Tiene tres modos: solo registrar la violación (`EXPECT`), descartar la fila (`ON VIOLATION DROP ROW`) o detener el pipeline (`ON VIOLATION FAIL UPDATE`). Aquí usamos descartar.
- Las dos reglas: `id_not_null` y `email_valid` (el email debe tener `@`, salvo en borrados).
- Las métricas de cada regla quedan en el **event log** del pipeline y se ven en la interfaz, sin código extra.

**Código a explicar:** el bloque `CONSTRAINT ... EXPECT` de `02_silver.sql`.

**En vivo: lote 3 (dato inválido)**

1. Mostrar `sql/batch3_bad_data.sql`: un cliente con email `correo-sin-arroba`.
2. Ejecutar:
   ```bash
   bash scripts/lote.sh 3
   ```
3. Mostrar que llegó a bronze pero no pasó a silver:
   ```bash
   python scripts/q.py "SELECT (SELECT count(*) FROM dbw_lakeflow_cdc.cdc_demo.customers_bronze WHERE customer_id=24) en_bronze, (SELECT count(*) FROM dbw_lakeflow_cdc.cdc_demo.customers_current WHERE customer_id=24) en_current"
   ```
   Resultado: 1 en bronze, 0 en current.
4. Métricas de las Expectations (necesita el id del pipeline, con `databricks pipelines list-pipelines`):
   ```bash
   python scripts/q.py "SELECT details:flow_progress.data_quality.expectations FROM event_log('<pipeline_id>') WHERE event_type='flow_progress' AND details:flow_progress.data_quality.expectations IS NOT NULL ORDER BY timestamp DESC LIMIT 1"
   ```
   `email_valid` aparece con `failed_records: 1`. También se ve en la pestaña de calidad de datos de la tabla `customers_clean`, en el grafo del pipeline.

**Resultado verificado:** bronze 31, current 21.

**Genie Code (manual, en la interfaz de Databricks)**

Este bloque no está automatizado. Se hace en el editor del pipeline. Qué decir: Genie Code es el asistente de IA de Databricks, que entiende el pipeline abierto, así que sirve para explicar código, generar transformaciones y depurar errores. Prompts sugeridos:
1. "Explica qué hace este pipeline y cómo viaja un DELETE desde bronze hasta customers_current."
2. "Agrega una Expectation que descarte clientes con city nula."
3. Provocar un error a propósito (por ejemplo, escribir mal un nombre de columna en `02_silver.sql`), ejecutar y pedirle que depure el fallo.

Ensaya este bloque antes: no se probó, y su respuesta no es determinista. Si cambias el archivo en el workspace, `bundle deploy` lo sobrescribirá la próxima vez.

## Bloque 5. Orquestación con Lakeflow Jobs

**Qué se hizo:** un job de dos tareas, `extract` y `refresh_pipeline`, donde la segunda depende de la primera. Se define como código en `databricks.yml` y se despliega con Databricks Asset Bundles.

**Qué decir**
- El job es lo que une el mundo "fuera de Databricks" (extraer de SQL) con el pipeline declarativo. El pipeline por sí solo no sabe de dónde vienen los archivos.
- Se ve el orden de tareas, la duración de cada una, los reintentos y el historial de ejecuciones.
- Hoy se ejecuta a demanda. En producción se le agrega un trigger: programación por cron o llegada de archivos.
- Infraestructura como código: el mismo `databricks.yml` recrea pipeline y job en otro workspace con `bundle deploy`.

**Código a explicar:** `databricks.yml`, secciones `pipelines` y `jobs`.

**En vivo**
1. En el navegador: Jobs & Pipelines, abrir `customers_cdc_job`, mostrar el DAG de dos tareas y el historial de ejecuciones (los tres lotes).
2. Abrir la última ejecución y entrar a la tarea `extract` para ver el mensaje que imprimió.
3. Mostrar `bundle validate` (opcional):
   ```bash
   DATABRICKS_HOST=<url> DATABRICKS_AUTH_TYPE=azure-cli databricks bundle validate
   ```

## Bloque 6. Cierre

- Recapitulación en una frase por bloque: incremental con Auto Loader, CDC con AUTO CDC, capas con Streaming Tables y Materialized Views, calidad con Expectations, orquestación con Jobs.
- Costo: la base SQL serverless factura solo mientras está activa y el workspace no cobra en reposo. Detalles en la Parte II.
- Para repetir la demo: `bash scripts/reset.sh` y de nuevo `lote.sh 1`, `2`, `3`.
- Para apagar todo al final: ver "Costo y limpieza" en la Parte II.

## Si algo falla en vivo

| Síntoma | Qué hacer |
|---------|-----------|
| El job falla en `extract` con error de conexión | La base estaba pausada. Ejecuta `bash scripts/s.sh "SELECT 1"` y vuelve a lanzar `lote.sh`. Si la tabla ya recibió el lote, ejecuta solo `databricks bundle run customers_cdc_job` |
| `lote.sh` imprime `No new changes.` | CDC no alcanzó a capturar. Espera 15 segundos y ejecuta `databricks bundle run customers_cdc_job` |
| Ejecuté un lote dos veces | Los `INSERT` fallan por clave duplicada. En el lote 2 los `UPDATE` sí se reaplican y generan cambios de más. Ejecuta `bash scripts/reset.sh` y empieza de nuevo |
| Los números no coinciden | Probablemente se saltó o repitió un lote. Ejecuta `bash scripts/reset.sh` y empieza de nuevo |
| El warehouse tarda | Es el primer arranque. Espera cerca de un minuto |

---

# Parte II. Construcción paso a paso

Todos los comandos de esta parte se ejecutaron desde la terminal, sin usar la UI de Databricks, salvo Genie Code.

## Qué se construye

```
Azure SQL Database (CDC activo en dbo.customers)
  -> Job extract (Python, serverless) lee cdc.dbo_customers_CT desde el último LSN
  -> archivos JSON en un Volume de Unity Catalog (landing)
  -> Pipeline declarativo (SQL)
       bronze  customers_bronze   Streaming Table con Auto Loader
       silver  customers_clean    Expectations
               customers_current  AUTO CDC, SCD tipo 1
               customers_history  AUTO CDC, SCD tipo 2
       gold    customers_by_city, customers_with_changes   Materialized Views
  -> Lakeflow Job: extract -> refresh_pipeline
```

Temas cubiertos: ingesta incremental (Auto Loader y Streaming Tables), CDC con AUTO CDC (SCD 1 y 2), arquitectura Medallion, Expectations, Lakeflow Jobs y Genie Code.

## Decisiones

| Tema | Decisión | Motivo |
|------|----------|--------|
| Base de datos | Azure SQL Database vCore serverless, 1 vCore, auto-pause 60 min | CDC funciona en cualquier tier vCore. En DTU exige S3 o superior; Basic, S0, S1 y S2 no lo soportan |
| Transporte | Job que lee las tablas `_CT` y escribe JSON a un Volume | Deja visibles Auto Loader y AUTO CDC. La alternativa, el conector gestionado Lakeflow Connect, oculta esa parte |
| Infra Azure | `az` CLI | Son pocos recursos. Terraform es opcional |
| Recursos Databricks | Databricks Asset Bundles | Un `deploy` crea pipeline y job |
| Cómputo | Serverless | No consume quota de vCPU de la suscripción |
| Autenticación | CLI de Databricks con `DATABRICKS_AUTH_TYPE=azure-cli` | Reutiliza `az login`, sin tokens |

## Requisitos

- Suscripción de Azure que no sea Free Trial (Databricks no se puede crear en Free Trial).
- Providers registrados: `Microsoft.Databricks` y `Microsoft.Sql`.
- Herramientas: Azure CLI, Databricks CLI, `sqlcmd` (go-sqlcmd) y Python 3.
- Sesión iniciada con `az login`.

Versiones usadas: Azure CLI 2.87.0, Databricks CLI 1.12.1, Python 3.13.

## Paso 1. Workspace de Databricks

```bash
az extension add -n databricks --yes
az group create -n rg-lakeflow-cdc-demo -l eastus2
az databricks workspace create -g rg-lakeflow-cdc-demo -n dbw-lakeflow-cdc -l eastus2 --sku premium \
  --query "{state:provisioningState,url:workspaceUrl}" -o json
```

Guardar la URL que devuelve. Configurar la CLI de Databricks:

```bash
export DATABRICKS_HOST=https://<workspaceUrl>
export DATABRICKS_AUTH_TYPE=azure-cli
databricks current-user me          # debe responder tu usuario
databricks metastores current       # debe mostrar un metastore y default_catalog_name
```

El workspace trae Unity Catalog automático. El catálogo por defecto se llama como el workspace con guiones bajos (`dbw_lakeflow_cdc`). El resto de la guía usa ese nombre.

## Paso 2. Azure SQL con base serverless

**Problema encontrado:** `eastus2` rechazó crear el servidor con `RegionDoesNotAllowProvisioning`. Se usó `centralus` para SQL. El workspace puede quedarse en `eastus2`, el tráfico de la demo es mínimo.

```bash
SRV=sql-lakeflow-cdc-$RANDOM
PW="Lk$(openssl rand -hex 8)!Aa"

az sql server create -g rg-lakeflow-cdc-demo -n $SRV -l centralus -u sqladmin -p "$PW"

az sql db create -g rg-lakeflow-cdc-demo -s $SRV -n cdcdemo \
  --edition GeneralPurpose --compute-model Serverless --family Gen5 \
  --capacity 1 --min-capacity 0.5 --auto-pause-delay 60 \
  --max-size 2GB --backup-storage-redundancy Local

# Firewall: servicios de Azure (incluye el cómputo serverless) y tu IP
az sql server firewall-rule create -g rg-lakeflow-cdc-demo -s $SRV -n AllowAzure \
  --start-ip-address 0.0.0.0 --end-ip-address 0.0.0.0
MYIP=$(curl -s https://api.ipify.org)
az sql server firewall-rule create -g rg-lakeflow-cdc-demo -s $SRV -n myip \
  --start-ip-address $MYIP --end-ip-address $MYIP
```

Guardar las credenciales en un `.env` local, que no se sube a ningún repositorio:

```
SQL_SERVER=<servidor>.database.windows.net
SQL_DB=cdcdemo
SQL_USER=sqladmin
SQL_PASSWORD=<password>
```

La regla `0.0.0.0` permite a cualquier servicio de Azure llegar al servidor. Es aceptable para una demo y no para producción.

## Paso 3. Tabla fuente y CDC

Archivo `sql/00_schema_cdc.sql`:

```sql
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
```

Ejecutar, cargar el lote 1 y comprobar que CDC capturó:

```bash
set -a; . ./.env; set +a
sqlcmd -S "$SQL_SERVER" -d "$SQL_DB" -U "$SQL_USER" -P "$SQL_PASSWORD" -i sql/00_schema_cdc.sql
sqlcmd -S "$SQL_SERVER" -d "$SQL_DB" -U "$SQL_USER" -P "$SQL_PASSWORD" -i sql/batch1_initial.sql
sqlcmd -S "$SQL_SERVER" -d "$SQL_DB" -U "$SQL_USER" -P "$SQL_PASSWORD" -h -1 \
  -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM cdc.dbo_customers_CT"    # 20
```

No hace falta SQL Server Agent: en Azure SQL Database un scheduler interno captura los cambios.

`sql/batch1_initial.sql` inserta 20 clientes (ids 1 a 20) repartidos en cinco ciudades (Lima, Bogota, Quito, Santiago, Montevideo), todos con `status = 'active'`:

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
GO
```

## Paso 4. Base en Databricks: schema, Volume y secretos

```bash
C=dbw_lakeflow_cdc
databricks schemas create cdc_demo $C
databricks volumes create $C cdc_demo landing MANAGED

databricks secrets create-scope sql_demo
databricks secrets put-secret sql_demo host     --string-value "$SQL_SERVER"
databricks secrets put-secret sql_demo db       --string-value "$SQL_DB"
databricks secrets put-secret sql_demo user     --string-value "$SQL_USER"
databricks secrets put-secret sql_demo password --string-value "$SQL_PASSWORD"
```

## Paso 5. Extractor incremental

Archivo `src/extract_cdc.py`. Lee solo los cambios posteriores al último LSN procesado, los escribe como JSON al Volume y avanza el watermark.

```python
"""Incremental CDC extractor: Azure SQL cdc.dbo_customers_CT -> JSON files in a Unity Catalog Volume."""
from datetime import datetime, timezone

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

CATALOG, SCHEMA = "dbw_lakeflow_cdc", "cdc_demo"
LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/landing/customers"
WATERMARK = f"{CATALOG}.{SCHEMA}._cdc_watermark"

spark = SparkSession.builder.getOrCreate()
secrets = DBUtils(spark).secrets

spark.sql(f"CREATE TABLE IF NOT EXISTS {WATERMARK} (last_lsn STRING, updated_at TIMESTAMP)")
row = spark.sql(f"SELECT last_lsn FROM {WATERMARK} ORDER BY updated_at DESC LIMIT 1").first()
last_lsn = row.last_lsn if row else None

lower = f"AND c.__$start_lsn > CONVERT(binary(10), '{last_lsn}', 1)" if last_lsn else ""
# __$operation: 1 = delete, 2 = insert, 3 = update (before image, skipped), 4 = update (after image)
query = f"""(
  SELECT CONVERT(varchar(22), c.__$start_lsn, 1) AS lsn,
         CONVERT(varchar(22), c.__$seqval, 1)    AS seqval,
         CASE c.__$operation WHEN 1 THEN 'D' WHEN 2 THEN 'I' ELSE 'U' END AS op,
         c.customer_id, c.name, c.email, c.city, c.status, c.updated_at,
         sys.fn_cdc_map_lsn_to_time(c.__$start_lsn) AS commit_time
  FROM cdc.dbo_customers_CT c
  WHERE c.__$operation IN (1, 2, 4)
    AND c.__$start_lsn <= sys.fn_cdc_get_max_lsn()
    {lower}
) q"""

df = (
    spark.read.format("sqlserver")
    .option("host", secrets.get("sql_demo", "host"))
    .option("port", "1433")
    .option("database", secrets.get("sql_demo", "db"))
    .option("user", secrets.get("sql_demo", "user"))
    .option("password", secrets.get("sql_demo", "password"))
    .option("dbtable", query)
    .load()
)

# Write first, then derive count and watermark from what was actually written
# (serverless has no cache(), and a second read from SQL could see newer changes).
batch_path = f"{LANDING}/batch_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}"
df.coalesce(1).write.mode("overwrite").json(batch_path)
try:
    written = spark.read.json(batch_path)
    n = written.count()
except AnalysisException:  # no rows written, nothing to infer a schema from
    n = 0
if n == 0:
    print("No new changes.")
else:
    new_lsn = written.agg(F.max("lsn")).first()[0]  # fixed-width hex string, sorts like the binary LSN
    spark.sql(f"INSERT INTO {WATERMARK} VALUES ('{new_lsn}', current_timestamp())")
    print(f"Wrote {n} change rows to {batch_path}, watermark -> {new_lsn}")
```

Puntos a explicar a la audiencia:
- Los updates generan dos filas en la tabla `_CT` (imagen anterior y posterior). Solo se conserva la posterior.
- El LSN se convierte a texto hexadecimal de ancho fijo, por eso se puede comparar como cadena.
- `sys.fn_cdc_get_max_lsn()` evita leer cambios que SQL todavía está consolidando.

## Paso 6. Pipeline declarativo (SQL)

Tres archivos en `src/pipeline/`.

`01_bronze.sql`

```sql
-- Bronze: raw CDC rows, ingested incrementally with Auto Loader
CREATE OR REFRESH STREAMING TABLE customers_bronze
COMMENT 'Raw CDC rows from Azure SQL dbo.customers'
AS SELECT
  *,
  _metadata.file_path AS source_file,
  current_timestamp() AS ingested_at
FROM STREAM read_files(
  '/Volumes/dbw_lakeflow_cdc/cdc_demo/landing/customers/',
  format => 'json'
);
```

`02_silver.sql`

```sql
-- Silver: typed and validated rows, then AUTO CDC into SCD1 (current) and SCD2 (history)
CREATE OR REFRESH STREAMING TABLE customers_clean (
  CONSTRAINT id_not_null EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT email_valid EXPECT (op = 'D' OR email LIKE '%@%') ON VIOLATION DROP ROW
)
COMMENT 'Typed CDC rows that passed data quality checks'
AS SELECT
  CAST(customer_id AS INT) AS customer_id,
  name, email, city, status, op,
  CAST(commit_time AS TIMESTAMP) AS commit_time
FROM STREAM(customers_bronze);

-- SCD Type 1: one row per customer, latest state; deletes remove the row
CREATE OR REFRESH STREAMING TABLE customers_current;

CREATE FLOW customers_current_flow AS AUTO CDC INTO customers_current
FROM STREAM(customers_clean)
KEYS (customer_id)
APPLY AS DELETE WHEN op = 'D'
SEQUENCE BY commit_time
COLUMNS * EXCEPT (op)
STORED AS SCD TYPE 1;

-- SCD Type 2: a new version row each time city or status changes
CREATE OR REFRESH STREAMING TABLE customers_history;

CREATE FLOW customers_history_flow AS AUTO CDC INTO customers_history
FROM STREAM(customers_clean)
KEYS (customer_id)
APPLY AS DELETE WHEN op = 'D'
SEQUENCE BY commit_time
COLUMNS * EXCEPT (op)
STORED AS SCD TYPE 2
TRACK HISTORY ON city, status;
```

`03_gold.sql`

```sql
-- Gold: business-ready aggregates
CREATE OR REFRESH MATERIALIZED VIEW customers_by_city
COMMENT 'Active customers per city'
AS SELECT city, COUNT(*) AS active_customers
FROM customers_current
WHERE status = 'active'
GROUP BY city;

CREATE OR REFRESH MATERIALIZED VIEW customers_with_changes
COMMENT 'Customers whose city or status changed at least once'
AS SELECT customer_id, COUNT(*) AS versions
FROM customers_history
GROUP BY customer_id
HAVING COUNT(*) > 1;
```

Límite conocido: `SEQUENCE BY commit_time` empata si el mismo cliente cambia dos veces dentro del mismo milisegundo. Para producción se usa una estructura con el LSN y `seqval` como desempate.

## Paso 7. Bundle: pipeline y job

Archivo `databricks.yml` en la raíz:

```yaml
bundle:
  name: lakeflow_cdc_demo

workspace:
  host: https://<workspaceUrl>

resources:
  pipelines:
    customers_pipeline:
      name: customers_cdc_pipeline
      catalog: dbw_lakeflow_cdc
      schema: cdc_demo
      serverless: true
      libraries:
        - file:
            path: src/pipeline/01_bronze.sql
        - file:
            path: src/pipeline/02_silver.sql
        - file:
            path: src/pipeline/03_gold.sql

  jobs:
    customers_cdc_job:
      name: customers_cdc_job
      environments:
        - environment_key: default
          spec:
            environment_version: "3"
      tasks:
        - task_key: extract
          environment_key: default
          spark_python_task:
            python_file: src/extract_cdc.py
        - task_key: refresh_pipeline
          depends_on:
            - task_key: extract
          pipeline_task:
            pipeline_id: ${resources.pipelines.customers_pipeline.id}
```

```bash
databricks bundle validate
databricks bundle deploy
databricks bundle run customers_cdc_job      # tarda unos 2 minutos
```

## Paso 8. Ver resultados con SQL desde la terminal

Script `scripts/q.py`: ejecuta una sentencia en el SQL warehouse serverless y muestra las filas. Hay que poner el id de tu warehouse (`databricks warehouses list`).

```python
"""Run one SQL statement on the demo serverless warehouse and print rows. Usage: python scripts/q.py "SELECT 1" """
import json
import os
import subprocess
import sys

WAREHOUSE = "<warehouse id>"
HOST = "https://<workspaceUrl>"
env = {**os.environ, "DATABRICKS_HOST": HOST, "DATABRICKS_AUTH_TYPE": "azure-cli"}
body = {"warehouse_id": WAREHOUSE, "statement": sys.argv[1], "wait_timeout": "50s"}
out = subprocess.run(
    ["databricks", "api", "post", "/api/2.0/sql/statements", "--json", json.dumps(body)],
    capture_output=True, text=True, env=env, shell=os.name == "nt",
)
res = json.loads(out.stdout)
if res["status"]["state"] != "SUCCEEDED":
    sys.exit(json.dumps(res["status"], indent=2))
cols = [c["name"] for c in res["manifest"]["schema"]["columns"]]
print(" | ".join(cols))
for row in res.get("result", {}).get("data_array", []):
    print(" | ".join("" if v is None else str(v) for v in row))
```

## Paso 9. Guion de la demo

Cada lote sigue el mismo ciclo: cambiar datos en SQL, esperar unos segundos a que CDC los capture, ejecutar el job y consultar. El script `scripts/lote.sh N` hace los tres primeros pasos; el guion completo con qué decir en cada momento está en la Parte I.

Scripts de apoyo (están en `scripts/`):

| Script | Para qué |
|--------|----------|
| `lote.sh 1\|2\|3` | Aplica el lote en SQL, espera a CDC y ejecuta el job |
| `reset.sh` | Deja la demo en blanco: borra job, pipeline, tablas, archivos del Volume y datos de SQL, y vuelve a desplegar. Conserva los recursos de Azure, el schema, el Volume y los secretos |
| `s.sh "<T-SQL>"` | Ejecuta una consulta en Azure SQL sin escribir credenciales |
| `q.py "<SQL>"` | Ejecuta una consulta en Databricks (SQL warehouse serverless) |

`reset.sh` borra las tablas del pipeline de forma explícita porque `bundle destroy` las dejó atrás en la prueba.

### Lote 1. Carga inicial

Ya ejecutado en los pasos 3 y 7. Resultado verificado:

| Tabla | Filas |
|-------|-------|
| customers_bronze | 20 |
| customers_current | 20 |
| customers_history | 20 |
| customers_by_city | 4 clientes activos por cada una de las 5 ciudades |

### Lote 2. Updates, deletes e inserts

`sql/batch2_changes.sql`

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
GO
```

Resultado verificado: bronze 30, current 21 (20 menos 2 borrados más 3 nuevos), history 28 (20 originales, 5 versiones nuevas por updates y 3 inserts).

Consulta para mostrar SCD2 en vivo:

```sql
SELECT customer_id, city, status, __START_AT, __END_AT
FROM dbw_lakeflow_cdc.cdc_demo.customers_history
WHERE customer_id IN (1, 3, 19)
ORDER BY customer_id, __START_AT;
```

El cliente 1 tiene dos versiones (Lima y luego Bogota), el 3 cambia de estado, y el 19, borrado, queda con su versión cerrada por `__END_AT`.

### Lote 3. Dato inválido y Expectations

`sql/batch3_bad_data.sql`

```sql
INSERT INTO dbo.customers (customer_id, name, email, city, status) VALUES
(24, N'Cliente Invalido', N'correo-sin-arroba', N'Lima', N'active');
GO
```

Resultado verificado: bronze sube a 31 y `customers_current` se queda en 21, porque la Expectation `email_valid` descarta la fila. Las métricas salen del event log del pipeline:

```sql
SELECT details:flow_progress.data_quality.expectations
FROM event_log('<pipeline_id>')
WHERE event_type = 'flow_progress'
  AND details:flow_progress.data_quality.expectations IS NOT NULL
ORDER BY timestamp DESC LIMIT 1;
```

Resultado: `email_valid` con `failed_records: 1`.

Para obtener el `pipeline_id`: `databricks pipelines list-pipelines`.

### Genie Code (manual, en la UI)

Este paso no se automatizó: se hace en el editor de pipelines del workspace. Ideas de prompts:

- "Explica qué hace este pipeline y cómo fluye un DELETE desde bronze hasta customers_current."
- "Agrega una Expectation que descarte clientes con city nula."
- Romper a propósito `02_silver.sql` (por ejemplo, un nombre de columna mal escrito) y pedirle que depure el error del pipeline.

## Problemas encontrados

| Problema | Causa | Solución |
|----------|-------|----------|
| `RegionDoesNotAllowProvisioning` al crear el servidor SQL | `eastus2` no aceptaba servidores nuevos en esta suscripción | Crear el servidor en `centralus` |
| `NOT_SUPPORTED_WITH_SERVERLESS: PERSIST TABLE` | `DataFrame.cache()` no existe en serverless | Escribir primero y calcular conteo y watermark leyendo lo escrito |
| `az databricks` pide instalar extensión y falla en modo no interactivo | Extensión no instalada | `az extension add -n databricks --yes` |
| `q.py` devolvía JSON vacío | La CLI de Databricks no tenía `DATABRICKS_HOST` en el entorno del subproceso | Fijar el host en el `env` del script |

## Costo y limpieza

- SQL serverless: factura solo mientras está activa. Con auto-pause de 60 minutos, la primera conexión tras una pausa tarda en reanudar.
- No se verificó si el scheduler interno de CDC impide el auto-pause. Si la base no pausa, destruirla al terminar la sesión.
- Databricks: el workspace no cobra en reposo. Se paga el cómputo serverless de cada ejecución y del warehouse mientras está encendido.
- Estimación gruesa: pocos USD por el día completo. Confirmar en Cost Management.

Para repetir la demo sin borrar la infraestructura: `bash scripts/reset.sh`.

Limpieza total (borra todo, incluida la base de datos):

```bash
az group delete -n rg-lakeflow-cdc-demo --yes --no-wait
```

El workspace crea además un resource group administrado propio, que Azure elimina junto con el workspace.
