# Configuration

## Selecting the Iceberg Backend

To use Portolake as your versioning backend, tell Portolan to use the `iceberg` backend:

```bash
# Option 1: Set in catalog config (persists)
portolan config set backend iceberg

# Option 2: Environment variable
export PORTOLAN_BACKEND=iceberg

# Option 3: Set during init
portolan init my-catalog --backend iceberg
```

Once set, all portolan commands will use Portolake's Iceberg backend. See [CLI Commands](cli.md) for how each command behaves with the Iceberg backend.

## Iceberg Catalog Configuration

Portolake uses [PyIceberg's configuration system](https://py.iceberg.apache.org/configuration/) to configure the Iceberg catalog. No custom configuration files or environment variables are needed — everything uses the standard PyIceberg conventions.

## Defaults

With no configuration, Portolake uses a local SQLite catalog:

| Setting | Default |
|---------|---------|
| Catalog type | `sql` (SQLite) |
| Catalog URI | `sqlite:///<cwd>/.portolan/iceberg.db` |
| Warehouse | `file:///<cwd>/.portolan/warehouse` |

This is ideal for local development and single-user workflows.

## Environment Variables

Configure the catalog backend using PyIceberg's environment variable convention:

```
PYICEBERG_CATALOG__PORTOLAKE__<PROPERTY>=<value>
```

The double underscore (`__`) separates the prefix, catalog name, and property.

### Common Properties

| Variable | Description | Example |
|----------|-------------|---------|
| `PYICEBERG_CATALOG__PORTOLAKE__TYPE` | Catalog type | `sql`, `rest`, `glue` |
| `PYICEBERG_CATALOG__PORTOLAKE__URI` | Connection URI | `sqlite:///...`, `https://...` |
| `PYICEBERG_CATALOG__PORTOLAKE__WAREHOUSE` | Data file storage location | `file:///...`, `s3://...` |

## Catalog Backends

Portolake supports all catalog backends provided by PyIceberg.

### SQLite (default)

Zero-configuration local catalog. Good for development and small teams.

```bash
# These are the defaults — no need to set them
export PYICEBERG_CATALOG__PORTOLAKE__TYPE=sql
export PYICEBERG_CATALOG__PORTOLAKE__URI=sqlite:///$(pwd)/.portolan/iceberg.db
export PYICEBERG_CATALOG__PORTOLAKE__WAREHOUSE=file:///$(pwd)/.portolan/warehouse
```

### PostgreSQL

Shared SQL catalog for multi-user teams.

```bash
export PYICEBERG_CATALOG__PORTOLAKE__TYPE=sql
export PYICEBERG_CATALOG__PORTOLAKE__URI=postgresql+psycopg2://user:pass@host:5432/catalog_db
export PYICEBERG_CATALOG__PORTOLAKE__WAREHOUSE=s3://my-bucket/warehouse
```

Requires `pyiceberg[sql-postgres]`:

```bash
pip install "pyiceberg[sql-postgres]"
```

### REST Catalog

Connect to any Iceberg REST catalog server (Tabular, Polaris, Unity Catalog, Nessie).

```bash
export PYICEBERG_CATALOG__PORTOLAKE__TYPE=rest
export PYICEBERG_CATALOG__PORTOLAKE__URI=https://my-catalog.example.com
export PYICEBERG_CATALOG__PORTOLAKE__WAREHOUSE=s3://my-bucket/warehouse
```

### AWS Glue

Native AWS integration using Glue Data Catalog.

```bash
export PYICEBERG_CATALOG__PORTOLAKE__TYPE=glue
export PYICEBERG_CATALOG__PORTOLAKE__WAREHOUSE=s3://my-bucket/warehouse
```

Requires `pyiceberg[glue]`:

```bash
pip install "pyiceberg[glue]"
```

### AWS DynamoDB

Serverless catalog using DynamoDB.

```bash
export PYICEBERG_CATALOG__PORTOLAKE__TYPE=dynamodb
export PYICEBERG_CATALOG__PORTOLAKE__WAREHOUSE=s3://my-bucket/warehouse
```

Requires `pyiceberg[dynamodb]`:

```bash
pip install "pyiceberg[dynamodb]"
```

### Hive Metastore

For Hadoop/on-premise deployments.

```bash
export PYICEBERG_CATALOG__PORTOLAKE__TYPE=hive
export PYICEBERG_CATALOG__PORTOLAKE__URI=thrift://hive-metastore:9083
export PYICEBERG_CATALOG__PORTOLAKE__WAREHOUSE=hdfs:///warehouse
```

Requires `pyiceberg[hive]`:

```bash
pip install "pyiceberg[hive]"
```

### Google BigQuery

GCP-native catalog.

```bash
export PYICEBERG_CATALOG__PORTOLAKE__TYPE=bigquery
export PYICEBERG_CATALOG__PORTOLAKE__WAREHOUSE=gs://my-bucket/warehouse
```

## Configuration Precedence

PyIceberg resolves configuration in this order (highest to lowest):

1. **Environment variables** (`PYICEBERG_CATALOG__PORTOLAKE__*`)
2. **PyIceberg config file** (`~/.pyiceberg.yaml`)
3. **Portolake defaults** (SQLite in `.portolan/`)

## Programmatic Override

You can also pass a catalog directly when creating the backend:

```python
from pyiceberg.catalog import load_catalog
from portolake.backend import IcebergBackend

catalog = load_catalog(
    "my-catalog",
    **{
        "type": "rest",
        "uri": "https://my-catalog.example.com",
        "warehouse": "s3://my-bucket/warehouse",
    },
)
backend = IcebergBackend(catalog=catalog)
```

This is useful for testing or when you need multiple catalogs in the same application.
