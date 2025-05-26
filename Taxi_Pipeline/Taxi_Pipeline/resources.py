from dagster_duckdb import DuckDBResource
import dagster as dg

# database_resource = DuckDBResource(
#     database="data/staging/data.duckdb"
# )

database_resource = DuckDBResource(
    database=dg.EnvVar("DUCKDB_DATABASE")
)
