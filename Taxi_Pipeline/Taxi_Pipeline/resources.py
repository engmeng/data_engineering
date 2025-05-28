from dagster_duckdb import DuckDBResource
from dagster_azure.adls2 import ADLS2Resource,ADLS2SASToken
import dagster as dg

# database_resource = DuckDBResource(
#     database="data/staging/data.duckdb"
# )

database_resource = DuckDBResource(
    database=dg.EnvVar("DUCKDB_DATABASE")
)

# Technically can use connection strings as well
# But we will be doing it directly using the storage accounts
adls2_token_resource = ADLS2Resource(
    storage_account = "dataoperations002",
    credential= ADLS2SASToken(token=dg.EnvVar("SAS_TOKEN")),
)

