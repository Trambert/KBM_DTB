# Databricks notebook source


# COMMAND ----------

# Connexion à la base de données
import psycopg2
from psycopg2 import sql

host = "dkbmkmaeuwpgdtb.postgres.database.azure.com"
port = '5432'
password = dbutils.secrets.get(scope = "p-kbm-kma-euw-kv", key = "p-kbm-db2psql-pw")
user = dbutils.secrets.get(scope = "p-kbm-kma-euw-kv", key = "p-kbm-db2psql-uid")
dbname = 'dev'

conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
print('Connexion réussie')

# COMMAND ----------

# Paramètres Data factory
schema_cible = 'test2'
schema_input = 'adls_input'
date = '20190101'
# ajouter des dates de début et de fin

# COMMAND ----------

sql_voyages = sql.SQL('''
CREATE TABLE IF NOT EXISTS {schema_cible}.voyages (
carte bigint,
date_de_validation timestamp,
code_produit text,
type_de_validation text,
source_validation text,
code_vehicule text,
code_arret_montee text,
code_arret_descente text,
ligne text,
numero_course text,
id_deplacement text,
premier_voyage bool,
dernier_voyage bool,
created_at timestamp with time zone,
updated_at timestamp with time zone
) PARTITION BY RANGE (date_de_validation)
''').format(sql.Identifier(schema_cible))

# COMMAND ----------

with conn.cursor() as cur:
  cur.execute(sql_voyages)

# COMMAND ----------



# COMMAND ----------

conn.close()
