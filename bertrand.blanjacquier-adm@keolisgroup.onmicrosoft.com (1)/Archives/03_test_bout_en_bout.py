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
# ajouter des dates de début et de fin

# COMMAND ----------

sql_billettique_journaliere = sql.SQL('''
CREATE TABLE {}.billettique_journaliere AS
SELECT count(*) AS validations, count(distinct id_de_validation) AS users, date_de_validation::date AS date, type_de_validation, code_produit
FROM {}.billettique
GROUP BY 3, 4, 5
''').format(sql.Identifier(schema_cible), sql.Identifier(schema_input))

# COMMAND ----------

with conn.cursor() as cur:
  cur.execute(sql_billettique_journaliere)

# COMMAND ----------

conn.close()
