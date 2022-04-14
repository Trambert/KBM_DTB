# Databricks notebook source
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

# Paramètres
schema_input = 'adls_input'
schema_output = 'od'
date_calcul = dbutils.widgets.get("date_calcul")

# COMMAND ----------

# Création de la table des résultats
create_voyages = sql.SQL('''
CREATE TABLE IF NOT EXISTS {schema}.voyages (
	id_de_validation bigint,
	tcn text,
    card text,
    ticket text,
	date_de_validation timestamp,
	code_produit text,
	type_de_validation text,
	source_validation text,
	code_vehicule integer,
	code_arret_montee text,
	code_arret_descente text,
	ligne text,
    sens text,
	numero_course integer,
	id_deplacement text,
	premier_voyage bool,
	dernier_voyage bool,
	created_at timestamp with time zone,
	updated_at timestamp with time zone
) PARTITION BY RANGE (date_de_validation);
''').format(schema = sql.Identifier(schema_output))

# COMMAND ----------

#Création de la partition
from dateutil.relativedelta import *
from datetime import date

create_partition = sql.SQL('''
CREATE TABLE IF NOT EXISTS {schema}.{voyages_yyyymm} PARTITION OF {schema}.voyages
    FOR VALUES FROM (%s) TO (%s);
''')

from_date = date.fromisoformat(date_calcul).replace(day = 1)
to_date = from_date + relativedelta(months = 1)
  
with conn.cursor() as cur:
  cur.execute(create_voyages)
  cur.execute(create_partition.format(schema = sql.Identifier(schema_output), voyages_yyyymm = sql.Identifier('voyages_' + from_date.strftime("%Y%m"))), (from_date.isoformat(), to_date.isoformat()))
  conn.commit()

# COMMAND ----------

# Algorithme de croisement
algo1 = sql.SQL('''
INSERT INTO {schema_output}.voyages(
  id_de_validation,
  tcn,
  card,
  ticket,
  date_de_validation,
  code_produit,
  type_de_validation,
  source_validation,
  code_vehicule,
  code_arret_montee,
  ligne,
  sens,
  numero_course,
  created_at)
SELECT DISTINCT ON (date_de_validation, id_de_validation)
  id_de_validation,
  tcn,
  card,
  ticket,
  date_de_validation,
  code_produit,
  type_de_validation,
  source AS source_validation,
  rv.code_sae_affretes AS code_vehicule,
  arret_jhp AS code_arret_montee,
  ligne,
  sens,
  numero_course,
  now() AS created_at
FROM {schema_input}.billettique b
LEFT JOIN {schema_input}.referentiel_vehicules rv
  ON (source = 'Thales' AND b.code_vehicule = rv.code_thales)
	OR (source = 'Vix' AND b.code_vehicule = rv.code_vix)
LEFT JOIN {schema_input}.sae s
  ON rv.code_sae_affretes = s.code_vehicule
    AND date_de_validation > heure_arrivee_reelle - interval '20 minute'
    AND date_de_validation < heure_arrivee_reelle + interval '2 minute'
    AND date_exploitation BETWEEN date(%(date)s) - interval '1 day' AND date(%(date)s) + interval '1 day'
WHERE date_de_validation >= date(%(date)s) AND date_de_validation < date(%(date)s) + interval '1 day'
ORDER BY
  date_de_validation,
  id_de_validation,
  CASE
    WHEN date_de_validation >= heure_arrivee_reelle THEN date_de_validation - heure_arrivee_reelle
	WHEN date_de_validation < heure_arrivee_reelle THEN 10 * (heure_arrivee_reelle - date_de_validation)
  END;
''').format(schema_input = sql.Identifier(schema_input), schema_output = sql.Identifier(schema_output))

# COMMAND ----------

with conn.cursor() as cur:
  cur.execute(algo1, {'date': date_calcul})
  conn.commit()

# COMMAND ----------

#algo1.as_string(conn)
#conn.rollback()

# COMMAND ----------

conn.close()
