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

# Paramètres (data factory?)
schema = 'adls_input'

# COMMAND ----------

privileges = sql.SQL('''
ALTER DEFAULT PRIVILEGES
  IN SCHEMA {schema}
  GRANT ALL ON TABLES TO dev WITH GRANT OPTION;
ALTER DEFAULT PRIVILEGES
  IN SCHEMA {schema}
  GRANT SELECT ON TABLES TO guest WITH GRANT OPTION;
ALTER DEFAULT PRIVILEGES
  IN SCHEMA {schema}
  GRANT ALL ON SEQUENCES TO dev WITH GRANT OPTION;
ALTER DEFAULT PRIVILEGES
  IN SCHEMA {schema}
  GRANT USAGE ON SEQUENCES TO guest WITH GRANT OPTION;
GRANT ALL ON ALL SEQUENCES IN SCHEMA {schema} TO dev WITH GRANT OPTION;
''').format(schema = sql.Identifier(schema))
with conn.cursor() as cur:
  cur.execute(privileges)
  conn.commit()

# COMMAND ----------

create_referentiel_vehicules = sql.SQL('''
CREATE TABLE IF NOT EXISTS {schema}.referentiel_vehicules
(
code_thales integer,
code_sae_affretes integer,
societe varchar(50),
reseau varchar(50),
depot varchar(50),
annee_mise_en_service integer,
type_vehicule varchar(50),
code_vix integer
)
''').format(schema = sql.Identifier(schema))

# COMMAND ----------

create_comptage = sql.SQL('''
CREATE TABLE IF NOT EXISTS {schema}.comptage
(
date_heure timestamp,
vehicule integer,
ligne varchar(50),
arrets varchar(50),
nb_montees integer,
nb_descentes integer
)
''').format(schema = sql.Identifier(schema))

# COMMAND ----------

create_referentiel_produit = sql.SQL('''
CREATE TABLE IF NOT EXISTS {schema}.referentiel_produit
(
code_produit varchar(50),
designation_du_produit varchar(100),
famille_de_titres_dmci varchar(50),
regroupement_titres_dmci varchar(50),
regroupement_titres_dmci_x varchar(50)
)
''').format(schema = sql.Identifier(schema))

# COMMAND ----------

create_billettique = sql.SQL('''
CREATE SEQUENCE IF NOT EXISTS {schema}.billettique_id_de_validation_seq
  INCREMENT BY -1
  START WITH -1;

CREATE TABLE IF NOT EXISTS {schema}.billettique
(
statut_de_validation varchar(50),
code_vehicule integer,
type_de_validation varchar(50),
code_produit integer,
nom_commercial varchar(50),
date_de_validation timestamp,
id_de_validation bigint NOT NULL DEFAULT nextval('{schema}.billettique_id_de_validation_seq'),
nb_de_validations integer NOT NULL DEFAULT 1,
tcn varchar(255),
card varchar(255),
ticket varchar(50),
source varchar(50),
date_chargement timestamp with time zone,
date_traitement timestamp with time zone
) PARTITION BY RANGE (date_de_validation);

ALTER SEQUENCE {schema}.billettique_id_de_validation_seq
  OWNED BY {schema}.billettique.id_de_validation;
''').format(schema = sql.Identifier(schema))

# COMMAND ----------

create_sae = sql.SQL('''
CREATE TABLE IF NOT EXISTS {schema}.sae
(
id bigint,
date_exploitation date,
sm varchar(50),
code_vehicule integer,
type_vehicule varchar(50),
moteur varchar(50),
transporteur varchar(50),
ligne varchar(50),
numero_service_vehicule integer,
matricule_agent integer,
nom_agent varchar(255),
numero_chainage integer,
sens varchar(10),
libelle_chainage varchar(255),
rang integer,
type_course varchar(10),
arret_jhp varchar(25),
type_arret varchar(10),
defaut varchar(255),
deviation varchar(255),
horaire_estime varchar(50),
heure_depart_theorique timestamp,
heure_depart_reelle timestamp,
heure_depart_appli timestamp,
heure_arrivee_theorique timestamp,
heure_arrivee_reelle timestamp,
heure_arrivee_appli timestamp,
battement_theo_sec integer,
battement_reel_sec integer,
battement_appli_sec integer,
refco_mnemo_sb varchar(50),
recar_tps_passe integer,
recar_hre_arr_theo timestamp,
refco_no_ch_theo integer,
num_course varchar(50),
numero_course integer,
mode varchar(50),
date_chargement timestamp with time zone,
date_traitement timestamp with time zone
) PARTITION BY RANGE (date_exploitation)
''').format(schema = sql.Identifier(schema))

# COMMAND ----------

with conn.cursor() as cur:
  cur.execute(create_referentiel_vehicules)
  cur.execute(create_comptage)
  cur.execute(create_referentiel_produit)
  cur.execute(create_billettique)
  cur.execute(create_sae)
  conn.commit()


# COMMAND ----------

conn.rollback()

# COMMAND ----------

from dateutil.relativedelta import *
from datetime import date

create_billettique_partition = sql.SQL('''
CREATE TABLE IF NOT EXISTS {schema}.{billettique_yyyymm} PARTITION OF {schema}.billettique
    FOR VALUES FROM (%s) TO (%s);
''')
create_sae_partition = sql.SQL('''
CREATE TABLE IF NOT EXISTS {schema}.{sae_yyyymm} PARTITION OF {schema}.sae
    FOR VALUES FROM (%s) TO (%s);
''')

from_date = date(2019,1,1)

with conn.cursor() as cur:
  for x in range(24):
    to_date = from_date + relativedelta(months=1)
    cur.execute(create_billettique_partition.format(schema = sql.Identifier(schema), billettique_yyyymm = sql.Identifier('billettique_' + from_date.strftime("%Y%m"))), (from_date.isoformat(), to_date.isoformat()))
    cur.execute(create_sae_partition.format(schema = sql.Identifier(schema), sae_yyyymm = sql.Identifier('sae_' + from_date.strftime("%Y%m"))), (from_date.isoformat(), to_date.isoformat()))
    from_date = to_date
  conn.commit()

# COMMAND ----------

conn.close()
