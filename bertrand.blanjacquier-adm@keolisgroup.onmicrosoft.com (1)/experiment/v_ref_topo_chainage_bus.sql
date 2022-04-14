-- Databricks notebook source
create database titan_fraude;

-- COMMAND ----------

select count(*) from saebus.V_REF_TOPO_CHAINAGE 43821
--select count(*) from saebus.V_REF_TOPO_CHAINAGE_BUS 44464

-- COMMAND ----------

-- MAGIC %sql --v_ref_topo_chainage_bus
-- MAGIC SELECT
-- MAGIC   ptc.no_periode_topo,
-- MAGIC   ptc.id_ligne,
-- MAGIC   ptc.mnemo_ligne,
-- MAGIC   ptc.no_ligne,
-- MAGIC   ptc.no_chainage,
-- MAGIC   ptc.sens_parcours,
-- MAGIC   ptc.ordre_point,
-- MAGIC   ptc.id_point_pcdt,
-- MAGIC   pt.mnemo_long mnemo_pcdt,
-- MAGIC   ptc.id_point,
-- MAGIC   ptc.mnemo_orig,
-- MAGIC   NVL(ipt.distance, 0) distance_inter_point,
-- MAGIC   NVL(
-- MAGIC     SUM(ipt.distance) over(
-- MAGIC       partition BY ptc.no_periode_topo,
-- MAGIC       ptc.id_ligne,
-- MAGIC       ptc.no_chainage,
-- MAGIC       ptc.sens_parcours
-- MAGIC       order by
-- MAGIC         ptc.ordre_point RANGE UNBOUNDED PRECEDING
-- MAGIC     ),
-- MAGIC     0
-- MAGIC   ) DISTANCE_CUMULEE,
-- MAGIC   COUNT(ptc.id_point) over(
-- MAGIC     partition BY ptc.no_periode_topo,
-- MAGIC     ptc.id_ligne,
-- MAGIC     ptc.no_chainage,
-- MAGIC     ptc.sens_parcours
-- MAGIC   ) NB_POINTS_TOPO_CHAINAGE,
-- MAGIC   COUNT(ptc.id_point) over(partition BY ptc.no_periode_topo) NB_POINTS_TOPO_TOTAL
-- MAGIC FROM
-- MAGIC   (
-- MAGIC     SELECT
-- MAGIC       lg.id_ligne,
-- MAGIC       pt.no_periode_topo,
-- MAGIC       lg.mnemo_ligne,
-- MAGIC       lg.no_ligne,
-- MAGIC       no_chainage,
-- MAGIC       sens_parcours,
-- MAGIC       id_point_chaine,
-- MAGIC       ordre_point,
-- MAGIC       pc.id_point,
-- MAGIC       lag(pc.id_point, 1, 0) over(
-- MAGIC         partition BY pc.id_ligne,
-- MAGIC         pc.no_chainage,
-- MAGIC         pc.sens_parcours
-- MAGIC         order by
-- MAGIC           pc.ordre_point
-- MAGIC       ) ID_POINT_PCDT,
-- MAGIC       pt.mnemo_long MNEMO_ORIG
-- MAGIC     FROM
-- MAGIC       saebus.point_chaine pc,
-- MAGIC       saebus.point pt,
-- MAGIC       saebus.ligne lg
-- MAGIC     WHERE
-- MAGIC       pc.id_point = pt.id_point
-- MAGIC       AND pt.no_periode_topo = lg.no_periode_topo
-- MAGIC       AND pc.id_ligne = lg.id_ligne
-- MAGIC   ) ptc
-- MAGIC   left outer join inter_point ipt on ptc.id_point_pcdt = ipt.id_point_orig
-- MAGIC   and ptc.id_point = ipt.id_point_extr
-- MAGIC   left outer join point pt on ptc.id_point_pcdt = pt.id_point
-- MAGIC ORDER BY
-- MAGIC   id_ligne,
-- MAGIC   no_chainage,
-- MAGIC   sens_parcours,
-- MAGIC   ordre_point

-- COMMAND ----------


