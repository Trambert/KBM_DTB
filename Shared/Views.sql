-- Databricks notebook source
-- MAGIC %sql 
-- MAGIC 
-- MAGIC --v_ref_topo_chainage_bus
-- MAGIC 
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
-- MAGIC       point_chaine pc,
-- MAGIC       point pt,
-- MAGIC       ligne lg
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

--v_ref_topo_chainage_tram
SELECT
  ptc.code_ligne,
  ptc.sens_parcours SENS,
  ptc.no_chainage CODE_PARCOURS,
  ptc.nom_chainage LIBELLE_PARCOURS,
  ptc.NOM_POINT ARRET_ORIG,
  ptc.NO_ARRET NO_ARRET_ORIG,
  ptc.X X_ARRET_ORIG,
  ptc.Y Y_ARRET_ORIG,
  ptc.ordre_point ORDRE_POINT,
  pt.NOM_POINT ARRET_PCTD,
  pt.no_arret no_arret_pctd,
  pt.X X_ARRET_PCTD,
  pt.Y Y_ARRET_PCTD
FROM
  (
    SELECT
      slg.code_ligne,
      lg.id_ligne,
      pt.no_periode_topo,
      lg.mnemo_ligne,
      lg.no_ligne,
      pc.no_chainage,
      pc.sens_parcours,
      pc.id_point_chaine,
      pc.ordre_point,
      pc.id_point,
      ch.nom_chainage,
      pt.NOM_POINT,
      pt.NO_ARRET,
      pt.X,
      pt.Y,
      lag(pc.id_point, 1, 0) over(
        partition BY pc.id_ligne,
        pc.no_chainage,
        pc.sens_parcours
        order by
          pc.ordre_point
      ) ID_POINT_PCDT,
      pt.mnemo_long MNEMO_ORIG,
      d.DESTINATION_AFFICHEUR_BUS,
      CASE
        WHEN (NULL IS NULL) THEN 'm'
      end as unite_distance
    FROM
      (
        SELECT
          DISTINCT CASE
            WHEN (MNEMO_SOUS_LIGNE LIKE 'A%') THEN 59
            WHEN (MNEMO_SOUS_LIGNE LIKE 'B%') THEN 60
            WHEN (
              MNEMO_SOUS_LIGNE LIKE 'C%'
              OR MNEMO_SOUS_LIGNE LIKE 'TM%'
            ) THEN 61
            WHEN (MNEMO_SOUS_LIGNE LIKE 'D%') THEN 62
            ELSE NULL
          END AS code_ligne,
          ID_LIGNE,
          ID_SOUS_LIGNE
        FROM
          saetram.SOUS_LIGNE
      ) slg,
      saetram.point_chaine pc,
      saetram.point pt,
      saetram.ligne lg,
      saetram.topologie t,
      saetram.periode_horaire ph,
      saetram.ref_calendrier c,
      saetram.DESTINATION d,
      saetram.chainage ch
    WHERE
      pc.id_point = pt.id_point
      AND pt.no_periode_topo = lg.no_periode_topo
      AND pc.id_ligne = lg.id_ligne
      and pt.no_periode_topo = t.no_periode_topo
      and ph.no_periode_topo = t.no_periode_topo
      and c.no_periode_horaire = ph.no_periode_horaire
      and d.id_destination = ch.id_destination
      and pc.no_chainage = ch.no_chainage
      AND pc.SENS_PARCOURS = ch.SENS_PARCOURS
      AND pc.ID_LIGNE = ch.ID_LIGNE
      AND ch.ID_SOUS_LIGNE = slg.ID_SOUS_LIGNE
      and c.date_calendrier = '2022-02-11T00:00:00.000+0000'
  ) ptc left outer join saetram.inter_point ipt on ptc.id_point_pcdt = ipt.id_point_orig and  ptc.id_point = ipt.id_point_extr
  left outer join saetram.point pt on ptc.id_point_pcdt = pt.id_point
ORDER BY
  id_ligne,
  no_chainage,
  sens_parcours,
  ordre_point

-- COMMAND ----------

CREATE OR REPLACE VIEW saebus.v_ref_topo_chainage_bus
    AS SELECT
  ptc.no_periode_topo,
  ptc.id_ligne,
  ptc.mnemo_ligne,
  ptc.no_ligne,
  ptc.no_chainage,
  ptc.sens_parcours,
  ptc.ordre_point,
  ptc.id_point_pcdt,
  pt.mnemo_long mnemo_pcdt,
  ptc.id_point,
  ptc.mnemo_orig,
  NVL(ipt.distance, 0) distance_inter_point,
  NVL(
    SUM(ipt.distance) over(
      partition BY ptc.no_periode_topo,
      ptc.id_ligne,
      ptc.no_chainage,
      ptc.sens_parcours
      order by
        ptc.ordre_point RANGE UNBOUNDED PRECEDING
    ),
    0
  ) DISTANCE_CUMULEE,
  COUNT(ptc.id_point) over(
    partition BY ptc.no_periode_topo,
    ptc.id_ligne,
    ptc.no_chainage,
    ptc.sens_parcours
  ) NB_POINTS_TOPO_CHAINAGE,
  COUNT(ptc.id_point) over(partition BY ptc.no_periode_topo) NB_POINTS_TOPO_TOTAL
FROM
  (
    SELECT
      lg.id_ligne,
      pt.no_periode_topo,
      lg.mnemo_ligne,
      lg.no_ligne,
      no_chainage,
      sens_parcours,
      id_point_chaine,
      ordre_point,
      pc.id_point,
      lag(pc.id_point, 1, 0) over(
        partition BY pc.id_ligne,
        pc.no_chainage,
        pc.sens_parcours
        order by
          pc.ordre_point
      ) ID_POINT_PCDT,
      pt.mnemo_long MNEMO_ORIG
    FROM
      point_chaine pc,
      point pt,
      ligne lg
    WHERE
      pc.id_point = pt.id_point
      AND pt.no_periode_topo = lg.no_periode_topo
      AND pc.id_ligne = lg.id_ligne
  ) ptc
  left outer join inter_point ipt on ptc.id_point_pcdt = ipt.id_point_orig
  and ptc.id_point = ipt.id_point_extr
  left outer join point pt on ptc.id_point_pcdt = pt.id_point
ORDER BY
  id_ligne,
  no_chainage,
  sens_parcours,
  ordre_point

-- COMMAND ----------

CREATE OR REPLACE VIEW saetram.v_ref_topo_chainage_tram
    AS SELECT
  ptc.code_ligne,
  ptc.sens_parcours SENS,
  ptc.no_chainage CODE_PARCOURS,
  ptc.nom_chainage LIBELLE_PARCOURS,
  ptc.NOM_POINT ARRET_ORIG,
  ptc.NO_ARRET NO_ARRET_ORIG,
  ptc.X X_ARRET_ORIG,
  ptc.Y Y_ARRET_ORIG,
  ptc.ordre_point ORDRE_POINT,
  pt.NOM_POINT ARRET_PCTD,
  pt.no_arret no_arret_pctd,
  pt.X X_ARRET_PCTD,
  pt.Y Y_ARRET_PCTD
FROM
  (
    SELECT
      slg.code_ligne,
      lg.id_ligne,
      pt.no_periode_topo,
      lg.mnemo_ligne,
      lg.no_ligne,
      pc.no_chainage,
      pc.sens_parcours,
      pc.id_point_chaine,
      pc.ordre_point,
      pc.id_point,
      ch.nom_chainage,
      pt.NOM_POINT,
      pt.NO_ARRET,
      pt.X,
      pt.Y,
      lag(pc.id_point, 1, 0) over(
        partition BY pc.id_ligne,
        pc.no_chainage,
        pc.sens_parcours
        order by
          pc.ordre_point
      ) ID_POINT_PCDT,
      pt.mnemo_long MNEMO_ORIG,
      d.DESTINATION_AFFICHEUR_BUS,
      CASE
        WHEN (NULL IS NULL) THEN 'm'
      end as unite_distance
    FROM
      (
        SELECT
          DISTINCT CASE
            WHEN (MNEMO_SOUS_LIGNE LIKE 'A%') THEN 59
            WHEN (MNEMO_SOUS_LIGNE LIKE 'B%') THEN 60
            WHEN (
              MNEMO_SOUS_LIGNE LIKE 'C%'
              OR MNEMO_SOUS_LIGNE LIKE 'TM%'
            ) THEN 61
            WHEN (MNEMO_SOUS_LIGNE LIKE 'D%') THEN 62
            ELSE NULL
          END AS code_ligne,
          ID_LIGNE,
          ID_SOUS_LIGNE
        FROM
          saetram.SOUS_LIGNE
      ) slg,
      saetram.point_chaine pc,
      saetram.point pt,
      saetram.ligne lg,
      saetram.topologie t,
      saetram.periode_horaire ph,
      saetram.ref_calendrier c,
      saetram.DESTINATION d,
      saetram.chainage ch
    WHERE
      pc.id_point = pt.id_point
      AND pt.no_periode_topo = lg.no_periode_topo
      AND pc.id_ligne = lg.id_ligne
      and pt.no_periode_topo = t.no_periode_topo
      and ph.no_periode_topo = t.no_periode_topo
      and c.no_periode_horaire = ph.no_periode_horaire
      and d.id_destination = ch.id_destination
      and pc.no_chainage = ch.no_chainage
      AND pc.SENS_PARCOURS = ch.SENS_PARCOURS
      AND pc.ID_LIGNE = ch.ID_LIGNE
      AND ch.ID_SOUS_LIGNE = slg.ID_SOUS_LIGNE
      and c.date_calendrier = '2022-02-11T00:00:00.000+0000'
  ) ptc left outer join saetram.inter_point ipt on ptc.id_point_pcdt = ipt.id_point_orig and  ptc.id_point = ipt.id_point_extr
  left outer join saetram.point pt on ptc.id_point_pcdt = pt.id_point
ORDER BY
  id_ligne,
  no_chainage,
  sens_parcours,
  ordre_point

-- COMMAND ----------

SELECT DISTINCT
l.MNEMO_LIGNE_COMMERCIAL AS CODE_LIGNE,
rtc.NO_CHAINAGE as CODE_PARCOURS,
rtc.SENS_PARCOURS AS SENS, 
c.NOM_CHAINAGE AS LIBELLE_PARCOURS,
p1.NOM_POINT  AS arret_pctd,
rtc.ORDRE_POINT,
p1.NO_ARRET AS no_arret_pctd,
p1.X AS x_arret_pctd,
p1.Y  AS y_arret_pctd,
p2.NOM_POINT  AS arret_orig,
p2.NO_ARRET AS no_arret_orig,
p2.X AS x_arret_orig,
p2.Y AS y_arret_orig,
rtc.NB_POINTS_TOPO_CHAINAGE

FROM saebus.V_REF_TOPO_CHAINAGE rtc
LEFT JOIN saebus.CHAINAGE c ON (c.ID_LIGNE =rtc.ID_LIGNE AND c.SENS_PARCOURS =rtc.SENS_PARCOURS AND c.NO_CHAINAGE =rtc.NO_CHAINAGE )
LEFT JOIN saebus.POINT_FORME_ENTRE_ARRET pfea on(pfea.ID_POINT1 =rtc.ID_POINT_PCDT  AND pfea.ID_POINT2=rtc.ID_POINT )
LEFT JOIN saebus.COORD_PF_ENTRE_ARRET cpfea ON (CPFEA.ID_COORD =PFEA.ID_COORD )
LEFT JOIN saebus.POINT p1 ON (p1.ID_POINT=rtc.ID_POINT_PCDT)
LEFT JOIN saebus.POINT p2 ON (p2.ID_POINT=rtc.ID_POINT )
LEFT JOIN saebus.LIGNE l ON (l.ID_LIGNE =c.ID_LIGNE )
LEFT JOIN (
SELECT c.DATE_CALENDRIER ,ph.NO_PERIODE_TOPO 
FROM saebus.REF_CALENDRIER c
LEFT JOIN  saebus.PERIODE_HORAIRE ph ON (c.NO_PERIODE_HORAIRE =ph.NO_PERIODE_HORAIRE )
) periode_topo ON (periode_topo.NO_PERIODE_TOPO=rtc.NO_PERIODE_TOPO )


union

SELECT DISTINCT
    ptc.code_ligne,
    ptc.no_chainage CODE_PARCOURS,
    ptc.sens_parcours SENS,
    ptc.nom_chainage LIBELLE_PARCOURS,
    pt.NOM_POINT ARRET_PCTD ,
    ptc.ordre_point ORDRE_POINT,
    pt.no_arret no_arret_pctd,
    pt.X X_ARRET_PCTD,
    pt.Y Y_ARRET_PCTD,
    ptc.NOM_POINT arret_orig ,
    ptc.NO_ARRET no_arret_orig ,
    ptc.X X_ARRET_ORIG,
    ptc.Y Y_ARRET_ORIG,
    COUNT(ptc.id_point) over(partition BY ptc.no_periode_topo, ptc.id_ligne, ptc.no_chainage, ptc.sens_parcours) NB_POINTS_TOPO_CHAINAGE
  FROM
    (SELECT 
      slg.code_ligne,
      lg.id_ligne,
      pt.no_periode_topo,
      lg.mnemo_ligne,
      lg.no_ligne,
      pc.no_chainage,
      pc.sens_parcours,
      pc.id_point_chaine,
      pc.ordre_point,
      pc.id_point,
      ch.nom_chainage,
      pt.NOM_POINT,
      pt.NO_ARRET,
      pt.X,
      pt.Y,
      lag(pc.id_point,1,0) over(partition BY pc.id_ligne, pc.no_chainage, pc.sens_parcours order by pc.ordre_point) ID_POINT_PCDT,
      pt.mnemo_long MNEMO_ORIG,
     d.DESTINATION_AFFICHEUR_BUS,
    CASE WHEN (NULL IS NULL) THEN 'm' end  as unite_distance
    FROM 
		   ( SELECT DISTINCT 
		CASE WHEN (MNEMO_SOUS_LIGNE LIKE 'A%') THEN 59 
		WHEN (MNEMO_SOUS_LIGNE LIKE 'B%') THEN 60
		WHEN (MNEMO_SOUS_LIGNE LIKE 'C%' OR MNEMO_SOUS_LIGNE LIKE 'TM%' ) THEN 61
		WHEN (MNEMO_SOUS_LIGNE LIKE 'D%' ) THEN 62
		
		ELSE NULL END AS code_ligne,ID_LIGNE,ID_SOUS_LIGNE 
		FROM saetram.SOUS_LIGNE ) slg 
        left outer join saetram.chainage ch on ch.ID_SOUS_LIGNE =slg.ID_SOUS_LIGNE
        left outer join saetram.DESTINATION d on d.id_destination=ch.id_destination,    
      saetram.point_chaine pc,
      saetram.point pt,
      saetram.ligne lg,
      saetram.topologie t,
      saetram.periode_horaire ph,
      saetram.REF_calendrier c
	  
    WHERE pc.id_point     = pt.id_point
    AND pt.no_periode_topo=lg.no_periode_topo
    AND pc.id_ligne       =lg.id_ligne
    and pt.no_periode_topo=t.no_periode_topo
    and ph.no_periode_topo=t.no_periode_topo
    and c.no_periode_horaire=ph.no_periode_horaire
    and pc.no_chainage=ch.no_chainage AND pc.SENS_PARCOURS =ch.SENS_PARCOURS AND pc.ID_LIGNE =ch.ID_LIGNE 

    ) ptc left outer join saetram.inter_point ipt on (ptc.id_point_pcdt = ipt.id_point_orig and ptc.id_point=ipt.id_point_extr)
    left outer join saetram.point pt on ptc.id_point_pcdt   = pt.id_point
  

-- COMMAND ----------


