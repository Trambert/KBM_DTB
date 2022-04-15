-- Databricks notebook source
-- %sql --TRAM Frequence



select 
 REFCO_DATE_EXPLOIT,
  REFCO_MNEMO_LG,
  REFPT_MNEMO_LONG,
  PLHOR_MNEMO,
REFCO_SENS_REC,
 RECAR_HRE_ARR_THEO,
HRE_ARR_THEO_seconds,
lag_HRE_ARR_THEO_seconds,
lag_HRE_ARR_REEL_seconds,
INTERVAL_THEO_SECOND, 
INTERVAL_REEL_SECOND,
case when INTERVAL_REEL_SECOND <= INTERVAL_THEO_SECOND + 59 then 1 else  0 end as intervalok59,
case when INTERVAL_REEL_SECOND <= INTERVAL_THEO_SECOND + 119 then 1 else  0 end as intervalok119,
CASE WHEN HEURE_ARRIVEE_RELLE_SECOND>=lag_HRE_ARR_REEL_seconds THEN HEURE_ARRIVEE_RELLE_SECOND-lag_HRE_ARR_REEL_seconds ELSE 
lag_HRE_ARR_REEL_seconds-HEURE_ARRIVEE_RELLE_SECOND END ,
case when INTERVAL_REEL_SECOND is null then 0 else 1 end as NBINTERVELREELSECOND

--   ([Heure Arrivée Réelle (en seconde)]>=[_Vrai précédent réel])
-- Alors([Heure Arrivée Réelle (en seconde)]-[_Vrai précédent réel])
-- Sinon([_Vrai précédent réel]-[Heure Arrivée Réelle (en seconde)])
from 
(
SELECT
  REF_COURSE.REFCO_DATE_EXPLOIT,
  REF_COURSE.REFCO_MNEMO_LG,
  REF_POINT.REFPT_MNEMO_LONG,
  PLAGE_HORAIRE.PLHOR_MNEMO,
 REF_COURSE.REFCO_SENS_REC,
  REC_ARRET.RECAR_HRE_ARR_THEO,
  unix_seconds(REC_ARRET.RECAR_HRE_ARR_THEO) as HRE_ARR_THEO_seconds,
  lag(
    unix_seconds(REC_ARRET.RECAR_HRE_ARR_THEO) ,
    + 1
  ) OVER (
    PARTITION BY REF_COURSE.REFCO_DATE_EXPLOIT,
    REF_COURSE.REFCO_MNEMO_LG,
 --   REF_COURSE.REFCO_SENS_REC,
    REF_POINT.REFPT_MNEMO_LONG,
    PLAGE_HORAIRE.PLHOR_MNEMO,
    REF_COURSE.REFCO_SENS_REC
    ORDER BY
      REF_COURSE.REFCO_DATE_EXPLOIT,
      REF_COURSE.REFCO_MNEMO_LG,
 --     REF_COURSE.REFCO_SENS_REC,
      REF_POINT.REFPT_MNEMO_LONG,
      PLAGE_HORAIRE.PLHOR_MNEMO,
      REC_ARRET.RECAR_HRE_ARR_THEO,
      REC_ARRET.RECAR_ECART_DEP
  ) as lag_HRE_ARR_THEO_seconds,
    lag(
    unix_seconds(REC_ARRET.RECAR_HRE_ARR_THEO)+(REC_ARRET.RECAR_ECART_DEP) ,
    + 1
  ) OVER (
    PARTITION BY REF_COURSE.REFCO_DATE_EXPLOIT,
    REF_COURSE.REFCO_MNEMO_LG,
--    REF_COURSE.REFCO_SENS_REC,
    REF_POINT.REFPT_MNEMO_LONG,
    PLAGE_HORAIRE.PLHOR_MNEMO,
    REF_COURSE.REFCO_SENS_REC
    ORDER BY
      REF_COURSE.REFCO_DATE_EXPLOIT,
      REF_COURSE.REFCO_MNEMO_LG,
      REF_POINT.REFPT_MNEMO_LONG,
      PLAGE_HORAIRE.PLHOR_MNEMO,
  --    REF_COURSE.REFCO_SENS_REC,
      REC_ARRET.RECAR_HRE_ARR_THEO,
      REC_ARRET.RECAR_ECART_DEP
  ) as lag_HRE_ARR_REEL_seconds,
   unix_seconds(REC_ARRET.RECAR_HRE_ARR_THEO)-lag(
    unix_seconds(REC_ARRET.RECAR_HRE_ARR_THEO) ,
    + 1
  ) OVER (
    PARTITION BY REF_COURSE.REFCO_DATE_EXPLOIT,
    REF_COURSE.REFCO_MNEMO_LG,
 --   REF_COURSE.REFCO_SENS_REC,
    REF_POINT.REFPT_MNEMO_LONG,
    PLAGE_HORAIRE.PLHOR_MNEMO
    ORDER BY
      REF_COURSE.REFCO_DATE_EXPLOIT,
      REF_COURSE.REFCO_MNEMO_LG,
      REF_POINT.REFPT_MNEMO_LONG,
      PLAGE_HORAIRE.PLHOR_MNEMO,
--     REF_COURSE.REFCO_SENS_REC,
      REC_ARRET.RECAR_HRE_ARR_THEO,
      REC_ARRET.RECAR_ECART_DEP
  ) as INTERVAL_THEO_SECOND, 
  
  ABS(unix_seconds(REC_ARRET.RECAR_HRE_ARR_THEO)+(REC_ARRET.RECAR_ECART_DEP)-lag(
    unix_seconds(REC_ARRET.RECAR_HRE_ARR_THEO)+(REC_ARRET.RECAR_ECART_DEP) ,
    + 1
  ) OVER (
    PARTITION BY REF_COURSE.REFCO_DATE_EXPLOIT,
    REF_COURSE.REFCO_MNEMO_LG,
--    REF_COURSE.REFCO_SENS_REC,
    REF_POINT.REFPT_MNEMO_LONG,
    PLAGE_HORAIRE.PLHOR_MNEMO
    ORDER BY
      REF_COURSE.REFCO_DATE_EXPLOIT,
      REF_COURSE.REFCO_MNEMO_LG,
      REF_POINT.REFPT_MNEMO_LONG,
      PLAGE_HORAIRE.PLHOR_MNEMO,
  --    REF_COURSE.REFCO_SENS_REC,
      REC_ARRET.RECAR_HRE_ARR_THEO,
      REC_ARRET.RECAR_ECART_DEP
  )) as INTERVAL_REEL_SECOND,
  unix_seconds(REC_ARRET.RECAR_HRE_ARR_THEO)+(REC_ARRET.RECAR_ECART_DEP) as HEURE_ARRIVEE_RELLE_SECOND

  
FROM
  saetram.REC_ARRET,
  saetram.REF_COURSE,
  saetram.REC_CALENDRIER,
  saetram.REF_POINT,
  saetram.PLAGE_HORAIRE
WHERE
  (REC_ARRET.RECAR_ID = REF_COURSE.REFCO_ID)
  AND (
    REF_COURSE.REFCO_DATE_EXPLOIT = REC_CALENDRIER.CAL_DATE_EXPLOIT
  )
  AND (REF_POINT.REFPT_ID = REC_ARRET.RECAR_REFPT_ID)
  AND (
    REC_ARRET.RECAR_HRE_ARR_THEO between PLAGE_HORAIRE.PLHOR_HREDEB
    and PLAGE_HORAIRE.PLHOR_HREFIN
  )
  AND (
    REF_COURSE.REFCO_DATE_EXPLOIT = '2022-02-11T00:00:00.000+0000'
    AND PLAGE_HORAIRE.PLHOR_MNEMO IN ('06-07', '09-16', '19-23')
    AND REF_COURSE.REFCO_TYPE_THEO IN (0)
    AND REF_COURSE.REFCO_MNEMO_LG IN ('A')
    and 
    ((REF_COURSE.REFCO_SENS_REC='A' AND REFPT_MNEMO_LONG NOT IN ('HAILL_V1')) or (REF_COURSE.REFCO_SENS_REC='R' AND REFPT_MNEMO_LONG NOT IN ('GARD V1','DRAVE V1')) )

    
  )
  )

-- COMMAND ----------

