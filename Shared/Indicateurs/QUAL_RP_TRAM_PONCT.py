# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC --TRAM Ponctualite
# MAGIC 
# MAGIC SELECT
# MAGIC   REF_COURSE.REFCO_DATE_EXPLOIT,
# MAGIC   REF_COURSE.REFCO_MNEMO_LG,
# MAGIC   REF_COURSE.REFCO_SENS_REC,
# MAGIC   REF_COURSE.REFCO_MNEMO_SB,
# MAGIC   REF_POINT.REFPT_MNEMO_LONG,
# MAGIC   REF_COURSE.REFCO_RANG,
# MAGIC   REF_COURSE.REFCO_NO_CH_THEO,
# MAGIC   PLAGE_HORAIRE.PLHOR_MNEMO,
# MAGIC   REC_ARRET.RECAR_HRE_ARR_THEO,
# MAGIC   REC_ARRET.RECAR_ECART_DEP,
# MAGIC   REF_COURSE.REFCO_NO_CH_REC,
# MAGIC   row_number() OVER ( PARTITION BY
# MAGIC     REF_COURSE.REFCO_DATE_EXPLOIT,
# MAGIC     REF_COURSE.REFCO_MNEMO_LG,
# MAGIC     REF_COURSE.REFCO_SENS_REC,
# MAGIC     REF_COURSE.REFCO_RANG,
# MAGIC     REF_COURSE.REFCO_NO_CH_THEO,
# MAGIC     REF_POINT.REFPT_MNEMO_LONG,
# MAGIC     PLAGE_HORAIRE.PLHOR_MNEMO
# MAGIC     ORDER BY
# MAGIC       REF_COURSE.REFCO_DATE_EXPLOIT,
# MAGIC       REF_COURSE.REFCO_MNEMO_LG,
# MAGIC       REF_COURSE.REFCO_SENS_REC,
# MAGIC       REF_COURSE.REFCO_NO_CH_THEO,
# MAGIC       REF_POINT.REFPT_MNEMO_LONG,
# MAGIC       PLAGE_HORAIRE.PLHOR_MNEMO,
# MAGIC       REC_ARRET.RECAR_HRE_ARR_THEO
# MAGIC   ) as numLigne,
# MAGIC   size(
# MAGIC         collect_set(REF_POINT.REFPT_MNEMO_LONG) over (partition by REF_COURSE.REFCO_MNEMO_LG,REF_COURSE.REFCO_NO_CH_THEO)
# MAGIC       ) as nbArretChainage,
# MAGIC   --(unix_seconds(REC_ARRET.RECAR_HRE_DEP_THEO) +REC_ARRET.RECAR_ECART_DEP),
# MAGIC   --cast((unix_seconds(REC_ARRET.RECAR_HRE_DEP_THEO) +REC_ARRET.RECAR_ECART_DEP) as timestamp),
# MAGIC   case when
# MAGIC     (REC_ARRET.RECAR_ECART_DEP >=-59 AND REC_ARRET.RECAR_ECART_DEP <=179) then 1 else 0 end as aLHeure
# MAGIC FROM
# MAGIC   saetram.REC_ARRET,
# MAGIC   saetram.REF_COURSE,
# MAGIC   saetram.REC_CALENDRIER,
# MAGIC   saetram.REF_POINT,
# MAGIC   saetram.PLAGE_HORAIRE
# MAGIC WHERE
# MAGIC   ( REC_ARRET.RECAR_ID=REF_COURSE.REFCO_ID  )
# MAGIC   and REF_COURSE.REFCO_MNEMO_LG = 'B'
# MAGIC   AND  ( REF_COURSE.REFCO_DATE_EXPLOIT=REC_CALENDRIER.CAL_DATE_EXPLOIT  )
# MAGIC   AND  ( REF_POINT.REFPT_ID=REC_ARRET.RECAR_REFPT_ID  )
# MAGIC   AND  ( REC_ARRET.RECAR_HRE_ARR_THEO between PLAGE_HORAIRE.PLHOR_HREDEB and PLAGE_HORAIRE.PLHOR_HREFIN  )
# MAGIC   AND  
# MAGIC   (
# MAGIC    REF_COURSE.REFCO_DATE_EXPLOIT ='2022-02-12T00:00:00.000+0000' 
# MAGIC    AND
# MAGIC    REF_COURSE.REFCO_TYPE_THEO  IN  ( 0  )
# MAGIC    AND
# MAGIC    PLAGE_HORAIRE.PLHOR_MNEMO  IN  ( '0345-6','23-27'  )
# MAGIC    AND
# MAGIC    (
# MAGIC     (
# MAGIC      REF_COURSE.REFCO_MNEMO_LG  IN  ( 'A'  )
# MAGIC      AND
# MAGIC      (
# MAGIC       (
# MAGIC        REF_COURSE.REFCO_SENS_REC  IN  ( 'A'  )
# MAGIC        AND
# MAGIC        REF_POINT.REFPT_MNEMO_LONG  IN  ( 'GARD V1','DRAVE V1','CENON_A','HDV_A_A','AUGUS_A'  )
# MAGIC       )
# MAGIC       OR
# MAGIC       (
# MAGIC        REF_COURSE.REFCO_SENS_REC  IN  ( 'R'  )
# MAGIC        AND
# MAGIC        REF_POINT.REFPT_MNEMO_LONG  IN  ( 'HAILL_V1','MERIG V2','HDV_A_R','CARNOT_R'  )
# MAGIC       )
# MAGIC      )
# MAGIC     )
# MAGIC     OR
# MAGIC     (
# MAGIC      REF_COURSE.REFCO_MNEMO_LG  IN  ( 'B'  )
# MAGIC      AND
# MAGIC      (
# MAGIC       (
# MAGIC        REF_COURSE.REFCO_SENS_REC  IN  ( 'A'  )
# MAGIC        AND
# MAGIC        REF_POINT.REFPT_MNEMO_LONG  IN  ( 'FRAALO_A','PESSA V1','MONTAI_A','HDV_B_A','CHART_A'  )
# MAGIC       )
# MAGIC       OR
# MAGIC       (
# MAGIC        REF_COURSE.REFCO_SENS_REC  IN  ( 'R'  )
# MAGIC        AND
# MAGIC        REF_POINT.REFPT_MNEMO_LONG  IN  ( 'GARON_V1','HANGAR_R','QUINCB_R','UNITEC_R'  )
# MAGIC       )
# MAGIC      )
# MAGIC     )
# MAGIC     OR
# MAGIC     (
# MAGIC      (
# MAGIC       (
# MAGIC        REF_COURSE.REFCO_SENS_REC  IN  ( 'A'  )
# MAGIC        AND
# MAGIC        REF_POINT.REFPT_MNEMO_LONG  IN  ( 'LYCVAC_A','CALAIS_A','VERNE_A','PUBLIC_A'  )
# MAGIC       )
# MAGIC       OR
# MAGIC       (
# MAGIC        REF_COURSE.REFCO_SENS_REC  IN  ( 'R'  )
# MAGIC        AND
# MAGIC        REF_POINT.REFPT_MNEMO_LONG  IN  ( 'PARCX_V2','CRACO_R','BQF_A','QUINCC_R','TERRE V2'  )
# MAGIC       )
# MAGIC      )
# MAGIC      AND
# MAGIC      REF_COURSE.REFCO_MNEMO_LG  IN  ( 'C'  )
# MAGIC     )
# MAGIC    )
# MAGIC   )
# MAGIC ORDER BY
# MAGIC     REF_COURSE.REFCO_DATE_EXPLOIT,
# MAGIC     REF_COURSE.REFCO_MNEMO_LG,
# MAGIC     REF_COURSE.REFCO_SENS_THEO,
# MAGIC     --REF_COURSE.REFCO_MNEMO_SB,
# MAGIC     --REF_COURSE.REFCO_RANG,
# MAGIC     REFCO_DIST_THEO
# MAGIC   --REF_COURSE.PLHOR_MNEMO,
# MAGIC   
# MAGIC   --REC_ARRET.RECAR_HRE_ARR_THEO

# COMMAND ----------


