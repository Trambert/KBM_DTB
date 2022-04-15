-- Databricks notebook source
SELECT
  REF_COURSE.REFCO_DATE_EXPLOIT as DATE_EXPLOITATION,
  REF_COURSE.REFCO_MNEMO_LG AS MNEMO_LIGNE,
--   REF_COURSE.REFCO_NO_CH_THEO AS NO_CHAINAGE_THEO,
--   decode(
--     REF_COURSE.REFCO_SENS_THEO,
--     'A',
--     'Aller',
--     'R',
--     'Retour',
--     ''
--   ) as SENS_THEO,
  POINT.REFPT_MNEMO_LONG AS MNEMO_ARRET,
  REC_ARRET.RECAR_HRE_DEP_THEO as HEURE_DEPART_APPLI_ARRET,
  unix_seconds(REC_ARRET.RECAR_HRE_DEP_THEO) as SECOND_HEURE_DEPART_APPLI_ARRET,
  (
    to_timestamp(
      REC_ARRET.RECAR_HRE_DEP_THEO,
      'yyyy-MM-dd hh:mm:ss'
    )
  ) + make_interval(0, 0, 0, 0, 0, 0, REC_ARRET.RECAR_ECART_DEP) as HEURE_DEPART_REELLE_ARRET,
  unix_seconds(REC_ARRET.RECAR_HRE_DEP_THEO) + REC_ARRET.RECAR_ECART_DEP as SECOND_HEURE_DEPART_REELLE_ARRET,
  lag(unix_seconds(REC_ARRET.RECAR_HRE_DEP_THEO), + 1) OVER (
    PARTITION BY 
    REF_COURSE.REFCO_DATE_EXPLOIT,
    REF_COURSE.REFCO_MNEMO_LG,
    POINT.REFPT_MNEMO_LONG
    ORDER BY
      REF_COURSE.REFCO_DATE_EXPLOIT,
      REF_COURSE.REFCO_MNEMO_LG,
      POINT.REFPT_MNEMO_LONG,
      unix_seconds(REC_ARRET.RECAR_HRE_DEP_THEO),
      unix_seconds(REC_ARRET.RECAR_HRE_DEP_THEO) + REC_ARRET.RECAR_ECART_DEP
  ) as lag_SECOND_HEURE_DEPART_APPLI_ARRET,
  lag(
    unix_seconds(
      (
        to_timestamp(
          REC_ARRET.RECAR_HRE_DEP_THEO,
          'yyyy-MM-dd hh:mm:ss'
        )
      ) + make_interval(0, 0, 0, 0, 0, 0, REC_ARRET.RECAR_ECART_DEP)
    ),
    + 1
  ) OVER (
    PARTITION BY 
    REF_COURSE.REFCO_DATE_EXPLOIT,
    REF_COURSE.REFCO_MNEMO_LG,
    POINT.REFPT_MNEMO_LONG
    ORDER BY
      REF_COURSE.REFCO_DATE_EXPLOIT,
      REF_COURSE.REFCO_MNEMO_LG,
      POINT.REFPT_MNEMO_LONG,
      unix_seconds(REC_ARRET.RECAR_HRE_DEP_THEO),
      unix_seconds(REC_ARRET.RECAR_HRE_DEP_THEO) + REC_ARRET.RECAR_ECART_DEP
  ) as lag_SECOND_HEURE_DEPART_REELLE_ARRET,
  unix_seconds(REC_ARRET.RECAR_HRE_DEP_THEO) - lag(unix_seconds(REC_ARRET.RECAR_HRE_DEP_THEO), + 1) OVER (
    PARTITION BY 
    REF_COURSE.REFCO_DATE_EXPLOIT,
    REF_COURSE.REFCO_MNEMO_LG,
    POINT.REFPT_MNEMO_LONG
    ORDER BY
      REF_COURSE.REFCO_DATE_EXPLOIT,
      REF_COURSE.REFCO_MNEMO_LG,
      POINT.REFPT_MNEMO_LONG,
      unix_seconds(REC_ARRET.RECAR_HRE_DEP_THEO),
      unix_seconds(REC_ARRET.RECAR_HRE_DEP_THEO) + REC_ARRET.RECAR_ECART_DEP
  ) as diff_SECOND_HEURE_DEPART_APPLI_ARRET,
  abs(unix_seconds(
    (
      to_timestamp(
        REC_ARRET.RECAR_HRE_DEP_THEO,
        'yyyy-MM-dd hh:mm:ss'
      )
    ) + make_interval(0, 0, 0, 0, 0, 0, REC_ARRET.RECAR_ECART_DEP)
  ) - lag(
    unix_seconds(
      (
        to_timestamp(
          REC_ARRET.RECAR_HRE_DEP_THEO,
          'yyyy-MM-dd hh:mm:ss'
        )
      ) + make_interval(0, 0, 0, 0, 0, 0, REC_ARRET.RECAR_ECART_DEP)
    ),
    + 1
  ) OVER (
    PARTITION BY 
    REF_COURSE.REFCO_DATE_EXPLOIT,
    REF_COURSE.REFCO_MNEMO_LG,
    POINT.REFPT_MNEMO_LONG
    ORDER BY
      REF_COURSE.REFCO_DATE_EXPLOIT,
      REF_COURSE.REFCO_MNEMO_LG,
      POINT.REFPT_MNEMO_LONG,
      unix_seconds(REC_ARRET.RECAR_HRE_DEP_THEO),
      unix_seconds(REC_ARRET.RECAR_HRE_DEP_THEO) + REC_ARRET.RECAR_ECART_DEP
  )) as diff_SECOND_HEURE_DEPART_REELLE_ARRET,
  (
    unix_seconds(REC_ARRET.RECAR_HRE_DEP_THEO) - unix_seconds(
      lag(REC_ARRET.RECAR_HRE_DEP_THEO, + 1) OVER (
    PARTITION BY 
    REF_COURSE.REFCO_DATE_EXPLOIT,
    REF_COURSE.REFCO_MNEMO_LG,
    POINT.REFPT_MNEMO_LONG
    ORDER BY
      REF_COURSE.REFCO_DATE_EXPLOIT,
      REF_COURSE.REFCO_MNEMO_LG,
      POINT.REFPT_MNEMO_LONG,
      unix_seconds(REC_ARRET.RECAR_HRE_DEP_THEO),
      unix_seconds(REC_ARRET.RECAR_HRE_DEP_THEO) + REC_ARRET.RECAR_ECART_DEP
      )
    )
  ) + 179 as SECOND_HEURE_DEPART_APPLI_ARRET_SEUIL,
  case
    when (
     abs(unix_seconds(
        (
          to_timestamp(
            REC_ARRET.RECAR_HRE_DEP_THEO,
            'yyyy-MM-dd hh:mm:ss'
          )
        ) + make_interval(0, 0, 0, 0, 0, 0, REC_ARRET.RECAR_ECART_DEP)
      ) - lag(
        unix_seconds(
          (
            to_timestamp(
              REC_ARRET.RECAR_HRE_DEP_THEO,
              'yyyy-MM-dd hh:mm:ss'
            )
          ) + make_interval(0, 0, 0, 0, 0, 0, REC_ARRET.RECAR_ECART_DEP)
        ),
        + 1
      ) OVER (
     PARTITION BY 
    REF_COURSE.REFCO_DATE_EXPLOIT,
    REF_COURSE.REFCO_MNEMO_LG,
    POINT.REFPT_MNEMO_LONG
    ORDER BY
      REF_COURSE.REFCO_DATE_EXPLOIT,
      REF_COURSE.REFCO_MNEMO_LG,
      POINT.REFPT_MNEMO_LONG,
      unix_seconds(REC_ARRET.RECAR_HRE_DEP_THEO),
      unix_seconds(REC_ARRET.RECAR_HRE_DEP_THEO) + REC_ARRET.RECAR_ECART_DEP
      )) < ((
        unix_seconds(REC_ARRET.RECAR_HRE_DEP_THEO) - unix_seconds(
          lag(REC_ARRET.RECAR_HRE_DEP_THEO, + 1) OVER (
    PARTITION BY 
    REF_COURSE.REFCO_DATE_EXPLOIT,
    REF_COURSE.REFCO_MNEMO_LG,
    POINT.REFPT_MNEMO_LONG
    ORDER BY
      REF_COURSE.REFCO_DATE_EXPLOIT,
      REF_COURSE.REFCO_MNEMO_LG,
      POINT.REFPT_MNEMO_LONG,
      unix_seconds(REC_ARRET.RECAR_HRE_DEP_THEO),
      unix_seconds(REC_ARRET.RECAR_HRE_DEP_THEO) + REC_ARRET.RECAR_ECART_DEP
          )
        )
      ) + 180) --ajout d'une seconde pour correspondre au inférieur à au dessus qui ne renvoit pas les bonnes data si inéfrieur ou égale <=
    ) then 1
    else 0
  end as DataCorrectes,
  case
    when (
      unix_seconds(REC_ARRET.RECAR_HRE_DEP_THEO) - lag(unix_seconds(REC_ARRET.RECAR_HRE_DEP_THEO), + 1) OVER (
    PARTITION BY 
    REF_COURSE.REFCO_DATE_EXPLOIT,
    REF_COURSE.REFCO_MNEMO_LG,
    POINT.REFPT_MNEMO_LONG
    ORDER BY
      REF_COURSE.REFCO_DATE_EXPLOIT,
      REF_COURSE.REFCO_MNEMO_LG,
      POINT.REFPT_MNEMO_LONG,
      unix_seconds(REC_ARRET.RECAR_HRE_DEP_THEO),
      unix_seconds(REC_ARRET.RECAR_HRE_DEP_THEO) + REC_ARRET.RECAR_ECART_DEP
      ) is null
    ) then 0
    else 1
  end as DataTotales
FROM
  REC_ARRET,
  REF_COURSE,
  RECCO_COM,
  REC_CALENDRIER,
  REF_POINT POINT,
  TRANCHE_HORAIRE,
  LIEN_TRANCHE_SEGMENT,
  SEGMENT_HORAIRE,
  LIEN_SEGMENT_PLAGE,
  PLAGE_HORAIRE
WHERE
  REF_COURSE.REFCO_MNEMO_LG IN ('01')
  and TRANCHE_HORAIRE.TRHOR_MNEMO IN ('IQ_F10F')
  AND decode(REC_ARRET.RECAR_TYPE_PT, 'Y', 'Oui', 'N', 'Non', '') IN ('Oui')
  AND (REF_COURSE.REFCO_TYPE_THEO = 0)
  AND decode(
    REF_COURSE.REFCO_FLAG_GRAPH,
    '0',
    'Non',
    '1',
    'Oui',
    'Oui'
  ) = 'Oui'
  AND DECODE(
    RECCO_COM.RECCOM_FLAG_OK,
    'Y',
    'Complète',
    'E',
    'Incomplète',
    'N',
    'Invalidée par l''utilisateur',
    'D',
    'Déviée et non relocalisée',
    RECCO_COM.RECCOM_FLAG_OK
  ) = 'Complète'
  AND RECCO_COM.RECCOM_FLAG_OK = 'Y'
  AND Decode(REC_ARRET.RECAR_FLAG_DEVIA, 'Y', 'Oui', 'Non') = 'Non'
  AND decode(REF_COURSE.REFCO_FLAG_REAL_THEO, '1', 'Oui', 'Non') IN ('Oui')
  and (REF_COURSE.REFCO_ID = REC_ARRET.RECAR_ID)
  AND (
    REF_COURSE.REFCO_DATE_EXPLOIT = REC_CALENDRIER.CAL_DATE_EXPLOIT
  )
  AND (REF_COURSE.REFCO_ID = RECCO_COM.RECCOM_ID)
  AND (
    TRANCHE_HORAIRE.TRHOR_MNEMO = LIEN_TRANCHE_SEGMENT.TRSEG_MNEMO_TR
  )
  AND (
    LIEN_TRANCHE_SEGMENT.TRSEG_MNEMO_SEG = SEGMENT_HORAIRE.SEGHOR_MNEMO
  )
  AND (
    LIEN_SEGMENT_PLAGE.SEGPL_MNEMO_SEG = SEGMENT_HORAIRE.SEGHOR_MNEMO
  )
  AND (
    PLAGE_HORAIRE.PLHOR_MNEMO = LIEN_SEGMENT_PLAGE.SEGPL_MNEMO_PL
  )
  AND (
    REF_COURSE.REFCO_HRE_DEP_THEO between PLAGE_HORAIRE.PLHOR_HREDEB
    and PLAGE_HORAIRE.PLHOR_HREFIN
  )
  AND (
    REF_COURSE.REFCO_HRE_DEP_THEO between PLAGE_HORAIRE.PLHOR_HREDEB
    and PLAGE_HORAIRE.PLHOR_HREFIN
  )
  AND (REC_ARRET.RECAR_REFPT_ID = POINT.REFPT_ID)
  AND REC_CALENDRIER.CAL_DATE_EXPLOIT = '2022-02-11T00:00:00.000+0000'
order by
  REF_COURSE.REFCO_DATE_EXPLOIT,
  REF_COURSE.REFCO_MNEMO_LG,
--   REF_COURSE.REFCO_NO_CH_THEO,
--   decode(
--     REF_COURSE.REFCO_SENS_THEO,
--     'A',
--     'Aller',
--     'R',
--     'Retour',
--     ''
--   ),
  POINT.REFPT_MNEMO_LONG,
    unix_seconds(REC_ARRET.RECAR_HRE_DEP_THEO),
  unix_seconds(REC_ARRET.RECAR_HRE_DEP_THEO) + REC_ARRET.RECAR_ECART_DEP

-- COMMAND ----------

--TODO: Ajout des indicateurs
SELECT DATE_EXPLOITATION, MNEMO_LIGNE, NO_CHAINAGE_THEO, SENS_THEO, MNEMO_ARRET, RECAR_TYPE_PT, numLigne, ordre, nbArretChainage
from (
SELECT
  REF_COURSE.REFCO_DATE_EXPLOIT as DATE_EXPLOITATION,
  REF_COURSE.REFCO_MNEMO_LG AS MNEMO_LIGNE,
  REF_COURSE.REFCO_NO_CH_THEO AS NO_CHAINAGE_THEO,
  decode(
    REF_COURSE.REFCO_SENS_THEO,
    'A',
    'Aller',
    'R',
    'Retour',
    ''
  ) as SENS_THEO,
  POINT.REFPT_MNEMO_LONG AS MNEMO_ARRET,
  decode(REC_ARRET.RECAR_TYPE_PT,'Y','Oui','N','Non','')  as RECAR_TYPE_PT,
  NUM_LIGNE.ordre,
  NUM_LIGNE.nbArretChainage,
  row_number() OVER(PARTITION BY REF_COURSE.REFCO_MNEMO_LG, REF_COURSE.REFCO_NO_CH_THEO, REF_COURSE.REFCO_SENS_THEO ORDER BY REF_COURSE.REFCO_MNEMO_LG, REF_COURSE.REFCO_NO_CH_THEO, REF_COURSE.REFCO_SENS_THEO, Num_Ligne.ordre desc) as numLigne
FROM
   saebus.REC_ARRET inner join saebus.REF_COURSE on  (REF_COURSE.REFCO_ID = REC_ARRET.RECAR_ID)
   inner join saebus.REF_POINT POINT on (REC_ARRET.RECAR_REFPT_ID = POINT.REFPT_ID)
   inner join saebus.REC_CALENDRIER on (saebus.REF_COURSE.REFCO_DATE_EXPLOIT = REC_CALENDRIER.CAL_DATE_EXPLOIT)
   inner join saebus.RECCO_COM on (REF_COURSE.REFCO_ID = RECCO_COM.RECCOM_ID)
   inner join saebus.PLAGE_HORAIRE on (REF_COURSE.REFCO_HRE_DEP_THEO between PLAGE_HORAIRE.PLHOR_HREDEB and PLAGE_HORAIRE.PLHOR_HREFIN)
   inner join saebus.LIEN_SEGMENT_PLAGE on ( PLAGE_HORAIRE.PLHOR_MNEMO = LIEN_SEGMENT_PLAGE.SEGPL_MNEMO_PL)
   inner join saebus.SEGMENT_HORAIRE on ( LIEN_SEGMENT_PLAGE.SEGPL_MNEMO_SEG = SEGMENT_HORAIRE.SEGHOR_MNEMO)
   inner join saebus.LIEN_TRANCHE_SEGMENT on (LIEN_TRANCHE_SEGMENT.TRSEG_MNEMO_SEG = SEGMENT_HORAIRE.SEGHOR_MNEMO)
   inner join saebus.TRANCHE_HORAIRE on (TRANCHE_HORAIRE.TRHOR_MNEMO = LIEN_TRANCHE_SEGMENT.TRSEG_MNEMO_TR)  
   left join (
     SELECT MNEMO_LIGNE,SENS_PARCOURS,NO_CHAINAGE,MNEMO_ORIG, ORDRE_POINT AS ordre, 
         size(
          collect_set(MNEMO_ORIG) over (partition by MNEMO_LIGNE,SENS_PARCOURS,NO_CHAINAGE)
        ) as nbArretChainage
      FROM saebus.V_REF_TOPO_CHAINAGE_BUS
      where no_periode_topo=(SELECT ph.NO_PERIODE_TOPO 
                            FROM saebus.REF_CALENDRIER c
                            LEFT JOIN saebus.PERIODE_HORAIRE ph ON (ph.NO_PERIODE_HORAIRE =c.NO_PERIODE_HORAIRE )
                            WHERE 
                            c.DATE_CALENDRIER='2022-01-30 ')
   ) NUM_LIGNE on (REF_COURSE.REFCO_MNEMO_LG = NUM_LIGNE.MNEMO_LIGNE and cast(REF_COURSE.REFCO_NO_CH_THEO as int)= cast(NUM_LIGNE.NO_CHAINAGE as int) and REF_COURSE.REFCO_SENS_THEO=NUM_LIGNE.SENS_PARCOURS and POINT.REFPT_MNEMO_LONG=NUM_LIGNE.MNEMO_ORIG)
  
WHERE
  REF_COURSE.REFCO_MNEMO_LG IN ('01','02','03','04')
  and TRANCHE_HORAIRE.TRHOR_MNEMO IN ('IQ_F10F')
  AND decode(REC_ARRET.RECAR_TYPE_PT, 'Y', 'Oui', 'N', 'Non', '') IN ('Oui')
  AND (REF_COURSE.REFCO_TYPE_THEO = 0)
  AND decode(
    REF_COURSE.REFCO_FLAG_GRAPH,
    '0',
    'Non',
    '1',
    'Oui',
    'Oui'
  ) = 'Oui'
  AND DECODE(
    RECCO_COM.RECCOM_FLAG_OK,
    'Y',
    'Complète',
    'E',
    'Incomplète',
    'N',
    'Invalidée par l''utilisateur',
    'D',
    'Déviée et non relocalisée',
    RECCO_COM.RECCOM_FLAG_OK
  ) = 'Complète'
  AND RECCO_COM.RECCOM_FLAG_OK = 'Y'
  AND Decode(REC_ARRET.RECAR_FLAG_DEVIA, 'Y', 'Oui', 'Non') = 'Non'
  AND decode(REF_COURSE.REFCO_FLAG_REAL_THEO, '1', 'Oui', 'Non') IN ('Oui')  
  AND REC_CALENDRIER.CAL_DATE_EXPLOIT = '2022-02-11T00:00:00.000+0000'
  GROUP BY 
    REF_COURSE.REFCO_DATE_EXPLOIT ,
  REF_COURSE.REFCO_MNEMO_LG ,
  REF_COURSE.REFCO_NO_CH_THEO ,
  REF_COURSE.REFCO_SENS_THEO,
  decode(
    REF_COURSE.REFCO_SENS_THEO,
    'A',
    'Aller',
    'R',
    'Retour',
    ''
  ) ,
  POINT.REFPT_MNEMO_LONG ,
  decode(REC_ARRET.RECAR_TYPE_PT,'Y','Oui','N','Non',''), 
  NUM_LIGNE.ordre,
  NUM_LIGNE.nbArretChainage
  ) a 
  where numLigne >2

-- COMMAND ----------

SELECT MNEMO_LIGNE,SENS_PARCOURS,NO_CHAINAGE,MNEMO_ORIG, ORDRE_POINT AS ordre,
      row_number() OVER (
        PARTITION BY 
       MNEMO_LIGNE,
       SENS_PARCOURS, 
       NO_CHAINAGE
        order by
       MNEMO_LIGNE,
       SENS_PARCOURS, 
       NO_CHAINAGE
      ) as numLigne,
      size(
        collect_set(MNEMO_ORIG) over (partition by MNEMO_LIGNE,SENS_PARCOURS,NO_CHAINAGE)
      ) as nbArretChainage
FROM saebus.V_REF_TOPO_CHAINAGE
where no_periode_topo=(SELECT ph.NO_PERIODE_TOPO 
FROM saebus.REF_CALENDRIER c
LEFT JOIN saebus.PERIODE_HORAIRE ph ON (ph.NO_PERIODE_HORAIRE =c.NO_PERIODE_HORAIRE )
WHERE 
c.DATE_CALENDRIER='2022-01-30 ')

-- and 

-- MNEMO_LIGNE='01'
-- and 
-- cast(no_chainage as int)=111

-- COMMAND ----------

     SELECT MNEMO_LIGNE,SENS_PARCOURS,NO_CHAINAGE,MNEMO_ORIG,ordre, nb, nbArretChainage from(
     SELECT MNEMO_LIGNE,SENS_PARCOURS,NO_CHAINAGE,MNEMO_ORIG, ORDRE_POINT AS ordre, row_number() over(PARTITION BY MNEMO_LIGNE,SENS_PARCOURS,NO_CHAINAGE order by MNEMO_LIGNE,SENS_PARCOURS,NO_CHAINAGE,ORDRE_POINT desc) as nb, 
         size(
          collect_set(MNEMO_ORIG) over (partition by MNEMO_LIGNE,SENS_PARCOURS,NO_CHAINAGE)
        ) as nbArretChainage
      FROM saebus.V_REF_TOPO_CHAINAGE_BUS
      where no_periode_topo=5) a
