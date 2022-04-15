-- Databricks notebook source
SELECT
  REFCO_DATE_EXPLOIT,
  REFCO_MNEMO_LG,
  REFCO_NO_CH_THEO,
 -- sensTheo,
  REFCO_MNEMO_SB,
  REFCO_RANG,
 -- REFCO_NO_SV,
  REFPT_MNEMO_LONG,
  RECAR_REFPT_ID,
  RECAR_ECART_DEP,
  RECAR_DIST_THEO,
  VERPT_NOM,
  TRHOR_MNEMO,
  DESCOPT_HRE_PASS,
  DESCOPT_TPS_BAT,
  RECAR_HRE_DEP_THEO,
 -- REFCO_RANG,
  DepartReelArretSeconds,
  DepartReelArret,
  numLigne,
  nbArretChainage,
  nbDataRef,
  aLHeure,
  case
    when (nbDataRef> 0) then aLHeure
    else 0 
  end as aLHeureRef
FROM
  (
    SELECT
      REF_COURSE.REFCO_DATE_EXPLOIT,
      REF_COURSE.REFCO_MNEMO_LG,
      cast(REF_COURSE.REFCO_NO_CH_THEO as int) as REFCO_NO_CH_THEO,
--       decode(
--         REF_COURSE.REFCO_SENS_THEO,
--         'A',
--         'Aller',
--         'R',
--         'Retour',
--         ''
--       ) as sensTheo,
      REF_COURSE.REFCO_MNEMO_SB,
      REF_COURSE.REFCO_RANG,
      REF_COURSE.REFCO_NO_SV,
      POINT.REFPT_MNEMO_LONG,
      REC_ARRET.RECAR_REFPT_ID,
      cast(REC_ARRET.RECAR_ECART_DEP as int) as RECAR_ECART_DEP,
      cast(REC_ARRET.RECAR_DIST_THEO as int) as RECAR_DIST_THEO,
      VERS_POINT.VERPT_NOM,
      TRANCHE_HORAIRE.TRHOR_MNEMO,
      VUE_HRE_THEO_ARRET.DESCOPT_HRE_PASS,
      VUE_HRE_THEO_ARRET.DESCOPT_TPS_BAT,
      REC_ARRET.RECAR_HRE_DEP_THEO,
      --REFCO_RANG,
      cast(
        unix_seconds(REC_ARRET.RECAR_HRE_DEP_THEO) + REC_ARRET.RECAR_ECART_DEP as int
      ) as DepartReelArretSeconds,
      cast(
        (
          unix_seconds(REC_ARRET.RECAR_HRE_DEP_THEO) + REC_ARRET.RECAR_ECART_DEP
        ) as timestamp
      ) as DepartReelArret,
      row_number() OVER (
        PARTITION BY 
        REF_COURSE.REFCO_DATE_EXPLOIT,
        REF_COURSE.REFCO_MNEMO_LG,
        REF_COURSE.REFCO_NO_CH_THEO,
        REF_COURSE.REFCO_MNEMO_SB,
        REF_COURSE.REFCO_RANG
        order by
          REF_COURSE.REFCO_DATE_EXPLOIT,
          REF_COURSE.REFCO_MNEMO_LG,
          REF_COURSE.REFCO_NO_CH_THEO,
          REF_COURSE.REFCO_MNEMO_SB,
          REC_ARRET.RECAR_HRE_DEP_THEO,
          REF_COURSE.REFCO_RANG
      ) as numLigne,
      size(
        collect_set(POINT.REFPT_MNEMO_LONG) over (partition by REF_COURSE.REFCO_NO_CH_THEO)
      ) as nbArretChainage,
      case
        when row_number() OVER (
          PARTITION BY 
        REF_COURSE.REFCO_DATE_EXPLOIT,
        REF_COURSE.REFCO_MNEMO_LG,
        REF_COURSE.REFCO_NO_CH_THEO,
        REF_COURSE.REFCO_MNEMO_SB,
        REF_COURSE.REFCO_RANG
        order by
          REF_COURSE.REFCO_DATE_EXPLOIT,
          REF_COURSE.REFCO_MNEMO_LG,
          REF_COURSE.REFCO_NO_CH_THEO,
          REF_COURSE.REFCO_MNEMO_SB,
          REC_ARRET.RECAR_HRE_DEP_THEO,
          REF_COURSE.REFCO_RANG
        ) <= (
          size(
            collect_set(POINT.REFPT_MNEMO_LONG) over (partition by REF_COURSE.REFCO_NO_CH_THEO)
          )
        ) -2 then 1
        else null
      end as nbDataRef,
      case
        when (
          cast(REC_ARRET.RECAR_ECART_DEP as int) >= -59
          and cast(REC_ARRET.RECAR_ECART_DEP as int) <= 239
          and (
            REF_COURSE.REFCO_SENS_THEO = 'R'
            or(
              REF_COURSE.REFCO_SENS_THEO = 'A'
              and REF_COURSE.REFCO_MNEMO_LG <> 'GSJEAN'
            )
          )
        ) then 1
        else 0
      end as aLHeure
    FROM
      SAEBUS.REC_ARRET,
      SAEBUS.VUE_HRE_THEO_ARRET,
      SAEBUS.REF_COURSE,
      SAEBUS.RECCO_COM,
      SAEBUS.REC_CALENDRIER,
      SAEBUS.REF_POINT POINT,
      SAEBUS.VERS_POINT,
      SAEBUS.TRANCHE_HORAIRE,
      SAEBUS.LIEN_TRANCHE_SEGMENT,
      SAEBUS.SEGMENT_HORAIRE,
      SAEBUS.LIEN_SEGMENT_PLAGE,
      SAEBUS.PLAGE_HORAIRE
    WHERE
      (REF_COURSE.REFCO_ID = REC_ARRET.RECAR_ID)
      AND (
        REF_COURSE.REFCO_DATE_EXPLOIT = REC_CALENDRIER.CAL_DATE_EXPLOIT
      )
      AND (
        REC_ARRET.RECAR_ID = VUE_HRE_THEO_ARRET.RECAR_ID
        and REC_ARRET.RECAR_REFPT_ID = VUE_HRE_THEO_ARRET.RECAR_REFPT_ID
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
      AND (
        REC_ARRET.RECAR_REFPT_ID = VERS_POINT.VERPT_REFPT_ID
        AND VERS_POINT.verpt_date_vers = (
          select
            max(verpt_date_vers)
          from
            vers_point vpt2
          where
            vpt2.verpt_refpt_id = VERS_POINT.verpt_refpt_id
        )
      )
      AND REC_CALENDRIER.CAL_DATE_EXPLOIT = '2022-02-11T00:00:00.000+0000'
      AND REF_COURSE.REFCO_MNEMO_LG IN ('01')
      AND TRANCHE_HORAIRE.TRHOR_MNEMO IN ('IQ_F10H')
      AND (REF_COURSE.REFCO_TYPE_THEO = 0)
      AND decode(
        REC_ARRET.RECAR_TYPE_PT,
        'Y',
        'Oui',
        'N',
        'Non',
        ''
      ) IN ('Oui')
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
      AND decode(
        REF_COURSE.REFCO_FLAG_REAL_THEO,
        '1',
        'Oui',
        'Non'
      ) IN ('Oui')
    ORDER BY
      REF_COURSE.REFCO_DATE_EXPLOIT,
      REF_COURSE.REFCO_MNEMO_LG,
      REF_COURSE.REFCO_MNEMO_SB,
     -- REF_COURSE.REFCO_NO_SV,
      REF_COURSE.REFCO_NO_CH_THEO,
      REC_ARRET.RECAR_HRE_DEP_THEO
  )

-- COMMAND ----------


