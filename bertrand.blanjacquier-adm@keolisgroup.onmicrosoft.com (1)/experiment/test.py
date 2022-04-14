# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC select * from (
# MAGIC select count(*) as nb, Num_transaction
# MAGIC from titan_recette.ventes
# MAGIC group by Num_transaction
# MAGIC having count(*)>1) 
# MAGIC order by nb desc
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC /*
# MAGIC select count(*), Date_transaction from titan_recette.ventes
# MAGIC where Date_transaction > '2021-10-01 00:00:00'
# MAGIC group by Date_transaction
# MAGIC order by Date_transaction
# MAGIC */

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT
# MAGIC   REFCO_DATE_EXPLOIT,
# MAGIC   REFCO_MNEMO_LG,
# MAGIC   REFCO_NO_CH_THEO,
# MAGIC  -- sensTheo,
# MAGIC   REFCO_MNEMO_SB,
# MAGIC  -- REFCO_NO_SV,
# MAGIC   REFPT_MNEMO_LONG,
# MAGIC   RECAR_REFPT_ID,
# MAGIC   RECAR_ECART_DEP,
# MAGIC   RECAR_DIST_THEO,
# MAGIC   VERPT_NOM,
# MAGIC   TRHOR_MNEMO,
# MAGIC   DESCOPT_HRE_PASS,
# MAGIC   DESCOPT_TPS_BAT,
# MAGIC   RECAR_HRE_DEP_THEO,
# MAGIC  -- REFCO_RANG,
# MAGIC   DepartReelArretSeconds,
# MAGIC   DepartReelArret,
# MAGIC   numLigne,
# MAGIC   nbArretChainage,
# MAGIC   nbDataRef,
# MAGIC   aLHeure,
# MAGIC   case
# MAGIC     when (nbDataRef> 0) then aLHeure
# MAGIC     else 0 
# MAGIC   end as aLHeureRef
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       REF_COURSE.REFCO_DATE_EXPLOIT,
# MAGIC       REF_COURSE.REFCO_MNEMO_LG,
# MAGIC       cast(REF_COURSE.REFCO_NO_CH_THEO as int) as REFCO_NO_CH_THEO,
# MAGIC --       decode(
# MAGIC --         REF_COURSE.REFCO_SENS_THEO,
# MAGIC --         'A',
# MAGIC --         'Aller',
# MAGIC --         'R',
# MAGIC --         'Retour',
# MAGIC --         ''
# MAGIC --       ) as sensTheo,
# MAGIC       REF_COURSE.REFCO_MNEMO_SB,
# MAGIC       REF_COURSE.REFCO_NO_SV,
# MAGIC       POINT.REFPT_MNEMO_LONG,
# MAGIC       REC_ARRET.RECAR_REFPT_ID,
# MAGIC       cast(REC_ARRET.RECAR_ECART_DEP as int) as RECAR_ECART_DEP,
# MAGIC       cast(REC_ARRET.RECAR_DIST_THEO as int) as RECAR_DIST_THEO,
# MAGIC       VERS_POINT.VERPT_NOM,
# MAGIC       TRANCHE_HORAIRE.TRHOR_MNEMO,
# MAGIC       VUE_HRE_THEO_ARRET.DESCOPT_HRE_PASS,
# MAGIC       VUE_HRE_THEO_ARRET.DESCOPT_TPS_BAT,
# MAGIC       REC_ARRET.RECAR_HRE_DEP_THEO,
# MAGIC       --REFCO_RANG,
# MAGIC       cast(
# MAGIC         unix_seconds(REC_ARRET.RECAR_HRE_DEP_THEO) + REC_ARRET.RECAR_ECART_DEP as int
# MAGIC       ) as DepartReelArretSeconds,
# MAGIC       cast(
# MAGIC         (
# MAGIC           unix_seconds(REC_ARRET.RECAR_HRE_DEP_THEO) + REC_ARRET.RECAR_ECART_DEP
# MAGIC         ) as timestamp
# MAGIC       ) as DepartReelArret,
# MAGIC       row_number() OVER (
# MAGIC         PARTITION BY 
# MAGIC         REF_COURSE.REFCO_DATE_EXPLOIT,
# MAGIC         REF_COURSE.REFCO_MNEMO_LG,
# MAGIC         REF_COURSE.REFCO_NO_CH_THEO,
# MAGIC         REF_COURSE.REFCO_MNEMO_SB
# MAGIC         ,REF_COURSE.REFCO_SENS_THEO
# MAGIC        --,REF_COURSE.REFCO_RANG
# MAGIC         order by
# MAGIC           REF_COURSE.REFCO_DATE_EXPLOIT,
# MAGIC           REF_COURSE.REFCO_MNEMO_LG,
# MAGIC           REF_COURSE.REFCO_NO_CH_THEO,
# MAGIC           REF_COURSE.REFCO_MNEMO_SB,
# MAGIC           REC_ARRET.RECAR_HRE_DEP_THEO--,
# MAGIC          -- REF_COURSE.REFCO_RANG
# MAGIC       ) as numLigne,
# MAGIC       size(
# MAGIC         collect_set(POINT.REFPT_MNEMO_LONG) over (partition by REF_COURSE.REFCO_NO_CH_THEO)
# MAGIC       ) as nbArretChainage,
# MAGIC       case
# MAGIC         when row_number() OVER (
# MAGIC           PARTITION BY REF_COURSE.REFCO_DATE_EXPLOIT,
# MAGIC           REF_COURSE.REFCO_MNEMO_LG,
# MAGIC           REF_COURSE.REFCO_NO_CH_THEO,
# MAGIC           REF_COURSE.REFCO_MNEMO_SB--,
# MAGIC           --REF_COURSE.REFCO_RANG
# MAGIC           order by
# MAGIC             REF_COURSE.REFCO_DATE_EXPLOIT,
# MAGIC             REF_COURSE.REFCO_MNEMO_LG,
# MAGIC             REF_COURSE.REFCO_NO_CH_THEO,
# MAGIC             REF_COURSE.REFCO_MNEMO_SB,
# MAGIC             REC_ARRET.RECAR_HRE_DEP_THEO--,
# MAGIC            --REF_COURSE.REFCO_RANG
# MAGIC         ) <= (
# MAGIC           size(
# MAGIC             collect_set(POINT.REFPT_MNEMO_LONG) over (partition by REF_COURSE.REFCO_NO_CH_THEO)
# MAGIC           )
# MAGIC         ) -2 then 1
# MAGIC         else null
# MAGIC       end as nbDataRef,
# MAGIC       case
# MAGIC         when (
# MAGIC           cast(REC_ARRET.RECAR_ECART_DEP as int) >= -59
# MAGIC           and cast(REC_ARRET.RECAR_ECART_DEP as int) <= 239
# MAGIC           and (
# MAGIC             REF_COURSE.REFCO_SENS_THEO = 'R'
# MAGIC             or(
# MAGIC               REF_COURSE.REFCO_SENS_THEO = 'A'
# MAGIC               and REF_COURSE.REFCO_MNEMO_LG <> 'GSJEAN'
# MAGIC             )
# MAGIC           )
# MAGIC         ) then 1
# MAGIC         else 0
# MAGIC       end as aLHeure
# MAGIC     FROM
# MAGIC       SAEBUS.REC_ARRET,
# MAGIC       SAEBUS.VUE_HRE_THEO_ARRET,
# MAGIC       SAEBUS.REF_COURSE,
# MAGIC       SAEBUS.RECCO_COM,
# MAGIC       SAEBUS.REC_CALENDRIER,
# MAGIC       SAEBUS.REF_POINT POINT,
# MAGIC       SAEBUS.VERS_POINT,
# MAGIC       SAEBUS.TRANCHE_HORAIRE,
# MAGIC       SAEBUS.LIEN_TRANCHE_SEGMENT,
# MAGIC       SAEBUS.SEGMENT_HORAIRE,
# MAGIC       SAEBUS.LIEN_SEGMENT_PLAGE,
# MAGIC       SAEBUS.PLAGE_HORAIRE
# MAGIC     WHERE
# MAGIC       (REF_COURSE.REFCO_ID = REC_ARRET.RECAR_ID)
# MAGIC       AND (
# MAGIC         REF_COURSE.REFCO_DATE_EXPLOIT = REC_CALENDRIER.CAL_DATE_EXPLOIT
# MAGIC       )
# MAGIC       AND (
# MAGIC         REC_ARRET.RECAR_ID = VUE_HRE_THEO_ARRET.RECAR_ID
# MAGIC         and REC_ARRET.RECAR_REFPT_ID = VUE_HRE_THEO_ARRET.RECAR_REFPT_ID
# MAGIC       )
# MAGIC       AND (REF_COURSE.REFCO_ID = RECCO_COM.RECCOM_ID)
# MAGIC       AND (
# MAGIC         TRANCHE_HORAIRE.TRHOR_MNEMO = LIEN_TRANCHE_SEGMENT.TRSEG_MNEMO_TR
# MAGIC       )
# MAGIC       AND (
# MAGIC         LIEN_TRANCHE_SEGMENT.TRSEG_MNEMO_SEG = SEGMENT_HORAIRE.SEGHOR_MNEMO
# MAGIC       )
# MAGIC       AND (
# MAGIC         LIEN_SEGMENT_PLAGE.SEGPL_MNEMO_SEG = SEGMENT_HORAIRE.SEGHOR_MNEMO
# MAGIC       )
# MAGIC       AND (
# MAGIC         PLAGE_HORAIRE.PLHOR_MNEMO = LIEN_SEGMENT_PLAGE.SEGPL_MNEMO_PL
# MAGIC       )
# MAGIC       AND (
# MAGIC         REF_COURSE.REFCO_HRE_DEP_THEO between PLAGE_HORAIRE.PLHOR_HREDEB
# MAGIC         and PLAGE_HORAIRE.PLHOR_HREFIN
# MAGIC       )
# MAGIC       AND (
# MAGIC         REF_COURSE.REFCO_HRE_DEP_THEO between PLAGE_HORAIRE.PLHOR_HREDEB
# MAGIC         and PLAGE_HORAIRE.PLHOR_HREFIN
# MAGIC       )
# MAGIC       AND (REC_ARRET.RECAR_REFPT_ID = POINT.REFPT_ID)
# MAGIC       AND (
# MAGIC         REC_ARRET.RECAR_REFPT_ID = VERS_POINT.VERPT_REFPT_ID
# MAGIC         AND VERS_POINT.verpt_date_vers = (
# MAGIC           select
# MAGIC             max(verpt_date_vers)
# MAGIC           from
# MAGIC             vers_point vpt2
# MAGIC           where
# MAGIC             vpt2.verpt_refpt_id = VERS_POINT.verpt_refpt_id
# MAGIC         )
# MAGIC       )
# MAGIC       AND REC_CALENDRIER.CAL_DATE_EXPLOIT = '2022-02-11T00:00:00.000+0000'
# MAGIC       AND REF_COURSE.REFCO_MNEMO_LG IN ('01')
# MAGIC       AND TRANCHE_HORAIRE.TRHOR_MNEMO IN ('IQ_F10H')
# MAGIC       AND (REF_COURSE.REFCO_TYPE_THEO = 0)
# MAGIC       AND decode(
# MAGIC         REC_ARRET.RECAR_TYPE_PT,
# MAGIC         'Y',
# MAGIC         'Oui',
# MAGIC         'N',
# MAGIC         'Non',
# MAGIC         ''
# MAGIC       ) IN ('Oui')
# MAGIC       AND decode(
# MAGIC         REF_COURSE.REFCO_FLAG_GRAPH,
# MAGIC         '0',
# MAGIC         'Non',
# MAGIC         '1',
# MAGIC         'Oui',
# MAGIC         'Oui'
# MAGIC       ) = 'Oui'
# MAGIC       AND DECODE(
# MAGIC         RECCO_COM.RECCOM_FLAG_OK,
# MAGIC         'Y',
# MAGIC         'Complète',
# MAGIC         'E',
# MAGIC         'Incomplète',
# MAGIC         'N',
# MAGIC         'Invalidée par l''utilisateur',
# MAGIC         'D',
# MAGIC         'Déviée et non relocalisée',
# MAGIC         RECCO_COM.RECCOM_FLAG_OK
# MAGIC       ) = 'Complète'
# MAGIC       AND RECCO_COM.RECCOM_FLAG_OK = 'Y'
# MAGIC       AND Decode(REC_ARRET.RECAR_FLAG_DEVIA, 'Y', 'Oui', 'Non') = 'Non'
# MAGIC       AND decode(
# MAGIC         REF_COURSE.REFCO_FLAG_REAL_THEO,
# MAGIC         '1',
# MAGIC         'Oui',
# MAGIC         'Non'
# MAGIC       ) IN ('Oui')
# MAGIC     ORDER BY
# MAGIC       REF_COURSE.REFCO_DATE_EXPLOIT,
# MAGIC       REF_COURSE.REFCO_MNEMO_LG,
# MAGIC       REF_COURSE.REFCO_MNEMO_SB,
# MAGIC      -- REF_COURSE.REFCO_NO_SV,
# MAGIC       REF_COURSE.REFCO_NO_CH_THEO,
# MAGIC       REC_ARRET.RECAR_HRE_DEP_THEO
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bi.validation

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from saebus.v_ref_topo_chainage_bus
# MAGIC where no_chainage = 113 and mnemo_ligne = '01'

# COMMAND ----------

def lsR(path):
  return([fname for flist in [([fi.path] if fi.isFile() else lsR(fi.path)) for fi in dbutils.fs.ls(path)] for fname in flist])
                
files = lsR('mnt/raw/EVEX/activite')

for fi in files: 
  print(fi)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   REF_COURSE.REFCO_DATE_EXPLOIT as DATE_EXPLOITATION,
# MAGIC   REF_COURSE.REFCO_MNEMO_LG AS MNEMO_LIGNE,
# MAGIC   REF_COURSE.REFCO_NO_CH_THEO AS NO_CHAINAGE_THEO,
# MAGIC   decode(REF_COURSE.REFCO_SENS_THEO,'A','Aller','R','Retour','') as SENS_THEO,
# MAGIC   POINT.REFPT_MNEMO_LONG AS MNEMO_ARRET,
# MAGIC   REC_ARRET.RECAR_HRE_DEP_THEO as HEURE_DEPART_APPLI_ARRET, 
# MAGIC   REC_ARRET.RECAR_ECART_DEP as test,
# MAGIC   make_interval(0, 0, 0, 0, 0, 0,  REC_ARRET.RECAR_ECART_DEP) as test2,
# MAGIC   (to_timestamp(REC_ARRET.RECAR_HRE_DEP_THEO, 'yyyy-MM-dd hh:mm:ss'))+ make_interval(0, 0, 0, 0, 0, 0,  REC_ARRET.RECAR_ECART_DEP)  as test3,
# MAGIC   lag(REC_ARRET.RECAR_HRE_DEP_THEO) OVER (PARTITION BY REF_COURSE.REFCO_DATE_EXPLOIT ORDER BY REF_COURSE.REFCO_DATE_EXPLOIT,
# MAGIC     REF_COURSE.REFCO_MNEMO_LG ,
# MAGIC     REF_COURSE.REFCO_NO_CH_THEO,
# MAGIC     decode(REF_COURSE.REFCO_SENS_THEO,'A','Aller','R','Retour',''),
# MAGIC     POINT.REFPT_MNEMO_LONG,
# MAGIC     REC_ARRET.RECAR_HRE_DEP_THEO) as test4
# MAGIC  --(REC_ARRET.RECAR_HRE_DEP_THEO+(REC_ARRET.RECAR_ECART_DEP/86400))
# MAGIC FROM
# MAGIC   REC_ARRET,
# MAGIC   REF_COURSE,
# MAGIC   RECCO_COM,
# MAGIC   REC_CALENDRIER,
# MAGIC   REF_POINT  POINT,
# MAGIC   TRANCHE_HORAIRE,
# MAGIC   LIEN_TRANCHE_SEGMENT,
# MAGIC   SEGMENT_HORAIRE,
# MAGIC   LIEN_SEGMENT_PLAGE,
# MAGIC   PLAGE_HORAIRE
# MAGIC WHERE
# MAGIC    REF_COURSE.REFCO_MNEMO_LG  IN  ( '01'  )
# MAGIC    and 
# MAGIC    TRANCHE_HORAIRE.TRHOR_MNEMO  IN  ( 'IQ_F10F'  )
# MAGIC    AND
# MAGIC    decode(REC_ARRET.RECAR_TYPE_PT,'Y','Oui','N','Non','')  IN  ( 'Oui'  )
# MAGIC    AND
# MAGIC    ( REF_COURSE.REFCO_TYPE_THEO=0  )
# MAGIC    AND
# MAGIC    decode(REF_COURSE.REFCO_FLAG_GRAPH,'0','Non','1','Oui','Oui')  =  'Oui'
# MAGIC    AND
# MAGIC    DECODE(RECCO_COM.RECCOM_FLAG_OK,'Y','Complète','E','Incomplète','N','Invalidée par l''utilisateur','D','Déviée et non relocalisée',RECCO_COM.RECCOM_FLAG_OK)  =  'Complète'
# MAGIC    AND
# MAGIC    RECCO_COM.RECCOM_FLAG_OK  =  'Y'
# MAGIC    AND
# MAGIC    Decode(REC_ARRET.RECAR_FLAG_DEVIA,'Y','Oui','Non')  =  'Non'
# MAGIC    AND
# MAGIC    decode(REF_COURSE.REFCO_FLAG_REAL_THEO,'1','Oui','Non')  IN  ( 'Oui'  )
# MAGIC and
# MAGIC   ( REF_COURSE.REFCO_ID=REC_ARRET.RECAR_ID  )
# MAGIC   AND  ( REF_COURSE.REFCO_DATE_EXPLOIT=REC_CALENDRIER.CAL_DATE_EXPLOIT  )
# MAGIC   AND  ( REF_COURSE.REFCO_ID=RECCO_COM.RECCOM_ID  )
# MAGIC   AND  ( TRANCHE_HORAIRE.TRHOR_MNEMO=LIEN_TRANCHE_SEGMENT.TRSEG_MNEMO_TR  )
# MAGIC   AND  ( LIEN_TRANCHE_SEGMENT.TRSEG_MNEMO_SEG=SEGMENT_HORAIRE.SEGHOR_MNEMO  )
# MAGIC   AND  ( LIEN_SEGMENT_PLAGE.SEGPL_MNEMO_SEG=SEGMENT_HORAIRE.SEGHOR_MNEMO  )
# MAGIC   AND  ( PLAGE_HORAIRE.PLHOR_MNEMO=LIEN_SEGMENT_PLAGE.SEGPL_MNEMO_PL  )
# MAGIC   AND  ( REF_COURSE.REFCO_HRE_DEP_THEO between PLAGE_HORAIRE.PLHOR_HREDEB and PLAGE_HORAIRE.PLHOR_HREFIN
# MAGIC   )
# MAGIC   AND  ( REF_COURSE.REFCO_HRE_DEP_THEO between PLAGE_HORAIRE.PLHOR_HREDEB and PLAGE_HORAIRE.PLHOR_HREFIN  )
# MAGIC   AND  ( REC_ARRET.RECAR_REFPT_ID=POINT.REFPT_ID  )
# MAGIC   AND  
# MAGIC   REC_CALENDRIER.CAL_DATE_EXPLOIT='2022-02-11T00:00:00.000+0000'
# MAGIC   
# MAGIC   order by 
# MAGIC     REF_COURSE.REFCO_DATE_EXPLOIT,
# MAGIC     REF_COURSE.REFCO_MNEMO_LG ,
# MAGIC     REF_COURSE.REFCO_NO_CH_THEO,
# MAGIC     decode(REF_COURSE.REFCO_SENS_THEO,'A','Aller','R','Retour',''),
# MAGIC     POINT.REFPT_MNEMO_LONG,
# MAGIC     REC_ARRET.RECAR_HRE_DEP_THEO

# COMMAND ----------

# MAGIC %sql --v_ref_topo_chainage_bus
# MAGIC SELECT
# MAGIC   ptc.no_periode_topo,
# MAGIC   ptc.id_ligne,
# MAGIC   ptc.mnemo_ligne,
# MAGIC   ptc.no_ligne,
# MAGIC   ptc.no_chainage,
# MAGIC   ptc.sens_parcours,
# MAGIC   ptc.ordre_point,
# MAGIC   ptc.id_point_pcdt,
# MAGIC   pt.mnemo_long mnemo_pcdt,
# MAGIC   ptc.id_point,
# MAGIC   ptc.mnemo_orig,
# MAGIC   NVL(ipt.distance, 0) distance_inter_point,
# MAGIC   NVL(
# MAGIC     SUM(ipt.distance) over(
# MAGIC       partition BY ptc.no_periode_topo,
# MAGIC       ptc.id_ligne,
# MAGIC       ptc.no_chainage,
# MAGIC       ptc.sens_parcours
# MAGIC       order by
# MAGIC         ptc.ordre_point RANGE UNBOUNDED PRECEDING
# MAGIC     ),
# MAGIC     0
# MAGIC   ) DISTANCE_CUMULEE,
# MAGIC   COUNT(ptc.id_point) over(
# MAGIC     partition BY ptc.no_periode_topo,
# MAGIC     ptc.id_ligne,
# MAGIC     ptc.no_chainage,
# MAGIC     ptc.sens_parcours
# MAGIC   ) NB_POINTS_TOPO_CHAINAGE,
# MAGIC   COUNT(ptc.id_point) over(partition BY ptc.no_periode_topo) NB_POINTS_TOPO_TOTAL
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       lg.id_ligne,
# MAGIC       pt.no_periode_topo,
# MAGIC       lg.mnemo_ligne,
# MAGIC       lg.no_ligne,
# MAGIC       no_chainage,
# MAGIC       sens_parcours,
# MAGIC       id_point_chaine,
# MAGIC       ordre_point,
# MAGIC       pc.id_point,
# MAGIC       lag(pc.id_point, 1, 0) over(
# MAGIC         partition BY pc.id_ligne,
# MAGIC         pc.no_chainage,
# MAGIC         pc.sens_parcours
# MAGIC         order by
# MAGIC           pc.ordre_point
# MAGIC       ) ID_POINT_PCDT,
# MAGIC       pt.mnemo_long MNEMO_ORIG
# MAGIC     FROM
# MAGIC       point_chaine pc,
# MAGIC       point pt,
# MAGIC       ligne lg
# MAGIC     WHERE
# MAGIC       pc.id_point = pt.id_point
# MAGIC       AND pt.no_periode_topo = lg.no_periode_topo
# MAGIC       AND pc.id_ligne = lg.id_ligne
# MAGIC   ) ptc
# MAGIC   left outer join inter_point ipt on ptc.id_point_pcdt = ipt.id_point_orig
# MAGIC   and ptc.id_point = ipt.id_point_extr
# MAGIC   left outer join point pt on ptc.id_point_pcdt = pt.id_point
# MAGIC ORDER BY
# MAGIC   id_ligne,
# MAGIC   no_chainage,
# MAGIC   sens_parcours,
# MAGIC   ordre_point

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE VIEW [dbo].[VwPaymentExport] AS
# MAGIC SELECT
# MAGIC   /* Payer Infos*/
# MAGIC   Payer.Id AS PayerId,
# MAGIC   Payer.TitleId AS PayerTitleId,
# MAGIC   Payer.FirstName AS PayerFirstName,
# MAGIC   Payer.LastName AS PayerLastName,
# MAGIC   Payer.BirthDate AS PayerBirthDate,
# MAGIC   PayerAddress.ToponymTypeId AS PayerAddressToponym,
# MAGIC   PayerAddress.Address AS PayerAddressAddress,
# MAGIC   PayerAddress.AddressComplement AS PayerAddressComplement,
# MAGIC   PayerAddress.AddressComplement2 AS PayerAddressComplement2,
# MAGIC   PayerAddress.HouseNumber AS PayerAddressHouseNumber,
# MAGIC   PayerAddress.City AS PayerAddressCity,
# MAGIC   PayerAddress.Postcode AS PayerAddressPostcode,
# MAGIC   PayerAddress.Province AS PayerAddressProvince,
# MAGIC   PayerAddress.Hamlet AS PayerAddressHamlet,
# MAGIC   PayerAddress.CountryCd AS PayerAddressCountryCd,
# MAGIC   (
# MAGIC     SELECT
# MAGIC       TOP(1) Address
# MAGIC     FROM
# MAGIC       dbo.CustomerContactEmail as PayerPersonalEmail1
# MAGIC     WHERE
# MAGIC       PayerPersonalEmail1.CustomerId = Payer.Id
# MAGIC       and PayerPersonalEmail1.ContactTypeId = 4
# MAGIC       and PayerPersonalEmail1.IsDeleted = 0
# MAGIC   ) AS PayerPersonalEmail1,
# MAGIC   (
# MAGIC     SELECT
# MAGIC       TOP(1) Address
# MAGIC     FROM
# MAGIC       dbo.CustomerContactEmail as PayerPersonalEmail2
# MAGIC     WHERE
# MAGIC       PayerPersonalEmail2.CustomerId = Payer.Id
# MAGIC       and PayerPersonalEmail2.ContactTypeId = 5
# MAGIC       and PayerPersonalEmail2.IsDeleted = 0
# MAGIC   ) AS PayerPersonalEmail2,
# MAGIC   (
# MAGIC     SELECT
# MAGIC       TOP(1) Address
# MAGIC     FROM
# MAGIC       dbo.CustomerContactEmail as PayerProfessionalEmail1
# MAGIC     WHERE
# MAGIC       PayerProfessionalEmail1.CustomerId = Payer.Id
# MAGIC       and PayerProfessionalEmail1.ContactTypeId = 6
# MAGIC       and PayerProfessionalEmail1.IsDeleted = 0
# MAGIC   ) AS PayerProfessionalEmail1,
# MAGIC   (
# MAGIC     SELECT
# MAGIC       TOP(1) Address
# MAGIC     FROM
# MAGIC       dbo.CustomerContactEmail as PayerProfessionalEmail2
# MAGIC     WHERE
# MAGIC       PayerProfessionalEmail2.CustomerId = Payer.Id
# MAGIC       and PayerProfessionalEmail2.ContactTypeId = 7
# MAGIC       and PayerProfessionalEmail2.IsDeleted = 0
# MAGIC   ) AS PayerProfessionalEmail2,
# MAGIC   (
# MAGIC     SELECT
# MAGIC       Prefix COLLATE DATABASE_DEFAULT + CallNumber COLLATE DATABASE_DEFAULT + ' '
# MAGIC     FROM
# MAGIC       dbo.CustomerContactPhone AS phone
# MAGIC     WHERE
# MAGIC       (IsDeleted = 0)
# MAGIC       AND (Payer.Id = CustomerId)
# MAGIC       AND (CallNumber IS NOT NULL)
# MAGIC     ORDER BY
# MAGIC       phone.Id FOR XML PATH('')
# MAGIC   ) AS PayerPhones,
# MAGIC   /* Card Holder Infos*/
# MAGIC   dbo.CustomerBankAccount.Id AS IbanId,
# MAGIC   dbo.CustomerBankAccount.IBAN,
# MAGIC   dbo.CustomerBankAccount.BIC,
# MAGIC   Holder.Id AS HolderId,
# MAGIC   Holder.CustomerTypeId AS HolderCustomerType,
# MAGIC   Holder.TitleId AS HolderTitleId,
# MAGIC   Holder.FirstName AS HolderFirstName,
# MAGIC   Holder.LastName AS HolderLastName,
# MAGIC   Holder.BirthDate AS HolderBirthDate,
# MAGIC   HolderAddress.ToponymTypeId AS HolderAddressToponym,
# MAGIC   HolderAddress.Address AS HolderAddressAddress,
# MAGIC   HolderAddress.AddressComplement AS HolderAddressComplement,
# MAGIC   HolderAddress.AddressComplement2 AS HolderAddressComplement2,
# MAGIC   HolderAddress.HouseNumber AS HolderAddressHouseNumber,
# MAGIC   HolderAddress.City AS HolderAddressCity,
# MAGIC   HolderAddress.Postcode AS HolderAddressPostcode,
# MAGIC   HolderAddress.Province AS HolderAddressProvince,
# MAGIC   HolderAddress.Hamlet AS HolderAddressHamlet,
# MAGIC   HolderAddress.CountryCd AS HolderAddressCountryCd,
# MAGIC   (
# MAGIC     SELECT
# MAGIC       Prefix COLLATE DATABASE_DEFAULT + CallNumber COLLATE DATABASE_DEFAULT + ' '
# MAGIC     FROM
# MAGIC       dbo.CustomerContactPhone AS phone
# MAGIC     WHERE
# MAGIC       (IsDeleted = 0)
# MAGIC       AND (Holder.Id = CustomerId)
# MAGIC       AND (CallNumber IS NOT NULL)
# MAGIC     ORDER BY
# MAGIC       phone.Id FOR XML PATH('')
# MAGIC   ) AS HolderPhones,
# MAGIC   (
# MAGIC     SELECT
# MAGIC       TOP(1) Address
# MAGIC     FROM
# MAGIC       dbo.CustomerContactEmail as HolderPersonalEmail1
# MAGIC     WHERE
# MAGIC       HolderPersonalEmail1.CustomerId = Holder.Id
# MAGIC       and HolderPersonalEmail1.ContactTypeId = 4
# MAGIC       and HolderPersonalEmail1.IsDeleted = 0
# MAGIC   ) AS HolderPersonalEmail1,
# MAGIC   (
# MAGIC     SELECT
# MAGIC       TOP(1) Address
# MAGIC     FROM
# MAGIC       dbo.CustomerContactEmail as HolderPersonalEmail2
# MAGIC     WHERE
# MAGIC       HolderPersonalEmail2.CustomerId = Holder.Id
# MAGIC       and HolderPersonalEmail2.ContactTypeId = 5
# MAGIC       and HolderPersonalEmail2.IsDeleted = 0
# MAGIC   ) AS HolderPersonalEmail2,
# MAGIC   (
# MAGIC     SELECT
# MAGIC       TOP(1) Address
# MAGIC     FROM
# MAGIC       dbo.CustomerContactEmail as HolderProfessionalEmail1
# MAGIC     WHERE
# MAGIC       HolderProfessionalEmail1.CustomerId = Holder.Id
# MAGIC       and HolderProfessionalEmail1.ContactTypeId = 6
# MAGIC       and HolderProfessionalEmail1.IsDeleted = 0
# MAGIC   ) AS HolderProfessionalEmail1,
# MAGIC   (
# MAGIC     SELECT
# MAGIC       TOP(1) Address
# MAGIC     FROM
# MAGIC       dbo.CustomerContactEmail as HolderProfessionalEmail2
# MAGIC     WHERE
# MAGIC       HolderProfessionalEmail2.CustomerId = Holder.Id
# MAGIC       and HolderProfessionalEmail2.ContactTypeId = 7
# MAGIC       and HolderProfessionalEmail2.IsDeleted = 0
# MAGIC   ) AS HolderProfessionalEmail2,
# MAGIC   Company.Id AS CompanyId,
# MAGIC   Company.LastName AS CompanyName,
# MAGIC   dbo.Installment.Id AS PlanId,
# MAGIC   dbo.PaymentOrder.Rum,
# MAGIC   /* Contract Infos*/
# MAGIC   dbo.Product.Code AS ContractCode,
# MAGIC   dbo.FareProductItemDescription.CommercialName AS ContractDescription,
# MAGIC   CASE
# MAGIC     WHEN Installment.IsRenewable = 1 THEN Installment.EndValidityDate
# MAGIC     ELSE CardImageItem.EndValidityDate
# MAGIC   END as ContractEndValidity,
# MAGIC   dbo.Card.SerialNumber AS CardSerialNumber,
# MAGIC   dbo.InstallmentDetail.PaymentDate,
# MAGIC   dbo.InstallmentDetail.PaymentAmount,
# MAGIC   status.Id AS InstallmentStatusId,
# MAGIC   statusTrans.Transl AS InstallmentStatus,
# MAGIC   dbo.InstallmentDetail.ModDate AS InstallmentChangeDate,
# MAGIC   dbo.InstallmentDetail.RejectDate,
# MAGIC   dbo.InstallmentDetail.RejectReasonId AS InstallmentRejectReasonId,
# MAGIC   rejectReasonTrans.Transl AS InstallmentRejectReason,
# MAGIC   statusReasonTrans.Transl AS InstallmentStatusReason,
# MAGIC   amountReasonTrans.Transl AS InstallmentAmountReason,
# MAGIC   dbo.FareProductItemDescription.LanguageCode,
# MAGIC   dbo.Installment.PlanStatusId,
# MAGIC   --->>> DS#16416 - 12/05/2020 - PréFat_Patch3_HF1 - CS - "Export des échéances rejetées" number of rejected payment is not correctly calculated <<<---
# MAGIC   isnull(NRejected.NRejected, 0) As NRejected --->>> *************************************************************************************************************************************** <<<---
# MAGIC FROM
# MAGIC   dbo.SaleDetail
# MAGIC   INNER JOIN dbo.PaymentOrder
# MAGIC   INNER JOIN dbo.Installment
# MAGIC   INNER JOIN dbo.InstallmentDetail ON dbo.Installment.Id = dbo.InstallmentDetail.InstalmentId
# MAGIC   INNER JOIN dbo.InstallmentStatus AS status ON dbo.InstallmentDetail.StatusId = status.Id ON dbo.PaymentOrder.Id = dbo.Installment.PaymentOrderId
# MAGIC   INNER JOIN dbo.CustomerBankAccount
# MAGIC   INNER JOIN dbo.Customer AS Payer ON dbo.CustomerBankAccount.CustomerId = Payer.Id ON dbo.PaymentOrder.CustomerBankAccountId = dbo.CustomerBankAccount.Id ON dbo.SaleDetail.Id = dbo.Installment.SaleDetailId
# MAGIC   INNER JOIN dbo.CustomerAddress AS PayerAddress ON PayerAddress.Id = (
# MAGIC     SELECT
# MAGIC       TOP (1) Id
# MAGIC     FROM
# MAGIC       dbo.CustomerAddress
# MAGIC     WHERE
# MAGIC       (CustomerId = Payer.Id)
# MAGIC       AND (IsDeleted = 0)
# MAGIC   )
# MAGIC   INNER JOIN dbo.FareProductItem
# MAGIC   INNER JOIN dbo.FareProductItemDescription ON dbo.FareProductItem.Id = dbo.FareProductItemDescription.FareProductItemId ON dbo.SaleDetail.FareProductItemId = dbo.FareProductItem.Id
# MAGIC   INNER JOIN dbo.Product ON dbo.Product.Id = dbo.FareProductItem.ProductId
# MAGIC   INNER JOIN dbo.Card ON dbo.SaleDetail.CardId = dbo.Card.Id
# MAGIC   INNER JOIN dbo.Customer AS Holder ON dbo.Card.CustomerId = Holder.Id
# MAGIC   INNER JOIN dbo.CustomerAddress AS HolderAddress ON HolderAddress.Id = (
# MAGIC     SELECT
# MAGIC       TOP (1) Id
# MAGIC     FROM
# MAGIC       dbo.CustomerAddress
# MAGIC     WHERE
# MAGIC       (CustomerId = Holder.Id)
# MAGIC       AND (IsDeleted = 0)
# MAGIC   )
# MAGIC   LEFT OUTER JOIN dbo.CustomerGroup AS HolderGroup ON HolderGroup.Id = (
# MAGIC     SELECT
# MAGIC       TOP (1) GroupId
# MAGIC     FROM
# MAGIC       dbo.CustomerCustGroup
# MAGIC     WHERE
# MAGIC       (CustomerId = Holder.Id)
# MAGIC       AND (IsDeleted = 0)
# MAGIC   )
# MAGIC   AND (HolderGroup.GroupTypeId = 2)
# MAGIC   LEFT OUTER JOIN dbo.Customer AS Company ON Company.LastName COLLATE DATABASE_DEFAULT = HolderGroup.Code COLLATE DATABASE_DEFAULT
# MAGIC   AND Company.CustomerTypeId = 2
# MAGIC   LEFT OUTER JOIN dbo.Translation AS statusTrans ON statusTrans.TableName = 'InstallmentStatus'
# MAGIC   AND status.Id = statusTrans.ObjectId
# MAGIC   AND statusTrans.LanguageCode = dbo.FareProductItemDescription.LanguageCode
# MAGIC   LEFT OUTER JOIN dbo.Translation AS statusReasonTrans ON statusReasonTrans.TableName = 'InstallmentStatusReason'
# MAGIC   AND dbo.InstallmentDetail.StatusReasonId = statusReasonTrans.ObjectId
# MAGIC   AND statusReasonTrans.LanguageCode = dbo.FareProductItemDescription.LanguageCode
# MAGIC   LEFT OUTER JOIN dbo.Translation AS rejectReasonTrans ON rejectReasonTrans.TableName = 'InstallmentRejectReason'
# MAGIC   AND dbo.InstallmentDetail.RejectReasonId = rejectReasonTrans.ObjectId
# MAGIC   AND rejectReasonTrans.LanguageCode = dbo.FareProductItemDescription.LanguageCode
# MAGIC   LEFT OUTER JOIN dbo.Translation AS amountReasonTrans ON amountReasonTrans.TableName = 'InstallmentAmountReason'
# MAGIC   AND dbo.InstallmentDetail.AmountReasonId = amountReasonTrans.ObjectId
# MAGIC   AND amountReasonTrans.LanguageCode = dbo.FareProductItemDescription.LanguageCode
# MAGIC   INNER JOIN dbo.CardImageItem as CardImageItem ON CardImageItem.CardId = SaleDetail.CardId
# MAGIC   and CardImageItem.Tcn = SaleDetail.Tcn --->>> DS#16416 - 12/05/2020 - PréFat_Patch3_HF1 - CS - "Export des échéances rejetées" number of rejected payment is not correctly calculated <<<---
# MAGIC   LEFT JOIN (
# MAGIC     Select
# MAGIC       InstalmentId,
# MAGIC       count(*) As NRejected
# MAGIC     From
# MAGIC       InstallmentDetail id
# MAGIC     Where
# MAGIC       Id.StatusId = 3 --->>> DS#16959 - 03/08/2020 - FAT_R9_Patch3_HF2 | CS : "Export des échéances rejetées" nombre des échéances rejetées n'est calculé correctement <<<---
# MAGIC       AND --->>> NOTE, so also takes the Rejected in the future <<<---
# MAGIC       datediff(
# MAGIC         month,
# MAGIC         cast(id.PaymentDate as date),
# MAGIC         cast(GETDATE() as date)
# MAGIC       ) < 12 --->>> ***************************************************************************************************************************************** <<<---
# MAGIC     group by
# MAGIC       InstalmentId
# MAGIC   ) As NRejected On NRejected.InstalmentId = Installment.Id --->>> *************************************************************************************************************************************** <<<---

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select
# MAGIC   compte.NUMCOMPTE,
# MAGIC   compte.LIBCOMPTE,
# MAGIC   ventes.Num_transaction,
# MAGIC   ventes.Date_transaction,
# MAGIC   ventes.Code_produit,
# MAGIC   produit.LIBPRO,
# MAGIC   produit.CODFAMPRO,
# MAGIC   table_produit.regroupement_validations_produit_dmci,
# MAGIC   case
# MAGIC     when table_produit.Quantite_voyages_comptables is null then 1
# MAGIC     else table_produit.Quantite_voyages_comptables
# MAGIC   end as Quantite_voyages_comptables,
# MAGIC   regroupement_dmci.famille_de_titres_dmci,
# MAGIC   regroupement_dmci.regroupement_titres_dmci,
# MAGIC   ventes.Validite_debut,
# MAGIC   case
# MAGIC     when (
# MAGIC       ventes.Validite_debut is null
# MAGIC       and regroupement_dmci.famille_de_titres_dmci = 'Abonnements'
# MAGIC       and regroupement_dmci.regroupement_titres_dmci ilike 'Annuel%'
# MAGIC     ) then ventes.Date_transaction
# MAGIC     when (
# MAGIC       ventes.Validite_debut is null
# MAGIC       and regroupement_dmci.famille_de_titres_dmci in ('Abonnements', 'Mensualités')
# MAGIC       and regroupement_dmci.regroupement_titres_dmci ilike 'Mensu%'
# MAGIC     ) then ventes.Date_transaction
# MAGIC     when (
# MAGIC       ventes.Validite_debut is null
# MAGIC       and regroupement_dmci.famille_de_titres_dmci in ('Occasionnels')
# MAGIC     ) then ventes.Date_transaction
# MAGIC     when (
# MAGIC       ventes.Validite_debut is null
# MAGIC     ) then ventes.Date_transaction
# MAGIC     else ventes.Validite_debut
# MAGIC   end AS Validite_debut_date_metier,
# MAGIC   ventes.Validite_fin,
# MAGIC   case
# MAGIC     when (
# MAGIC       ventes.Validite_fin is null
# MAGIC       and regroupement_dmci.famille_de_titres_dmci = 'Abonnements'
# MAGIC       and regroupement_dmci.regroupement_titres_dmci ilike 'Annuel%'
# MAGIC     ) then add_months(ventes.Date_transaction, 11)
# MAGIC     when (
# MAGIC       ventes.Validite_fin is null
# MAGIC       and regroupement_dmci.famille_de_titres_dmci in ('Abonnements', 'Mensualités')
# MAGIC       and regroupement_dmci.regroupement_titres_dmci ilike 'Mensu%'
# MAGIC     ) then to_timestamp(last_day(ventes.Date_transaction))
# MAGIC     when (
# MAGIC       ventes.Validite_fin is null
# MAGIC       and regroupement_dmci.famille_de_titres_dmci in ('Occasionnels')
# MAGIC     ) then ventes.Date_transaction
# MAGIC     when (
# MAGIC       ventes.Validite_fin is null
# MAGIC     ) then ventes.Date_transaction
# MAGIC     else ventes.Validite_fin
# MAGIC   end AS Validite_fin_date_metier,
# MAGIC   ventes.Quantite_vendue,
# MAGIC   case
# MAGIC     when table_produit.Quantite_voyages_comptables is null then 1
# MAGIC     else table_produit.Quantite_voyages_comptables
# MAGIC   end * ventes.Quantite_vendue as Quantite_Vendue_metier,
# MAGIC   ventes.Prix_vente,
# MAGIC   ventes.Mt_vente,
# MAGIC   ventes.Tx_TVA,
# MAGIC   case
# MAGIC     when (
# MAGIC       regroupement_dmci.famille_de_titres_dmci = 'Abonnements'
# MAGIC       and regroupement_dmci.regroupement_titres_dmci rlike 'Annuel'
# MAGIC     ) then ventes.Prix_vente / 12
# MAGIC     else ventes.Prix_vente
# MAGIC   end as Prix_ventes_metier,
# MAGIC   case
# MAGIC     when (
# MAGIC       regroupement_dmci.famille_de_titres_dmci = 'Abonnements'
# MAGIC       and regroupement_dmci.regroupement_titres_dmci rlike 'Annuel'
# MAGIC     ) then ventes.Prix_vente / 12
# MAGIC     else ventes.Prix_vente
# MAGIC   end * 100 /(100 + ventes.Tx_TVA) as Prix_ventes_metier_ht
# MAGIC from
# MAGIC   titan_recette.ventes
# MAGIC   left join titan_recette.produit on (produit.CODPRO = ventes.Code_produit)
# MAGIC   left join titan_recette.compte on (compte.NUMCOMPTE = produit.NUMCOMPTE)
# MAGIC   left join ref_produit.table_produit on (table_produit.Code_produit = ventes.Code_produit)
# MAGIC   left join ref_produit.regroupement_dmci on (regroupement_dmci.id_regroupement_produit_dmci = table_produit.id_regroupement_produit_dmci)
# MAGIC 
# MAGIC where 
# MAGIC 
# MAGIC ventes.Date_transaction>='2021-10-01'
# MAGIC and
# MAGIC ventes.Date_transaction<'2021-11-01'
# MAGIC and (
# MAGIC compte.NUMCOMPTE = 487200)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from titan_recette.produit
# MAGIC where codpro in (35214,35414,35614,36414,36614,35014)

# COMMAND ----------


