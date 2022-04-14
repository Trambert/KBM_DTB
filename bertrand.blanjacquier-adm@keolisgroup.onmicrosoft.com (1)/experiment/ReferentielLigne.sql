-- Databricks notebook source
select substring_index(numero,'/',1), *
from 
  evex.ligne

-- COMMAND ----------

select * 
from 
  saebus.ligne as ligneBus
left outer join billettique.line as ligneBillettique on ligneBus.NO_LIGNE=ligneBillettique.num
  --left outer join saetram.ligne as ligneTram on ligneTram.NO_LIGNE=ligneBillettique.num
left outer join sig.ligne_evw as ligneSIG on ligneBus.NO_LIGNE = ligneSIG.CODE_SAE
where
  ligneBus.NO_PERIODE_TOPO = (select max(NO_PERIODE_TOPO) from saebus.topologie)
order by ligneBus.MNEMO_LIGNE

-- COMMAND ----------

select * 
from 
  saebus.ligne as ligneBus
  inner join saetram.ligne as ligneTram on ligneBus.NO_LIGNE = ligneTram.NO_LIGNE

-- COMMAND ----------

select
  ligneBus.ID_LIGNE,
  NO_PERIODE_TOPO,
  MNEMO_LIGNE,
  --MNEMO_LIGNE_COMMERCIAL,
  NO_LIGNE,
  NO_LIGNE_INTERNE,
  LIBELLE_LIGNE,
  SENS_ALLER,
  LOCALISATION_ABSOLUE,
  DIST_AFFICHEUR,
  DIST_GIROUETTE,
  MNEMO_TRANSPORTEUR,
  TYPE_LIGNE,
  NUM_PRIO_FEU,
  SEUIL_1_TRAIN_BUS_DIST,
  SEUIL_2_TRAIN_BUS_DIST,
  SEUIL_1_TRAIN_BUS_TPS,
  SEUIL_2_TRAIN_BUS_TPS,
  SEUIL_AVANCE_ARR,
  SEUIL_RETARD_ARR,
  SEUIL_1_AV_ALM,
  SEUIL_2_AV_ALM,
  SEUIL_1_RET_ALM,
  SEUIL_2_RET_ALM,
  LANGUE,
  ligneSIGBus.OBJECTID,
  ligneSIGBus.Shape,
  ligneSIGBus.GDB_GEOMATTR_DATA,
  ligneSIGBus.NOMRL,
  ligneSIGBus.NOMCOULEUR,
  ligneSIGBus.NOM,
  ligneSIGBus.LIGNESAE,
  ligneSIGBus.LIGNEQUALITEPLUS,
  ligneSIGBus.ID_LIGNE,
  ligneSIGBus.INACTIF,
  ligneSIGBus.MODE,
  ligneSIGBus.ORDRE_TRI,
  ligneSIGBus.CODE_SAE,
  ligneSIGBus.GlobalID,
  ligneSIGBus.SDE_STATE_ID,
  BillettiqueBus.LINEID,
  BillettiqueBus.CODE,
  BillettiqueBus.DESCRIPTION,
  BillettiqueBus.NUM,
  BillettiqueBus.ISSUBST,
  BillettiqueBus.TOPOLOGYPERIODID,
  BillettiqueBus.TRANSPORTOPERATORID,
  BillettiqueBus.TOPOLOGYVERSIONID,
  BillettiqueBus.RV
from
  saebus.ligne as ligneBus
  inner join sig.ligne_evw as ligneSIGBus on ligneBus.NO_LIGNE = ligneSIGBus.CODE_SAE
  inner join billettique.line as BillettiqueBus on ligneBus.NO_LIGNE=BillettiqueBus.num
where
  ligneBus.NO_PERIODE_TOPO = (
    select
      max(NO_PERIODE_TOPO)
    from
      saebus.topologie
  )
union
select
  ligneTram.ID_LIGNE,
  NO_PERIODE_TOPO,
  MNEMO_LIGNE,
  NO_LIGNE,
  NO_LIGNE_INTERNE,
  LIBELLE_LIGNE,
  SENS_ALLER,
  LOCALISATION_ABSOLUE,
  DIST_AFFICHEUR,
  DIST_GIROUETTE,
  MNEMO_TRANSPORTEUR,
  TYPE_LIGNE,
  NUM_PRIO_FEU,
  SEUIL_1_TRAIN_BUS_DIST,
  SEUIL_2_TRAIN_BUS_DIST,
  SEUIL_1_TRAIN_BUS_TPS,
  SEUIL_2_TRAIN_BUS_TPS,
  SEUIL_AVANCE_ARR,
  SEUIL_RETARD_ARR,
  SEUIL_1_AV_ALM,
  SEUIL_2_AV_ALM,
  SEUIL_1_RET_ALM,
  SEUIL_2_RET_ALM,
  LANGUE,
  ligneSIGTram.OBJECTID,
  ligneSIGTram.Shape,
  ligneSIGTram.GDB_GEOMATTR_DATA,
  ligneSIGTram.NOMRL,
  ligneSIGTram.NOMCOULEUR,
  ligneSIGTram.NOM,
  ligneSIGTram.LIGNESAE,
  ligneSIGTram.LIGNEQUALITEPLUS,
  ligneSIGTram.ID_LIGNE,
  ligneSIGTram.INACTIF,
  ligneSIGTram.MODE,
  ligneSIGTram.ORDRE_TRI,
  ligneSIGTram.CODE_SAE,
  ligneSIGTram.GlobalID,
  ligneSIGTram.SDE_STATE_ID,
  BillettiqueTram.LINEID,
  BillettiqueTram.CODE,
  BillettiqueTram.DESCRIPTION,
  BillettiqueTram.NUM,
  BillettiqueTram.ISSUBST,
  BillettiqueTram.TOPOLOGYPERIODID,
  BillettiqueTram.TRANSPORTOPERATORID,
  BillettiqueTram.TOPOLOGYVERSIONID,
  BillettiqueTram.RV
from
  saetram.ligne as ligneTram
  inner join sig.ligne_evw as ligneSIGTram on ligneTram.NO_LIGNE = ligneSIGTram.ID_LIGNE
  inner join billettique.line as BillettiqueTram on ligneTram.NO_LIGNE=BillettiqueTram.num
where
  ligneTram.NO_PERIODE_TOPO = (
    select
      max(NO_PERIODE_TOPO)
    from
      saetram.topologie
  )

-- COMMAND ----------

select
  ligneTram.ID_LIGNE,
  NO_PERIODE_TOPO,
  MNEMO_LIGNE,
  NO_LIGNE,
  NO_LIGNE_INTERNE,
  LIBELLE_LIGNE,
  SENS_ALLER,
  LOCALISATION_ABSOLUE,
  DIST_AFFICHEUR,
  DIST_GIROUETTE,
  MNEMO_TRANSPORTEUR,
  TYPE_LIGNE,
  NUM_PRIO_FEU,
  SEUIL_1_TRAIN_BUS_DIST,
  SEUIL_2_TRAIN_BUS_DIST,
  SEUIL_1_TRAIN_BUS_TPS,
  SEUIL_2_TRAIN_BUS_TPS,
  SEUIL_AVANCE_ARR,
  SEUIL_RETARD_ARR,
  SEUIL_1_AV_ALM,
  SEUIL_2_AV_ALM,
  SEUIL_1_RET_ALM,
  SEUIL_2_RET_ALM,
  LANGUE,
  ligneSIGTram.OBJECTID,
  ligneSIGTram.Shape,
  ligneSIGTram.GDB_GEOMATTR_DATA,
  ligneSIGTram.NOMRL,
  ligneSIGTram.NOMCOULEUR,
  ligneSIGTram.NOM,
  ligneSIGTram.LIGNESAE,
  ligneSIGTram.LIGNEQUALITEPLUS,
  ligneSIGTram.ID_LIGNE,
  ligneSIGTram.INACTIF,
  ligneSIGTram.MODE,
  ligneSIGTram.ORDRE_TRI,
  ligneSIGTram.CODE_SAE,
  ligneSIGTram.GlobalID,
  ligneSIGTram.SDE_STATE_ID
from
  saetram.ligne as ligneTram
  inner join sig.ligne_evw as ligneSIGTram on ligneTram.NO_LIGNE = ligneSIGTram.ID_LIGNE
where
  ligneTram.NO_PERIODE_TOPO = (
    select
      max(NO_PERIODE_TOPO)
    from
      saetram.topologie
  )

-- COMMAND ----------


