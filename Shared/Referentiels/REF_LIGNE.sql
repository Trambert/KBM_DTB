-- Databricks notebook source
select
  lignebus.ID_LIGNE,
  ligneBus.MNEMO_LIGNE ,
  ligneBus.NO_LIGNE,
  ligneBus.NO_LIGNE_INTERNE,
  ligneBus.LIBELLE_LIGNE,
  ligneSIGBus.Shape,
  ligneSIGBus.NOMCOULEUR,
  ligneSIGBus.NOM,
  ligneSIGBus.LIGNESAE,
  ligneSIGBus.LIGNEQUALITEPLUS,
  ligneSIGBus.ID_LIGNE,
  ligneSIGBus.INACTIF,
  ligneSIGBus.MODE,
  ligneSIGBus.ORDRE_TRI,
  ligneSIGBus.CODE_SAE,
  BillettiqueBus.LINEID,
  BillettiqueBus.CODE,
  BillettiqueBus.DESCRIPTION,
  BillettiqueBus.NUM

from
  saebus.ligne as ligneBus
  LEFT JOIN  sig.ligne_evw as ligneSIGBus on ligneBus.NO_LIGNE = ligneSIGBus.CODE_SAE
  LEFT join billettique.line as BillettiqueBus on ligneBus.NO_LIGNE=BillettiqueBus.num
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
  ligneTram.MNEMO_LIGNE,
  --ligneTram.MNEMO_LIGNE_COMMERCIAL,
  ligneTram.NO_LIGNE,
  ligneTram.NO_LIGNE_INTERNE,
  ligneTram.LIBELLE_LIGNE,
  ligneSIGTram.Shape,
  ligneSIGTram.NOMCOULEUR,
  ligneSIGTram.NOM,
  ligneSIGTram.LIGNESAE,
  ligneSIGTram.LIGNEQUALITEPLUS,
  ligneSIGTram.ID_LIGNE,
  ligneSIGTram.INACTIF,
  ligneSIGTram.MODE,
  ligneSIGTram.ORDRE_TRI,
  ligneSIGTram.CODE_SAE,
  BillettiqueTram.LINEID,
  BillettiqueTram.CODE,
  BillettiqueTram.DESCRIPTION,
  BillettiqueTram.NUM
from
  saetram.ligne as ligneTram
  left join sig.ligne_evw as ligneSIGTram on ligneTram.NO_LIGNE = ligneSIGTram.ID_LIGNE
  left join billettique.line as BillettiqueTram on ligneTram.NO_LIGNE=BillettiqueTram.num
where
  ligneTram.NO_PERIODE_TOPO = (
    select
      max(NO_PERIODE_TOPO)
    from
      saetram.topologie
  )
  
  union 
  
  
SELECT
   souslignetram.ID_SOUS_LIGNE as ID_LIGNE,
   souslignetram.MNEMO_SOUS_LIGNE as MNEMO_LIGNE,
   souslignetram.NO_SOUS_LIGNE AS NO_LIGNE,
  souslignetram.LIBELLE_SOUS_LIGNE as LIBELLE_LIGNE,
  souslignetram.NO_SOUS_LIGNE AS NO_LIGNE,
  ligneSIGTram.Shape,
  ligneSIGTram.NOMCOULEUR,
  ligneSIGTram.NOM,
  ligneSIGTram.LIGNESAE,
  ligneSIGTram.LIGNEQUALITEPLUS,
  ligneSIGTram.ID_LIGNE,
  ligneSIGTram.INACTIF,
  ligneSIGTram.MODE,
  ligneSIGTram.ORDRE_TRI,
  ligneSIGTram.CODE_SAE,
  BillettiqueTram.LINEID,
  BillettiqueTram.CODE,
  BillettiqueTram.DESCRIPTION,
  BillettiqueTram.NUM
  FROM 
    saetram.sous_ligne AS souslignetram
  left join sig.ligne_evw as ligneSIGTram on souslignetram.NO_SOUS_LIGNE = ligneSIGTram.ID_LIGNE
  left join billettique.line as BillettiqueTram on souslignetram.NO_SOUS_LIGNE=BillettiqueTram.num
  WHERE 
  
  souslignetram.NO_SOUS_LIGNE=62


-- COMMAND ----------



