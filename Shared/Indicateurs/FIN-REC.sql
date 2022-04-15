-- Databricks notebook source
select
--ventes.Date_transaction,
compte.NUMCOMPTE,compte.LIBCOMPTE,produit.CODPRO, sum(ventes.Mt_vente), sum(ventes.Quantite_vendue)
from titan_recette.ventes 
left join titan_recette.produit on (produit.CODPRO=ventes.Code_produit)
left join titan_recette.compte on (compte.NUMCOMPTE=produit.NUMCOMPTE)
where 
-- ventes.Date_transaction>='2021-10-01'
-- and 
-- ventes.Date_transaction<'2021-11-01'
-- and 
compte.NUMCOMPTE in ('487502')
group by
--ventes.Date_transaction, 
compte.NUMCOMPTE,compte.LIBCOMPTE,produit.CODPRO



-- COMMAND ----------

select 
compte.NUMCOMPTE, 
coalesce(titres.LIBCOMPTE,compte.LIBCOMPTE) as LIBCOMPTE,
titres.Quantite_Vendue_metier,
titres.Mt_vente,
titres.MtVentesHt

from titan_recette.compte

left join 

(
select
ventes_titres.NUMCOMPTE,
ventes_titres.LIBCOMPTE,
sum(ventes_titres.Quantite_Vendue_metier) as Quantite_Vendue_metier,
sum(ventes_titres.Mt_vente) as Mt_vente,
sum(ventes_titres.Mt_vente)* 100 /(100 + 10 ) as MtVentesHt
from
(
select
compte.NUMCOMPTE, 
case
when 
compte.NUMCOMPTE in ('701080','701081') then regroupement_voyages_comptables.Nom_produits_voyages_comptable
else compte.LIBCOMPTE end as LIBCOMPTE,
ventes.Num_transaction,
  ventes.Date_transaction,
  ventes.Code_produit,
  produit.LIBPRO,
  produit.CODFAMPRO,
  table_produit.regroupement_validations_produit_dmci,
  case
    when table_produit.Quantite_voyages_comptables is null then 1
    else table_produit.Quantite_voyages_comptables
  end as Quantite_voyages_comptables,
  regroupement_dmci.famille_de_titres_dmci,
  regroupement_dmci.regroupement_titres_dmci,
  ventes.Validite_debut,
  case
    when (
      ventes.Validite_debut is null
      and regroupement_dmci.famille_de_titres_dmci = 'Abonnements'
      and regroupement_dmci.regroupement_titres_dmci ilike 'Annuel%'
    ) then ventes.Date_transaction
    when (
      ventes.Validite_debut is null
      and regroupement_dmci.famille_de_titres_dmci in ('Abonnements', 'Mensualités')
      and regroupement_dmci.regroupement_titres_dmci ilike 'Mensu%'
    ) then ventes.Date_transaction
    when (
      ventes.Validite_debut is null
      and regroupement_dmci.famille_de_titres_dmci in ('Occasionnels')
    ) then ventes.Date_transaction
    when (
      ventes.Validite_debut is null
    ) then ventes.Date_transaction
    else ventes.Validite_debut
  end AS Validite_debut_date_metier,
  ventes.Validite_fin,
  case
    when (
      ventes.Validite_fin is null
      and regroupement_dmci.famille_de_titres_dmci = 'Abonnements'
      and regroupement_dmci.regroupement_titres_dmci ilike 'Annuel%'
    ) then add_months(ventes.Date_transaction, 11)
    when (
      ventes.Validite_fin is null
      and regroupement_dmci.famille_de_titres_dmci in ('Abonnements', 'Mensualités')
      and regroupement_dmci.regroupement_titres_dmci ilike 'Mensu%'
    ) then to_timestamp(last_day(ventes.Date_transaction))
    when (
      ventes.Validite_fin is null
      and regroupement_dmci.famille_de_titres_dmci in ('Occasionnels')
    ) then ventes.Date_transaction
    when (
      ventes.Validite_fin is null
    ) then ventes.Date_transaction
    else ventes.Validite_fin
  end AS Validite_fin_date_metier,
  ventes.Quantite_vendue,
  case
    when table_produit.Quantite_voyages_comptables is null then 1
    else table_produit.Quantite_voyages_comptables
  end * ventes.Quantite_vendue as Quantite_Vendue_metier,
  ventes.Prix_vente,
  ventes.Mt_vente,
  ventes.Tx_TVA,
  case
    when (
      regroupement_dmci.famille_de_titres_dmci = 'Abonnements'
      and regroupement_dmci.regroupement_titres_dmci rlike 'Annuel'
    ) then ventes.Prix_vente / 12
    else ventes.Prix_vente
  end as Prix_ventes_metier,
  case
    when (
      regroupement_dmci.famille_de_titres_dmci = 'Abonnements'
      and regroupement_dmci.regroupement_titres_dmci rlike 'Annuel'
    ) then ventes.Prix_vente / 12
    else ventes.Prix_vente
  end * 100 /(100 + ventes.Tx_TVA) as Prix_ventes_metier_ht
from
titan_recette.compte
  left join  titan_recette.produit on (compte.NUMCOMPTE = produit.NUMCOMPTE)
  left join titan_recette.ventes on (produit.CODPRO = ventes.Code_produit)
  left join ref_produit.table_produit on (table_produit.Code_produit = ventes.Code_produit)
  left join ref_produit.regroupement_dmci on (regroupement_dmci.id_regroupement_produit_dmci = table_produit.id_regroupement_produit_dmci)
  left join ref_produit.regroupement_voyages_comptables on (regroupement_voyages_comptables.id_regroupement_voyages_comptables = table_produit.id_regroupement_voyages_comptables)

  ) as ventes_titres
  where
ventes_titres.Validite_debut_date_metier>='2021-10-01'
and
ventes_titres.Validite_fin_date_metier<'2021-11-01'
group by
  ventes_titres.NUMCOMPTE,
  ventes_titres.LIBCOMPTE
) titres on (titres.NUMCOMPTE=compte.NUMCOMPTE)



-- COMMAND ----------

--agregats des abonnements annuels comptants

select 
compte.NUMCOMPTE, 
coalesce(titres.LIBCOMPTE,compte.LIBCOMPTE) as LIBCOMPTE,
titres.Quantite_Vendue_metier,
titres.Mt_vente,
titres.MtVentesHt

from titan_recette.compte

left join 

(
select
ventes_titres.NUMCOMPTE,
ventes_titres.LIBCOMPTE,
sum(ventes_titres.Quantite_Vendue_metier) as Quantite_Vendue_metier,
sum(ventes_titres.Mt_vente) as Mt_vente,
sum(ventes_titres.Mt_vente)* 100 /(100 + 10 ) as MtVentesHt
from
(
select
compte.NUMCOMPTE, 
case
when 
compte.NUMCOMPTE in ('701080','701081') then regroupement_voyages_comptables.Nom_produits_voyages_comptable
else compte.LIBCOMPTE end as LIBCOMPTE,
ventes.Num_transaction,
  ventes.Date_transaction,
  ventes.Code_produit,
  produit.LIBPRO,
  produit.CODFAMPRO,
  table_produit.regroupement_validations_produit_dmci,
  case
    when table_produit.Quantite_voyages_comptables is null then 1
    else table_produit.Quantite_voyages_comptables
  end as Quantite_voyages_comptables,
  regroupement_dmci.famille_de_titres_dmci,
  regroupement_dmci.regroupement_titres_dmci,
  ventes.Validite_debut,
  case
    when (
      (ventes.Validite_debut is null or ventes.Validite_debut < ventes.Date_transaction)
      and regroupement_dmci.famille_de_titres_dmci = 'Abonnements'
      and regroupement_dmci.regroupement_titres_dmci ilike 'Annuel%'
    ) then date_add(last_day(add_months(Date_transaction, -1)),1)
    when (
      (ventes.Validite_debut is not null and ventes.Validite_debut >= ventes.Date_transaction)
      and regroupement_dmci.famille_de_titres_dmci = 'Abonnements'
      and regroupement_dmci.regroupement_titres_dmci ilike 'Annuel%'
    ) then date_add(last_day(add_months(ventes.Validite_debut, -1)),1)
    when (
      ventes.Validite_debut is null
      and regroupement_dmci.famille_de_titres_dmci in ('Abonnements', 'Mensualités')
      and regroupement_dmci.regroupement_titres_dmci ilike 'Mensu%'
    ) then ventes.Date_transaction
    when (
      ventes.Validite_debut is null
      and regroupement_dmci.famille_de_titres_dmci in ('Occasionnels')
    ) then ventes.Date_transaction
    when (
      ventes.Validite_debut is null
    ) then ventes.Date_transaction
    else ventes.Validite_debut
  end AS Validite_debut_date_metier,
  ventes.Validite_fin,
  case
    when (
      (ventes.Validite_debut is null or ventes.Validite_debut < ventes.Date_transaction)
      and regroupement_dmci.famille_de_titres_dmci = 'Abonnements'
      and regroupement_dmci.regroupement_titres_dmci ilike 'Annuel%'
    ) then last_day(add_months(date_add(last_day(add_months(Date_transaction, -1)),1),11))
    when (
      (ventes.Validite_debut is not null and ventes.Validite_debut >= ventes.Date_transaction)
      and regroupement_dmci.famille_de_titres_dmci = 'Abonnements'
      and regroupement_dmci.regroupement_titres_dmci ilike 'Annuel%'
    ) then last_day(add_months(date_add(last_day(add_months(ventes.Validite_debut, -1)),1),11))
    /*when (
      ventes.Validite_fin is null
      and regroupement_dmci.famille_de_titres_dmci = 'Abonnements'
      and regroupement_dmci.regroupement_titres_dmci ilike 'Annuel%'
    ) then last_day(add_months(ventes.Date_transaction, 11))*/
    when (
      ventes.Validite_fin is null
      and regroupement_dmci.famille_de_titres_dmci in ('Abonnements', 'Mensualités')
      and regroupement_dmci.regroupement_titres_dmci ilike 'Mensu%'
    ) then to_timestamp(last_day(ventes.Date_transaction))
    when (
      ventes.Validite_fin is null
      and regroupement_dmci.famille_de_titres_dmci in ('Occasionnels')
    ) then ventes.Date_transaction
    when (
      ventes.Validite_fin is null
    ) then ventes.Date_transaction
    else ventes.Validite_fin
  end AS Validite_fin_date_metier,
  ventes.Quantite_vendue,
  case
    when table_produit.Quantite_voyages_comptables is null then 1
    else table_produit.Quantite_voyages_comptables
  end * ventes.Quantite_vendue as Quantite_Vendue_metier,
  ventes.Prix_vente,
  ventes.Mt_vente,
  ventes.Tx_TVA,
  case
    when (
      regroupement_dmci.famille_de_titres_dmci = 'Abonnements'
      and regroupement_dmci.regroupement_titres_dmci rlike 'Annuel'
    ) then ventes.Prix_vente / 12
    else ventes.Prix_vente
  end as Prix_ventes_metier,
  case
    when (
      regroupement_dmci.famille_de_titres_dmci = 'Abonnements'
      and regroupement_dmci.regroupement_titres_dmci rlike 'Annuel'
    ) then ventes.Prix_vente / 12
    else ventes.Prix_vente
  end * 100 /(100 + ventes.Tx_TVA) as Prix_ventes_metier_ht
from
titan_recette.compte
  left join  titan_recette.produit on (compte.NUMCOMPTE = produit.NUMCOMPTE)
  left join titan_recette.ventes on (produit.CODPRO = ventes.Code_produit)
  left join ref_produit.table_produit on (table_produit.Code_produit = ventes.Code_produit)
  left join ref_produit.regroupement_dmci on (regroupement_dmci.id_regroupement_produit_dmci = table_produit.id_regroupement_produit_dmci)
  left join ref_produit.regroupement_voyages_comptables on (regroupement_voyages_comptables.id_regroupement_voyages_comptables = table_produit.id_regroupement_voyages_comptables)
  where produit.CODPRO in (35214,35414,35614,36414,36614,35014)
  ) as ventes_titres
  where
  (ventes_titres.Validite_debut_date_metier<'2021-10-01' and ventes_titres.Validite_fin_date_metier > '2021-10-01')
  or
  (ventes_titres.Validite_debut_date_metier<'2021-11-01' and ventes_titres.Validite_fin_date_metier > '2021-11-01')
group by
  ventes_titres.NUMCOMPTE,
  ventes_titres.LIBCOMPTE
) titres on (titres.NUMCOMPTE=compte.NUMCOMPTE)




-- COMMAND ----------

--Detail des lignes pour les produits d'abonnements annuels comptant
select
  compte.NUMCOMPTE,
  compte.LIBCOMPTE,
  ventes.Num_transaction,
  ventes.Date_transaction,
  ventes.Code_produit,
  produit.LIBPRO,
  produit.CODFAMPRO,
  table_produit.regroupement_validations_produit_dmci,
  case
    when table_produit.Quantite_voyages_comptables is null then 1
    else table_produit.Quantite_voyages_comptables
  end as Quantite_voyages_comptables,
  regroupement_dmci.famille_de_titres_dmci,
  regroupement_dmci.regroupement_titres_dmci,
  ventes.Validite_debut,
  case
    when (
      (ventes.Validite_debut is null or ventes.Validite_debut < ventes.Date_transaction)
      and regroupement_dmci.famille_de_titres_dmci = 'Abonnements'
      and regroupement_dmci.regroupement_titres_dmci ilike 'Annuel%'
    ) then date_add(last_day(add_months(Date_transaction, -1)),1)
    when (
      (ventes.Validite_debut is not null and ventes.Validite_debut >= ventes.Date_transaction)
      and regroupement_dmci.famille_de_titres_dmci = 'Abonnements'
      and regroupement_dmci.regroupement_titres_dmci ilike 'Annuel%'
    ) then date_add(last_day(add_months(ventes.Validite_debut, -1)),1)
    when (
      ventes.Validite_debut is null
      and regroupement_dmci.famille_de_titres_dmci in ('Abonnements', 'Mensualités')
      and regroupement_dmci.regroupement_titres_dmci ilike 'Mensu%'
    ) then ventes.Date_transaction
    when (
      ventes.Validite_debut is null
      and regroupement_dmci.famille_de_titres_dmci in ('Occasionnels')
    ) then ventes.Date_transaction
    when (
      ventes.Validite_debut is null
    ) then ventes.Date_transaction
    else ventes.Validite_debut
  end AS Validite_debut_date_metier,
  ventes.Validite_fin,
  case
    when (
      (ventes.Validite_debut is null or ventes.Validite_debut < ventes.Date_transaction)
      and regroupement_dmci.famille_de_titres_dmci = 'Abonnements'
      and regroupement_dmci.regroupement_titres_dmci ilike 'Annuel%'
    ) then last_day(add_months(date_add(last_day(add_months(Date_transaction, -1)),1),11))
    when (
      (ventes.Validite_debut is not null and ventes.Validite_debut >= ventes.Date_transaction)
      and regroupement_dmci.famille_de_titres_dmci = 'Abonnements'
      and regroupement_dmci.regroupement_titres_dmci ilike 'Annuel%'
    ) then last_day(add_months(date_add(last_day(add_months(ventes.Validite_debut, -1)),1),11))
    /*when (
      ventes.Validite_fin is null
      and regroupement_dmci.famille_de_titres_dmci = 'Abonnements'
      and regroupement_dmci.regroupement_titres_dmci ilike 'Annuel%'
    ) then last_day(add_months(ventes.Date_transaction, 11))*/
    when (
      ventes.Validite_fin is null
      and regroupement_dmci.famille_de_titres_dmci in ('Abonnements', 'Mensualités')
      and regroupement_dmci.regroupement_titres_dmci ilike 'Mensu%'
    ) then to_timestamp(last_day(ventes.Date_transaction))
    when (
      ventes.Validite_fin is null
      and regroupement_dmci.famille_de_titres_dmci in ('Occasionnels')
    ) then ventes.Date_transaction
    when (
      ventes.Validite_fin is null
    ) then ventes.Date_transaction
    else ventes.Validite_fin
  end AS Validite_fin_date_metier,
  ventes.Quantite_vendue,
  case
    when table_produit.Quantite_voyages_comptables is null then 1
    else table_produit.Quantite_voyages_comptables
  end * ventes.Quantite_vendue as Quantite_Vendue_metier,
  ventes.Prix_vente,
  ventes.Mt_vente,
  ventes.Tx_TVA,
  case
    when (
      regroupement_dmci.famille_de_titres_dmci = 'Abonnements'
      and regroupement_dmci.regroupement_titres_dmci rlike 'Annuel'
    ) then ventes.Mt_vente / 12
    else ventes.Mt_vente
  end as Prix_ventes_metier,
  case
    when (
      regroupement_dmci.famille_de_titres_dmci = 'Abonnements'
      and regroupement_dmci.regroupement_titres_dmci rlike 'Annuel'
    ) then ventes.Prix_vente / 12
    else ventes.Prix_vente
  end * 100 /(100 + ventes.Tx_TVA) as Prix_ventes_metier_ht
from
  titan_recette.ventes
  left join titan_recette.produit on (produit.CODPRO = ventes.Code_produit)
  left join titan_recette.compte on (compte.NUMCOMPTE = produit.NUMCOMPTE)
  left join ref_produit.table_produit on (table_produit.Code_produit = ventes.Code_produit)
  left join ref_produit.regroupement_dmci on (regroupement_dmci.id_regroupement_produit_dmci = table_produit.id_regroupement_produit_dmci)

where 
ventes.Date_transaction>='2021-10-01'
and
ventes.Date_transaction<'2021-11-01'
and produit.CODPRO in (35214,35414,35614,36414,36614,35014)
--and (compte.NUMCOMPTE=701590 or compte.NUMCOMPTE = 487200)

-- COMMAND ----------

select 
produit.NUMCOMPTE,
produit.*
from titan_recette.produit
where produit.CODPRO in (35214, 35414, 35614, 36414, 36614, 35614)
order by produit.CODPRO

-- COMMAND ----------


