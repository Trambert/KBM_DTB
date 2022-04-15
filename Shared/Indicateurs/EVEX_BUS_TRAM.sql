-- Databricks notebook source
SELECT
  evenement.id,
  evenement.jour_exploitation_debut,
  evenement.jour_exploitation_fin,
  evenement.date_heure_reelle_debut,
  evenement.date_heure_reelle_fin,
  evenement.date_heure_saisie,
  activite.libelle,
  ligne.numero,
  split(ligne.numero, '/') [0] as numero_ligne_seul,
  evenement.atelier,
  evenement.sv,
  evenement.lieu,
  motif.libelle,
  consequence.libelle,
  direction_evenement.libelle,
  evenement.observations,
  evenement.evt_ratt,
  --agent_de_maitrise.nom,
  concat(
    right(
      concat(
        '0',
        cast(
          hour(evenement.date_heure_reelle_debut) +(
            datediff(
              evenement.date_heure_reelle_debut,
              evenement.jour_exploitation_debut
            ) * 24
          ) as string
        )
      ),
      2
    ),
    ':',
    right(
      concat(
        '0',
        cast(
          minute(evenement.date_heure_reelle_debut) as string
        )
      ),
      2
    )
  ) as HeureDebutFormat24h,
  concat(
    right(
      concat(
        '0',
        cast(
          hour(evenement.date_heure_reelle_fin) +(
            datediff(
              evenement.date_heure_reelle_fin,
              evenement.jour_exploitation_fin
            ) * 24
          ) as string
        )
      ),
      2
    ),
    ':',
    right(
      concat(
        '0',
        cast(
          minute(evenement.date_heure_reelle_fin) as string
        )
      ),
      2
    )
  ) as HeureFinFormat24h,
  from_unixtime(
    unix_seconds(evenement.date_heure_reelle_fin) - unix_seconds(evenement.date_heure_reelle_debut),
    'HH:mm'
  ) as duree,
  (
    (
      unix_seconds(evenement.date_heure_reelle_fin) - unix_seconds(evenement.date_heure_reelle_debut)
    ) / 60
  ) as dureemin
FROM
  evex.motif
  INNER JOIN evex.evenement ON (motif.id = evenement.motif_id)
  INNER JOIN evex.consequence ON (consequence.id = evenement.consequence_id)
  INNER JOIN evex.agent_de_maitrise ON (
    agent_de_maitrise.id = evenement.agent_de_maitrise_id
  )
  INNER JOIN evex.direction direction_evenement ON (evenement.direction_id = direction_evenement.id)
  INNER JOIN evex.ligne ON (ligne.id = evenement.ligne_id)
  INNER JOIN evex.activite ON (ligne.activite_id = activite.id)
where
  evenement.jour_exploitation_debut = '2022-02-01'
order by
  evenement.id
