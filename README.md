# jobapi — Asynchronous job processing API

Une API HTTP qui accepte des tâches, les met en file, et les fait exécuter en
arrière-plan par un pool de goroutines. Le client n'attend pas : il reçoit un
identifiant immédiatement et consulte l'état de son job quand il veut.

**Bibliothèque standard Go uniquement.** Aucune dépendance, aucun framework —
`go.mod` ne contient pas une seule ligne `require`.

## Pourquoi

Une requête HTTP synchrone est inadaptée à un travail qui dure : le client
time-out, réessaie, et déclenche le même travail deux fois. La réponse
classique est de découpler l'acceptation de l'exécution — c'est le motif
derrière Celery, Sidekiq, BullMQ ou SQS + Lambda. Ce projet en implémente le
cœur à la main.

## Architecture

```
                   HTTP
                    │
        ┌───────────▼───────────┐
        │        api            │  routes net/http, encodage JSON
        └─────┬───────────┬─────┘
              │ Create    │ Get / List / Cancel
        ┌─────▼───────────▼─────┐
        │        store          │  map[string]*Job + sync.RWMutex
        │  (source de vérité)   │
        └───────────▲───────────┘
                    │ Start / Finish
              ┌─────┴─────┐
              │  worker   │  N goroutines lisant le channel
              └─────▲─────┘
                    │
              chan string (IDs en attente)
```

`store` n'importe personne. `worker` importe `store`. `api` importe `store`.
`main` câble le tout. Aucun cycle : chaque paquet se teste isolément.

## Démarrer

```bash
go run ./cmd/server      # écoute sur :8080
```

## API

| Méthode | Route | Réponse |
|---|---|---|
| `POST` | `/jobs` | `202` + le job à l'état `queued` |
| `GET` | `/jobs/{id}` | `200` + le job, `404` si inconnu |
| `GET` | `/jobs?status=running` | `200` + `{count, jobs}` |
| `DELETE` | `/jobs/{id}` | `200` si l'annulation est prise en compte, `404` si inconnu ou déjà terminé |
| `GET` | `/healthz` | `200` |

Types de jobs livrés : `sleep` (payload `{"ms":5000}`) et `uppercase`
(payload `{"text":"..."}`). En ajouter un se fait en une ligne dans
`DefaultRegistry`, sans toucher au pool.

### Exemple

```bash
# soumettre — la réponse est immédiate
curl -X POST localhost:8080/jobs -d '{"type":"sleep","payload":{"ms":5000}}'
# {"id":"4db8ed81757589f8","type":"sleep","status":"queued","created_at":"..."}

# consulter
curl localhost:8080/jobs/4db8ed81757589f8
# {"id":"...","status":"running","started_at":"..."}

# annuler
curl -X DELETE localhost:8080/jobs/4db8ed81757589f8
# le job passe à "canceled" dès que le handler observe ctx.Done()

# lister les jobs en cours
curl 'localhost:8080/jobs?status=running'
```

Cycle de vie d'un job :

```
queued ──> running ──> succeeded
   │          │
   │          └──────> failed
   └─────────────────> canceled
```

## Points d'implémentation

- **Aucun pointeur interne ne sort du store.** Toute lecture rend un `Clone()`
  réalisé sous verrou. Sans ça, un handler HTTP sérialiserait un job pendant
  qu'un worker écrit dedans.
- **`RWMutex`** : les lectures (nombreuses) sont concurrentes, seules les
  écritures sont exclusives.
- **File bufferisée** : `POST /jobs` n'attend jamais un worker. Si la file est
  pleine, l'API répond `503` au lieu de bloquer la requête.
- **Annulation coopérative** : `DELETE` déclenche le `context` du job ; c'est
  au handler d'observer `ctx.Done()`. Rien n'est tué de force.
- **Arrêt gracieux** : sur SIGINT/SIGTERM, on ferme d'abord le serveur HTTP
  (plus aucune entrée), puis on ferme la file et on attend que les workers
  aient terminé le travail déjà accepté.

## Tests

```bash
go test ./...           # tests unitaires par couche
go test -race ./...     # + détecteur de data races
```

Les tests HTTP utilisent `httptest` : aucun port n'est ouvert. Les tests du
pool attendent un état par sondage court plutôt qu'avec un `Sleep` fixe, pour
rester rapides sans devenir instables.

## Limites assumées

Le stockage est en mémoire : redémarrer perd les jobs. Comme toutes les
écritures passent par les méthodes de `Store`, brancher Redis ou Postgres
derrière la même interface ne toucherait aucun autre paquet. Il n'y a pas non
plus de reprise sur erreur (retry) ni de priorités.
