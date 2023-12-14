# [HARVESTING-DATASET-DIFF]: Filter triples already published

Look at previous completed jobs and check if the target url for the ongoing job is equivalent.
If so, build a model of all previously completed jobs from the result files of the mirroring step,
and compare it to the model of the result files for this ongoing job.

Generate a result graph with only the new triples, and an intersection file with all  triples that were
previously harvested. 


## Setup using docker-compose

- Add the service to your docker-compose:

```yml
  dataset-diff:
    image: lblod/harvesting-dataset-diff
    environment:
      TARGET_GRAPH: "http://mu.semte.ch/graphs/harvesting"
    volumes:
      - ./data/files:/share
    labels:
      - "logging=true"
    restart: always
    logging: *default-logging

```

- add delta rule:

```js
{
    match: {
      predicate: {
        type: 'uri',
        value: 'http://www.w3.org/ns/adms#status'
      },
      object: {
        type: 'uri',
        value: 'http://redpencil.data.gift/id/concept/JobStatus/scheduled'
      },
    },
    callback: {
      method: 'POST',
      url: 'http://harvesting-diff/delta'
    },
    options: {
      resourceFormat: 'v0.0.1',
      gracePeriod: 1000,
      ignoreFromSelf: true,
      optOutMuScopeIds: ['http://redpencil.data.gift/id/concept/muScope/deltas/initialSync']
    }
  }
```

- add job controller config step:


```js
      {
        "currentOperation": "http://lblod.data.gift/id/jobs/concept/TaskOperation/diff",
        "nextOperation": "http://lblod.data.gift/id/jobs/concept/TaskOperation/publishHarvestedTriples",
        "nextIndex": "6"
      }

```

## Environment variables

- `SERVER_PORT` : default set to `80`
- `SHARE_FOLDER_DIRECTORY`: default set to `/share`
- `BATCH_SIZE` : default set to `100`
- `LOGGING_LEVEL` : default set to `INFO`
- `SPARQL_ENDPOINT` : default set to `http://database:8890/sparql`
- `MAX_REQUEST_SIZE` : default set to `512MB`
- `MAX_FILE_SIZE` : default set to `512MB`
- `TARGET_GRAPH` : default set to `http://mu.semte.ch/application`
- `HIGH_LOAD_SPARQL_ENDPOINT`:  default set to `http://virtuoso:8890/sparql`
