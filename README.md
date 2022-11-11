# Harvesting diff

- add the rule:

```json
{
    match: {
      predicate: {
        type: 'uri',
        value: 'http://www.w3.org/ns/adms#status',
      },
      object: {
        type: 'uri',
        value: 'http://redpencil.data.gift/id/concept/JobStatus/scheduled',
      },
    },
    callback: {
      method: 'POST',
      url: 'http://harvesting-diff/delta',
    },
    options: {
      resourceFormat: 'v0.0.1',
      gracePeriod: 1000,
      ignoreFromSelf: true,
      optOutMuScopeIds: ['http://redpencil.data.gift/id/concept/muScope/deltas/initialSync'],
    },
  },
```


Add step:

```
      {
        "currentOperation": "http://lblod.data.gift/id/jobs/concept/TaskOperation/diff",
        "nextOperation": "http://lblod.data.gift/id/jobs/concept/TaskOperation/publishHarvestedTriples",
        "nextIndex": "6"
      },

```


Add container:

```yaml
  harvesting-diff:
    build: /home/nbittich/lblod/dataset-diff/.
    environment:
      TARGET_GRAPH: "http://mu.semte.ch/graphs/harvesting"
    volumes:
      - ./data/files:/share
    labels:
      - "logging=true"
    restart: always
    logging: *default-logging
```