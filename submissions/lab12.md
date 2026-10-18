# Lab 12 — Advanced Kubernetes Resilience

## Task 1

Task 1 evidence is recorded in `Task1-1-Replica-PDB-Topology-corrected.txt`.

Observed replica state:

```text
events          2/2
payments        2/2
notifications   2/2
gateway         5/5
```

The four PodDisruptionBudgets were present:

```text
NAME                MIN AVAILABLE   MAX UNAVAILABLE   ALLOWED DISRUPTIONS
events-pdb          1                N/A                1
gateway-pdb         2                N/A                3
notifications-pdb   N/A              1                  1
payments-pdb        1                N/A                1
```

The live gateway topology-spread constraint was:

```json
[
  {
    "labelSelector": {"matchLabels": {"app": "gateway"}},
    "maxSkew": 1,
    "topologyKey": "kubernetes.io/hostname",
    "whenUnsatisfiable": "ScheduleAnyway"
  }
]
```

All five gateway Pods were on the single node `k3d-quickticket-server-0`, as recorded in the same evidence file. This is expected for single-node k3d; the constraint is present in the live spec but cannot distribute Pods without additional nodes.

The corrected coordinated gateway/events pod deletion and recovery are recorded in `Task1-2-Failover-before-kill-corrected.txt` and `Task1-2-Failover-after-kill-corrected.txt`. The official global 5xx query returned zero before and after:

```text
before: 0
after:  0
```

The old failed capture is superseded by this corrected run. The replacement Pods were confirmed ready only after the deleted Pod names disappeared; recovery took 22 seconds. This preserved the zero-5xx result but exceeded the handout's approximate five-second recovery target.

The load generator was running during the test, but its `2/2` status was not included in the saved before snapshot; the evidence therefore proves the observed query result and Pod recovery, while the load-state linkage remains a traceability limitation.

The corrected after snapshot shows two Events Pods and five Gateway Pods at `1/1 Running`, and the per-path 5xx query also returned zero.

The tightened PDB showed zero allowed disruptions in `Task1-5-PDB-tightened.txt`. The eviction API returned HTTP 429, recorded in `Task1-5-PDB-Eviction-429.txt`:

```json
{
  "status": "Failure",
  "message": "Cannot evict pod as it would violate the pod's disruption budget.",
  "reason": "TooManyRequests",
  "details": {
    "causes": [
      {
        "reason": "DisruptionBudget",
        "message": "The disruption budget events-pdb needs 2 healthy pods and has 2 currently"
      }
    ]
  },
  "code": 429
}
```

With three gateway replicas and `minAvailable: 1`, at most two Pods can be evicted simultaneously because one healthy Pod must remain. The five-replica `gateway-pdb` uses `minAvailable: 2` so three gateway Pods may be disrupted while two remain available to serve traffic and maintenance can still complete.

On three nodes, `maxSkew: 1` distributes five gateway Pods as `2/2/1` across the nodes. Seven Pods can be distributed as `3/2/2` (or an equivalent permutation).

## Task 2

Task 2 is marked optional by the official lab. The implementation includes `terminationGracePeriodSeconds: 40`, a 10-second `preStop` sleep, and an HTTP readiness probe on `/health`:

```yaml
terminationGracePeriodSeconds: 40
containers:
  - name: gateway
    lifecycle:
      preStop:
        exec:
          command: ["sh", "-c", "sleep 10"]
    readinessProbe:
      httpGet:
        path: /health
        port: 8080
      periodSeconds: 2
      failureThreshold: 1
```

This block is present in the live `k8s/gateway.yaml`.

The corrected rolling-restart evidence is in `Task2-1-Rolling-Restart-before-corrected.txt` and `Task2-1-Rolling-Restart-after-corrected.txt`. The official global 5xx query returned:

```text
before: 0
after:  0
```

The final Rollout status was `Healthy`, step `6/6`, with five desired, current, updated, ready, and available replicas. The corrected evidence supersedes the earlier nonzero capture. The separate non-health query also returned zero after the restart.

The concurrent-index migration used Alembic autocommit mode, as shown in `Task2-3-Concurrent-Index-Code.txt`:

```python
def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.create_index(
            "idx_events_event_date",
            "events",
            ["event_date"],
            unique=False,
            postgresql_concurrently=True,
            if_not_exists=True,
        )

def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.drop_index(
            "idx_events_event_date",
            table_name="events",
            postgresql_concurrently=True,
            if_exists=True,
        )
```

The original migration capture in `Task2-5-Migration-5xx.txt` returned empty vectors before and after the DDL, so that file alone does not distinguish a measured zero from the absence of a recent series. A follow-up verification with the mixed-load Deployment at `2/2` returned an explicit numeric `0` through the `or on() vector(0)` query; it is recorded in `Task2-5-Migration-5xx-corrected.txt`. This follow-up confirms the post-migration error value under active load, while the original file preserves the migration run's before/after timestamps. Because the migration-time before value was not captured as an explicit numeric zero, this remains a limitation of the migration proof rather than a claim of complete before/after numeric evidence. The index was visible in `Task2-4-Index-Visible.txt`:

```text
idx_events_event_date | CREATE INDEX idx_events_event_date ON public.events USING btree (event_date)
```

`CREATE INDEX CONCURRENTLY` matters because PostgreSQL can build the index without the long `ACCESS EXCLUSIVE` lock that a plain build can hold while blocking normal reads and writes. On a 10-million-row table, omitting it could make application queries wait or fail for minutes. The concurrent form must run outside Alembic’s default transaction, which is why `autocommit_block()` is used.

The expand-and-contract design for renaming `events.event_date` to `events.scheduled_at` is:

1. Migration 1 adds nullable `scheduled_at`; existing rows continue using `event_date`.
2. Deploy A writes both columns and reads `scheduled_at` with a fallback to `event_date` using `COALESCE`.
3. Migration 2 backfills `scheduled_at` where it is null, then makes it non-nullable.
4. Deploy B writes and reads only `scheduled_at`, while preserving the response alias `event_date` if compatibility requires it.
5. Migration 3 drops `event_date` only after Deploy B has fully rolled out.

Migration 3 must follow the complete Deploy B rollout because old application Pods still reference `event_date` during a rollout. Dropping the column first would make those Pods fail queries and could produce 5xx responses.

## Bonus Task

The migration and Deploy A/B evidence is recorded in `Bonus-1-Baseline-and-Transitions-valid.txt`, `Bonus-2-Deploy-A-and-B-Diff.txt`, and `Bonus-3-Schema-Before-After.txt`. The valid run used a temporary fallback-reading Deploy A image and restored the tracked `quickticket-events:v2` image for Deploy B.

Migration upgrade bodies:

```python
# Migration 1
op.add_column(
    "events",
    sa.Column("scheduled_at", sa.DateTime(timezone=True), nullable=True),
)

# Migration 2
op.execute(
    sa.text(
        "UPDATE events SET scheduled_at = event_date "
        "WHERE scheduled_at IS NULL"
    )
)
op.alter_column(
    "events",
    "scheduled_at",
    existing_type=sa.DateTime(timezone=True),
    nullable=False,
)

# Migration 3
op.drop_column("events", "event_date")
```

Deploy A used `COALESCE(e.scheduled_at, e.event_date)` for reads and ordering. Deploy B used `e.scheduled_at` for reads and ordering while retaining the response alias `event_date`. The evidence records no runtime event insert path, so seed data was updated for the new column.

The final schema contained non-null `scheduled_at` and no `event_date`, with five non-null rows. A valid cumulative run is recorded in `Bonus-4-5xx-Baseline-Final-valid.txt`: baseline `1455`, final `1457`, and the diff shows a two-count increase. The transition file records `1455` after Migration 1, `1456` after Deploy A, `1456` after Migration 2, `1457` after Deploy B, and `1457` after Migration 3. The file records `mixedload 2/2` at the baseline; it does not repeat that status at every transition or include a separate Deploy B rollout-status block. Therefore the required zero-delta `diff /tmp/5xx.baseline /tmp/5xx.final` was not achieved and the Bonus is not claimed as satisfied. The earlier invalid captures remain only as audit artifacts and are not used for this conclusion.

The live Deploy A and Deploy B observations used locally built/imported Events images. The final tracked `k8s/events.yaml` points to `quickticket-events:v2` with `IfNotPresent`, matching Deploy B. Applying that manifest on a new cluster still requires building and importing (or publishing and retagging) that image first; the manifest does not make a local image available by itself.

The single reordering that would cause 5xx is dropping `event_date` before Deploy B has fully rolled out. Old Pods still query that column and would receive database errors.

A production-scale backfill should keep each transaction small:

```text
last_id = 0
while true:
    begin transaction
    rows = UPDATE events
           SET scheduled_at = event_date
           WHERE id > last_id AND id <= last_id + 10000
             AND scheduled_at IS NULL
    commit transaction
    if rows == 0:
        break
    last_id = last_id + 10000
    sleep briefly
```

The downgrade that re-adds `event_date` is not sufficient for safe rollback because Deploy B may still be running code that assumes the new schema and writes made after downgrade can diverge between columns. Safe rollback requires moving traffic to code compatible with the restored schema, verifying all active Pods, and preserving synchronized values during the transition before removing the new-column dependency.

## Optional HPA

The HPA manifest in `k8s/gateway-hpa.yaml` targeted the gateway Rollout with `minReplicas: 5`, `maxReplicas: 12`, and a CPU target of 70%.

The observation in `Optional-12-9-HPA-Under-Load.txt` showed CPU above target and the gateway scaling from 5 to 12 replicas. The final HPA state was `cpu: 84%/70%`, `REPLICAS 12`, with all 12 gateway Pods available. The HPA capture does not include a per-Pod placement listing; the single-node limitation is directly evidenced by the Task 1 placement capture and therefore this observation does not demonstrate multi-node elasticity. No screenshot was captured, so the optional screenshot-specific proof is incomplete.

The Locust run was unsuccessful: 1,062 of 1,204 requests failed (88.21%), including 503, 502, 504, connection-refused, and reset errors. The HPA scaling observation is recorded, but the load test did not succeed and single-node placement does not demonstrate multi-node elasticity.


## Embedded proof outputs

### Task 1 raw outputs

```text
[Task1-1-Replica-PDB-Topology-corrected.txt]
=== UTC ===
2026-10-16 07:35:07 UTC
=== DEPLOYMENTS ===
NAME            READY   UP-TO-DATE   AVAILABLE   AGE
events          2/2     2            2           19d
payments        2/2     2            2           19d
notifications   2/2     2            2           35h
=== GATEWAY ROLLOUT ===
Status:          ✔ Healthy
Step:            6/6
Desired:         5
Current:         5
Updated:         5
Ready:           5
Available:       5
=== PDB ===
NAME                MIN AVAILABLE   MAX UNAVAILABLE   ALLOWED DISRUPTIONS   AGE
events-pdb          1               N/A               1                     33h
gateway-pdb         2               N/A               3                     33h
notifications-pdb   N/A             1                 1                     33h
payments-pdb        1               N/A               1                     33h
=== TOPOLOGY SPEC ===
[
    {
        "labelSelector": {
            "matchLabels": {
                "app": "gateway"
            }
        },
        "maxSkew": 1,
        "topologyKey": "kubernetes.io/hostname",
        "whenUnsatisfiable": "ScheduleAnyway"
    }
]
=== GATEWAY PLACEMENT ===
NAME                      READY   STATUS    RESTARTS   AGE     IP           NODE                       NOMINATED NODE   READINESS GATES
gateway-698cf6b6c8-2pbv7   1/1     Running   0          18h   10.42.0.141   k3d-quickticket-server-0   <none>           <none>
gateway-698cf6b6c8-bgx74   1/1     Running   0          18h   10.42.0.140   k3d-quickticket-server-0   <none>           <none>
gateway-698cf6b6c8-cwhh5   1/1     Running   0          18h   10.42.0.142   k3d-quickticket-server-0   <none>           <none>
gateway-698cf6b6c8-tqrh9   1/1     Running   0          18h   10.42.0.144   k3d-quickticket-server-0   <none>           <none>
gateway-698cf6b6c8-xzgtz   1/1     Running   0          18h   10.42.0.143   k3d-quickticket-server-0   <none>           <none>
```

```text
[Task1-2-Failover-before-kill-corrected.txt]
=== UTC BASELINE === 2026-10-15 12:42:58 UTC
gateway=gateway-698cf6b6c8-d5ws9 events=events-85d957cfc-sxktc
=== GLOBAL_5XX_BEFORE_3M ===
{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1792068179.334,"0"]}]}}
pod "gateway-698cf6b6c8-d5ws9" deleted from default namespace
pod "events-85d957cfc-sxktc" deleted from default namespace
```

```text
[Task1-2-Failover-after-kill-corrected.txt]
=== UTC READY === 2026-10-15 12:43:20 UTC
RECOVERY_SECONDS=22
NAME                       READY   STATUS    RESTARTS      AGE     IP            NODE                       NOMINATED NODE   READINESS GATES
events-85d957cfc-6zgmt     1/1     Running   0             21s     10.42.0.139   k3d-quickticket-server-0   <none>           <none>
events-85d957cfc-x42bq     1/1     Running   0             13h     10.42.0.130   k3d-quickticket-server-0   <none>           <none>
gateway-698cf6b6c8-98vk9   1/1     Running   0             21s     10.42.0.138   k3d-quickticket-server-0   <none>           <none>
gateway-698cf6b6c8-dzs2z   1/1     Running   0             4m37s   10.42.0.135   k3d-quickticket-server-0   <none>           <none>
gateway-698cf6b6c8-gbvsm   1/1     Running   1 (14h ago)   14h     10.42.0.115   k3d-quickticket-server-0   <none>           <none>
gateway-698cf6b6c8-lj6ns   1/1     Running   0             14h     10.42.0.118   k3d-quickticket-server-0   <none>           <none>
gateway-698cf6b6c8-plf9j   1/1     Running   0             14h     10.42.0.119   k3d-quickticket-server-0   <none>           <none>
=== GLOBAL_5XX_AFTER_1M ===
{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1792068202.602,"0"]}]}}
=== BY_PATH_STATUS_AFTER_1M ===
{"status":"success","data":{"resultType":"vector","result":[{"metric":{"path":"/health","status":"503"},"value":[1792068203.985,"0"]},{"metric":{"path":"/events/{id}/reserve","status":"504"},"value":[1792068203.985,"0"]},{"metric":{"path":"/events","status":"502"},"value":[1792068203.985,"0"]},{"metric":{"path":"/events/{id}/reserve","status":"502"},"value":[1792068203.985,"0"]}]}}
```

```text
[Task1-5-PDB-Eviction-429.txt]
HTTP/1.1 429 Too Many Requests
Audit-Id: 061fa111-043a-4a49-8c3c-3784ee1240f2
Cache-Control: no-cache, private
Content-Length: 404
Content-Type: application/json
Date: Wed, 14 Oct 2026 21:59:38 GMT
X-Kubernetes-Pf-Flowschema-Uid: 2154fdcf-ac9e-4f05-b6fe-7cc633f80f4e
X-Kubernetes-Pf-Prioritylevel-Uid: 014ed8f0-e252-4ba0-9921-84b662b292a7

{
  "kind": "Status",
  "apiVersion": "v1",
  "metadata": {},
  "status": "Failure",
  "message": "Cannot evict pod as it would violate the pod's disruption budget.",
  "reason": "TooManyRequests",
  "details": {
    "causes": [
      {
        "reason": "DisruptionBudget",
        "message": "The disruption budget events-pdb needs 2 healthy pods and has 2 currently"
      }
    ]
  },
  "code": 429
}```

### Task 2 raw outputs

```text
[Task2-1-Rolling-Restart-before-corrected.txt]
=== UTC BEFORE ROLLING RESTART === 2026-10-15 12:43:50 UTC
=== OFFICIAL 5XX BEFORE (1m) ===
{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1792068230.951,"0"]}]}}
=== NON-HEALTH 5XX BEFORE (1m) ===
{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1792068232.395,"0"]}]}}
=== RESTART COMMAND OUTPUT ===
rollout 'gateway' restarts in 0s
```

```text
[Task2-1-Rolling-Restart-after-corrected.txt]
=== UTC AFTER ROLLING RESTART === 2026-10-15 12:45:43 UTC
=== OFFICIAL 5XX AFTER (3m) ===
{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1792068344.092,"0"]}]}}
=== NON-HEALTH 5XX AFTER (3m) ===
{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1792068344.914,"0"]}]}}
=== ROLLOUT STATUS ===
Status: Healthy
Step: 6/6
Desired: 5
Current: 5
Updated: 5
Ready: 5
Available: 5
```

```text
[Task2-4-Index-Visible.txt]
2026-10-14 22:16:54 UTC
EVENTS_COLUMNS
  column_name  |        data_type         | is_nullable 
---------------+--------------------------+-------------
 id            | integer                  | NO
 name          | text                     | NO
 venue         | text                     | NO
 event_date    | timestamp with time zone | NO
 total_tickets | integer                  | NO
 price_cents   | integer                  | NO
 email         | character varying        | YES
(7 rows)

INDEX_DEFINITION
       indexname       |                                   indexdef                                   
-----------------------+------------------------------------------------------------------------------
 idx_events_event_date | CREATE INDEX idx_events_event_date ON public.events USING btree (event_date)
(1 row)

```

```text
[Task2-5-Migration-5xx.txt]
2026-10-14 22:16:24 UTC
QUERY=global_5xx_1m_before_migration
{"status":"success","data":{"resultType":"vector","result":[]}}
2026-10-14 22:16:47 UTC
QUERY=global_5xx_1m_after_migration
{"status":"success","data":{"resultType":"vector","result":[]}}
```

```text
[Task2-5-Migration-5xx-corrected.txt]
=== UTC ===
2026-10-16 07:38:33 UTC
=== MIXEDLOAD ===
NAME        READY   UP-TO-DATE   AVAILABLE
mixedload   2/2     2            2
=== GLOBAL_5XX_1M_EXPLICIT_ZERO ===
{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1792136316.235,"0"]}]}}
```

### Bonus raw outputs

```text
[Bonus-1-Baseline-and-Transitions-valid.txt]
=== VALID BONUS BASELINE UTC === 2026-10-18 10:38:35 UTC
MIXEDLOAD=2/2
SCHEMA_BEFORE_MIGRATION_1: event_date NOT NULL; no scheduled_at
BASELINE_VALUE=1455
AFTER_MIGRATION_1=1455
DEPLOY_A_IMAGE=quickticket-events:a; READ_MODE=COALESCE(scheduled_at,event_date); VALUE=1456
AFTER_MIGRATION_2=1456; ROW_PARITY=5/5
DEPLOY_B_IMAGE=quickticket-events:v2; READ_MODE=scheduled_at; VALUE=1457
AFTER_MIGRATION_3=1457
SCHEMA_AFTER_MIGRATION_3: scheduled_at NOT NULL; no event_date
```

```text
[Bonus-4-5xx-Baseline-Final-valid.txt]
BASELINE_VALUE=1455
FINAL_VALUE=1457
diff: -1455 / +1457
RESULT=FAILED_ZERO_DELTA
```

```text
[Bonus-2-Deploy-A-and-B-Diff.txt]
DEPLOY_A_CODE_DIFF
@@ list_events/get_event read paths
SELECT e.id, e.name, e.venue,
       COALESCE(e.scheduled_at, e.event_date) AS event_date,
       e.total_tickets, e.price_cents,
       COALESCE(SUM(o.quantity), 0) as confirmed
GROUP BY e.id ORDER BY COALESCE(e.scheduled_at, e.event_date)
@@ no runtime event insert path
191:@app.post("/events/{event_id}/reserve")
DEPLOY_A_TO_B_DIFF
SELECT e.id, e.name, e.venue,
       e.scheduled_at AS event_date,
       e.total_tickets, e.price_cents,
       COALESCE(SUM(o.quantity), 0) as confirmed
GROUP BY e.id ORDER BY e.scheduled_at
@@ seed schema
event_date -> scheduled_at in CREATE TABLE and INSERT
```

```text
[Bonus-3-Schema-Before-After.txt]
SCHEMA_BEFORE_AFTER
Before Migration 1 and after Migration 3 are recorded in Bonus-1-Baseline-and-Transitions-valid.txt.
  column_name  | is_nullable 
---------------+-------------
 id            | NO
 name          | NO
 venue         | NO
 total_tickets | NO
 price_cents   | NO
 email         | YES
 scheduled_at  | NO
(7 rows)

```

### Optional HPA manifest and observation

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: gateway
spec:
  scaleTargetRef:
    apiVersion: argoproj.io/v1alpha1
    kind: Rollout
    name: gateway
  minReplicas: 5
  maxReplicas: 12
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
```

```text
[Optional-12-9-HPA-Under-Load.txt]
2026-10-14 22:20:44 UTC
HPA_DURING_LOAD
NAME      REFERENCE         TARGETS              MINPODS   MAXPODS   REPLICAS   AGE
gateway   Rollout/gateway   cpu: <unknown>/70%   5         12        0          2s
NAME           STATUS    COMPLETIONS   DURATION   AGE   CONTAINERS   IMAGES                   SELECTOR
load-hpa-200   Running   0/1           1s         1s    locust       locustio/locust:2.43.4   batch.kubernetes.io/controller-uid=2d414d71-6fe8-4974-8c7e-9f4b0356919a
NAME      REFERENCE         TARGETS              MINPODS   MAXPODS   REPLICAS   AGE
gateway   Rollout/gateway   cpu: <unknown>/70%   5         12        0          14s
NAME           STATUS    COMPLETIONS   DURATION   AGE   CONTAINERS   IMAGES                   SELECTOR
load-hpa-200   Running   0/1           15s        15s   locust       locustio/locust:2.43.4   batch.kubernetes.io/controller-uid=2d414d71-6fe8-4974-8c7e-9f4b0356919a
NAME      REFERENCE         TARGETS         MINPODS   MAXPODS   REPLICAS   AGE
gateway   Rollout/gateway   cpu: 116%/70%   5         12        5          29s
NAME           STATUS    COMPLETIONS   DURATION   AGE   CONTAINERS   IMAGES                   SELECTOR
load-hpa-200   Running   0/1           31s        31s   locust       locustio/locust:2.43.4   batch.kubernetes.io/controller-uid=2d414d71-6fe8-4974-8c7e-9f4b0356919a
NAME      REFERENCE         TARGETS         MINPODS   MAXPODS   REPLICAS   AGE
gateway   Rollout/gateway   cpu: 162%/70%   5         12        9          44s
NAME           STATUS    COMPLETIONS   DURATION   AGE   CONTAINERS   IMAGES                   SELECTOR
load-hpa-200   Running   0/1           44s        44s   locust       locustio/locust:2.43.4   batch.kubernetes.io/controller-uid=2d414d71-6fe8-4974-8c7e-9f4b0356919a
NAME      REFERENCE         TARGETS         MINPODS   MAXPODS   REPLICAS   AGE
gateway   Rollout/gateway   cpu: 108%/70%   5         12        12         57s
NAME           STATUS    COMPLETIONS   DURATION   AGE   CONTAINERS   IMAGES                   SELECTOR
load-hpa-200   Running   0/1           57s        57s   locust       locustio/locust:2.43.4   batch.kubernetes.io/controller-uid=2d414d71-6fe8-4974-8c7e-9f4b0356919a
NAME      REFERENCE         TARGETS         MINPODS   MAXPODS   REPLICAS   AGE
gateway   Rollout/gateway   cpu: 148%/70%   5         12        12         69s
NAME           STATUS    COMPLETIONS   DURATION   AGE   CONTAINERS   IMAGES                   SELECTOR
load-hpa-200   Running   0/1           69s        69s   locust       locustio/locust:2.43.4   batch.kubernetes.io/controller-uid=2d414d71-6fe8-4974-8c7e-9f4b0356919a
NAME      REFERENCE         TARGETS         MINPODS   MAXPODS   REPLICAS   AGE
gateway   Rollout/gateway   cpu: 148%/70%   5         12        12         82s
NAME           STATUS    COMPLETIONS   DURATION   AGE   CONTAINERS   IMAGES                   SELECTOR
load-hpa-200   Running   0/1           83s        83s   locust       locustio/locust:2.43.4   batch.kubernetes.io/controller-uid=2d414d71-6fe8-4974-8c7e-9f4b0356919a
NAME      REFERENCE         TARGETS        MINPODS   MAXPODS   REPLICAS   AGE
gateway   Rollout/gateway   cpu: 83%/70%   5         12        12         98s
NAME           STATUS    COMPLETIONS   DURATION   AGE    CONTAINERS   IMAGES                   SELECTOR
load-hpa-200   Running   0/1           100s       100s   locust       locustio/locust:2.43.4   batch.kubernetes.io/controller-uid=2d414d71-6fe8-4974-8c7e-9f4b0356919a
NAME      REFERENCE         TARGETS         MINPODS   MAXPODS   REPLICAS   AGE
gateway   Rollout/gateway   cpu: 169%/70%   5         12        12         113s
NAME           STATUS    COMPLETIONS   DURATION   AGE    CONTAINERS   IMAGES                   SELECTOR
load-hpa-200   Running   0/1           114s       114s   locust       locustio/locust:2.43.4   batch.kubernetes.io/controller-uid=2d414d71-6fe8-4974-8c7e-9f4b0356919a
NAME      REFERENCE         TARGETS         MINPODS   MAXPODS   REPLICAS   AGE
gateway   Rollout/gateway   cpu: 150%/70%   5         12        12         2m7s
NAME           STATUS    COMPLETIONS   DURATION   AGE    CONTAINERS   IMAGES                   SELECTOR
load-hpa-200   Running   0/1           2m8s       2m8s   locust       locustio/locust:2.43.4   batch.kubernetes.io/controller-uid=2d414d71-6fe8-4974-8c7e-9f4b0356919a
NAME      REFERENCE         TARGETS         MINPODS   MAXPODS   REPLICAS   AGE
gateway   Rollout/gateway   cpu: 131%/70%   5         12        12         2m20s
NAME           STATUS    COMPLETIONS   DURATION   AGE     CONTAINERS   IMAGES                   SELECTOR
load-hpa-200   Running   0/1           2m21s      2m21s   locust       locustio/locust:2.43.4   batch.kubernetes.io/controller-uid=2d414d71-6fe8-4974-8c7e-9f4b0356919a
NAME      REFERENCE         TARGETS         MINPODS   MAXPODS   REPLICAS   AGE
gateway   Rollout/gateway   cpu: 131%/70%   5         12        12         2m34s
NAME           STATUS    COMPLETIONS   DURATION   AGE     CONTAINERS   IMAGES                   SELECTOR
load-hpa-200   Running   0/1           2m36s      2m36s   locust       locustio/locust:2.43.4   batch.kubernetes.io/controller-uid=2d414d71-6fe8-4974-8c7e-9f4b0356919a
NAME      REFERENCE         TARGETS         MINPODS   MAXPODS   REPLICAS   AGE
gateway   Rollout/gateway   cpu: 106%/70%   5         12        12         2m50s
NAME           STATUS   COMPLETIONS   DURATION   AGE     CONTAINERS   IMAGES                   SELECTOR
load-hpa-200   Failed   0/1           2m51s      2m51s   locust       locustio/locust:2.43.4   batch.kubernetes.io/controller-uid=2d414d71-6fe8-4974-8c7e-9f4b0356919a
NAME      REFERENCE         TARGETS         MINPODS   MAXPODS   REPLICAS   AGE
gateway   Rollout/gateway   cpu: 106%/70%   5         12        12         3m4s
NAME           STATUS   COMPLETIONS   DURATION   AGE    CONTAINERS   IMAGES                   SELECTOR
load-hpa-200   Failed   0/1           3m5s       3m5s   locust       locustio/locust:2.43.4   batch.kubernetes.io/controller-uid=2d414d71-6fe8-4974-8c7e-9f4b0356919a
NAME      REFERENCE         TARGETS         MINPODS   MAXPODS   REPLICAS   AGE
gateway   Rollout/gateway   cpu: 113%/70%   5         12        12         3m18s
NAME           STATUS   COMPLETIONS   DURATION   AGE     CONTAINERS   IMAGES                   SELECTOR
load-hpa-200   Failed   0/1           3m19s      3m19s   locust       locustio/locust:2.43.4   batch.kubernetes.io/controller-uid=2d414d71-6fe8-4974-8c7e-9f4b0356919a
NAME      REFERENCE         TARGETS        MINPODS   MAXPODS   REPLICAS   AGE
gateway   Rollout/gateway   cpu: 84%/70%   5         12        12         3m32s
   locust       locustio/locust:2.43.4   batch.kubernetes.io/controller-uid=2d414d71-6fe8-4974-8c7e-9f4b0356919a
FINAL_LOCUST_LOG
[2026-10-14 22:20:53,474] load-hpa-200-fz9rr/INFO/locust.main: Starting Locust 2.43.4
[2026-10-14 22:20:53,475] load-hpa-200-fz9rr/INFO/locust.main: Run time limit set to 120 seconds
[2026-10-14 22:20:53,477] load-hpa-200-fz9rr/INFO/locust.runners: Ramping to 200 users at a rate of 20.00 per second
[2026-10-14 22:21:06,871] load-hpa-200-fz9rr/INFO/locust.runners: All users spawned: {"QuickTicketUser": 200} (200 total users)
[2026-10-14 22:23:00,366] load-hpa-200-fz9rr/INFO/locust.main: --run-time limit reached, shutting down
[2026-10-14 22:23:11,473] load-hpa-200-fz9rr/INFO/locust.main: Shutting down (exit code 1)
Type     Name                                                                          # reqs      # fails |    Avg     Min     Max    Med |   req/s  failures/s
--------|----------------------------------------------------------------------------|-------|-------------|-------|-------|-------|-------|--------|-----------
GET      /events                                                                          799  663(82.98%) |  16132      28   46403  13000 |    5.79        4.81
POST     /events/3/reserve                                                                211 211(100.00%) |  16195     113   43364  12000 |    1.53        1.53
POST     /events/5/reserve                                                                 62  62(100.00%) |  18447     246   45368  13000 |    0.45        0.45
GET      /health                                                                          132  126(95.45%) |  17308     365   59254  11000 |    0.96        0.91
--------|----------------------------------------------------------------------------|-------|-------------|-------|-------|-------|-------|--------|-----------
         Aggregated                                                                      1204 1062(88.21%) |  16391      28   59254  12000 |    8.73        7.70

Response time percentiles (approximated)
Type     Name                                                                                  50%    66%    75%    80%    90%    95%    98%    99%  99.9% 99.99%   100% # reqs
--------|--------------------------------------------------------------------------------|--------|------|------|------|------|------|------|------|------|------|------|------
GET      /events                                                                             13000  19000  27000  29000  38000  42000  42000  43000  46000  46000  46000    799
POST     /events/3/reserve                                                                   12000  17000  28000  31000  37000  41000  43000  43000  43000  43000  43000    211
POST     /events/5/reserve                                                                   14000  25000  29000  32000  39000  43000  43000  45000  45000  45000  45000     62
GET      /health                                                                             12000  19000  26000  36000  44000  49000  56000  56000  59000  59000  59000    132
--------|--------------------------------------------------------------------------------|--------|------|------|------|------|------|------|------|------|------|------|------
         Aggregated                                                                          12000  19000  27000  30000  38000  42000  43000  46000  56000  59000  59000   1204

Error report
# occurrences      Error                                                                                               
------------------|---------------------------------------------------------------------------------------------------------------------------------------------
19                 POST /events/3/reserve: HTTPError('409 Client Error: Conflict for url: /events/3/reserve')          
107                GET /health: HTTPError('503 Server Error: Service Unavailable for url: /health')                    
156                GET /events: HTTPError('502 Server Error: Bad Gateway for url: /events')                            
32                 POST /events/3/reserve: HTTPError('500 Server Error: Internal Server Error for url: /events/3/reserve')
386                GET /events: HTTPError('504 Server Error: Gateway Timeout for url: /events')                        
119                POST /events/3/reserve: HTTPError('504 Server Error: Gateway Timeout for url: /events/3/reserve')   
12                 POST /events/5/reserve: HTTPError('500 Server Error: Internal Server Error for url: /events/5/reserve')
5                  POST /events/5/reserve: HTTPError('502 Server Error: Bad Gateway for url: /events/5/reserve')       
10                 POST /events/3/reserve: HTTPError('502 Server Error: Bad Gateway for url: /events/3/reserve')       
43                 GET /events: ConnectionRefusedError(111, 'Connection refused')                                      
9                  POST /events/3/reserve: ConnectionRefusedError(111, 'Connection refused')                           
4                  GET /health: ConnectionRefusedError(111, 'Connection refused')                                      
60                 GET /events: RemoteDisconnected('Remote end closed connection without response')                    
10                 GET /health: RemoteDisconnected('Remote end closed connection without response')                    
18                 GET /events: ConnectionResetError(104, 'Connection reset by peer')                                  
1                  POST /events/5/reserve: ConnectionRefusedError(111, 'Connection refused')                           
15                 POST /events/3/reserve: RemoteDisconnected('Remote end closed connection without response')         
30                 POST /events/5/reserve: HTTPError('504 Server Error: Gateway Timeout for url: /events/5/reserve')   
5                  GET /health: ConnectionResetError(104, 'Connection reset by peer')                                  
9                  POST /events/5/reserve: HTTPError('409 Client Error: Conflict for url: /events/5/reserve')          
7                  POST /events/3/reserve: ConnectionResetError(104, 'Connection reset by peer')                       
4                  POST /events/5/reserve: RemoteDisconnected('Remote end closed connection without response')         
1                  POST /events/5/reserve: ConnectionResetError(104, 'Connection reset by peer')                       
------------------|---------------------------------------------------------------------------------------------------------------------------------------------

FINAL_HPA
NAME      REFERENCE         TARGETS        MINPODS   MAXPODS   REPLICAS   AGE
gateway   Rollout/gateway   cpu: 84%/70%   5         12        12         3m29s
FINAL_ROLLOUT
NAME      DESIRED   CURRENT   UP-TO-DATE   AVAILABLE   AGE
gateway   12        12        12           12          10d
2026-10-14 22:24:15 UTC
NAME      REFERENCE         TARGETS        MINPODS   MAXPODS   REPLICAS   AGE
gateway   Rollout/gateway   cpu: 81%/70%   5         12        12         3m35s
2026-10-14 22:24:31 UTC
2026-10-14 22:24:53 UTC
2026-10-14 22:25:08 UTC
```
