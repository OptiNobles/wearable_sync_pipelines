# wearable_sync_pipelines

Databricks Asset Bundle that turns raw wearable data into analytics tables:

```text
bronze  /Volumes/wearable_sync_bronze/polar/files/*.json   (written by wearable_sync)
  │     job wearable_sync_polar (Python wheel, serverless, triggered by new files)
  │       task silver
silver  wearable_sync_silver.polar.{exercises, exercise_hr_zones, exercise_samples, body_measurements,
  │                                rejected_records}
  │       task gold (after silver)
gold    wearable_sync_gold.body.{weight_by_period, vo2max_by_period}
        wearable_sync_gold.training.{runs, run_profile, run_splits, running_by_period,
                                     other_sessions_by_period}
```

Silver:

- **Incremental**: Auto Loader processes only bronze files it has not seen
  (checkpoints in a Unity Catalog volume).
- **Idempotent**: rows are `MERGE`d on their keys, so reprocessing never duplicates.
- **Resilient**: a file that cannot be parsed goes to `rejected_records` (quarantine);
  the other files keep flowing and the run fails so the problem is noticed.
- **Privacy**: GPS routes and personal attributes (birthday, gender) stay in bronze.
- **Testable**: all parsing is plain Python (`src/wearable_sync_pipelines/polar/parsing.py`)
  and unit-tested locally on fictional data; the Spark job only moves rows.

Gold:

- **Full rebuild**: every table is recomputed from the whole of silver on each run and
  replaced with `CREATE OR REPLACE` (history and grants are kept). Corrections in
  silver always reach gold, and at this data volume it costs the same as incremental.
- **Runs after a failed silver task too** (`run_if: ALL_DONE`): a quarantined file
  fails silver, but the other files are merged and reach gold. Gold reflects whatever
  silver holds; if silver stopped partway (cancelled, timed out), the next run's full
  rebuild fills in what is missing.
- **Ready for dashboards**: all granularities of a metric live in one table with a
  `granularity` column: day, week and month for `body`, week and month for the
  `training` period tables. Dates are local (`timezone` variable, `Europe/Warsaw`),
  weeks start on Monday.
- **Running**: runs are exercises with sport `RUNNING`. Paces (min/km) are always
  time / distance; heart rate per period is weighted by duration; cadence excludes
  stops (0 spm). `run_profile` (100 m) and `run_splits` (1 km) charge each sample with
  the time and distance to the next one, so a partial last split has its real distance
  and a comparable pace. Segment lengths are accurate to one sampling step (3-4 m at
  1 s), because a step that crosses a boundary counts towards the earlier segment.
  Segment heart rate and cadence are time-weighted, and samples without a distance
  (no GPS fix) keep the last known one. Other sessions are grouped by Polar's detailed
  sport (e.g. `STRENGTH_TRAINING`).
- **Testable**: transformations are PySpark functions (`src/wearable_sync_pipelines/gold/`),
  unit-tested on local Spark with fictional data.

## Development

Requirements: [uv](https://docs.astral.sh/uv/) (version pinned in `pyproject.toml`),
Java 17 for the local Spark the gold tests use (the same version as serverless
environment 6), and the Databricks CLI, logged in with `az login` (Azure CLI
authentication).

The workspace URL is not stored in this public repository. Put it in the default
profile of `~/.databrickscfg` once:

```ini
[DEFAULT]
host      = https://<workspace>.azuredatabricks.net
auth_type = azure-cli
```

```powershell
uv run --locked pytest -q                        # unit tests
databricks bundle validate -t dev                # check the bundle configuration
databricks bundle deploy -t dev                  # deploy a personal dev copy
databricks bundle run -t dev wearable_sync_polar # run it once
```

| Target | Schemas | Trigger | Runs as | Deployed by |
| --- | --- | --- | --- | --- |
| `dev` | `dev_<user>_polar`, `dev_<user>_body`, `dev_<user>_training` | paused | the deploying user | you, from your machine |
| `prod` | `polar`, `body`, `training` | active (new bronze files) | `id-wearable-sync-pipelines` | GitHub Actions from `main` |

The job runs in serverless standard performance mode (slower start, fewer DBUs) and is
tagged `project: wearable_sync` for cost reporting in `system.billing.usage`.

## CI/CD

`.github/workflows/ci-cd.yml`, signing in to Azure and Databricks with GitHub OIDC
(no stored credentials):

- **Pull requests**: `test` (pytest) and `validate` (`databricks bundle validate -t prod`)
  as a CI identity that has workspace access but no access to any data.
- **Push to `main`**: the same checks, then `databricks bundle deploy -t prod` in the
  `production` environment as `id-wearable-sync-pipelines`.

Identities, grants, the repository settings and the OIDC trust are managed in
`cloud_infra` (`wearable_sync/platform`, `wearable_sync/catalog`, `wearable_sync/github`).
