# wearable_sync_pipelines

Databricks Asset Bundle that turns raw wearable data into analytics tables:

```text
bronze  /Volumes/wearable_sync_bronze/polar/files/*.json   (written by wearable_sync)
  │     job: silver_polar  (Python wheel, serverless, triggered by new files)
silver  wearable_sync_silver.polar.{exercises, exercise_hr_zones, exercise_samples, body_measurements,
                                   rejected_records}
  │     (gold and dashboards: next steps)
gold    wearable_sync_gold.*
```

- **Incremental**: Auto Loader processes only bronze files it has not seen
  (checkpoints in a Unity Catalog volume).
- **Idempotent**: rows are `MERGE`d on their keys, so reprocessing never duplicates.
- **Resilient**: a file that cannot be parsed goes to `rejected_records` (quarantine);
  the other files keep flowing and the run fails so the problem is noticed.
- **Privacy**: GPS routes and personal attributes (birthday, gender) stay in bronze.
- **Testable**: all parsing is plain Python (`src/wearable_sync_pipelines/polar/parsing.py`)
  and unit-tested locally on fictional data; the Spark job only moves rows.

## Development

Requirements: [uv](https://docs.astral.sh/uv/) (version pinned in `pyproject.toml`) and
the Databricks CLI, logged in with `az login` (Azure CLI authentication).

The workspace URL is not stored in this public repository. Put it in the default
profile of `~/.databrickscfg` once:

```ini
[DEFAULT]
host      = https://<workspace>.azuredatabricks.net
auth_type = azure-cli
```

```powershell
uv run --locked pytest -q                 # unit tests
databricks bundle validate -t dev         # check the bundle configuration
databricks bundle deploy -t dev           # deploy a personal dev copy
databricks bundle run -t dev silver_polar # run it once
```

| Target | Schema | Trigger | Runs as | Deployed by |
| --- | --- | --- | --- | --- |
| `dev` | `dev_<user>_polar` | paused | the deploying user | you, from your machine |
| `prod` | `polar` | active (new bronze files) | `id-wearable-sync-pipelines` | GitHub Actions from `main` |

## CI/CD

`.github/workflows/ci-cd.yml`, signing in to Azure and Databricks with GitHub OIDC
(no stored credentials):

- **Pull requests**: `test` (pytest) and `validate` (`databricks bundle validate -t prod`)
  as a CI identity that has workspace access but no access to any data.
- **Push to `main`**: the same checks, then `databricks bundle deploy -t prod` in the
  `production` environment as `id-wearable-sync-pipelines`.

Identities, grants, the repository settings and the OIDC trust are managed in
`cloud_infra` (`wearable_sync/platform`, `wearable_sync/catalog`, `wearable_sync/github`).
