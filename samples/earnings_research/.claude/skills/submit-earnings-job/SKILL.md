---
name: submit-earnings-job
description: Submit an earnings research job (basket study, sentiment analysis, market data, content pipeline) to AWS Batch — pick the queue, the dispatcher command and the job size, and check the image is current first. Use when running any package of samples/earnings_research on Batch.
---

# Submit an earnings research job

All packages run from ONE image and ONE job definition. The container command picks the
package: `run <package> <command|script.py> [args...]`.

## 1. Which queue — ask the MCP server, do not guess

Call the `describe_queues` tool of the `earnings-research` MCP server (`python mcp/server.py`).
It returns, per queue id, the purpose, the consumers, why it is capped, egress, and the live
state and vCPU cap. Queue purposes are documented ONLY there; do not restate them from memory.
If a queue shows `not found` or `not set`, do step 2 first.

## 2. First deploy (once per account)

1. `infrastructure/config/parameters.json`: `"app_with_earnings_research": true`; adjust
   `batch.queues[]` (shape only) and `earnings_research.secret_names`; add the data buckets to
   `s3.custom_arns`. `infrastructure/.env`: account, region, `GITHUB_*` (the token secret the
   image build uses).
2. `cdk deploy --all` from `infrastructure/`.
3. `samples/earnings_research/config.json`, from the stack outputs:
   - `batch.job_queue` = `<NAMESPACE>-earnings-research-cpu` (queue id `cpu`)
   - `batch.alpaca_job_queue` = `<NAMESPACE>-earnings-research-capped` (queue id `capped`)
   - `batch.job_definition` = `<NAMESPACE>-earnings-research`
4. Commit and push `config.json`. The ECR repository is empty until the first build; start it
   by hand once: `aws codebuild start-build --project-name <NAMESPACE>-earnings-research-image-build`.

## 3. The image must match the code

Every push under `samples/earnings_research/` rebuilds `:latest`. basket_study and
sentiment_analysis children refuse to run (exit 3) when the image's `CODE_VERSION` differs
from the planner's commit (`git rev-parse HEAD | cut -c 1-12`). Before planning: commit, push,
and wait until the latest build of `<NAMESPACE>-earnings-research-image-build` SUCCEEDED
(`aws codebuild list-builds-for-project` + `batch-get-builds`).

## 4. Submit

Planners run locally and submit their own arrays (queue, job definition and sizes from
`config.json`, via `settings.job_overrides`):

| Task | Run locally (from the package directory) |
|---|---|
| Basket study events (+ aggregation) | `python evaluate.py plan --dates ... [--aggregate]` |
| Basket study sentiment scoring | `python evaluate.py plan-sentiment ...` |
| Sentiment analysis arms | `python evaluate.py plan --arm R` / `--arm S` |
| Market data panels | `python main.py plan --start YYYY-MM-DD --end YYYY-MM-DD` |
| Universe refresh | `python refresh_universe.py ... --apply` |
| Per-ticker history (capped queue) | `python fetch_xle_history.py ...` |

Add `--dry-run` first where the planner supports it.

The content pipeline has no planner; submit its jobs directly. Size them with the matching
`batch.resources` entry (`content_pipeline:<command>`, else `content_pipeline`):

```bash
aws batch submit-job \
  --job-name content-fetch \
  --job-queue "$(jq -r .batch.job_queue config.json)" \
  --job-definition "$(jq -r .batch.job_definition config.json)" \
  --array-properties size=<num_shards> \
  --container-overrides '{"command":["content_pipeline","fetch","--config","config.json"],
    "resourceRequirements":[{"type":"VCPU","value":"1"},{"type":"MEMORY","value":"4096"}]}'
```

Commands: `build-manifest` (single job), `fetch`, `backfill-scan`, `backfill-fetch` (array
jobs, one child per shard; `backfill-fetch` starts a virtual display for the browser).

## 5. Sizing

Job vCPU/memory live in `config.json` `batch.resources`, keyed `<package>:<command>` then
`<package>`. Change sizes there, not in code or in the job definition.
