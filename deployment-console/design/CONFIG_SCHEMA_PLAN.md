# Config Schema Plan — The Agent's Contract

> The single most important artifact. Everything (agent tools, validation, UI, deploy)
> hangs off this. Derived directly from `infrastructure/config/parameters.json` and
> `infrastructure/.env.template`.

---

## 1. Why this is the contract

The agent's *only* power is producing a config object that passes this schema. It cannot
emit code or shell. Validation (this schema + the cross-field rules in §4) is the security
and correctness boundary. CodeBuild then runs:

```
cdk deploy --all -c parameters='<the validated `parameters` object>'
```

…and the `.env`-style fields (account/region/namespace) are passed as CodeBuild
environment variables, **not** as CDK context (they're read via `EnvironmentConfig.from_env()`).

---

## 2. Two config namespaces (mirror the existing split)

| Namespace | Source today | How it's passed to deploy | Agent-editable? |
|---|---|---|---|
| **Deployment identity** | `.env` (`AWS_ACCOUNT_ID`, `AWS_REGION`, `NAMESPACE`, GitHub*) | CodeBuild env vars | Mostly **fixed** (single account). `NAMESPACE` editable. GitHub only if pipeline on. |
| **Infrastructure shape** | `config/parameters.json` | `-c parameters='{...}'` | **Yes — this is the agent's playground.** |

For a single-account internal tool, `AWS_ACCOUNT_ID` / `AWS_REGION` are **system-fixed**
(set by ops, not the user). The agent should treat them as read-only context.

---

## 3. The `parameters` schema (annotated)

Field-by-field, mapped to the source `parameters.json`. Legend:
🟢 agent-editable · 🟡 editable with guardrails · 🔴 system-fixed / advanced.

```jsonc
{
  "availability_zone": {
    "name": "us-east-1a",   // 🟡 must be an AZ in the fixed region
    "id":   "use1-az4"      // 🟡 REQUIRED iff app_with_s3express=true (AZ ID, not name)
  },

  "app_with_codepipeline": false,  // 🟢 toggle → DeploymentPipelineStack CI/CD
  "app_with_fsx":          false,  // 🟢 toggle → FSxStack (fast scratch)
  "app_with_s3express":    false,  // 🟢 toggle → S3 Express One Zone bucket

  "batch": {
    "deployment_type": "ALL",      // 🟢 enum: SINGLE_NODE | MULTI_NODE | ALL

    "single_node": {               // applies when SINGLE_NODE or ALL
      "maxv_cpus":  50,            // 🟢 int, >= num_queues, soft ceiling (quota)
      "minv_cpus":  0,             // 🟡 int >= 0, <= maxv_cpus
      "num_queues": 1,             // 🟡 int >= 1; maxv_cpus is divided across queues
      "container_cpu":    8,       // 🟢 vCPUs per job
      "container_memory": 16384,   // 🟢 MiB; must fit chosen instance classes
      "container_command": ["python3","main.py"],  // 🔴 advanced
      "instance_classes": ["C6I","C7A","C7I","R5","R5A","R5B","R5D","R5N","R6A","R6I","R7I"], // 🟡 enum list
      "allocation_strategy": "BEST_FIT_PROGRESSIVE", // 🟡 enum
      "spot": false                // 🟢 bool
    },

    "multi_node": {                // applies when MULTI_NODE or ALL
      "maxv_cpus": 100,            // 🟢 int
      "minv_cpus": 0,              // 🟡
      "main": {                    // node 0
        "start_node_index": 0, "end_node_index": 0,  // 🔴 advanced (topology)
        "container_cpu": 32, "container_gpu": 4, "container_memory": 65536  // 🟢
      },
      "worker": {                  // nodes 1..N
        "start_node_index": 1, "end_node_index": 2,  // 🔴 advanced (worker count)
        "container_cpu": 32, "container_gpu": 4, "container_memory": 65536  // 🟢
      },
      "instance_classes": ["G5","C5"],          // 🟡 must include a GPU class if gpu>0
      "allocation_strategy": "BEST_FIT_PROGRESSIVE", // 🟡
      "spot": true                 // 🟢
    }
  },

  "fsx": {                         // only meaningful if app_with_fsx=true
    "per_unit_storage_throughput": 200,  // 🟡 >= 50; only applied for PERSISTENT_2
    "storage_capacity_gib": 1200,        // 🟡 >= 1200
    "deployment_type": "SCRATCH_2"       // 🟡 enum: SCRATCH_2 | PERSISTENT_1 | PERSISTENT_2
  },

  "s3": {
    "object_expiration_in_days": 1,      // 🟢 int >= 1
    "custom_arns": [                     // 🟡 list of valid S3 ARNs the job role may access
      "arn:aws:s3:::amzn-s3-demo-bucket/",
      "arn:aws:s3:::amzn-s3-demo-bucket1/"
    ]
  }
}
```

---

## 4. Cross-field validation rules (the `validate_config` tool)

These are what make the schema *safe* — enforce server-side in the validation Lambda, not
just in the prompt:

1. `app_with_s3express == true` ⟹ `availability_zone.id` is required and non-empty.
2. `app_with_fsx == true` ⟹ `fsx.storage_capacity_gib >= 1200` **and**
   `fsx.per_unit_storage_throughput >= 50` (mirrors `FSxStack._validate_inputs`).
3. `fsx.per_unit_storage_throughput` only takes effect when
   `fsx.deployment_type == "PERSISTENT_2"` — warn the user otherwise (no-op).
4. `batch.deployment_type` gates which sub-block is required:
   - `SINGLE_NODE` ⟹ `batch.single_node` required, `multi_node` ignored.
   - `MULTI_NODE`  ⟹ `batch.multi_node` required, `single_node` ignored.
   - `ALL`         ⟹ both required.
5. `single_node`: `minv_cpus <= maxv_cpus`; `num_queues >= 1`;
   `floor(maxv_cpus / num_queues) >= container_cpu` (else a job can never schedule).
6. `multi_node`: if any `container_gpu > 0`, `instance_classes` must include a
   GPU-capable family (G/P series); `maxv_cpus >= main.container_cpu + worker.container_cpu * worker_count`.
7. `worker_count = (worker.end_node_index - worker.start_node_index + 1)`; must be `>= 0`,
   and node index ranges for main/worker must not overlap and must start at 0.
8. `app_with_codepipeline == true` ⟹ GitHub identity fields
   (`GITHUB_OWNER`, `GITHUB_REPO`, `GITHUB_TOKEN_SECRET_NAME`) must be present in the
   deployment-identity namespace, and the referenced Secrets Manager secret must exist.
9. `container_memory` must be a value the chosen instance classes can satisfy
   (advisory check; AWS Batch will otherwise fail to place).
10. `NAMESPACE` must be DNS/S3-bucket safe (lowercase, hyphens) — it prefixes bucket names
    that have a 63-char limit (see `S3Stack._build_bucket_name`).

---

## 5. Agent-facing simplification (presets)

The full schema is too much to ask a user to fill conversationally. The agent should work
from **intent → preset → targeted overrides**. Suggested starter presets:

| Preset | Sets | For |
|---|---|---|
| **"CPU-only data prep"** | `deployment_type: SINGLE_NODE`, all toggles off | Cheapest; feature engineering only |
| **"GPU training"** | `deployment_type: MULTI_NODE`, GPU instances, spot on | Model training |
| **"Full platform"** | `deployment_type: ALL`, `app_with_fsx: true` | End-to-end pipeline |
| **"Full + CI/CD"** | Full platform + `app_with_codepipeline: true` | Team with GitHub source |

Agent flow: pick closest preset from the user's described goal → confirm the 2–3 fields
that matter for their case → validate → preview → approve.

---

## 6. Deliverable: the actual JSON Schema file

The above becomes a real `Draft 2020-12` JSON Schema (`config.schema.json`) used by:
- the `update_config` / `validate_config` Lambda tools (server-side enforcement),
- the UI Config tab (render + diff vs. defaults),
- AgentCore tool input validation.

Cross-field rules in §4 that pure JSON Schema can't express (conditionals, arithmetic) go
in the validation Lambda as code, layered on top of structural schema validation.

> **Next step when building:** generate `config.schema.json` from this plan, plus a
> `validate.ts`/`validate.py` for the §4 rules. Keep `parameters.json` as the canonical
> default/example that must itself pass the schema (regression test).
