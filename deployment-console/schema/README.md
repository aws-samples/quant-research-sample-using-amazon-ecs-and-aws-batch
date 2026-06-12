# Config Schema — the agent's contract

The validated `parameters` object the agent produces is the **only** thing it can influence;
a valid object that passes here is injected into the CDK app via
`cdk deploy -c parameters='<object>'`. This directory is that contract.

| File | Purpose |
|---|---|
| `config.schema.json` | JSON Schema (Draft 2020-12). Structural validation: types, enums, required fields, `additionalProperties:false` (rejects unknown keys, e.g. injected fields). Mirrors `infrastructure/config/parameters.json`. |
| `validate.py` | Two-layer validator: (1) JSON Schema, (2) cross-field rules JSON Schema can't express — see `../design/CONFIG_SCHEMA_PLAN.md` §4. Returns hard errors (block deploy) + advisory warnings. The `validate_config` agent tool calls this. |
| `requirements.txt` | `jsonschema` (the only dep). |

## Usage

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt

# validate a parameters object (+ optional .env-style identity for rules 8 & 10)
.venv/bin/python validate.py path/to/parameters.json [path/to/identity.json]
# exit 0 = ok, 1 = errors; prints {"ok", "errors", "warnings"} JSON
```

## Regression guarantee

The canonical `infrastructure/config/parameters.json` **must always pass**:

```bash
.venv/bin/python validate.py ../../infrastructure/config/parameters.json   # exit 0
```

If a change to the CDK app's parameters breaks this, either the schema or the params drifted
— fix before merging. (Verified passing as of this commit; the one warning about
`per_unit_storage_throughput` under `SCRATCH_2` is expected and correct.)

## What's enforced where

- **Structural (schema):** shape, types, enums, no unknown keys.
- **Cross-field (validate.py):** S3-Express⟹AZ-id, FSx minimums, deployment-type gating,
  CPU schedulability, GPU-class/capacity, node topology, CodePipeline⟹GitHub fields,
  namespace S3-safety.
- **Locked fields:** `container_command` is flagged if non-default — the tool layer must
  reject agent-originated values (arbitrary-code-execution risk). `s3.custom_arns` is
  shape-checked but should be an allow-list, never agent-freeform.

## Note on `instance_classes`

Validated as an uppercase family pattern (`^[A-Z][A-Z0-9]{1,7}$`), **not** an exhaustive
enum — CDK's `ec2.InstanceClass(c)` accepts a large, evolving set, so an enum would wrongly
reject valid classes. GPU-capability (rule 6) is checked by G/P family prefix in `validate.py`.
