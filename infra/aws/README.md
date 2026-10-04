# Bedrock access for the DAG generator

Terraform for running `dag_generator/` on **Amazon Bedrock** instead of the
Anthropic API, so prompts and responses stay inside your AWS account.

## Scope: inference only

AWS is the **model provider**, nothing more. The application, Airflow, Postgres,
MinIO, and the Iceberg catalog all run locally in Docker Compose, and every
pipeline executes there.

| | Where |
|---|---|
| Airflow scheduler / webserver | Local container |
| Source Postgres, MinIO (S3), Iceberg REST catalog | Local containers |
| Pipeline execution, Parquet files, table data | Local, never leaves the machine |
| Model inference | AWS Bedrock |

This module creates **no storage and no compute** — only an IAM policy and a
principal permitted to call `InvokeModel`, plus two optional extras. There is
nothing here to host the app on, by design.

The prompt sent to Bedrock carries **schema metadata only**: table and column
names, types, nullability, keys, and an estimated row count from
`information_schema`. The generator issues no `SELECT` against your tables, so
row values never leave the local stack. Column *names* do travel, which is what
the optional guardrail below is for.

## What this creates

Bedrock's foundation models are account-level managed resources — there is no
"create a model" to declare. What Terraform provisions is the **access path**:

| Resource | Purpose | Default |
|---|---|---|
| IAM policy | `InvokeModel` scoped to the exact model ids you list | always |
| IAM user **or** role | The principal the generator authenticates as | user |
| Access key | Long-lived credentials for local development | created |
| Guardrail | Anonymizes PII in model output | **off** |
| Invocation logging | Full prompts to CloudWatch | **off** |

## Before you start

**Request model access.** A fresh account cannot invoke any foundation model
until access is granted, and Terraform cannot do this — it is a console action.

1. Open **Bedrock → Model access** in the region you plan to use
2. Request access to the Anthropic models you want
3. Wait for status `Access granted`

Then confirm what you can actually reach:

```bash
aws bedrock list-inference-profiles --region us-east-1 \
  --query 'inferenceProfileSummaries[?contains(inferenceProfileId, `anthropic`)].inferenceProfileId'
```

Put those ids in `bedrock_model_ids`. The policy is an allowlist: an id absent
from it is denied at invoke time.

## Credentials: a separate .env

AWS credentials for Terraform live in **`infra/aws/.env`**, not the project-root
one:

```bash
cp .env.example .env    # in infra/aws/
```

```bash
# infra/aws/.env
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=us-east-1
```

The root `.env` defines the same three names for **MinIO**
(`minioadmin`/`minioadmin`, plus `AWS_ENDPOINT_URL=http://minio:9000`), and
`docker-compose` feeds them to Airflow and `iceberg-rest`. Those values are
hardcoded in the compose file, so they cannot be changed without breaking every
Iceberg read and write.

Two files keep the two sets apart structurally — `docker-compose` reads only
`./.env` at the project root and never sees this one, so both can use the
standard `AWS_*` names:

```
.env             ──> docker-compose ──> Airflow, iceberg-rest ──> MinIO
infra/aws/.env   ──> source + terraform                       ──> AWS Bedrock
```

Both real `.env` files are gitignored; the `.env.example` next to each is what
gets committed.

## Deploy

```bash
cd infra/aws
cp terraform.tfvars.example terraform.tfvars   # then edit

source ./.env
terraform init
terraform plan
terraform apply
```

Or from the project root: `make tf-init`, `make tf-plan`, `make tf-apply`.

To use an AWS CLI profile instead, skip the `.env` entirely:

```bash
AWS_PROFILE=myprofile terraform plan
```

> **Region.** The provider block sets `region = var.aws_region`, and an explicit
> provider argument beats `AWS_DEFAULT_REGION` from the environment.
> `var.aws_region` also builds the Bedrock ARNs, so if you deploy outside
> `us-east-1`, set it in `terraform.tfvars` as well as in `.env`.

## Connect the generator

```bash
terraform output -raw env_file_block >> ../../.env
```

That writes the provider, region, model id, and credentials. Then:

```bash
cd ../..
python -m dag_generator.cli "import the transactions table into iceberg" --provider bedrock
```

Or set `LLM_PROVIDER=bedrock` in `.env` and drop the flag.

## Choosing an access mode

```hcl
access_mode = "user"    # local development: long-lived keys
access_mode = "role"    # inside AWS: no static credentials
```

For `role`, name who may assume it:

```hcl
access_mode             = "role"
role_trusted_services   = ["ec2.amazonaws.com"]          # generator on EC2
role_trusted_principals = ["arn:aws:iam::016065103695:root"]
```

## Two details worth knowing

**Inference profiles need two kinds of ARN.** Ids like
`us.anthropic.claude-...` are cross-region inference profiles. Authorizing the
profile alone is not enough — invoking it fans out to the underlying foundation
model in whichever region absorbs the request, so the policy grants both the
profile ARN and the foundation-model ARN in every region listed in
`inference_profile_regions`. Trim that list and requests routed elsewhere fail
with `AccessDeniedException`.

**Invocation logging is a singleton.** Bedrock allows exactly one logging
configuration per account per region. Enabling it here **overwrites** any
existing one. Check first:

```bash
aws bedrock get-model-invocation-logging-configuration --region us-east-1
```

## State and secrets

With `create_access_key = true` the secret key is written to
`terraform.tfstate` **in cleartext**. It is gitignored; treat it as a credential.
Set `create_access_key = false` to skip it, or use `access_mode = "role"`.

## Cost

The IAM resources are free. You pay per token invoked, plus CloudWatch storage
and per-request guardrail charges if enabled. `terraform destroy` removes the
access path but not the logs.

## Troubleshooting

**`AccessDeniedException` on invoke**
- Model access not granted in the console (see [Before you start](#before-you-start))
- The model id is not in `bedrock_model_ids`
- The request was routed to a region missing from `inference_profile_regions`

**`ValidationException: model identifier is invalid`**
- On-demand throughput usually requires the inference profile id (`us.` prefix),
  not the bare foundation-model id. Check `list-inference-profiles`.

**`Could not connect to the endpoint URL: "http://minio:9000/"`**
- `AWS_ENDPOINT_URL` from `.env` is redirecting AWS calls. `unset` it.

**`UnrecognizedClientException` / `InvalidClientTokenId`**
- You are using the MinIO credentials from `.env`. Use real AWS credentials.
