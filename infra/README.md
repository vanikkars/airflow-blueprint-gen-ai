# Infrastructure

**The application runs entirely locally in Docker Compose. AWS is used for one
thing only: invoking the Bedrock model that powers the DAG generator.**

```
        LOCAL (docker-compose.airflow.yml)                 AWS
┌──────────────────────────────────────────────┐    ┌──────────────────┐
│  Airflow  ·  Postgres  ·  MinIO (S3)         │    │                  │
│  Iceberg REST catalog  ·  postgres-catalog   │    │  Bedrock         │
│                                              │    │  InvokeModel     │
│  dag_generator/  ────────── prompt ──────────┼───>│                  │
│                  <───────── DAG YAML ────────┼────│  (inference only)│
└──────────────────────────────────────────────┘    └──────────────────┘
   all pipelines, storage and orchestration          no data stored,
   stay on your machine                              no compute hosted
```

Nothing about a pipeline run touches AWS. Postgres, Airflow, the Iceberg
catalog, and the Parquet files in MinIO are all local. The only outbound call is
the generator sending a prompt to Bedrock and receiving YAML back.

## Structure

```
infra/
├── aws/                        # Bedrock access for dag_generator/
│   ├── main.tf                 # IAM policy + user/role
│   ├── guardrail.tf            # Optional PII guardrail
│   ├── logging.tf              # Optional invocation logging
│   ├── variables.tf
│   ├── outputs.tf              # Includes a ready-to-paste .env block
│   ├── backend.tf
│   ├── .env.example            # AWS credentials template (real .env gitignored)
│   ├── terraform.tfvars.example
│   └── README.md
└── README.md
```

## Quick start

```bash
# 1. Grant model access in the AWS console: Bedrock -> Model access
# 2. Put REAL AWS credentials in infra/aws/.env - a separate file from the
#    project-root .env, whose AWS_* names belong to MinIO (see below)
#      cp infra/aws/.env.example infra/aws/.env

# 3. Provision the IAM access path
cd infra/aws
cp terraform.tfvars.example terraform.tfvars    # then edit
source ./.env
terraform init
terraform plan
terraform apply

# 4. Write provider, region, model id and credentials into the project .env
terraform output -raw env_file_block >> ../../.env

# 5. Generate a DAG through Bedrock
cd ../..
python -m dag_generator.cli "import the transactions table into iceberg" --provider bedrock
```

Full detail, including access modes and troubleshooting, is in
[`aws/README.md`](aws/README.md).

## Two sets of AWS credentials

`.env` defines `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
`AWS_DEFAULT_REGION` and `AWS_ENDPOINT_URL` for **MinIO**, and
`docker-compose` passes them into Airflow and `iceberg-rest`. MinIO's root
credentials are hardcoded as `minioadmin`/`minioadmin` in the compose file, so
replacing those values with real AWS keys breaks every Iceberg read and write.

Real credentials therefore live in a **separate file**, `infra/aws/.env`, which
you source before running terraform. `docker-compose` reads only `./.env` at the
project root and never sees it, so both files can use the standard `AWS_*` names
without clashing. That file also ends with `unset AWS_ENDPOINT_URL`, so a shell
that already sourced the root `.env` does not send AWS calls to MinIO.

```
.env             ──> docker-compose ──> Airflow, iceberg-rest ──> MinIO
infra/aws/.env   ──> source + terraform                       ──> AWS Bedrock
```

Prefer an AWS CLI profile? Skip the wrapper: `AWS_PROFILE=myprofile terraform plan`.

## Resources created

- **IAM policy** — `InvokeModel` scoped to specific Bedrock model ids
- **IAM user or role** — the principal the DAG generator authenticates as
- **Guardrail** (optional, off by default) — anonymizes PII in model output
- **Invocation logging** (optional, off by default) — full prompts to CloudWatch

No storage, no compute, no data. Access to inference, nothing else.

## State

Local state in `aws/terraform.tfstate`, gitignored.

With `create_access_key = true` the IAM secret key is written to state **in
cleartext** — treat the file as a credential. Set `create_access_key = false`,
or use `access_mode = "role"`, to avoid it entirely.

For remote state, uncomment the S3 backend block in `aws/backend.tf` and re-run
`terraform init`.

## Commands

From the project root, the Makefile wraps these:

```bash
make tf-init      make tf-plan      make tf-apply
make tf-output    make tf-destroy   make tf-reinit
```

Or directly:

```bash
cd infra/aws

source ./.env              # load AWS credentials

terraform init             # initialize
terraform validate         # check configuration
terraform plan             # preview
terraform apply            # create
terraform output           # view outputs
terraform destroy          # remove the access path

terraform fmt -recursive   # format (no credentials needed)
```

## See Also

- [Bedrock setup](aws/README.md)
- [Main README](../README.md)
