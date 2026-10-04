###############################################################################
# Amazon Bedrock access for the LangChain DAG generator (dag_generator/).
#
# SCOPE: AWS is the model provider and nothing else. The application, Airflow,
# Postgres, MinIO and the Iceberg catalog all run locally in Docker Compose,
# and every pipeline executes there. This module deliberately creates no
# storage and no compute - only the permission to call InvokeModel.
#
# Bedrock's foundation models are a managed, account-level service: there is no
# "create a model" resource to declare. What this module provisions is the
# access path to them - an IAM principal scoped to exactly the models the
# generator uses, optional guardrails, optional prompt logging, and a place to
# keep the credentials.
###############################################################################

terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  name_prefix = "${var.project_name}-${var.environment}"

  # Inference profile IDs ("us.anthropic.claude-*") are what on-demand
  # throughput requires in most regions, but an IAM policy cannot authorize a
  # profile alone: invoking one fans out to the underlying foundation model in
  # each region the profile covers. The policy below therefore grants both the
  # profile ARNs and the foundation-model ARNs they resolve to.
  model_ids = var.bedrock_model_ids

  foundation_model_ids = [
    for id in local.model_ids :
    # Strip a leading geo prefix ("us.", "eu.", "apac.") to recover the
    # underlying foundation model id.
    replace(id, "/^(us|eu|apac)\\./", "")
  ]

  inference_profile_arns = [
    for id in local.model_ids :
    "arn:aws:bedrock:${var.aws_region}:${data.aws_caller_identity.current.account_id}:inference-profile/${id}"
    if length(regexall("^(us|eu|apac)\\.", id)) > 0
  ]

  # Cross-region inference can land the request in any region the profile
  # covers, so the foundation model must be allowed in each of them.
  foundation_model_arns = flatten([
    for id in local.foundation_model_ids : [
      for region in var.inference_profile_regions :
      "arn:aws:bedrock:${region}::foundation-model/${id}"
    ]
  ])

  tags = merge(var.common_tags, {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
    Component   = "bedrock-dag-generator"
  })
}

###############################################################################
# IAM policy: permission to invoke exactly the models the generator uses.
###############################################################################

data "aws_iam_policy_document" "bedrock_invoke" {
  statement {
    sid    = "InvokeGeneratorModels"
    effect = "Allow"
    actions = [
      "bedrock:InvokeModel",
      "bedrock:InvokeModelWithResponseStream",
    ]
    resources = concat(local.inference_profile_arns, local.foundation_model_arns)
  }

  # Listing models is how the generator (and `aws bedrock list-foundation-models`)
  # discovers what this account can reach. It returns no model content, so it is
  # safe to allow account-wide.
  statement {
    sid    = "DiscoverModels"
    effect = "Allow"
    actions = [
      "bedrock:ListFoundationModels",
      "bedrock:GetFoundationModel",
      "bedrock:ListInferenceProfiles",
      "bedrock:GetInferenceProfile",
    ]
    resources = ["*"]
  }

  dynamic "statement" {
    for_each = var.enable_guardrail ? [1] : []
    content {
      sid    = "ApplyGuardrail"
      effect = "Allow"
      actions = [
        "bedrock:ApplyGuardrail",
      ]
      resources = [aws_bedrock_guardrail.dag_generator[0].guardrail_arn]
    }
  }
}

resource "aws_iam_policy" "bedrock_invoke" {
  name        = "${local.name_prefix}-bedrock-invoke"
  description = "Invoke the Bedrock models used by the DAG generator"
  policy      = data.aws_iam_policy_document.bedrock_invoke.json
  tags        = local.tags
}

###############################################################################
# Principal. Two mutually exclusive shapes, because local development and
# in-cluster execution authenticate differently:
#
#   access_mode = "user" - an IAM user with long-lived keys, for running the
#                          generator from a laptop. Simple, and what the
#                          project's .env-based workflow already expects.
#   access_mode = "role" - a role assumed by a trusted principal (EC2, EKS,
#                          or another account), for anything running in AWS.
#                          No static credentials to leak.
###############################################################################

resource "aws_iam_user" "generator" {
  count = var.access_mode == "user" ? 1 : 0

  name = "${local.name_prefix}-dag-generator"
  path = "/service/"
  tags = local.tags
}

resource "aws_iam_user_policy_attachment" "generator" {
  count = var.access_mode == "user" ? 1 : 0

  user       = aws_iam_user.generator[0].name
  policy_arn = aws_iam_policy.bedrock_invoke.arn
}

# Terraform stores the secret in state in cleartext, which is why this is
# gated behind a variable and the state file must be treated as sensitive.
# Prefer access_mode = "role" wherever the runtime can assume one.
resource "aws_iam_access_key" "generator" {
  count = var.access_mode == "user" && var.create_access_key ? 1 : 0

  user = aws_iam_user.generator[0].name
}

data "aws_iam_policy_document" "assume_role" {
  count = var.access_mode == "role" ? 1 : 0

  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      identifiers = var.role_trusted_principals
    }
  }

  dynamic "statement" {
    for_each = length(var.role_trusted_services) > 0 ? [1] : []
    content {
      effect  = "Allow"
      actions = ["sts:AssumeRole"]

      principals {
        type        = "Service"
        identifiers = var.role_trusted_services
      }
    }
  }
}

resource "aws_iam_role" "generator" {
  count = var.access_mode == "role" ? 1 : 0

  name               = "${local.name_prefix}-dag-generator"
  description        = "Assumed by the LangChain DAG generator to invoke Bedrock"
  assume_role_policy = data.aws_iam_policy_document.assume_role[0].json
  tags               = local.tags
}

resource "aws_iam_role_policy_attachment" "generator" {
  count = var.access_mode == "role" ? 1 : 0

  role       = aws_iam_role.generator[0].name
  policy_arn = aws_iam_policy.bedrock_invoke.arn
}
