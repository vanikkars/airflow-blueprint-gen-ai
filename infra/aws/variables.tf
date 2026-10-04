variable "aws_region" {
  type        = string
  default     = "us-east-1"
  description = <<-EOT
    AWS region to invoke Bedrock in. Model availability varies by region.

    The provider block uses this explicitly and it also builds the Bedrock ARNs,
    so it - not AWS_DEFAULT_REGION - decides the region. Keep it in step with
    AWS_DEFAULT_REGION in infra/aws/.env, or set it in terraform.tfvars.
  EOT
}

variable "project_name" {
  type        = string
  default     = "pipelines-gen-ai"
  description = "Name prefix for all resources"
}

variable "environment" {
  type        = string
  default     = "dev"
  description = "Environment name, used in the resource name prefix"
}

variable "bedrock_model_ids" {
  type = list(string)
  default = [
    "us.anthropic.claude-sonnet-4-5-20250929-v1:0",
  ]
  description = <<-EOT
    Bedrock model ids the generator is allowed to invoke.

    The "us." prefix denotes a cross-region inference profile, which is what
    on-demand throughput requires for current Anthropic models. List the exact
    ids you intend to use - this is the allowlist, so an id absent here is
    denied at invoke time.

    Confirm what your account can actually reach:
      aws bedrock list-inference-profiles --region us-east-1
  EOT
}

variable "inference_profile_regions" {
  type        = list(string)
  default     = ["us-east-1", "us-east-2", "us-west-2"]
  description = <<-EOT
    Regions a cross-region inference profile may route a request to. The IAM
    policy must allow the foundation model in every one of them, otherwise a
    request silently routed elsewhere fails with AccessDeniedException.

    These are the regions behind the "us." profiles. Adjust for "eu." or
    "apac." profiles.
  EOT
}

variable "access_mode" {
  type        = string
  default     = "user"
  description = <<-EOT
    How the generator authenticates:
      "user" - IAM user with long-lived access keys, for local development.
      "role" - IAM role assumed by a trusted principal, for anything in AWS.
  EOT

  validation {
    condition     = contains(["user", "role"], var.access_mode)
    error_message = "access_mode must be either \"user\" or \"role\"."
  }
}

variable "create_access_key" {
  type        = bool
  default     = true
  description = <<-EOT
    Create an access key for the IAM user (access_mode = "user" only).

    The secret is stored in Terraform state in CLEARTEXT. Keep the state file
    out of version control and treat it as a credential. Set false to create
    the user without keys and issue them by hand.
  EOT
}

variable "role_trusted_principals" {
  type        = list(string)
  default     = []
  description = <<-EOT
    ARNs allowed to assume the generator role (access_mode = "role").
    For example ["arn:aws:iam::016065103695:root"] to trust the account, or a
    specific role ARN for tighter scoping.
  EOT
}

variable "role_trusted_services" {
  type        = list(string)
  default     = []
  description = <<-EOT
    AWS services allowed to assume the role, e.g. ["ec2.amazonaws.com"] when
    the generator runs on EC2, or ["ecs-tasks.amazonaws.com"] on ECS.
  EOT
}

variable "enable_guardrail" {
  type        = bool
  default     = false
  description = <<-EOT
    Create a Bedrock guardrail that anonymizes PII in model output.

    Off by default: it costs per request and can reject valid DAG YAML. Enable
    when pointing the generator at a source with genuinely sensitive data.
  EOT
}

variable "guardrail_pii_entities" {
  type = list(string)
  default = [
    "EMAIL",
    "PHONE",
    "US_SOCIAL_SECURITY_NUMBER",
    "US_BANK_ACCOUNT_NUMBER",
    "CREDIT_DEBIT_CARD_NUMBER",
    "ADDRESS",
  ]
  description = "PII entity types the guardrail anonymizes in model output"
}

variable "enable_invocation_logging" {
  type        = bool
  default     = false
  description = <<-EOT
    Log every Bedrock invocation to CloudWatch, including the full prompt.

    Off by default because Bedrock allows only ONE logging configuration per
    account per region - enabling this OVERWRITES any existing one. Check first:
      aws bedrock get-model-invocation-logging-configuration --region us-east-1
  EOT
}

variable "log_retention_days" {
  type        = number
  default     = 14
  description = "CloudWatch log retention. Prompts contain your source schema."
}

variable "common_tags" {
  type = map(string)
  default = {
    Environment = "dev"
    Project     = "pipelines-gen-ai"
    ManagedBy   = "terraform"
  }
  description = "Common tags applied to all resources"
}
