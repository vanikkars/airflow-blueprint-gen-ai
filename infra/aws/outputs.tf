output "bedrock_region" {
  value       = var.aws_region
  description = "Region to set as AWS_DEFAULT_REGION for the generator"
}

output "bedrock_model_ids" {
  value       = var.bedrock_model_ids
  description = "Model ids this account is now permitted to invoke"
}

output "primary_model_id" {
  value       = var.bedrock_model_ids[0]
  description = "Pass to the generator as --model"
}

output "invoke_policy_arn" {
  value       = aws_iam_policy.bedrock_invoke.arn
  description = "ARN of the Bedrock invoke policy, to attach to other principals"
}

output "generator_user_name" {
  value       = var.access_mode == "user" ? aws_iam_user.generator[0].name : null
  description = "IAM user created for the generator (access_mode = \"user\")"
}

output "generator_role_arn" {
  value       = var.access_mode == "role" ? aws_iam_role.generator[0].arn : null
  description = "Role the generator assumes (access_mode = \"role\")"
}

output "access_key_id" {
  value       = try(aws_iam_access_key.generator[0].id, null)
  description = "Access key id for the generator user"
}

output "secret_access_key" {
  value       = try(aws_iam_access_key.generator[0].secret, null)
  sensitive   = true
  description = "Secret key. Read with: terraform output -raw secret_access_key"
}

output "guardrail_id" {
  value       = try(aws_bedrock_guardrail.dag_generator[0].guardrail_id, null)
  description = "Guardrail id, if enabled"
}

output "guardrail_version" {
  value       = try(aws_bedrock_guardrail_version.dag_generator[0].version, null)
  description = "Guardrail version, if enabled"
}

output "log_group_name" {
  value       = try(aws_cloudwatch_log_group.bedrock[0].name, null)
  description = "CloudWatch log group receiving invocation logs, if enabled"
}

# Ready-to-paste .env block, so the handoff from Terraform to the generator is
# a copy rather than a lookup. Sensitive because it embeds the secret key.
output "env_file_block" {
  sensitive = true
  value     = <<-EOT
    # --- Bedrock: append to the project .env ---
    LLM_PROVIDER=bedrock
    AWS_DEFAULT_REGION=${var.aws_region}
    BEDROCK_MODEL_ID=${var.bedrock_model_ids[0]}
    ${var.access_mode == "user" && var.create_access_key ? "AWS_ACCESS_KEY_ID=${try(aws_iam_access_key.generator[0].id, "")}\n    AWS_SECRET_ACCESS_KEY=${try(aws_iam_access_key.generator[0].secret, "")}" : "# access_mode=role: obtain credentials via sts:AssumeRole ${try(aws_iam_role.generator[0].arn, "")}"}
    ${var.enable_guardrail ? "BEDROCK_GUARDRAIL_ID=${try(aws_bedrock_guardrail.dag_generator[0].guardrail_id, "")}\n    BEDROCK_GUARDRAIL_VERSION=${try(aws_bedrock_guardrail_version.dag_generator[0].version, "")}" : ""}
  EOT
}
