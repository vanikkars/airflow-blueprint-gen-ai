###############################################################################
# Optional guardrail.
#
# The generator's prompt already carries the live database schema, which for
# the banking source includes ssn, email, date_of_birth and address column
# names. A guardrail is defence in depth: it filters the model's own output, so
# it catches a response that echoes sensitive values back rather than blocking
# the schema going in.
#
# Off by default (enable_guardrail = false): it adds per-request cost and can
# reject legitimate DAG YAML. Turn it on when the generator points at a source
# whose schema or sample values are genuinely sensitive.
###############################################################################

resource "aws_bedrock_guardrail" "dag_generator" {
  count = var.enable_guardrail ? 1 : 0

  name                      = "${local.name_prefix}-dag-generator"
  description               = "Guards the DAG generator against leaking source data values"
  blocked_input_messaging   = "This request was blocked by a content guardrail."
  blocked_outputs_messaging = "The generated response was blocked by a content guardrail."
  tags                      = local.tags

  # Column *names* such as `ssn` must pass through - they are required
  # grounding. ANONYMIZE masks values matching these patterns in the model's
  # output without blocking the response, so a DAG still gets generated.
  sensitive_information_policy_config {
    dynamic "pii_entities_config" {
      for_each = var.guardrail_pii_entities
      content {
        type   = pii_entities_config.value
        action = "ANONYMIZE"
      }
    }
  }

  content_policy_config {
    filters_config {
      type            = "PROMPT_ATTACK"
      input_strength  = "HIGH"
      output_strength = "NONE" # PROMPT_ATTACK only supports NONE on output
    }
  }
}

resource "aws_bedrock_guardrail_version" "dag_generator" {
  count = var.enable_guardrail ? 1 : 0

  guardrail_arn = aws_bedrock_guardrail.dag_generator[0].guardrail_arn
  description   = "Managed by Terraform"
}
