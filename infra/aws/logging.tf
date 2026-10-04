###############################################################################
# Optional model-invocation logging.
#
# Bedrock permits exactly ONE logging configuration per account per region -
# it is a singleton, not a per-model setting. If the account already has one,
# applying this will overwrite it, so it is off by default.
#
# Worth enabling while developing the generator: the logs contain the full
# rendered prompt, which is the fastest way to see what grounding the model
# actually received.
###############################################################################

resource "aws_cloudwatch_log_group" "bedrock" {
  count = var.enable_invocation_logging ? 1 : 0

  name              = "/aws/bedrock/${local.name_prefix}"
  retention_in_days = var.log_retention_days
  tags              = local.tags
}

data "aws_iam_policy_document" "bedrock_logging_assume" {
  count = var.enable_invocation_logging ? 1 : 0

  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["bedrock.amazonaws.com"]
    }

    # Confused-deputy guards: only this account's Bedrock may assume the role.
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }

    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"
      values   = ["arn:aws:bedrock:${var.aws_region}:${data.aws_caller_identity.current.account_id}:*"]
    }
  }
}

data "aws_iam_policy_document" "bedrock_logging" {
  count = var.enable_invocation_logging ? 1 : 0

  statement {
    effect = "Allow"
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["${aws_cloudwatch_log_group.bedrock[0].arn}:*"]
  }
}

resource "aws_iam_role" "bedrock_logging" {
  count = var.enable_invocation_logging ? 1 : 0

  name               = "${local.name_prefix}-bedrock-logging"
  assume_role_policy = data.aws_iam_policy_document.bedrock_logging_assume[0].json
  tags               = local.tags
}

resource "aws_iam_role_policy" "bedrock_logging" {
  count = var.enable_invocation_logging ? 1 : 0

  name   = "write-logs"
  role   = aws_iam_role.bedrock_logging[0].id
  policy = data.aws_iam_policy_document.bedrock_logging[0].json
}

resource "aws_bedrock_model_invocation_logging_configuration" "this" {
  count = var.enable_invocation_logging ? 1 : 0

  logging_config {
    embedding_data_delivery_enabled = false
    image_data_delivery_enabled     = false
    text_data_delivery_enabled      = true

    cloudwatch_config {
      log_group_name = aws_cloudwatch_log_group.bedrock[0].name
      role_arn       = aws_iam_role.bedrock_logging[0].arn
    }
  }

  # The role must be able to write before Bedrock validates the configuration.
  depends_on = [aws_iam_role_policy.bedrock_logging]
}
