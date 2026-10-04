# Local state by default.
#
# When create_access_key = true, state holds an IAM secret key in CLEARTEXT.
# Treat terraform.tfstate as a credential; it is covered by .gitignore.
#
# For remote state, create the bucket and lock table, then uncomment:
# terraform {
#   backend "s3" {
#     bucket         = "your-terraform-state-bucket"
#     key            = "bedrock/terraform.tfstate"
#     region         = "us-east-1"
#     encrypt        = true
#     dynamodb_table = "terraform-locks"
#   }
# }
