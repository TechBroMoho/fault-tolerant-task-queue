# The long-lived part of the AWS setup: the cost alarm and the image repository.
# Its own root and state on purpose: `make aws-down` destroys the benchmark stack in
# ../stack, and must never take the alarm with it. Neither resource bills while idle: a
# budget without actions is free, and an ECR repo costs only for the images stored in
# it, which `make aws-down` deletes (ADR-045).

provider "aws" {
  region = "us-west-2"
  default_tags {
    tags = { project = "ftq" }
  }
}

variable "alert_email" {
  description = "Where the alerts go. Set via TF_VAR_alert_email or a gitignored *.tfvars: it stays out of the public repo."
  type        = string
}

resource "aws_budgets_budget" "ftq" {
  name         = "ftq-monthly-cost"
  budget_type  = "COST"
  limit_amount = "20"
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  # The whole point. On the Free plan every charge is paid with credits, so a budget that
  # nets credits sees ~$0 and never fires. Excluding credits (and refunds, which net the
  # same way) makes it track the real usage the credits are paying for.
  cost_types {
    include_credit = false
    include_refund = false
  }

  # SPEC: alerts at $5, $10, $20 of actual (not forecast) spend. Forecasts need weeks of
  # history to mean anything; actual spend is what drains the credits.
  dynamic "notification" {
    for_each = [5, 10, 20]
    content {
      comparison_operator        = "GREATER_THAN"
      threshold                  = notification.value
      threshold_type             = "ABSOLUTE_VALUE"
      notification_type          = "ACTUAL"
      subscriber_email_addresses = [var.alert_email]
    }
  }
}

# The worker image (also the loadgen's). Scanning on push is free (basic scanning).
resource "aws_ecr_repository" "ftq" {
  name                 = "ftq"
  image_tag_mutability = "IMMUTABLE" # a tag always means one build: results name it
  force_delete         = true        # destroying the repo never gets stuck on images
  image_scanning_configuration {
    scan_on_push = true
  }
}

# Keep storage near zero if a teardown is ever skipped: only the last 3 images survive.
resource "aws_ecr_lifecycle_policy" "ftq" {
  repository = aws_ecr_repository.ftq.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "keep the last 3 images"
      selection    = { tagStatus = "any", countType = "imageCountMoreThan", countNumber = 3 }
      action       = { type = "expire" }
    }]
  })
}

output "repository_url" {
  value = aws_ecr_repository.ftq.repository_url
}

output "budget_name" {
  value = aws_budgets_budget.ftq.name
}
