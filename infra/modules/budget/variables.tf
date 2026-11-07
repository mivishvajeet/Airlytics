variable "billing_account_id" {
  type        = string
  default     = "000000-000000-000000"
  description = "GCP Billing Account ID"
}

variable "project_id" {
  type        = string
  description = "GCP Project ID"
}

variable "environment" {
  type        = string
  description = "Deployment environment (dev or prod)"
}

variable "monthly_budget_amount" {
  type        = number
  default     = 25
  description = "Monthly budget limit in USD"
}

