output "budget_id" {
  value       = try(google_billing_budget.budget[0].id, "disabled")
  description = "Resource ID of the GCP billing budget"
}

