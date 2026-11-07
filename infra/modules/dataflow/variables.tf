variable "project_id" {
  type        = string
  description = "GCP Project ID"
}

variable "environment" {
  type        = string
  description = "Deployment environment (dev or prod)"
}

variable "region" {
  type        = string
  default     = "europe-west1"
  description = "Dataflow deployment region"
}
