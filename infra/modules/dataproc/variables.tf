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
  description = "GCP deployment region"
}

variable "enable_managed_cluster" {
  type        = bool
  default     = false
  description = "Whether to provision an ephemeral single-node Dataproc cluster (false uses Dataproc Serverless / local Spark for cost protection)"
}
