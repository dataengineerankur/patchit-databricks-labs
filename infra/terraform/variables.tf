variable "databricks_host" {
  type        = string
  description = "Databricks workspace host (https://...)"
}

variable "databricks_token" {
  type        = string
  description = "Databricks PAT (use minimal scope)"
  sensitive   = true
}

variable "dbx_region" {
  type        = string
  description = "Cloud region for documentation/labels"
  default     = "us-east-1"
}

variable "dbx_node_type_id" {
  type        = string
  description = "Databricks node type id (Azure uses Standard_* VM sizes)"
  default     = "Standard_DS3_v2"
}

variable "enable_apply" {
  type        = bool
  description = "Safety switch. Must be true to create cloud resources."
  default     = false
}
