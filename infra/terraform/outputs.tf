output "job_ids" {
  value       = { for k, v in databricks_job.patchit_jobs : k => v.id }
  description = "Databricks job ids (if created)"
}
