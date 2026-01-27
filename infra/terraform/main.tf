terraform {
  required_version = ">= 1.5.0"
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.44"
    }
  }
}

provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}

# Safety: resources are created only when enable_apply = true.

locals {
  notebooks = {
    ingest  = "${path.module}/../../notebooks/ingest_notebook.py"
    quality = "${path.module}/../../notebooks/quality_notebook.py"
    dedup   = "${path.module}/../../notebooks/dedup_notebook.py"
  }
  jobs = {
    ingest  = "/Shared/patchit/ingest_notebook"
    quality = "/Shared/patchit/quality_notebook"
    dedup   = "/Shared/patchit/dedup_notebook"
  }
}

resource "databricks_notebook" "patchit_notebooks" {
  for_each = var.enable_apply ? local.notebooks : {}
  path     = "/Shared/patchit/${each.key}_notebook"
  source   = each.value
  language = "PYTHON"
}

resource "databricks_job" "patchit_jobs" {
  for_each = var.enable_apply ? local.jobs : {}
  name     = "patchit-${each.key}-job"

  task {
    task_key = "${each.key}_task"
    new_cluster {
      spark_version = "13.3.x-scala2.12"
      node_type_id  = var.dbx_node_type_id
      num_workers   = 1
    }
    notebook_task {
      notebook_path = each.value
    }
  }
}
