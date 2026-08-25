# Illustrative export adapter: maps ONE pipeline (bronze -> silver ingestion)
# to its Azure equivalent. This proves IaC/multi-cloud literacy, not a
# working second cloud — `terraform plan` only, never `terraform apply`.
# No provider credentials are configured; `plan` runs against a mocked
# backend so it stays entirely local and free.

terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.90"
    }
  }
}

provider "azurerm" {
  features {}
  skip_provider_registration = true
}

variable "environment" {
  description = "Deployment environment tag"
  type        = string
  default     = "dev"
}

resource "azurerm_resource_group" "banking_warehouse" {
  name     = "rg-banking-engagement-warehouse-${var.environment}"
  location = "eastus"
}

# Equivalent of s3://bank-bronze — raw ingestion landing zone
resource "azurerm_storage_account" "bronze" {
  name                     = "bankbronze${var.environment}"
  resource_group_name      = azurerm_resource_group.banking_warehouse.name
  location                 = azurerm_resource_group.banking_warehouse.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true # ADLS Gen2, for parity with S3's hierarchical namespace
}

resource "azurerm_storage_data_lake_gen2_filesystem" "bronze_fs" {
  name               = "bronze"
  storage_account_id = azurerm_storage_account.bronze.id
}

# Equivalent of the Step Functions daily orchestration job
resource "azurerm_data_factory" "orchestration" {
  name                = "adf-banking-warehouse-${var.environment}"
  resource_group_name = azurerm_resource_group.banking_warehouse.name
  location            = azurerm_resource_group.banking_warehouse.location
}

output "bronze_equivalent" {
  value       = azurerm_storage_data_lake_gen2_filesystem.bronze_fs.id
  description = "Azure ADLS Gen2 filesystem standing in for s3://bank-bronze"
}
