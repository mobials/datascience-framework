### Infrastructure

(nearly) All infrastructure is managed via Terraform. All new infrastructure should be managed by Terraform.

To use:

1. `cd` into the appropriate directory. E.g., `cd staging/dns`
2. Run `terraform init` to initialize the directory. Note that you'll need proper AWS credentials in order to have
Terraform access the S3 bucket where all infrastructure is stored.
3. Make the changes you wanted....
4. Run them via `terraform apply -var-file=./../../staging.tfvars` and `terraform apply -var-file=./../../production.tfvars`
5. Review your changes - carefully.
6. Type `yes` if you wish to make the changes.
