# Terraform cannot query the coordination PostgreSQL journal. This state-owned
# sentinel makes a raw `terraform destroy` fail closed. Only the lifecycle may
# produce journal/tag-absence evidence; the documented cleanup flow then
# removes this one address from state before creating a destroy plan.
resource "terraform_data" "round5_destroy_guard" {
  input = merge({
    run_id = var.run_id
    }, local.v7_enabled ? {
    installation_round_slug = local.v7_round_slugs["r5"]
    round                   = "r5"
  } : {})

  lifecycle {
    prevent_destroy = true
  }
}
