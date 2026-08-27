provider "databricks" {
  # Reuses the same ~/.databrickscfg profile already used by the
  # `databricks` CLI locally — no token is stored in this repo.
  profile = "healthkit"
}
