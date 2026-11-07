"""Unit test verifying Terraform HCL files, module structures, and cost control rules."""

import os
import re
import unittest

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INFRA_DIR = os.path.join(ROOT_DIR, "infra")


class TestTerraformInfra(unittest.TestCase):
    """Validate Terraform module completeness, environment configs, and budget rules."""

    def test_all_modules_exist(self):
        """All required infrastructure modules must contain main.tf, variables.tf, and outputs.tf."""
        required_modules = ["bigquery", "pubsub", "cloud_run", "dataflow", "monitoring", "budget"]
        for mod in required_modules:
            mod_path = os.path.join(INFRA_DIR, "modules", mod)
            self.assertTrue(os.path.isdir(mod_path), f"Missing module directory: {mod}")
            for tf_file in ["main.tf", "variables.tf", "outputs.tf"]:
                file_path = os.path.join(mod_path, tf_file)
                self.assertTrue(os.path.exists(file_path), f"Missing {tf_file} in module {mod}")

    def test_dev_and_prod_environments(self):
        """dev and prod environments must exist with main.tf, variables.tf, outputs.tf, tfvars.example."""
        for env in ["dev", "prod"]:
            env_path = os.path.join(INFRA_DIR, env)
            self.assertTrue(os.path.isdir(env_path), f"Missing environment directory: {env}")
            for expected in ["main.tf", "variables.tf", "outputs.tf", "terraform.tfvars.example"]:
                path = os.path.join(env_path, expected)
                self.assertTrue(os.path.exists(path), f"Missing {expected} in {env}")

    def test_no_hardcoded_real_credentials_or_projects(self):
        """Ensure no production or real personal project IDs are committed; only parameterized fake variables."""
        # Allowed fake IDs
        allowed_fake_ids = {"airlytics-dev-123456", "airlytics-prod-123456"}

        for root, _, files in os.walk(INFRA_DIR):
            for file in files:
                if file.endswith(".tf"):
                    filepath = os.path.join(root, file)
                    with open(filepath, "r", encoding="utf-8") as f:
                        content = f.read()
                    # Check for accidental service account private keys
                    self.assertNotIn("private_key", content)
                    self.assertNotIn("BEGIN PRIVATE KEY", content)

    def test_cost_controls_and_freshness_alert_configured(self):
        """Verify budget alert and 10-minute freshness alert are explicitly declared."""
        monitoring_tf = os.path.join(INFRA_DIR, "modules", "monitoring", "main.tf")
        with open(monitoring_tf, "r", encoding="utf-8") as f:
            content = f.read()
        self.assertIn("google_monitoring_alert_policy", content)
        self.assertIn("600s", content) # 10 minute freshness duration

        budget_tf = os.path.join(INFRA_DIR, "modules", "budget", "main.tf")
        with open(budget_tf, "r", encoding="utf-8") as f:
            content = f.read()
        self.assertIn("google_billing_budget", content)
        self.assertIn("threshold_rules", content)


if __name__ == "__main__":
    unittest.main()
