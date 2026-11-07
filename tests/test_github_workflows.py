"""Unit test verifying GitHub Actions CI/CD workflows and security compliance."""

import os
import unittest

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WORKFLOWS_DIR = os.path.join(ROOT_DIR, ".github", "workflows")


class TestGitHubWorkflows(unittest.TestCase):
    """Ensure compliance with WIF OIDC security, multi-environment gates, and CI checks."""

    def test_workflow_files_exist(self):
        """ci.yml and cd.yml must exist."""
        ci_path = os.path.join(WORKFLOWS_DIR, "ci.yml")
        cd_path = os.path.join(WORKFLOWS_DIR, "cd.yml")
        self.assertTrue(os.path.exists(ci_path))
        self.assertTrue(os.path.exists(cd_path))

    def test_strictly_zero_service_account_keys(self):
        """Must never contain static GCP service account keys or credentials_json."""
        for file in ["ci.yml", "cd.yml"]:
            path = os.path.join(WORKFLOWS_DIR, file)
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
            self.assertNotIn("credentials_json", content)
            self.assertNotIn("PRIVATE KEY", content)
            self.assertNotIn(".json", content.lower().replace("package.json", "").replace("flight-pipeline.json", "").replace("metadata.json", ""))

    def test_workload_identity_federation_configured(self):
        """CD pipeline must authenticate via Workload Identity Federation OIDC."""
        cd_path = os.path.join(WORKFLOWS_DIR, "cd.yml")
        with open(cd_path, "r", encoding="utf-8") as f:
            content = f.read()
        self.assertIn("google-github-actions/auth", content)
        self.assertIn("workload_identity_provider", content)
        self.assertIn("service_account", content)
        self.assertIn("id-token: write", content)

    def test_production_environment_approval_gate(self):
        """Production deployment must use the 'production' GitHub environment."""
        cd_path = os.path.join(WORKFLOWS_DIR, "cd.yml")
        with open(cd_path, "r", encoding="utf-8") as f:
            content = f.read()
        self.assertIn("environment: production", content)
        self.assertIn("needs: [deploy-dev]", content)


if __name__ == "__main__":
    unittest.main()
