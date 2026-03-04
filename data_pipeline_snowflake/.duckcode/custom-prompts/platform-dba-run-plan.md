---
duckcode_run_plan:
  # Execution mode: 'generate-only' creates files only; 'generate+run' adds dry-run/what-if and optional apply steps
  mode: generate-only # or generate+run

  # Target: one of 'postgres_maintenance' | 'aws_s3' | 'azure_storage' | 'gcp_gcs' | 'other'
  target: postgres_maintenance

  # Environment and region
  env: dev
  region: us-east-1

  # Repository placement
  repo_root: "."                     # relative to workspace root
  subfolder: "db/postgres/maintenance" # recommended default for Postgres maintenance

  # Naming & tags (optional)
  name_prefix: "dc_"
  tags:
    env: dev
    owner: platform

  # Action toggles (used only when mode=generate+run)
  actions:
    create_scaffold: true
    dry_run: true
    apply: false
---
