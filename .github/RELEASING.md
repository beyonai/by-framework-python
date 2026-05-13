# Releasing Packages

This repository uses a monorepo layout with multiple independently published Python packages.

## Packages

- `by-framework`
- `by-framework-history-postgres`
- `by-framework-history-byclaw`

## Release Model

Each package is released independently.

- The package version comes from that package's `pyproject.toml`
- A Git tag determines which package is published
- GitHub Actions verifies that the tag version matches the package version before publishing

## Tag Format

Use one of these tag patterns:

```bash
by-framework-v0.1.7
by-framework-history-postgres-v0.1.0
by-framework-history-byclaw-v0.1.0
```

## Release Steps

### 1. Update the package version

Edit the target package's `pyproject.toml`.

Examples:

- Root package: `pyproject.toml`

### 2. Run local verification

Recommended checks:

```bash
make test
```

Or run only the relevant package:

```bash
uv run pytest -c libs/by-framework-history-postgres/pyproject.toml libs/by-framework-history-postgres/tests
uv run pytest -c libs/by-framework-history-byclaw/pyproject.toml libs/by-framework-history-byclaw/tests
```

You can also verify build output locally:

```bash
cd libs/by-framework-history-postgres
uv build
```

### 3. Commit the version change

```bash
git add .
git commit -m "release: bump by-framework-history-postgres to 0.1.0"
```

### 4. Create and push the release tag

```bash
git tag by-framework-history-postgres-v0.1.0
git push origin by-framework-history-postgres-v0.1.0
```

### 5. Let GitHub Actions publish

The `publish.yml` workflow will:

- resolve the target package from the tag
- verify the version matches `pyproject.toml`
- build the package from the correct directory
- publish it to PyPI

## Trusted Publishing Setup

Each PyPI project should be configured to trust this GitHub repository and the `publish.yml` workflow.

For each package on PyPI:

1. Open the project on PyPI
2. Go to Publishing
3. Add a trusted publisher
4. Set:
   - Owner: your GitHub org or user
   - Repository: this repository
   - Workflow: `publish.yml`
   - Environment: `pypi`

Do this once per PyPI project:

- `by-framework`
- `by-framework-history-postgres`
- `by-framework-history-byclaw`

## Release Ordering

If an extension package depends on new functionality from `by-framework`, publish in this order:

1. `by-framework`
2. dependent extension packages

This avoids dependency resolution failures for users installing the new extension version.

## Notes

- Workspace-only settings such as `[tool.uv.sources]` are for local development only
- Published dependency metadata comes from `[project.dependencies]`
- Keep package names and versions aligned with the release tag to avoid workflow failure
