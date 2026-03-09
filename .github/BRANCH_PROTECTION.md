# Branch Protection Setup

Apply these rules in GitHub: **Settings > Branches > Add branch protection rule**

**Branch name pattern:** `main`

## Required settings:
- [x] **Require a pull request before merging**
  - [x] Require approvals: 1
  - [x] Dismiss stale pull request approvals when new commits are pushed
- [x] **Require status checks to pass before merging**
  - Required checks:
    - `Lint`
    - `Test (Python 3.10)`
    - `Test (Python 3.11)`
    - `Test (Python 3.12)`
    - `Security scan`
  - [x] Require branches to be up to date before merging
- [x] **Require conversation resolution before merging**
- [x] **Do not allow bypassing the above settings**

## Optional (recommended):
- [x] **Require signed commits** (if team uses GPG)
- [x] **Include administrators** (maintainer follows same rules)
- [ ] Restrict who can push (only needed for large teams)

## Why each check matters:
| Check | What it catches |
|---|---|
| Lint | Code style, unused imports, security anti-patterns |
| Test (3 Python versions) | Regressions, Python version incompatibilities |
| Security scan | Known vulnerabilities in dependencies |
| PR approval | Human review before merge |
| Conversation resolution | Unaddressed review comments |
