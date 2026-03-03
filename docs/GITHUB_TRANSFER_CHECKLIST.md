# GitHub Transfer Checklist (cehbrecht → ESGF)

This runbook is tailored for moving this repository from the personal organization/user (`cehbrecht`) to the ESGF organization.

Use it as a short cutover procedure for migration day.

---

## 1) Pre-Transfer (Do this before the move)

### Access and ownership

- [ ] Confirm at least one ESGF org owner is available during transfer.
- [ ] Confirm the target repo name is free in ESGF (`ESGF/piddiplatsch`).
- [ ] Prepare admin team mapping in ESGF (for maintainers and CODEOWNERS updates).

### Freeze window

- [ ] Announce a short freeze window (no merges, no releases, no force-pushes).
- [ ] Ensure no release/tag pipeline is currently running.

### Capture current repository settings (for fallback/recreation)

- [ ] Branch protection rules (especially `main`).
- [ ] Repository variables and secrets names (do not export secret values).
- [ ] Environment-level settings/secrets (if used).
- [ ] Webhooks and delivery endpoints.
- [ ] Deploy keys and app installations.

### CI/CD compatibility checks for target org (ESGF)

- [ ] Verify ESGF Actions policy allows all actions used by this repo:
  - `actions/checkout@v4`
  - `actions/setup-python@v5`
- [ ] Verify runner availability for `ubuntu-latest`.
- [ ] Verify whether any workflows require org-level secrets in ESGF.

### References that should be updated after transfer

Current owner-specific references detected in this repo:

- README badge/action link: `https://github.com/cehbrecht/piddiplatsch/...`
- README clone example: `git clone git@github.com:cehbrecht/piddiplatsch.git`

---

## 2) Transfer Execution (Migration day)

1. Pause merges/releases for the freeze window.
2. In GitHub settings, transfer `cehbrecht/piddiplatsch` to `ESGF/piddiplatsch`.
3. Confirm transfer completion and visibility in target org.

GitHub will preserve redirects from old URLs, but integrations and badges should still be updated explicitly.

---

## 3) Immediate Post-Transfer Validation (first 30 minutes)

### Repository health

- [ ] Confirm default branch is still `main`.
- [ ] Confirm tags/releases are present.
- [ ] Confirm issues/PRs/discussions are intact.

### Permissions and protections

- [ ] Recheck branch protections and required status checks.
- [ ] Verify maintainer team permissions in ESGF.
- [ ] Update CODEOWNERS teams if org/team names changed.

### CI checks

- [ ] Trigger workflow manually (`workflow_dispatch`) and verify success.
- [ ] Open a small PR and verify PR workflow triggers correctly.
- [ ] Confirm required checks match branch protection rules.

### Integrations

- [ ] Revalidate webhooks (delivery status = success).
- [ ] Reconnect/reapprove GitHub Apps if required by org policy.
- [ ] Recreate missing secrets/variables in ESGF repo/org scope.

---

## 4) Follow-up edits in this repository

After transfer, update owner-specific links and examples:

- [ ] README build badge and Actions URL → `ESGF/piddiplatsch`
- [ ] README clone command to new owner
- [ ] Any docs/scripts mentioning `cehbrecht/piddiplatsch`

Suggested quick local check:

```bash
grep -RInE "cehbrecht/piddiplatsch|github.com/cehbrecht" .
```

---

## 5) Communication template

Suggested short announcement:

> The `piddiplatsch` repository has moved from `cehbrecht` to `ESGF`.
> New canonical URL: `https://github.com/ESGF/piddiplatsch`.
> Existing GitHub links should redirect automatically, but please update local remotes and automation configs.

For contributors with existing clones:

```bash
git remote set-url origin git@github.com:ESGF/piddiplatsch.git
git remote -v
```
