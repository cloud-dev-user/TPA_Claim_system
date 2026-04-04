# Runbook 01 — GitLab Module (8 Hours · Apr 24–27)

> **Goal by end of Apr 27:** Three GitLab repos (`claim-service`, `eligibility-service`, `fraud-service`), each with a `feature/claim-api` branch, a Merge Request workflow, and a CI pipeline running `pytest` + `flake8` on every push.

---

## Day 1 · Apr 24 · 2 Hours — Git Basics

### Theory Summary
- Git = distributed VCS; every developer has full history
- Working directory → Staging area → Local repo → Remote repo
- Commit = snapshot of staged changes, not a diff

### Lab Steps

#### 1. Configure Git identity
```bash
git config --global user.name  "Your Name"
git config --global user.email "you@healthonetpa.com"
git config --global core.editor "nano"   # or vim / code --wait

# Verify
git config --global --list
```

#### 2. Initialise the local repository
```bash
mkdir claims-project && cd claims-project
git init
ls -la   # inspect the .git directory
```

#### 3. Create the first file
Create `claim.py` (copy from `Training_code/claim-service/app/claim.py`) and add just the stub class first:

```python
# claim.py
class ClaimSubmit:
    """Stub — HealthOne TPA claim submission model"""
    pass
```

#### 4. First commit workflow
```bash
git status                          # untracked: claim.py
git add claim.py
git status                          # staged: claim.py
git commit -m "feat: add ClaimSubmit stub"
git log --oneline                   # see the commit hash
```

#### 5. Explore git diff
```bash
# Modify claim.py — add a docstring
git diff                            # unstaged changes
git add claim.py
git diff --staged                   # staged changes ready to commit
git commit -m "docs: add ClaimSubmit docstring"
```

### Deliverable
Local repo with 2 commits. Run `git log --oneline` and see both commits.

---

## Day 2 · Apr 25 · 4 Hours — GitLab + Branching

### Theory Summary
- Remote = named pointer to a URL (`origin`)
- Branch = lightweight pointer to a commit; `HEAD` = currently checked-out branch
- `git push -u origin main` = push AND set tracking

### Lab Steps

#### 1. Sign up to GitLab
1. Go to https://gitlab.com
2. Sign up with your work email
3. Create a **Group**: `healthone-tpa-claims`  
   - Visibility: Private

#### 2. Set up SSH key
```bash
# Generate SSH key (skip if you already have one)
ssh-keygen -t ed25519 -C "you@healthonetpa.com"
cat ~/.ssh/id_ed25519.pub   # copy this

# In GitLab: Profile → SSH Keys → Add new key → paste
# Test connection:
ssh -T git@gitlab.com
# Expected: Welcome to GitLab, @yourname!
```

#### 3. Create `claim-service` repository
In GitLab: `healthone-tpa-claims` group → New Project → `claim-service` → Private

```bash
cd claims-project
git remote add origin git@gitlab.com:healthone-tpa-claims/claim-service.git
git branch -M main
git push -u origin main

# Verify in GitLab UI: you should see the file and commit
```

#### 4. Create a feature branch
```bash
git checkout -b feature/claim-api
# or modern syntax:
git switch -c feature/claim-api

git branch    # lists all branches, * marks current
```

#### 5. Add `ClaimValidator` on the feature branch
Expand `claim.py` with the `ClaimValidator` class (from `Training_code/claim-service/app/claim.py`).

```bash
git add claim.py
git commit -m "feat: add ClaimValidator with HealthOne TPA business rules"
git push -u origin feature/claim-api
```

#### 6. Interactive staging (git add -p)
```bash
# Edit claim.py — make 2 unrelated changes in the same file
git add -p claim.py   # choose 'y' or 'n' for each hunk
git status            # see partially staged file
```

#### 7. Create remaining repositories
Repeat steps 3–5 for:
- `eligibility-service` → `EligibilityCheck` stub
- `fraud-service` → `FraudRule` stub

### Deliverable
Three repos in GitLab, each with `main` and `feature/claim-api` branches visible in the UI.

---

## Day 3 · Apr 27 · 2 Hours — CI Pipeline & Merge Requests

### Theory Summary
- `git revert` = safe undo (adds a new commit)
- `git reset` = rewrites history (dangerous on shared branches)
- Merge Request = code review + gate before merging to `main`
- `.gitlab-ci.yml` = pipeline definition; `stages` run in order

### Lab Steps

#### 1. Introduce a deliberate bug
In `claim.py`, change the `MAX_AMOUNT` threshold to a wrong value:
```python
MAX_AMOUNT: float = 100  # BUG: was 10_000_000
```

```bash
git add claim.py
git commit -m "fix: update claim amount validation"  # intentionally vague message
git push origin feature/claim-api
```

#### 2. Revert the bug
```bash
git log --oneline           # note the bad commit hash
git revert HEAD             # creates a new "revert" commit
git log --oneline           # you should see: "Revert 'fix: update...'"
git push origin feature/claim-api
```

#### 3. Raise a Merge Request
1. In GitLab: `claim-service` → Merge Requests → New MR
2. Source: `feature/claim-api` → Target: `main`
3. Title: `feat: add ClaimSubmit and ClaimValidator`
4. Description: explain what changed and why
5. Assign a reviewer (swap with a peer)
6. Click **Create MR**

Review the diff in the GitLab UI. Check the commit graph.

#### 4. Add the CI pipeline

Create `.gitlab-ci.yml` in `claim-service/`:

```yaml
image: python:3.12-slim

stages:
  - lint
  - test

before_script:
  - pip install --quiet -r requirements.txt

flake8:
  stage: lint
  script:
    - flake8 app/ tests/ --max-line-length=100 --extend-ignore=E203,W503

pytest:
  stage: test
  script:
    - pytest tests/ -v --tb=short --junitxml=report.xml
  artifacts:
    reports:
      junit: report.xml
```

> The full file is already in `Training_code/claim-service/.gitlab-ci.yml` — copy it in.

```bash
git add .gitlab-ci.yml requirements.txt tests/
git commit -m "ci: add flake8 lint and pytest pipeline"
git push origin feature/claim-api
```

Watch the pipeline run: GitLab UI → CI/CD → Pipelines.

#### 5. Make the pipeline fail, then fix it
```bash
# Add a syntax error to claim.py
echo "this is not python" >> app/claim.py
git add app/claim.py
git commit -m "test: trigger pipeline failure"
git push origin feature/claim-api
# Watch pipeline go RED

# Revert
git revert HEAD
git push origin feature/claim-api
# Watch pipeline go GREEN
```

#### 6. Merge the MR
Once pipeline is green, click **Merge** in GitLab. Observe the branch graph.

### Deliverable
- CI pipeline running on `claim-service` ✓
- Every push to `main` triggers tests ✓
- MR workflow established ✓

---

## Key Git Commands Reference

```bash
# Daily workflow
git status
git add <file>         # stage specific file
git add -p             # interactive staging (hunk by hunk)
git commit -m "msg"
git push origin <branch>
git pull origin main

# Branch operations
git branch             # list local branches
git checkout -b <name> # create + switch
git switch <name>      # switch (modern)
git merge <branch>     # merge into current branch

# Inspect
git log --oneline --graph --all  # full history with graph
git diff                          # unstaged changes
git diff --staged                 # staged changes

# Undo
git revert HEAD        # safe undo (new commit)
git restore <file>     # discard unstaged changes
git reset HEAD <file>  # unstage a file
```

---

## Module Checklist

- [ ] Git configured with name + email
- [ ] `claim-service`, `eligibility-service`, `fraud-service` repos created in GitLab group
- [ ] Each repo has `main` and `feature/claim-api` branches
- [ ] At least one Merge Request raised and merged
- [ ] CI pipeline (lint + test) running green on `claim-service`
- [ ] Know difference between `git revert` and `git reset`
