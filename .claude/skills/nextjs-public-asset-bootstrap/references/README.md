# Next.js Public Asset Bootstrap

## Overview

The MirDB project keeps shared image assets at `/workspace/assets/` (referenced by scaffold rule #6, "Asset References"), but the Next.js app at `homepage/` only serves files placed under `homepage/public/`. This skill captures the bootstrap step: copying the shared assets into the homepage public folder so the dev server, e2e tests, and production build all see the real images.

## When to Use This Skill

Use this skill when:

- An `<img src="/foo.gif">` 404s in the dev server but the file exists under `/workspace/assets/foo.gif`
- An e2e test inspects images and the page returns a broken image (no `naturalWidth`, axe `image-alt`/`image-redundant-alt` flapping)
- You are scaffolding a new homepage scenario and need to be sure the scenario's component can actually render its image
- Scaffold rule #6 (Asset References) is in play

## Core Capabilities

### 1. Identify which public/ assets the page references

Grep the homepage source for `src="/"` and `url(/...)`:

```bash
grep -rn 'src="/' homepage/src/
grep -rn 'url(/' homepage/src/
```

Each unique `/foo.gif` / `/bar.png` is a candidate that must exist under `homepage/public/`.

### 2. Copy from shared `/workspace/assets/`

```bash
cp /workspace/assets/logo.gif homepage/public/logo.gif
cp /workspace/assets/usage.gif homepage/public/usage.gif
```

This is intentionally a copy, not a symlink — the homepage Next.js build (especially the `output: 'export'` static path) needs concrete files under `public/`.

### 3. Verify the dev server serves them

```bash
curl -sI http://localhost:3000/usage.gif | head -1   # expect 200, not 404
```

If the dev server is already running, no restart is needed — Next.js serves `public/` files dynamically.

## Best Practices

- Treat `homepage/public/` as **bootstrap territory**, not a per-scenario folder. Copying an asset here is appropriate even if your scenario only owns `tests/`.
- Do not modify or rename the original under `/workspace/assets/`. Other scenarios reference it by name.
- After copying, commit both the copy and any test that depends on it in the same commit so reviewers see the dependency in one diff.

## Reference Implementation

`homepage/public/usage.gif`, `homepage/public/logo.gif` (copies of `/workspace/assets/usage.gif`, `/workspace/assets/logo.gif`).

## Resources

### references/

- `README.md` — This documentation
