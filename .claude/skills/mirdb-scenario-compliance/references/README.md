# MirDB Scenario Compliance

## Overview

End-to-end checklist for shipping a MirDB homepage scenario via the `/ralph-loop` flow. Each scenario in `.something/scenario.json` declares a small slice of REQ/US coverage that must compile, pass tests, push to the feature branch, produce a preview video, and document its line anchors in three `IMPLEMENTATION_*.json` artifacts before the completion promise can be emitted.

## When to Use This Skill

Use this skill when users request:

- Starting a `/ralph-loop` iteration on a `.something/scenario.json` task
- Verifying a scenario is ready to mark `status: "pass"`
- Investigating why a previous scenario completion failed (anchor drift, vitest config collision, missing preview, push not verified)

## Core Capabilities

### 1. Scaffold respect

Read `.something/scaffold.md` first. Only edit files the scenario owns; treat shared resources (`src/utils/constants.ts`, `src/types/homepage.ts`, etc.) as read-only unless they appear in the scenario's "First Builder" column. Touching a sibling scenario's files is the most common way to break the loop.

### 2. Required artifacts per scenario

All four must exist and be valid before the completion promise:

- `.something/scenario.json` — updated with `status: "pass"`, every `test_cases[i].status: "pass"`, and populated `test_code` + `implementation` arrays whose `start_line` / `end_line` / `start_line_anchor` / `end_line_anchor` match the on-disk source exactly.
- `.something/IMPLEMENTATION_REASONS.json` — array of `{title, content, path, line_range, start_line_anchor, end_line_anchor}` explaining design decisions.
- `.something/IMPLEMENTATION_REFERENCES.json` — array of the same shape with either a `url` (external docs) or a `knowledge_uuid` (internal knowledge node).
- `.something/IMPLEMENTATION_SUMMARY.json` — `{one_liner, description, key_changes[], design_notes, related_knowledge[]}` summarising the change for downstream tooling.

### 3. Anchor validation

Every `start_line_anchor` / `end_line_anchor` in the JSON artifacts must match the literal substring of the file at the declared `start_line` / `end_line` after the implementation is committed. A short Python script that re-reads each file and compares anchors catches off-by-one drift caused by import reorderings:

```python
import json, pathlib
for art in ['scenario.json', 'IMPLEMENTATION_REASONS.json', 'IMPLEMENTATION_REFERENCES.json']:
    data = json.loads(pathlib.Path(f'.something/{art}').read_text())
    # walk every {path, line_range, start_line_anchor, end_line_anchor} and verify
```

Run this validator after every code edit and after every JSON edit. Do not push until it prints "ALL OK".

### 4. Vitest from the right directory

Always `cd /workspace/frontend && npx vitest run`. The workspace-root has its own Vitest 4.x install without jsdom; running vitest from `/workspace` will fail every DOM test with `document is not defined`. See `mirdb-frontend-rtl-testing` for the full rationale.

### 5. Commit / push / verify

Use a conventional-commit message scoped to the feature area (`feat(homepage): …`). After pushing, verify with:

```bash
git log origin/master..origin/feature/<branch> --oneline
```

The commit hash must appear before declaring the scenario shipped — "pushed locally" without verification has caused silent failures.

### 6. preview.webm

Record `.something/preview.webm` with the `playwright-recording` skill. Use `--wait-strategy networkidle` for the start, batch interactions into one `action` call so the video stays in a single segment, and target an `activeDuration` of 15-30s. The file must exist before the completion promise.

### 7. Completion promise

Only after vitest is green, push is verified, all four JSON artifacts validate, and `preview.webm` exists may the loop emit:

```
<promise>SCENARIO_COMPLETE</promise>
```

Emitting the promise prematurely causes the harness to mark a half-finished scenario as done, which then blocks downstream scenarios that depend on it.

## Best Practices

- Read `.something/scaffold.md`, `.something/prd.md`, and the scenario JSON before touching any source — the scenario JSON defines the contract; the PRD defines the why; the scaffold defines what you may edit.
- When a test demands a phrase ("URL shortening", "analytics", "tracking"), embed it as a literal element in your component rather than relying on a value derived from `src/utils/constants.ts`. Shared constants belong to a different scenario and may legitimately change later.
- Set `document.title` from a `useEffect` in the page component as a head-manager fallback when the SEO scenario is not yet wired — once Helmet ships it will re-assert the same value.
- Avoid `role="banner"` on non-`<header>` regions; the navbar already owns that landmark. Use `data-testid` plus `aria-labelledby` for hero-style sections.
- Keep the `MemoryRouter` wrapper in a `renderX()` helper at the top of each spec to keep boilerplate consistent across scenarios.

## Resources

### references/

- `README.md` - This documentation
