# vitest-vite-build-payload-assertion

Self-contained payload-size assertions for a Vite production build, run from inside a vitest suite.

## Problem

Performance budgets (NFR-style "page size under X bytes") are usually enforced in CI by a separate step that runs `vite build` and inspects `dist/`. That makes the budget invisible to developers running `vitest` locally, and easy to break in PRs because the size regression only surfaces in CI.

This skill captures the alternative: keep the budget assertion next to the rest of the test suite so it is part of every `vitest run` and a fresh checkout can verify the budget without manual setup.

## Pattern

```tsx
import { describe, it, expect, beforeAll } from 'vitest';
import { execSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';

const PROJECT_ROOT = path.resolve(__dirname, '..', '..');
const DIST_DIR = path.join(PROJECT_ROOT, 'dist');
const HTML_CSS_BUDGET_BYTES = 500 * 1024;

function collectFiles(dir: string, predicate: (name: string) => boolean): string[] {
  if (!fs.existsSync(dir)) return [];
  const out: string[] = [];
  const stack = [dir];
  while (stack.length > 0) {
    const current = stack.pop()!;
    for (const entry of fs.readdirSync(current, { withFileTypes: true })) {
      const full = path.join(current, entry.name);
      if (entry.isDirectory()) stack.push(full);
      else if (predicate(entry.name)) out.push(full);
    }
  }
  return out;
}

function sumFileSizes(files: string[]): number {
  return files.reduce((total, file) => total + fs.statSync(file).size, 0);
}

describe('Production build payload', () => {
  beforeAll(() => {
    if (process.env.SKIP_BUILD === '1') return;
    if (fs.existsSync(path.join(DIST_DIR, 'index.html'))) return;
    execSync('npx vite build', {
      cwd: PROJECT_ROOT,
      stdio: 'pipe',
      env: { ...process.env, NODE_ENV: 'production' },
    });
  }, 180_000);

  it('HTML + CSS payload stays under the page-size budget', () => {
    const htmlSize = fs.statSync(path.join(DIST_DIR, 'index.html')).size;
    const cssSize = sumFileSizes(collectFiles(DIST_DIR, (n) => n.endsWith('.css')));
    expect(htmlSize + cssSize).toBeLessThan(HTML_CSS_BUDGET_BYTES);
  });
});
```

## Design notes

- **Lazy build in `beforeAll`.** The first run on a fresh checkout pays the build cost; subsequent runs short-circuit because `dist/index.html` already exists. Watch-mode iterations stay fast.
- **`NODE_ENV=production` forced.** Without this, Tailwind/DaisyUI CSS purging and other production-only optimisations may not run, inflating the measured size and misrepresenting the actual budget.
- **`SKIP_BUILD=1` escape hatch.** Pipelines that already ran `vite build` upstream can pass `SKIP_BUILD=1 vitest run` to avoid duplicating the work.
- **`stdio: 'pipe'`** keeps Vite's stdout out of the test reporter so success runs stay clean. On failure, surface the error via `console.warn` so the test reporter still gives a useful clue.
- **Walk `dist/` recursively.** Vite may place hashed CSS under nested `assets/` subdirectories; `collectFiles` accounts for arbitrary nesting.
- **Separate HTML+CSS from JS.** Most budgets specify "excluding scripts"; sum HTML and CSS for the headline assertion and document JS separately so JS-only regressions cannot silently pass the headline check.

## When to use

- Vite + React (or Vite + Preact / Vite + Vue / Vite + Svelte) projects.
- vitest as the runner.
- Performance budgets defined in PRDs / NFR docs.
- Per-route or per-page budgets where the project only ships a small number of HTML entry points.
