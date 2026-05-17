# mirdb-homepage-perf-budget

Patterns for the MirDB homepage performance and bundle-size regression suite at `frontend/tests/performance/homepage.perf.test.tsx`.

## When to use

Use this skill when:

- Adding a new performance regression check for the MirDB homepage.
- Reviewing or updating tests under `frontend/tests/performance/`.
- Investigating a failure of an NFR-1 (load time) or NFR-3 (page size) assertion.
- Validating that the public homepage stays free of authenticated API calls (`/api/users/me`, `/api/urls`) and render-blocking external scripts.

## Coverage

The suite covers five test cases from scenario 9 plus one secondary JS-budget assertion:

1. **JSDOM render-time regression guard.** Measures `performance.now()` before and after `render(<MemoryRouter><Home /></MemoryRouter>)` and asserts the delta is under 100 ms. JSDOM cannot reproduce the real 2-second 3G budget, so this is intentionally framed as a regression guard.
2. **Sibling-isolation re-render guard.** Renders `<UnrelatedCounter />` and `<CountingFeatures />` as siblings under a stateless wrapper. Clicking the counter must not increment the features render count because the counter owns its own state, leaving its sibling subtree untouched.
3. **Production HTML+CSS payload assertion.** `beforeAll` runs `npx vite build` (lazily, idempotently). The assertion sums `dist/index.html` plus every emitted `.css` file under `dist/` and verifies the total is under `500 * 1024` bytes (NFR-3).
4. **JS bundle budget (secondary).** Documents and bounds the JS payload separately so a regression that bloats only the scripts cannot silently pass the HTML+CSS assertion.
5. **Negative-network check.** Swaps `globalThis.fetch` with a spy, renders `<Home />`, and asserts no spy call targets `/api/users/me` or `/api/urls` (handles string, URL, and Request inputs).
6. **Script-hygiene check.** Snapshots `document.head.querySelectorAll('script[src]').length` before render. After render, any newly added head `<script src>` must carry `async` or `defer` — anything render-blocking fails the test. Container-rendered scripts must be empty.

## Conventions

- **Folder ownership.** Scenario 9 owns only `frontend/tests/performance/homepage.perf.test.tsx`. No `src/` files are modified by this scenario. If a future scenario needs to memoise `FeaturesSection`, that work belongs in scenario 3.
- **Self-contained build.** The bundle assertion runs `npx vite build` in `beforeAll` (180s timeout, idempotent on existing `dist/`). The `SKIP_BUILD=1` env var lets CI skip the build when a prior pipeline step already produced `dist/`.
- **Budget constants.**
  - `PAGE_SIZE_BUDGET_BYTES = 500 * 1024` (NFR-3)
  - `RENDER_BUDGET_MS = 100` (JSDOM regression guard)
  - `JS_BUDGET_BYTES = 1024 * 1024` (secondary, generous)
- **Test the contract, not the implementation.** The re-render guard intentionally avoids asserting on `React.memo` or the React DevTools profiler so it stays valid regardless of memoisation strategy.

## Code shape

```tsx
const PROJECT_ROOT = path.resolve(__dirname, '..', '..');
const DIST_DIR = path.join(PROJECT_ROOT, 'dist');

const PAGE_SIZE_BUDGET_BYTES = 500 * 1024;
const RENDER_BUDGET_MS = 100;
const JS_BUDGET_BYTES = 1024 * 1024;

const renderHome = () =>
  render(
    <MemoryRouter initialEntries={['/']}>
      <Home />
    </MemoryRouter>,
  );

describe('Production build payload (NFR-3)', () => {
  beforeAll(() => {
    if (process.env.SKIP_BUILD === '1') return;
    if (fs.existsSync(path.join(DIST_DIR, 'index.html'))) return;
    execSync('npx vite build', {
      cwd: PROJECT_ROOT,
      stdio: 'pipe',
      env: { ...process.env, NODE_ENV: 'production' },
    });
  }, 180_000);

  it('serves an index.html plus CSS payload under the 500KB page-size budget', () => {
    const htmlSize = fs.statSync(path.join(DIST_DIR, 'index.html')).size;
    const cssSize = collectFiles(DIST_DIR, (n) => n.endsWith('.css'))
      .reduce((sum, f) => sum + fs.statSync(f).size, 0);
    expect(htmlSize + cssSize).toBeLessThan(PAGE_SIZE_BUDGET_BYTES);
  });
});
```

## Reference run

On the current homepage:

| Asset | Size |
| --- | --- |
| `dist/index.html` | ~415 B |
| `dist/assets/*.css` | ~40 KB |
| `dist/assets/*.js` | ~190 KB |
| HTML + CSS total | ~40.5 KB |
| Budget (NFR-3) | 500 KB |
