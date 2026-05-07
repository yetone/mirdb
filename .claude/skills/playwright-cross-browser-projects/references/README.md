# Playwright Cross-Browser Projects

How the MirDB homepage runs cross-browser e2e tests on Chrome, Firefox, Safari (WebKit), and Edge while keeping the rest of the e2e suite scoped to chromium for CI cost.

## When to use

- Adding a new e2e spec that must validate engine-specific behavior (CSS, layout, font fallbacks).
- Verifying that cross-browser regressions show up in CI without quadrupling the runtime of unrelated specs.

## The pattern

In `homepage/playwright.config.ts`:

```ts
projects: [
  {
    name: 'chromium',
    use: { ...devices['Desktop Chrome'] },
  },
  {
    name: 'firefox',
    testMatch: /cross-browser\.spec\.ts/,
    use: { ...devices['Desktop Firefox'] },
  },
  {
    name: 'webkit',
    testMatch: /cross-browser\.spec\.ts/,
    use: { ...devices['Desktop Safari'] },
  },
  {
    name: 'edge',
    testMatch: /cross-browser\.spec\.ts/,
    use: { ...devices['Desktop Edge'] },
  },
]
```

Key points:

1. **`chromium` has no `testMatch`** so it runs every spec under `tests/e2e/` (the default). All non-cross-browser tests still execute.
2. **Firefox / WebKit / Edge each set `testMatch: /cross-browser\.spec\.ts/`** which restricts that project to a single file.
3. **Edge uses `Desktop Edge`** rather than a separate channel install. Modern Edge is Chromium-based and Playwright's Desktop Edge device exercises that engine through the Edge channel — matches reality, keeps CI hermetic.
4. **Run all four engines on the cross-browser file**: `npx playwright test tests/e2e/cross-browser.spec.ts` runs the spec on each project (~72 tests for the suite documented here).
5. **Run a single engine for debugging**: `npx playwright test --project=webkit tests/e2e/cross-browser.spec.ts`.

## Browser install (for fresh hosts)

Firefox and WebKit need system libs. The flow that worked on this Ubuntu 22.04 host:

```bash
npx playwright install chromium                          # ok without sudo
sudo -n npx playwright install-deps chromium             # apt deps
sudo -n npx playwright install --with-deps firefox webkit
```

The `--with-deps` form of `install` runs `apt-get install` for the required X/GTK libraries before downloading the browser binaries. Without sudo, Firefox/WebKit downloads still succeed but launching fails with the host validation banner.

## Test patterns that worked across all four engines

In the cross-browser spec, every assertion includes `${browserName}` in the message so a CI failure tells you which engine regressed:

```ts
expect(consoleErrors, `${browserName}: page should load without console errors`).toEqual([]);
```

For features with engine-specific prefixes (e.g. `backdrop-filter`), accept either the standard property OR a documented graceful fallback (background-color), so the test stays green on engines that lawfully lack the property without weakening the contract on engines that have it.

## Reference: this scenario

- `homepage/playwright.config.ts` lines 20–40
- `homepage/tests/e2e/cross-browser.spec.ts` (18 tests, all engines)
- All 4 projects pass: chromium 18/18, firefox 18/18, webkit 18/18, edge 18/18.
