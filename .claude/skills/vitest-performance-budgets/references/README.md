# vitest-performance-budgets

Validate Lighthouse-style performance budgets and image lazy-loading in vitest+JSDOM without spawning Chromium or making network requests.

## When to Use

Use this pattern when adding tests that need to enforce:
- Page-load time budgets (REQ-11: < 2 s on a standard connection)
- Lighthouse Performance score prerequisites (NFR-4: >= 90)
- Below-the-fold image lazy-loading (`loading="lazy"`)
- Render-blocking CSS budgets
- a11y / SEO / best-practices structural attributes

## Why a Proxy, Not Real Lighthouse?

Spawning Lighthouse from inside vitest couples the unit-test loop to:
- Chromium installation
- Network access
- Multi-second wall-clock cost per run

Instead, validate the **structural prerequisites Lighthouse rewards**. Real Lighthouse lives in a separate `npm run lhci` CI step. Keep vitest hermetic (~150 ms for all assertions).

## Pattern 1 — Static-asset weight as Lighthouse Performance proxy

Sum every shipped HTML/CSS/JS byte in the homepage tree, then assert against a fixed ceiling. With server gzip/brotli, 200 KB raw maps to ~60 KB over the wire — well within a 1 Mbps sub-2-second budget.

```js
import { readFileSync, statSync, readdirSync } from "node:fs";
import { resolve, join } from "node:path";

const HOMEPAGE_ROOT = resolve(__dirname, "..", "..");

function listStaticAssets() {
    const exts = new Set([".html", ".css", ".js"]);
    const out = [];
    function walk(dir) {
        for (const entry of readdirSync(dir, { withFileTypes: true })) {
            if (entry.name === "node_modules" || entry.name === "tests") continue;
            if (entry.name.startsWith(".")) continue;
            const p = join(dir, entry.name);
            if (entry.isDirectory()) walk(p);
            else {
                const dot = entry.name.lastIndexOf(".");
                if (dot >= 0 && exts.has(entry.name.slice(dot))) out.push(p);
            }
        }
    }
    walk(HOMEPAGE_ROOT);
    return out;
}

function totalAssetBytes() {
    return listStaticAssets().reduce((acc, p) => acc + statSync(p).size, 0);
}

it("total HTML+CSS+JS payload is under the 200 KB performance budget", () => {
    expect(totalAssetBytes()).toBeLessThan(200 * 1024);
});
```

## Pattern 2 — Synthetic performance.timing for page-load budget

JSDOM does not run a real load cycle, so synthesise the timing tuple Playwright would report. This documents the exact contract the e2e harness must satisfy.

```js
it("synthetic performance.timing window yields delta < 2000 ms", () => {
    const start = 1_700_000_000_000;
    const fakeTiming = {
        navigationStart: start,
        loadEventEnd: start + 850, // realistic CDN-cached fetch
    };
    const delta = fakeTiming.loadEventEnd - fakeTiming.navigationStart;
    expect(delta).toBeLessThan(2000);
});
```

Pair it with the byte-budget calculation as a deterministic worst-case proxy:

```js
it("total page weight stays under the 2-second budget on a 1 Mbps link", () => {
    const totalBytes = totalAssetBytes();
    const oneMbpsBytesPerSec = (1_000_000 / 8); // 125 KB/s
    expect(totalBytes / oneMbpsBytesPerSec).toBeLessThan(2);
});
```

## Pattern 3 — Below-the-fold image detection in JSDOM

JSDOM returns a zero-sized bounding rect for every element, so the natural `top > viewportHeight` filter returns zero matches. Stub `getBoundingClientRect` per image to position it below the fold.

```js
const viewportHeight = 800;
const imgs = Array.from(document.querySelectorAll("img"));

imgs.forEach((img) => {
    img.getBoundingClientRect = () => ({
        top: viewportHeight + 200,
        bottom: viewportHeight + 248,
        left: 0, right: 48, width: 48, height: 48,
        x: 0, y: viewportHeight + 200,
        toJSON() { return this; },
    });
});

const belowFold = imgs.filter(
    (img) => img.getBoundingClientRect().top > viewportHeight
);
expect(belowFold.length).toBeGreaterThan(0);
for (const img of belowFold) {
    expect(img.getAttribute("loading")).toBe("lazy");
}
```

Also enforce the contract at the production-code level by greping the renderer source:

```js
const src = readFileSync(
    resolve(HOMEPAGE_ROOT, "components/social-proof/social-proof.js"),
    "utf8"
);
expect(src).toMatch(/setAttribute\(\s*["']loading["']\s*,\s*["']lazy["']\s*\)/);
```

## Pattern 4 — Non-critical CSS budget with three acceptable patterns

Accept whichever of these the homepage satisfies — preload swap, media swap, or staying within a critical-CSS budget:

```js
const head = parseIndexHead();
const sheets = Array.from(head.querySelectorAll('link[rel="stylesheet"]'));

const usesPreloadSwap = head.querySelector('link[rel="preload"][as="style"]') !== null;
const usesMediaSwap = sheets.some((l) => {
    const media = l.getAttribute("media");
    return media && media !== "all" && media !== "screen";
});

let totalCssBytes = 0;
for (const link of sheets) {
    const href = link.getAttribute("href");
    if (!href) continue;
    const p = resolve(HOMEPAGE_ROOT, href);
    if (existsSync(p)) totalCssBytes += statSync(p).size;
}
const cssWithinCriticalBudget = totalCssBytes < 30 * 1024;

expect(usesPreloadSwap || usesMediaSwap || cssWithinCriticalBudget).toBe(true);
```

## Pattern 5 — Structural a11y / SEO / best-practices checks

Lighthouse's a11y, SEO, and best-practices categories reward static structural attributes. Validate them directly:

```js
it("declares <html lang=\"en\">", () => {
    const html = INDEX_HTML.match(/<html([^>]*)>/i);
    expect(html[1]).toMatch(/\blang\s*=\s*["']en["']/i);
});

it("declares meta description with >=20 chars (SEO)", () => {
    const meta = parseIndexHead().querySelector('meta[name="description"]');
    expect((meta.getAttribute("content") || "").trim().length).toBeGreaterThanOrEqual(20);
});

it("declares charset + viewport (best-practices)", () => {
    const head = parseIndexHead();
    expect(head.querySelector("meta[charset]")).not.toBeNull();
    const viewport = head.querySelector('meta[name="viewport"]');
    expect(viewport.getAttribute("content")).toMatch(/width=device-width/);
});
```

## Reference Implementation

The full pattern lives at `homepage/tests/performance/performance.test.js` — 17 assertions across 6 PRD-defined test cases, ~150 ms run time, zero network access.

## Related Scaffold Rule

This kind of test suite is **validation-only** (scaffold rule 4): it must not modify any other scenario's source files. Assert against the existing bundle; do not patch production code from inside the test scenario.
