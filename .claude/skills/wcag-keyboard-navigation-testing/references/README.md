# WCAG Keyboard Navigation Testing

## Overview

This skill captures the Playwright-driven keyboard reachability test used in `homepage/tests/e2e/accessibility.spec.ts` (Test Case 2). Instead of asserting a brittle, layout-specific tab order, we drive `page.keyboard.press('Tab')` up to N times, record `document.activeElement` at each step, and assert reachability + uniqueness invariants.

## When to Use This Skill

Use this skill when:

- A scenario requires "all interactive elements (links, buttons, copy buttons, theme toggle, mobile menu) are focusable and operable via keyboard"
- The interactive set varies across viewports (e.g., Header hides hamburger on desktop) so a hard-coded order would be fragile
- You need to detect both unfocusable elements (`tabindex="-1"`, `disabled`) AND keyboard traps (loops on a single element)

## Core Capabilities

### 1. Reset focus and scroll position

Before tabbing, blur the active element and scroll to the top so Tab starts from a deterministic state:

```ts
await page.evaluate(() => {
  (document.activeElement as HTMLElement)?.blur();
  window.scrollTo(0, 0);
});
```

### 2. Drive Tab and capture activeElement metadata

```ts
const focusedElements: string[] = [];
const maxTabs = 60;

for (let i = 0; i < maxTabs; i++) {
  await page.keyboard.press('Tab');
  const info = await page.evaluate(() => {
    const active = document.activeElement as HTMLElement | null;
    if (!active || active === document.body) return null;
    return {
      tag: active.tagName.toLowerCase(),
      text:
        active.getAttribute('aria-label') ||
        active.textContent?.trim().slice(0, 40) ||
        '',
      testid: active.getAttribute('data-testid') || '',
    };
  });
  if (info) focusedElements.push(`${info.tag}:${info.testid || info.text}`);
}
```

### 3. Three-pronged assertion

```ts
expect(focusedElements.length).toBeGreaterThanOrEqual(5);   // reachability
const uniqueFocused = new Set(focusedElements);
expect(uniqueFocused.size).toBeGreaterThanOrEqual(4);       // not stuck on one element
const focusedTags = focusedElements.map((s) => s.split(':')[0]);
expect(focusedTags).toContain('a');                          // links are reachable
```

These three assertions together catch the most common failure modes — invisible-but-focusable elements, focus traps, and "everything is a button" routing layouts.

## Best Practices

- Precompute an `interactiveCount` from `header a, header button, main a, main button, footer a` and assert `> 0` before tabbing — it surfaces "the page rendered nothing" failures separately from "tabbing failed".
- Filter to **visible** elements when counting interactive controls (`display !== 'none'`, `offsetParent !== null`). Otherwise hidden mobile-menu items inflate the count on desktop.
- 60 tabs is a conservative upper bound for the homepage. If you have a long page with many footer links, scale this up rather than down.
- Do not assert exact tab order — that is a contract you will regret. Assert invariants (reachable, unique, contains-an-anchor) instead.

## Reference Implementation

`homepage/tests/e2e/accessibility.spec.ts` lines 48–112 (Test Case 2).

## Resources

### references/

- `README.md` — This documentation
