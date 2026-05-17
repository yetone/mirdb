# Homepage Accessibility Test Patterns

## Overview

Patterns the MirDB homepage uses for WCAG 2.1 AA integration testing under Vitest + JSDOM. These patterns work around several gotchas: Testing Library's `getByRole('banner')` over-matching, JSDOM's missing computed styles, and the need to traverse heading levels without skipping.

## When to Use This Skill

Use this skill when:

- Adding accessibility integration tests for a new page in `frontend/tests/accessibility/`
- Diagnosing flaky banner / landmark assertions on nested `<header>` elements
- Auditing heading hierarchy or icon-only accessible names
- Writing keyboard navigation tests that need realistic Tab dispatch

## Core Capabilities

### 1. Heading hierarchy without level skips

Walk every heading in DOM order and assert that no level is more than one deeper than its predecessor.

```ts
const headings = Array.from(document.querySelectorAll('h1, h2, h3, h4, h5, h6'));
const levels = headings.map((h) => Number(h.tagName.slice(1)));
expect(levels[0]).toBe(1);
for (let i = 1; i < levels.length; i += 1) {
  expect(levels[i]).toBeLessThanOrEqual(levels[i - 1] + 1);
}
```

### 2. Banner landmark via ancestor walk (NOT getByRole)

Testing Library's `getByRole('banner')` matches every `<header>` regardless of nesting context, but per the HTML AAM spec a `<header>` inside `<main>`, `<section>`, `<article>`, `<aside>`, or `<nav>` does NOT expose the banner role. Resolve it manually:

```ts
const headers = Array.from(document.querySelectorAll('header'));
const topLevel = headers.filter(
  (h) => !h.closest('main, section, article, aside, nav')
);
expect(topLevel.length).toBe(1);
```

### 3. Tab from `document.body`

To prove the skip link is the first focusable element, focus `document.body` first, then dispatch a real Tab via `userEvent.tab()`:

```ts
const user = userEvent.setup();
renderHome();
document.body.focus();
expect(document.activeElement === document.body).toBe(true);

await user.tab();
const skip = screen.getByTestId('skip-to-content');
expect(document.activeElement).toBe(skip);
```

`userEvent.tab()` respects `tabindex` and disabled state in the way a browser would, unlike a raw `fireEvent.keyDown`.

### 4. Icon-only buttons need an accessible name

Every button with no visible text must carry `aria-label`, `aria-labelledby`, or `title`. Collect violators rather than asserting on a single one for better failures:

```ts
const buttons = Array.from(document.querySelectorAll('button, [role="button"]'));
const offenders = buttons.filter((btn) => {
  const visibleText = (btn.textContent ?? '').trim();
  if (visibleText.length > 0) return false;
  return !(
    btn.getAttribute('aria-label') ||
    btn.getAttribute('aria-labelledby') ||
    btn.getAttribute('title')
  );
});
expect(offenders.map((b) => b.outerHTML)).toEqual([]);
```

### 5. Image alt attribute audit

```ts
const imgs = Array.from(document.querySelectorAll('img'));
const missing = imgs.filter((img) => !img.hasAttribute('alt'));
expect(missing.map((img) => img.outerHTML)).toEqual([]);
```

Decorative SVGs that are NOT `<img>` should expose `aria-hidden="true"`:

```ts
const icons = document.querySelectorAll('[data-testid^="feature-card-icon-"]');
icons.forEach((icon) => expect(icon).toHaveAttribute('aria-hidden', 'true'));
```

### 6. Dark-theme audit cleanup

When testing dark mode, set the theme on the html element and reset in `afterEach`:

```ts
beforeEach + test: document.documentElement.setAttribute('data-theme', 'dark');
afterEach(() => document.documentElement.removeAttribute('data-theme'));
```

## Best Practices

- Always reset `data-theme` in `afterEach` so tests cannot leak theme state between cases.
- Render with `MemoryRouter` because `Home` uses React Router primitives.
- Use `cleanup()` from Testing Library in `afterEach` to drop the rendered tree before resetting attributes.
- Filter axe results to `serious`/`critical` impact only; see the `jest-axe-vitest` skill for details.
- When a heading or landmark assertion fails, dump `document.body.innerHTML` in the failure message to make debugging fast.

## Resources

### references/

- `README.md` - This documentation

## Source examples in this repo

- `frontend/tests/accessibility/homepage.a11y.test.tsx` — full pattern catalogue
- `frontend/tests/setup.ts` — shared setup for these tests
