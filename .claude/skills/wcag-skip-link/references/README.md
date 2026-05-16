# wcag-skip-link

## What this skill does

Implements the WCAG 2.4.1 Bypass Blocks pattern — a keyboard-accessible "Skip to main content" link that lets users bypass repeated navigation. Visually hidden by default, slides into view on `:focus`, and moves focus to the page's main landmark when activated.

Use this skill when:

- A page has repeated navigation that keyboard users would otherwise have to tab through on every visit
- You're shipping a WCAG 2.1 AA-compliant page (NFR-1 of the homepage scenario)
- You want a single, low-cost pattern that satisfies both SC 2.4.1 (Bypass Blocks) and supports SC 2.4.7 (Focus Visible)

## The pattern — three pieces

### 1. HTML — first child of `<body>`

```html
<body>
    <a class="skip-link" href="#main">Skip to main content</a>
    <header>...</header>
    <main id="main" tabindex="-1">
        ...
    </main>
    <footer>...</footer>
</body>
```

Critical details:

- The `<a>` is the **first** child of `<body>` — earlier than `<header>`. The first Tab keypress must land on it.
- `<main>` carries `id="main"` (matches the `href`) **and** `tabindex="-1"`. Without `tabindex="-1"`, browsers ignore the hash-anchor focus move and the link does nothing for keyboard users.

### 2. CSS — visually hidden but in focus order

```css
.skip-link {
    position: absolute;
    top: 0; left: 0;
    padding: 0.75rem 1rem;
    background-color: #1a1a1a;
    color: #ffffff;
    font-weight: 600;
    text-decoration: none;
    border-radius: 0 0 4px 0;
    z-index: 1000;
    /* Off-screen but focusable. display:none / visibility:hidden would
       REMOVE the element from the tab order — defeating the point. */
    transform: translateY(-100%);
    transition: transform 150ms ease;
}

.skip-link:focus,
.skip-link:focus-visible {
    transform: translateY(0);
    outline: 3px solid #f59e0b;
    outline-offset: 2px;
}
```

### 3. No JS required

Browsers handle hash-anchor focus natively once `tabindex="-1"` is on the target. JSDOM doesn't, but production browsers do.

## Why NOT `display: none` or `visibility: hidden`

Both remove the element from the focus order entirely. The user's first Tab would skip past the skip-link and land on the first nav item — the exact behaviour the skip-link is meant to prevent.

`transform: translateY(-100%)` (or `top: -9999px` with `position: absolute`) keeps the element in the focus order while pulling it off-screen visually.

## Why `tabindex="-1"` on `<main>`

Without it, the hash-anchor focus move fails silently in browsers. Setting `tabindex="-1"` makes `<main>` *programmatically* focusable (it still doesn't appear in the normal tab cycle, since the value is negative). When the browser sees `location.hash = "#main"` and `<main id="main" tabindex="-1">`, it moves focus to it.

## Verification under jsdom

JSDOM doesn't implement the hash-anchor focus move, so the test simulates it explicitly:

```js
const skip = document.querySelector('.skip-link');
const main = document.getElementById('main');

skip.focus();
expect(document.activeElement).toBe(skip);

skip.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', bubbles: true }));
if (main.hasAttribute('tabindex')) main.focus(); // mimic native browser behaviour
expect(document.activeElement).toBe(main);
```

## Verification under Playwright (real browser)

```js
await page.keyboard.press('Tab');                // focus the skip-link
const focused = await page.evaluate(() => document.activeElement.className);
expect(focused).toContain('skip-link');

await page.keyboard.press('Enter');              // activate
const newFocused = await page.evaluate(() => document.activeElement.id);
expect(newFocused).toBe('main');
```

## Dark-theme contrast

If the page supports a dark theme, override the skip-link's outline colour so it stays perceivable on the dark background:

```css
:root[data-theme="dark"] .skip-link:focus,
:root[data-theme="dark"] .skip-link:focus-visible {
    outline-color: #fbbf24;
}
```

## Related WCAG criteria

- **2.4.1 Bypass Blocks** (Level A) — the primary criterion the skip-link addresses
- **2.4.3 Focus Order** (Level A) — being first in tab order is what makes the link reachable
- **2.4.7 Focus Visible** (Level AA) — the outline on `:focus` satisfies this for the link itself
- **1.4.11 Non-text Contrast** (Level AA) — the outline must have ≥3:1 contrast against the page background (drives the per-theme colour)
