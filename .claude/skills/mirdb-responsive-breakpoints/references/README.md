# MirDB Responsive Breakpoints

## Overview

The MirDB homepage uses a single cross-cutting stylesheet (`homepage/src/styles/responsive.css`) that owns all viewport-dependent overrides. Per-component CSS files (`navigation.css`, `features.css`, `layout.css`, etc.) define the *default desktop* presentation; `responsive.css` reflows them at the three target widths.

## Breakpoint Table

| Name    | Query                                       | Features grid     | Nav                | Container max-width |
|---------|---------------------------------------------|-------------------|--------------------|---------------------|
| Mobile  | `(max-width: 640px)`                        | 1 column          | Hamburger          | n/a (full width)    |
| Tablet  | `(min-width: 641px) and (max-width: 1024px)`| 2 columns         | Inline             | 960px               |
| Desktop | `(min-width: 1025px)`                       | 3 columns         | Inline             | 1080px              |
| Wide    | `(min-width: 1440px)`                       | 4 columns         | Inline             | 1080px (inherited)  |

The boundaries are intentionally chosen so the three primary queries are *non-overlapping*: at 640px the mobile rule applies; at 641px the tablet rule kicks in; at 1025px the desktop rule wins. No two breakpoints can be active simultaneously.

## When to Use This Skill

Use this skill when users request:

- "Make this section responsive on mobile."
- "Add a tablet layout for the new component."
- "Add another breakpoint."
- "Why does the features grid switch from 3 to 4 columns at 1440px?"

## Core Convention

1. **Author the default in the component's own CSS.** Treat the component's intrinsic styles as the desktop / "no media query" presentation.
2. **Add the override inside `responsive.css`.** Group it next to the existing rules for that breakpoint, never inside the per-component file.
3. **Pair `display: none` on `.nav-menu` with `.nav-menu.open { display: flex; flex-direction: column; }`** — the existing `navigation.js` toggles a `.open` class on click, so the override composes cleanly with the script.
4. **Use the spacing tokens (`var(--space-*)`)**, never raw pixels for padding/gap. The token scale is declared in `theme.css`.

## Test Coverage Expectations

When adding a new responsive rule:

- Add a static CSS assertion in `homepage/tests/integration/responsive.test.js` using the `extractMediaBlocks` / `extractRuleBody` helpers (see the `css-media-block-parser` skill).
- For nav/JS interactions, exercise the existing helper using `makeMatchMediaStub` (see `jsdom-matchmedia-stub` skill).

## Why these boundaries?

- **640 / 641** is one off from the typical 600/700 "small phone vs. small tablet" break. It matches `navigation.css`'s pre-existing mobile breakpoint (so the two stylesheets agree on when the hamburger appears).
- **1024 / 1025** is the standard "iPad portrait vs. small laptop" inflection point. Going to a 3-column layout at exactly 1025px keeps tablets at 2 columns where horizontal real estate is tighter.
- **1440** is the wide-desktop tier; at 4 columns each card is still wide enough to keep the icon + heading + paragraph readable.

## Files Touched by Scenario 8

- `homepage/src/styles/responsive.css` — the file itself.
- `homepage/public/index.html` — `<link>` reference to load the stylesheet.
- `homepage/tests/integration/responsive.test.js` — Jest suite asserting structure.

## Resources

### references/

- `README.md` — This documentation
