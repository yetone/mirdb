# WCAG 2.4.1 Skip Link Pattern

## Overview

How MirDB implements the "Skip to main content" link required by WCAG 2.4.1 "Bypass Blocks". The link sits at the very top of the page in DOM order, is visually hidden via Tailwind's `sr-only` utility until focused, and programmatically moves keyboard focus into the page `<main>` when activated.

## When to Use This Skill

Use this skill when:

- Adding accessibility to any new full-page route (e.g., dashboard, settings) and a skip link is needed
- Auditing an existing page that fails WCAG 2.4.1 Bypass Blocks
- Investigating why focus is not landing on `<main>` after the skip link is activated
- Recreating the sr-only / focus reveal pattern in a new component

## Core Capabilities

### 1. Anchor element with a fragment target

The component renders an `<a href="#main-content">` and the page must have a `<main id="main-content" tabIndex={-1}>`. The anchor element is keyboard-focusable for free, is announced as a link by assistive technology, and activates with Enter. A `<button>` would not function as a fragment target.

```tsx
// SkipToContent.tsx
<a
  href={`#${targetId}`}
  onClick={handleClick}
  data-testid="skip-to-content"
  className="skip-to-content sr-only focus:not-sr-only focus-visible:not-sr-only absolute left-2 top-2 z-[9999] rounded-md bg-primary px-4 py-2 font-medium text-primary-content focus:outline focus:outline-2 focus:outline-offset-2"
>
  {label}
</a>
```

### 2. Sr-only by default, revealed on focus

`sr-only` removes the element from the visual layout but keeps it in the focus order. `focus:not-sr-only` and `focus-visible:not-sr-only` undo `sr-only` when the link is focused, revealing it in the top-left at a high z-index.

### 3. Programmatic focus management

In browsers, navigating to a fragment does not move keyboard focus unless the target is focusable. The handler:

1. Looks up the target by id.
2. Adds `tabindex=-1` on the fly when the target lacks one.
3. Calls `target.focus()`.
4. Calls `target.scrollIntoView()` guarded for JSDOM.
5. Prevents the default URL hash change so router state stays clean.

```tsx
const handleClick = (event: MouseEvent<HTMLAnchorElement>) => {
  const target = document.getElementById(targetId);
  if (target) {
    event.preventDefault();
    if (!target.hasAttribute('tabindex')) {
      target.setAttribute('tabindex', '-1');
    }
    target.focus();
    if (typeof target.scrollIntoView === 'function') {
      target.scrollIntoView({ behavior: 'auto', block: 'start' });
    }
  }
};
```

### 4. Placement in the page

The `<SkipToContent />` element must be the FIRST child of the page wrapper so it is the first thing Tab lands on:

```tsx
// Home.tsx
return (
  <>
    <SkipToContent />
    <SEOTags />
    <BackgroundEffect />
    <HomeNavbar />
    <main id="main-content" tabIndex={-1}>
      <Hero />
      <FeaturesSection />
    </main>
  </>
);
```

## Best Practices

- Place `<SkipToContent />` literally first; don't wrap it in providers that render other focusable nodes ahead of it.
- The `<main>` must have a stable `id` matching the link's `href`; default is `main-content`.
- Always guard browser-only APIs (`scrollIntoView`) so the component is JSDOM-safe for Vitest.
- Test that pressing Tab from `document.body` lands on the skip link, and that activating it focuses `<main>`.

## Resources

### references/

- `README.md` - This documentation

## Source examples in this repo

- `frontend/src/components/homepage/SkipToContent.tsx` — component
- `frontend/src/pages/Home.tsx` — placement at top of page wrapper
- `frontend/tests/components/homepage/SkipToContent.test.tsx` — unit suite
- `frontend/tests/accessibility/homepage.a11y.test.tsx` — integration coverage
