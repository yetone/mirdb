# MirDB Frontend RTL Testing

## Overview

Conventions for unit and integration tests on the MirDB frontend using Vitest + React Testing Library + jsdom + MemoryRouter. The frontend has its own pinned Vitest 1.6.1 install under `frontend/node_modules/`; tests must be run from inside `frontend/` so the workspace-root Vitest 4.x does not get picked up instead (the root install has no jsdom and will fail every DOM test with `document is not defined`).

## When to Use This Skill

Use this skill when users request:

- Adding or updating tests under `frontend/tests/` for any homepage component, page, or route
- Asserting REQ/US criteria for scenarios that touch React UI (h1 brand, hero region, document.title, accessibility roles, etc.)
- Setting up a vitest test for a component that uses `react-router-dom` `<Link>` or `useNavigate` (needs `MemoryRouter`)
- Verifying that a component renders gracefully without a context provider by reading `createContext`'s default value

## Core Capabilities

### 1. Running the suite

Always run vitest from `frontend/` so the pinned 1.6.1 + jsdom config wins:

```bash
cd /workspace/frontend && npx vitest run
```

Running `npx vitest run` from `/workspace` picks up the workspace-root Vitest 4.x install, which lacks jsdom and reports `document is not defined` for every DOM test. Symptom: 30 tests fail with the same error message. Fix: always `cd frontend` first.

### 2. Rendering with MemoryRouter

Any component that transitively renders a `react-router-dom` `<Link>` (e.g. `HeroCTA`, `HomeNavbar`) must be wrapped in `<MemoryRouter>` for tests:

```tsx
import { MemoryRouter } from 'react-router-dom';
import { render } from '@testing-library/react';

const renderHome = () =>
  render(
    <MemoryRouter initialEntries={['/']}>
      <Home />
    </MemoryRouter>,
  );
```

Use `MemoryRouter` rather than `BrowserRouter` — it does not touch the jsdom URL bar, so tests stay isolated.

### 3. Querying by role, then by testid

Prefer `screen.getByRole('heading', { level: 1, name: /MirDB/i })` over `getByTestId`. Use `data-testid` only when (a) the element has no meaningful role/name, or (b) the role would collide with an existing landmark — e.g. the hero section cannot use `role="banner"` because `<header>` in `HomeNavbar` already carries the implicit banner role; the hero uses `data-testid="hero"` + `aria-labelledby="hero-heading"` instead.

### 4. Scoping queries with `within`

When asserting multiple children of a region, scope the query so a stray match elsewhere on the page does not pass the test:

```tsx
const hero = screen.getByTestId('hero');
const h1 = within(hero).getByRole('heading', { level: 1 });
const tagline = within(hero).getByText(/url shortening/i);
```

### 5. document.title assertions

`document.title` is checked by reading the property directly after render:

```tsx
expect(document.title).toBe('MirDB - URL Shortening Service');
```

The title is set by a `useEffect` in `Home.tsx` as a head-manager fallback so the assertion passes even before Scenario 8's `<SEOTags />` Helmet integration lands.

### 6. Rendering without a context provider

Test case 5 of Scenario 1 requires that `<Home />` renders gracefully without a `ThemeProvider`. This works because `ThemeContext` is created with a default value via `createContext({...})`. The test simply renders without wrapping in the provider:

```tsx
it('renders gracefully without a ThemeContext provider', () => {
  render(
    <MemoryRouter>
      <Home />
    </MemoryRouter>,
  );
  expect(screen.getByRole('heading', { level: 1, name: /MirDB/i })).toBeInTheDocument();
});
```

## Best Practices

- Always run `npx vitest run` from `frontend/`, never from `/workspace`.
- One assertion family per `it` block — group related assertions, but keep test intent narrow so a failure points at one cause.
- Prefer accessibility-first queries (`getByRole`, `getByLabelText`) and reach for `getByTestId` only as a tie-breaker when roles collide with landmarks.
- Use the helper `renderHome()` / `renderHero()` pattern at the top of each spec file to keep MemoryRouter boilerplate in one place.
- Add a DOM-ordering regression check (e.g. hero is rendered before features inside `<main>`) when multiple sibling scenarios append content to the same page, so a reorder by another scenario does not silently regress.
- Keep `tests/setup.ts` polyfills (jest-dom, IntersectionObserver, matchMedia, jest-axe) untouched; if a new test needs an additional polyfill, add it there rather than per-spec.
- Treat `data-testid` as a stable contract — if a scenario's JSON references `data-testid="hero"`, do not rename it without updating the scenario.

## Resources

### references/

- `README.md` - This documentation
