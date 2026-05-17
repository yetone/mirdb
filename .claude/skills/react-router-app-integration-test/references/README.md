# React Router App-Level Integration Testing

When verifying that a click on a `<Link>` actually changes the route — not just that the link has the right `href` — render the full router-bearing component tree inside a `MemoryRouter` and assert that the destination route's content has mounted.

## When to use

- A component-level unit test confirms a link carries the right `href`, but you want a second test that proves the route table actually serves that path (catches regressions where someone forgets to declare the route in `AppRoutes` or wraps a public route in an auth guard).
- You need to test navigation behaviour without a real browser.

## Pattern

```tsx
import { render, screen, within } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import userEvent from '@testing-library/user-event';
import App from '../../../src/App';

it('integration: clicking the Register CTA inside <App /> navigates to /register', async () => {
  const user = userEvent.setup();

  render(
    <MemoryRouter initialEntries={['/']}>
      <App />
    </MemoryRouter>
  );

  const registerButton = screen.getByRole('link', {
    name: /register|get started|sign up/i,
  });
  await user.click(registerButton);

  expect(await screen.findByTestId('register-page')).toBeInTheDocument();
});
```

## Key elements

1. **`MemoryRouter initialEntries={['/']}`** — the integration test owns the router so we can seed an initial entry without using a real browser. The component tree under test (here `<App />`) renders `<Routes>` internally.
2. **`screen.getByRole('link', { name: /…/i })`** — query by ARIA role and an accessible-name regex, so the test does not depend on a specific test id and tolerates UX copy changes ("Register" → "Get Started").
3. **`screen.findByTestId('register-page')`** — the destination routes are stubbed in `AppRoutes` as `<div data-testid="register-page">`. The async `findBy` query waits for the route to mount after the click resolves.

## Scoping queries to disambiguate

When the homepage has both a hero `Login` CTA and a navbar `Login` link, both match `/login|sign in/i`. Use `within()` to scope:

```tsx
const nav = screen.getByRole('navigation', { name: /main navigation/i });
const loginLink = within(nav).getByRole('link', { name: /login|sign in/i });
await user.click(loginLink);
```

The navbar must have `aria-label="Main navigation"` on the `<nav>` element so the role-query name matcher can target it.

## Why not BrowserRouter?

`<App />` does NOT include its own router — `src/main.tsx` wraps it in `<BrowserRouter>` for production. Tests therefore have to supply their own router; `MemoryRouter` is the only option that runs in JSDOM without `window.history` quirks.

## Related files

- `frontend/tests/components/homepage/HeroCTA.test.tsx` — Register integration test
- `frontend/tests/components/homepage/HomeNavbar.test.tsx` — Login navbar integration test
- `frontend/src/routes/AppRoutes.tsx` — defines the stubbed `/login` and `/register` destinations
- `frontend/src/App.tsx` — top-level component that renders `<AppRoutes />`
