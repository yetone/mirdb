# MirDB Routes & NAV_LINKS Constants Pattern

The MirDB frontend keeps a single source of truth for route paths and primary navigation entries in `frontend/src/utils/constants.ts`. Components and the router import these constants instead of hard-coding `'/login'` / `'/register'` strings.

## Why

A `getAttribute('href')` test that compares against the literal string `'/login'` ties the component to a route name. If a future engineer renames `/login` to `/sign-in`, every component plus every test has to change. Centralising the literals means renaming the route is a one-line edit to `constants.ts`.

The "no CTA links to an undefined route" test enforces this contract by checking each rendered href against the allowed routes set.

## What lives in constants.ts

```ts
export const BRAND = {
  name: 'MirDB',
  tagline: '...',
} as const;

export const NAV_LINKS: NavLink[] = [
  { label: 'Login', to: '/login', ariaLabel: 'Go to login page' },
  { label: 'Register', to: '/register', ariaLabel: 'Go to registration page' },
];

export const ROUTES = {
  HOME: '/',
  LOGIN: '/login',
  REGISTER: '/register',
} as const;
```

## How to consume

### From a CTA component

```tsx
import { Link } from 'react-router-dom';
import { ROUTES } from '../../utils/constants';

<Link to={ROUTES.REGISTER} className="btn btn-primary btn-lg">
  Register
</Link>
```

### From the navbar

```tsx
import { NAV_LINKS } from '../../utils/constants';

<ul className="menu menu-horizontal">
  {NAV_LINKS.map((link) => (
    <li key={link.to}>
      <Link
        to={link.to}
        aria-label={link.ariaLabel}
        data-testid={`navbar-link-${link.label.toLowerCase()}`}
      >
        {link.label}
      </Link>
    </li>
  ))}
</ul>
```

### From the router

```tsx
import { ROUTES } from '../utils/constants';

<Route path={ROUTES.LOGIN} element={<LoginPage />} />
```

## Rules

1. **Never hard-code route paths** in components. Use `ROUTES.X`.
2. **Adding a top-level link**: append to `NAV_LINKS`. The navbar will render it automatically.
3. **Adding a new route**: add the path to `ROUTES`, declare the `<Route>` in `src/routes/AppRoutes.tsx`, and only then can CTA components reference it.
4. The CTA route-coverage test (`'does not link to any undefined route'`) hard-codes the allowed set to `['/login', '/register']`. If you add a third route reachable from the homepage, update that test's allow-list too.
5. Brand name (`BRAND.name`) is used for both visible text and `aria-label` on the navbar brand link — change in one place.

## Related files

- `frontend/src/utils/constants.ts` — declarations
- `frontend/src/components/homepage/HeroCTA.tsx` — uses `ROUTES.REGISTER`, `ROUTES.LOGIN`
- `frontend/src/components/homepage/HomeNavbar.tsx` — uses `BRAND.name`, `ROUTES.HOME`, and maps `NAV_LINKS`
- `frontend/src/routes/AppRoutes.tsx` — declares the routes
- `frontend/src/types/homepage.ts` — defines the `NavLink` interface
