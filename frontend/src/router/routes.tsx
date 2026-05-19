/**
 * Application route table. Owns the / route entry.
 * Owner: Scenario 3 - Authenticated User Redirect to Dashboard
 *
 * Expected export:
 * - routes array of route objects compatible with React Router
 */

import type { RouteObject } from 'react-router-dom';
import { HomeRouteGuard } from './AuthGuard';

/**
 * Application routes.
 *
 * The root route (/) is guarded by HomeRouteGuard, which redirects
 * authenticated users to /dashboard before rendering the homepage.
 */
export const routes: RouteObject[] = [
  {
    path: '/',
    element: <HomeRouteGuard />,
  },
  {
    path: '/dashboard',
    element: <div data-testid="dashboard-page">Dashboard</div>,
  },
  {
    path: '/login',
    element: <div data-testid="login-page">Login</div>,
  },
  {
    path: '/register',
    element: <div data-testid="register-page">Register</div>,
  },
];
