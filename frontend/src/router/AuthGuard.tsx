/**
 * Route guard that redirects authenticated users away from / to /dashboard.
 * Owner: Scenario 3 - Authenticated User Redirect to Dashboard
 *
 * Expected export:
 * - HomeRouteGuard component: wraps <Home /> and returns <Navigate to="/dashboard" /> when authenticated
 */

import React from 'react';
import { Navigate } from 'react-router-dom';
import { useAuth } from '../contexts/AuthContext';
import Home from '../pages/Home';

/**
 * HomeRouteGuard wraps the Home page and redirects authenticated users
 * to /dashboard before any homepage content is rendered.
 *
 * When isAuthenticated is true, this component renders a <Navigate />
 * element that triggers an immediate client-side redirect. The homepage
 * DOM is never mounted in this case, satisfying the requirement that
 * authenticated users never see the homepage.
 */
export function HomeRouteGuard() {
  const { isAuthenticated } = useAuth();

  if (isAuthenticated) {
    return <Navigate to="/dashboard" replace />;
  }

  return <Home />;
}
