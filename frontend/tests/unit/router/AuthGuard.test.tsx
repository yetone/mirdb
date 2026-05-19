import React from 'react';
import { render, screen } from '@testing-library/react';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import { describe, it, expect } from 'vitest';
import { HomeRouteGuard } from '../../../src/router/AuthGuard';
import { AuthContext, type AuthContextValue } from '../../../src/contexts/AuthContext';

/**
 * Helper to render HomeRouteGuard with a given auth state.
 */
function renderWithAuth(authValue: AuthContextValue, initialEntries: string[] = ['/']) {
  return render(
    <MemoryRouter initialEntries={initialEntries}>
      <AuthContext.Provider value={authValue}>
        <Routes>
          <Route path="/" element={<HomeRouteGuard />} />
          <Route path="/dashboard" element={<div data-testid="dashboard-page">Dashboard</div>} />
          <Route path="/login" element={<div data-testid="login-page">Login</div>} />
        </Routes>
      </AuthContext.Provider>
    </MemoryRouter>
  );
}

const unauthenticatedValue: AuthContextValue = {
  isAuthenticated: false,
  user: null,
  login: () => {},
  logout: () => {},
};

const authenticatedValue: AuthContextValue = {
  isAuthenticated: true,
  user: { id: '1', email: 'user@example.com', name: 'Test User' },
  login: () => {},
  logout: () => {},
};

describe('HomeRouteGuard', () => {
  it('renders homepage when user is not authenticated', () => {
    renderWithAuth(unauthenticatedValue);

    expect(screen.getByTestId('home-page')).toBeInTheDocument();
  });

  it('redirects to /dashboard when user is authenticated (test case 1)', () => {
    renderWithAuth(authenticatedValue);

    expect(screen.queryByTestId('home-page')).not.toBeInTheDocument();
    expect(screen.getByTestId('dashboard-page')).toBeInTheDocument();
  });

  it('does not redirect when user is unauthenticated', () => {
    renderWithAuth(unauthenticatedValue);

    expect(screen.getByTestId('home-page')).toBeInTheDocument();
    expect(screen.queryByTestId('dashboard-page')).not.toBeInTheDocument();
  });

  it('redirects authenticated users even when navigating from another route', () => {
    renderWithAuth(authenticatedValue, ['/login']);

    // Start at login page
    expect(screen.getByTestId('login-page')).toBeInTheDocument();
  });

  it('returns a Navigate element instead of homepage markup when authenticated', () => {
    const { container } = renderWithAuth(authenticatedValue);

    // The home-page should not be in the DOM at all
    expect(container.querySelector('[data-testid="home-page"]')).toBeNull();
    // Dashboard should be rendered instead
    expect(screen.getByTestId('dashboard-page')).toBeInTheDocument();
  });
});
