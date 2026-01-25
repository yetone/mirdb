/**
 * Shared test utilities for homepage unit tests.
 *
 * Provides:
 * - renderWithProviders: Wrapper with AuthContext, ThemeContext
 * - mockAuthContext: Helper to create mock auth state
 * - mockThemeContext: Helper to create mock theme state
 * - Common test fixtures and data
 */

import React, { ReactElement, ReactNode } from 'react';
import { render, RenderOptions } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import { AuthContext, AuthProvider } from '../../../src/contexts/AuthContext';
import { ThemeProvider } from '../../../src/contexts/ThemeContext';

interface ProvidersProps {
  children: React.ReactNode;
}

/**
 * All providers wrapper for unit tests (uses real providers)
 */
function AllProviders({ children }: ProvidersProps) {
  return (
    <ThemeProvider>
      <AuthProvider>
        <BrowserRouter>{children}</BrowserRouter>
      </AuthProvider>
    </ThemeProvider>
  );
}

/**
 * Mock authentication context values for testing different auth states
 */
export const mockAuthContext = {
  authenticated: {
    user: { id: 1, username: 'testuser', email: 'test@example.com', is_admin: false },
    isAuthenticated: true,
    isLoading: false,
    login: vi.fn(),
    logout: vi.fn(),
    register: vi.fn(),
  },
  unauthenticated: {
    user: null,
    isAuthenticated: false,
    isLoading: false,
    login: vi.fn(),
    logout: vi.fn(),
    register: vi.fn(),
  },
  loading: {
    user: null,
    isAuthenticated: false,
    isLoading: true,
    login: vi.fn(),
    logout: vi.fn(),
    register: vi.fn(),
  },
};

/**
 * Mock theme context values for testing different theme states
 */
export const mockThemeContext = {
  light: {
    theme: 'light',
    setTheme: vi.fn(),
  },
  dark: {
    theme: 'dark',
    setTheme: vi.fn(),
  },
};

interface RenderWithProvidersOptions extends Omit<RenderOptions, 'wrapper'> {
  authState?: typeof mockAuthContext.authenticated | typeof mockAuthContext.unauthenticated | typeof mockAuthContext.loading;
}

/**
 * Create a custom provider wrapper with optional auth state override
 */
function createProviderWrapper(authState = mockAuthContext.unauthenticated) {
  return function ProviderWrapper({ children }: { children: ReactNode }) {
    return (
      <ThemeProvider>
        <AuthContext.Provider value={authState}>
          <BrowserRouter>{children}</BrowserRouter>
        </AuthContext.Provider>
      </ThemeProvider>
    );
  };
}

/**
 * Render with providers for unit tests
 * Allows overriding auth state for testing different scenarios
 */
export function renderWithProviders(
  ui: ReactElement,
  { authState = mockAuthContext.unauthenticated, ...options }: RenderWithProvidersOptions = {}
) {
  const Wrapper = createProviderWrapper(authState);
  return render(ui, { wrapper: Wrapper, ...options });
}

/**
 * Render with all real providers (for tests that don't need mocked auth state)
 */
export function renderWithAllProviders(
  ui: ReactElement,
  options?: Omit<RenderOptions, 'wrapper'>
) {
  return render(ui, { wrapper: AllProviders, ...options });
}

/**
 * Common test data fixtures
 */
export const testFixtures = {
  heroHeadline: 'Shorten URLs. Track Every Click.',
  heroSubheadline: 'Transform long, unwieldy URLs into clean, memorable short links',
  primaryCtaText: 'Get Started',
  secondaryCtaText: 'Login',
  authenticatedCtaText: 'Go to Dashboard',
};
