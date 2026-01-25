/**
 * Shared test utilities for homepage unit tests.
 *
 * Provides:
 * - renderWithProviders: Wrapper with AuthContext, ThemeContext
 * - mockAuthContext: Helper to create mock auth state
 * - mockThemeContext: Helper to create mock theme state
 * - Common test fixtures and data
 */

import React, { ReactElement } from 'react';
import { render, RenderOptions } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import { AuthProvider } from '../../../src/contexts/AuthContext';
import { ThemeProvider } from '../../../src/contexts/ThemeContext';

interface ProvidersProps {
  children: React.ReactNode;
}

/**
 * All providers wrapper for unit tests
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
 * Render with all providers (AuthContext, ThemeContext, BrowserRouter)
 */
export function renderWithProviders(
  ui: ReactElement,
  options?: Omit<RenderOptions, 'wrapper'>
) {
  return render(ui, { wrapper: AllProviders, ...options });
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
