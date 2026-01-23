/**
 * Test utility for rendering components with required providers.
 *
 * Owner: First Builder (Shared Resource)
 *
 * Expected exports:
 * - renderWithProviders(ui, options): Renders component with Router, Auth, Theme contexts
 * - createMockAuthContext(overrides): Creates mock auth context
 * - createMockThemeContext(overrides): Creates mock theme context
 */

import React, { ReactElement, ReactNode } from 'react';
import { render, RenderOptions } from '@testing-library/react';
import { BrowserRouter, MemoryRouter } from 'react-router-dom';
import { AuthContext, AuthContextValue } from '../../src/contexts/AuthContext';
import { ThemeContext, ThemeContextValue } from '../../src/contexts/ThemeContext';

export interface MockAuthContextOptions {
  isAuthenticated?: boolean;
  isAdmin?: boolean;
  loading?: boolean;
  user?: { id: number; username: string; email: string; is_admin: number } | null;
}

export interface MockThemeContextOptions {
  theme?: string;
}

export const createMockAuthContext = (
  options: MockAuthContextOptions = {}
): AuthContextValue => {
  const {
    isAuthenticated = false,
    isAdmin = false,
    loading = false,
    user = null,
  } = options;

  return {
    isAuthenticated,
    isAdmin,
    loading,
    user,
    login: async () => {},
    logout: () => {},
    register: async () => {},
  };
};

export const createMockThemeContext = (
  options: MockThemeContextOptions = {}
): ThemeContextValue => {
  const { theme = 'dark' } = options;

  return {
    theme,
    setTheme: () => {},
  };
};

interface ProvidersProps {
  children: ReactNode;
  authContext?: AuthContextValue;
  themeContext?: ThemeContextValue;
  initialEntries?: string[];
}

const AllProviders: React.FC<ProvidersProps> = ({
  children,
  authContext = createMockAuthContext(),
  themeContext = createMockThemeContext(),
  initialEntries = ['/'],
}) => {
  return (
    <MemoryRouter initialEntries={initialEntries}>
      <AuthContext.Provider value={authContext}>
        <ThemeContext.Provider value={themeContext}>
          {children}
        </ThemeContext.Provider>
      </AuthContext.Provider>
    </MemoryRouter>
  );
};

export interface RenderWithProvidersOptions extends Omit<RenderOptions, 'wrapper'> {
  authContext?: AuthContextValue;
  themeContext?: ThemeContextValue;
  initialEntries?: string[];
}

export const renderWithProviders = (
  ui: ReactElement,
  options: RenderWithProvidersOptions = {}
) => {
  const { authContext, themeContext, initialEntries, ...renderOptions } = options;

  return render(ui, {
    wrapper: ({ children }) => (
      <AllProviders
        authContext={authContext}
        themeContext={themeContext}
        initialEntries={initialEntries}
      >
        {children}
      </AllProviders>
    ),
    ...renderOptions,
  });
};
