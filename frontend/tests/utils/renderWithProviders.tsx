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
import { QueryClient, QueryClientProvider } from 'react-query';
import { AuthContext } from '../../src/contexts/AuthContext';
import { ThemeProvider } from '../../src/contexts/ThemeContext';
import { vi } from 'vitest';

interface User {
  id: number;
  username: string;
  email: string;
  is_admin: number;
}

interface AuthContextType {
  user: User | null;
  loading: boolean;
  isAuthenticated: boolean;
  isAdmin: boolean;
  login: (username: string, password: string) => Promise<void>;
  logout: () => void;
}

type Theme = 'light' | 'dark' | 'cyberpunk' | 'synthwave' | 'retro' | 'valentine' | 'night';

interface ThemeContextType {
  theme: Theme;
  setTheme: (theme: Theme) => void;
}

export const createMockAuthContext = (overrides?: Partial<AuthContextType>): AuthContextType => ({
  user: null,
  loading: false,
  isAuthenticated: false,
  isAdmin: false,
  login: vi.fn(),
  logout: vi.fn(),
  ...overrides,
});

export const createMockThemeContext = (overrides?: Partial<ThemeContextType>): ThemeContextType => ({
  theme: 'light',
  setTheme: vi.fn(),
  ...overrides,
});

interface ProviderOptions {
  authContext?: Partial<AuthContextType>;
  themeContext?: Partial<ThemeContextType>;
  initialEntries?: string[];
  useMemoryRouter?: boolean;
}

interface AllProvidersProps {
  children: ReactNode;
  options?: ProviderOptions;
}

const AllProviders: React.FC<AllProvidersProps> = ({ children, options = {} }) => {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
      },
    },
  });

  const authValue = createMockAuthContext(options.authContext);

  const RouterWrapper = options.useMemoryRouter ? MemoryRouter : BrowserRouter;
  const routerProps = options.useMemoryRouter && options.initialEntries
    ? { initialEntries: options.initialEntries }
    : {};

  return (
    <QueryClientProvider client={queryClient}>
      <RouterWrapper {...routerProps}>
        <ThemeProvider>
          <AuthContext.Provider value={authValue}>
            {children}
          </AuthContext.Provider>
        </ThemeProvider>
      </RouterWrapper>
    </QueryClientProvider>
  );
};

export const renderWithProviders = (
  ui: ReactElement,
  options?: ProviderOptions & Omit<RenderOptions, 'wrapper'>
) => {
  const { authContext, themeContext, initialEntries, useMemoryRouter, ...renderOptions } = options || {};

  return render(ui, {
    wrapper: ({ children }) => (
      <AllProviders options={{ authContext, themeContext, initialEntries, useMemoryRouter }}>
        {children}
      </AllProviders>
    ),
    ...renderOptions,
  });
};

export default renderWithProviders;
