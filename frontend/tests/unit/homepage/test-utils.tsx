/**
 * Test Utilities for Homepage Tests
 * Owner: First builder (Scenario 1)
 *
 * Provides render wrappers with required providers:
 * - AuthContext provider (mock authenticated/unauthenticated)
 * - ThemeContext provider (mock theme selection)
 * - BrowserRouter for navigation testing
 */
import React, { ReactElement } from 'react';
import { render, RenderOptions, RenderResult } from '@testing-library/react';
import { BrowserRouter, MemoryRouter } from 'react-router-dom';
import { AuthProvider } from '../../../src/contexts/AuthContext';
import { ThemeProvider } from '../../../src/contexts/ThemeContext';

interface ProviderOptions {
  isAuthenticated?: boolean;
  user?: { username: string; email: string; is_admin: boolean } | null;
  theme?: 'light' | 'dark' | 'system' | 'cyberpunk' | 'synthwave' | 'retro' | 'valentine' | 'night';
  initialRoute?: string;
  useMemoryRouter?: boolean;
}

interface ProvidersProps {
  children: React.ReactNode;
  options?: ProviderOptions;
}

const AllProviders: React.FC<ProvidersProps> = ({ children, options = {} }) => {
  const {
    isAuthenticated = false,
    user = null,
    theme = 'dark',
    initialRoute = '/',
    useMemoryRouter = false,
  } = options;

  const RouterComponent = useMemoryRouter
    ? ({ children }: { children: React.ReactNode }) => (
        <MemoryRouter initialEntries={[initialRoute]}>{children}</MemoryRouter>
      )
    : BrowserRouter;

  return (
    <RouterComponent>
      <AuthProvider initialAuth={isAuthenticated} initialUser={user}>
        <ThemeProvider initialTheme={theme}>
          {children}
        </ThemeProvider>
      </AuthProvider>
    </RouterComponent>
  );
};

export function renderWithProviders(
  ui: ReactElement,
  options?: ProviderOptions,
  renderOptions?: Omit<RenderOptions, 'wrapper'>
): RenderResult {
  const Wrapper: React.FC<{ children: React.ReactNode }> = ({ children }) => (
    <AllProviders options={options}>{children}</AllProviders>
  );

  return render(ui, { wrapper: Wrapper, ...renderOptions });
}

export function mockAuthContext(isAuthenticated: boolean) {
  return {
    isAuthenticated,
    user: isAuthenticated
      ? { username: 'testuser', email: 'test@example.com', is_admin: false }
      : null,
    login: vi.fn(),
    logout: vi.fn(),
  };
}

export function mockThemeContext(theme: string = 'dark') {
  return {
    theme,
    setTheme: vi.fn(),
  };
}
