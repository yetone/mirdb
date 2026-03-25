/**
 * Test Utilities for Homepage Tests
 * Owner: First builder (Scenario 1)
 *
 * Provides render wrappers with required providers.
 */

import React, { ReactElement, ReactNode } from 'react';
import { render, RenderOptions, RenderResult } from '@testing-library/react';
import { BrowserRouter, MemoryRouter } from 'react-router-dom';
import { AuthProvider } from '../../../src/contexts/AuthContext';
import { ThemeProvider, Theme } from '../../../src/contexts/ThemeContext';

interface MockAuthOptions {
  isAuthenticated?: boolean;
  user?: { id: string; username: string; email: string } | null;
}

interface MockThemeOptions {
  theme?: Theme;
}

interface RenderWithProvidersOptions extends Omit<RenderOptions, 'wrapper'> {
  authOptions?: MockAuthOptions;
  themeOptions?: MockThemeOptions;
  initialRoute?: string;
  useMemoryRouter?: boolean;
}

function createWrapper(options: RenderWithProvidersOptions = {}) {
  const {
    authOptions = {},
    themeOptions = {},
    initialRoute = '/',
    useMemoryRouter = false,
  } = options;

  const authState = {
    isAuthenticated: authOptions.isAuthenticated ?? false,
    user: authOptions.user ?? null,
  };

  return function Wrapper({ children }: { children: ReactNode }) {
    const RouterComponent = useMemoryRouter ? MemoryRouter : BrowserRouter;
    const routerProps = useMemoryRouter ? { initialEntries: [initialRoute] } : {};

    return (
      <RouterComponent {...routerProps}>
        <AuthProvider initialState={authState}>
          <ThemeProvider initialTheme={themeOptions.theme ?? 'light'}>
            {children}
          </ThemeProvider>
        </AuthProvider>
      </RouterComponent>
    );
  };
}

export function renderWithProviders(
  ui: ReactElement,
  options: RenderWithProvidersOptions = {}
): RenderResult {
  const Wrapper = createWrapper(options);
  return render(ui, { wrapper: Wrapper, ...options });
}

export function mockAuthContext(isAuthenticated: boolean) {
  return {
    isAuthenticated,
    user: isAuthenticated ? { id: '1', username: 'testuser', email: 'test@example.com' } : null,
  };
}

export function mockThemeContext(theme: Theme) {
  return { theme };
}
