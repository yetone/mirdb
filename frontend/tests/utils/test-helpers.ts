/**
 * Shared Test Utilities.
 *
 * Expected exports:
 * - renderWithProviders(component, options): Custom render with contexts
 * - mockAuthContext(overrides): Create mock auth context
 * - mockThemeContext(theme): Create mock theme context
 * - createMockUser(): Generate test user data
 */

import React from 'react';
import { render, RenderOptions } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';

interface MockUser {
  id: string;
  email: string;
  is_admin: boolean;
}

export function createMockUser(overrides: Partial<MockUser> = {}): MockUser {
  return {
    id: '1',
    email: 'test@example.com',
    is_admin: false,
    ...overrides,
  };
}

export function mockAuthContext(overrides: {
  isAuthenticated?: boolean;
  user?: MockUser | null;
  login?: () => Promise<void>;
  logout?: () => void;
} = {}) {
  return {
    isAuthenticated: false,
    user: null,
    login: async () => {},
    logout: () => {},
    ...overrides,
  };
}

export function mockThemeContext(theme = 'dark') {
  return {
    theme,
    setTheme: () => {},
  };
}

interface CustomRenderOptions extends Omit<RenderOptions, 'wrapper'> {
  withRouter?: boolean;
}

export function renderWithProviders(
  ui: React.ReactElement,
  options: CustomRenderOptions = {}
) {
  const { withRouter = true, ...renderOptions } = options;

  function Wrapper({ children }: { children: React.ReactNode }) {
    if (withRouter) {
      return React.createElement(BrowserRouter, null, children);
    }
    return React.createElement(React.Fragment, null, children);
  }

  return render(ui, { wrapper: Wrapper, ...renderOptions });
}
