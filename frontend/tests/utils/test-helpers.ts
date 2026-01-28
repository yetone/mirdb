/**
 * Shared Test Utilities.
 *
 * Provides helper functions for testing React components with
 * the required providers (Router, Auth, Theme contexts).
 */

import React, { ReactElement } from 'react'
import { render, RenderOptions } from '@testing-library/react'
import { BrowserRouter, MemoryRouter } from 'react-router-dom'
import { AuthContext, AuthProvider } from '@/contexts/AuthContext'
import { ThemeContext, ThemeProvider } from '@/contexts/ThemeContext'
import type { User, AuthContextType, ThemeContextType } from '@/types/custom'

interface CustomRenderOptions extends Omit<RenderOptions, 'wrapper'> {
  initialEntries?: string[]
  authOverrides?: Partial<AuthContextType>
  themeOverrides?: Partial<ThemeContextType>
}

/**
 * Creates a mock user for testing
 */
export function createMockUser(overrides?: Partial<User>): User {
  return {
    id: 1,
    username: 'testuser',
    email: 'test@example.com',
    is_admin: false,
    ...overrides,
  }
}

/**
 * Creates a mock auth context for testing
 */
export function mockAuthContext(overrides?: Partial<AuthContextType>): AuthContextType {
  return {
    user: null,
    isAuthenticated: false,
    isLoading: false,
    login: async () => {},
    logout: () => {},
    register: async () => {},
    ...overrides,
  }
}

/**
 * Creates a mock theme context for testing
 */
export function mockThemeContext(theme: string = 'light'): ThemeContextType {
  return {
    theme,
    setTheme: () => {},
  }
}

/**
 * Renders a component with all required providers
 */
export function renderWithProviders(
  ui: ReactElement,
  {
    initialEntries = ['/'],
    authOverrides,
    themeOverrides,
    ...renderOptions
  }: CustomRenderOptions = {}
) {
  const authContext = mockAuthContext(authOverrides)
  const themeContext = mockThemeContext(themeOverrides?.theme)

  function Wrapper({ children }: { children: React.ReactNode }) {
    return React.createElement(
      MemoryRouter,
      { initialEntries },
      React.createElement(
        AuthContext.Provider,
        { value: authContext },
        React.createElement(
          ThemeContext.Provider,
          { value: themeContext },
          children
        )
      )
    )
  }

  return {
    ...render(ui, { wrapper: Wrapper, ...renderOptions }),
    authContext,
    themeContext,
  }
}

export { render }
