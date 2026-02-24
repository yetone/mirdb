/**
 * Test Helper Utilities
 * Owner: First builder (shared resource)
 *
 * Shared test utilities and mock factories.
 *
 * Expected exports:
 * - renderWithProviders(component, options): RenderResult
 * - createMockAuthContext(overrides): AuthContextType
 * - createMockThemeContext(overrides): ThemeContextType
 * - mockApiResponse(endpoint, response): void
 */

import React, { ReactElement } from 'react'
import { render, RenderOptions, RenderResult } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { AuthProvider } from '@/contexts/AuthContext'
import { ThemeProvider } from '@/contexts/ThemeContext'
import { vi } from 'vitest'

interface CustomRenderOptions extends Omit<RenderOptions, 'wrapper'> {
  initialRoute?: string
  authenticated?: boolean
  user?: { id: number; username: string; email: string; is_admin: boolean }
}

const defaultUser = {
  id: 1,
  username: 'testuser',
  email: 'test@example.com',
  is_admin: false,
}

export function renderWithProviders(
  ui: ReactElement,
  options: CustomRenderOptions = {}
): RenderResult {
  const { initialRoute = '/', authenticated = false, user = defaultUser, ...renderOptions } = options

  // Set initial route
  window.history.pushState({}, 'Test page', initialRoute)

  // Create a new QueryClient for each test
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
      },
    },
  })

  // Setup localStorage mock for auth state
  if (authenticated) {
    window.localStorage.getItem = (key: string) => {
      if (key === 'token') return 'mock-jwt-token'
      return null
    }
  } else {
    window.localStorage.getItem = () => null
  }

  function Wrapper({ children }: { children: React.ReactNode }) {
    return (
      <QueryClientProvider client={queryClient}>
        <BrowserRouter>
          <ThemeProvider>
            <AuthProvider>
              {children}
            </AuthProvider>
          </ThemeProvider>
        </BrowserRouter>
      </QueryClientProvider>
    )
  }

  return render(ui, { wrapper: Wrapper, ...renderOptions })
}

export interface MockAuthContext {
  user: typeof defaultUser | null
  isAuthenticated: boolean
  isLoading: boolean
  login: (username: string, password: string) => Promise<void>
  logout: () => void
  register: (username: string, email: string, password: string) => Promise<void>
}

export function createMockAuthContext(
  overrides: Partial<MockAuthContext> = {}
): MockAuthContext {
  return {
    user: null,
    isAuthenticated: false,
    isLoading: false,
    login: vi.fn().mockResolvedValue(undefined),
    logout: vi.fn(),
    register: vi.fn().mockResolvedValue(undefined),
    ...overrides,
  }
}

export interface MockThemeContext {
  theme: string
  setTheme: (theme: string) => void
  themes: string[]
}

export function createMockThemeContext(
  overrides: Partial<MockThemeContext> = {}
): MockThemeContext {
  return {
    theme: 'dark',
    setTheme: vi.fn(),
    themes: ['light', 'dark', 'cyberpunk', 'synthwave'],
    ...overrides,
  }
}

// Mock API response helper
export function createMockApiResponse<T>(data: T, status = 200) {
  return {
    data,
    status,
    statusText: 'OK',
    headers: {},
    config: {},
  }
}

export function mockApiResponse<T>(response: T): Promise<{ data: T }> {
  return Promise.resolve({ data: response })
}

// Create mock ShortenResult
export function createMockShortenResult(overrides: Partial<{
  shortUrl: string
  shortCode: string
  shareToken: string
}> = {}) {
  return {
    shortUrl: 'http://localhost/r/abc123',
    shortCode: 'abc123',
    shareToken: 'share-token-123',
    ...overrides,
  }
}
