/**
 * Test Helper Utilities
 * Owner: First builder (shared resource)
 *
 * Shared test utilities and mock factories.
 */

import { render, RenderOptions, RenderResult } from '@testing-library/react'
import { ReactElement, ReactNode } from 'react'
import { BrowserRouter } from 'react-router-dom'

interface ProvidersProps {
  children: ReactNode
}

function AllProviders({ children }: ProvidersProps) {
  return <BrowserRouter>{children}</BrowserRouter>
}

export function renderWithProviders(
  ui: ReactElement,
  options?: Omit<RenderOptions, 'wrapper'>
): RenderResult {
  return render(ui, { wrapper: AllProviders, ...options })
}

export interface MockAuthContext {
  isAuthenticated: boolean
  user: { id: number; username: string; is_admin: boolean } | null
  login: () => Promise<void>
  logout: () => void
}

export function createMockAuthContext(
  overrides: Partial<MockAuthContext> = {}
): MockAuthContext {
  return {
    isAuthenticated: false,
    user: null,
    login: vi.fn().mockResolvedValue(undefined),
    logout: vi.fn(),
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
    theme: 'light',
    setTheme: vi.fn(),
    themes: ['light', 'dark', 'cyberpunk', 'synthwave'],
    ...overrides,
  }
}

export function mockApiResponse<T>(response: T): Promise<{ data: T }> {
  return Promise.resolve({ data: response })
}
