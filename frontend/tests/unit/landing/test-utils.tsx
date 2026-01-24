/**
 * Shared Test Utilities for Landing Page Unit Tests
 *
 * Provides common test setup, mocks, and utilities:
 * - renderWithProviders: Wraps component with necessary providers
 * - mockThemeContext: Mock ThemeContext for testing
 * - mockRouter: Mock React Router for navigation tests
 */

import React, { ReactNode } from 'react'
import { render, RenderOptions } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import { ThemeProvider } from '../../../src/contexts/ThemeContext'
import { AuthProvider } from '../../../src/contexts/AuthContext'

interface WrapperProps {
  children: ReactNode
}

function AllProviders({ children }: WrapperProps) {
  return (
    <ThemeProvider>
      <AuthProvider>
        <BrowserRouter>
          {children}
        </BrowserRouter>
      </AuthProvider>
    </ThemeProvider>
  )
}

export function renderWithProviders(
  ui: React.ReactElement,
  options?: Omit<RenderOptions, 'wrapper'>
) {
  return render(ui, { wrapper: AllProviders, ...options })
}

export function createMockThemeContext(theme: string = 'dark') {
  return {
    theme,
    setTheme: vi.fn(),
  }
}

export function createMockRouter(initialRoute: string = '/') {
  window.history.pushState({}, 'Test page', initialRoute)
}
