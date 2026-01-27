/**
 * Custom test utilities for homepage testing.
 *
 * Provides custom render function that wraps components with:
 * - BrowserRouter for routing
 * - ThemeContext for theme testing
 * - AuthContext for auth state testing
 */

import { ReactElement, ReactNode } from 'react'
import { render, RenderOptions, RenderResult } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import { ThemeProvider } from '../../src/contexts/ThemeContext'
import { AuthProvider } from '../../src/contexts/AuthContext'

interface WrapperProps {
  children: ReactNode
}

function AllProviders({ children }: WrapperProps) {
  return (
    <BrowserRouter>
      <ThemeProvider>
        <AuthProvider>
          {children}
        </AuthProvider>
      </ThemeProvider>
    </BrowserRouter>
  )
}

export function renderWithProviders(
  ui: ReactElement,
  options?: Omit<RenderOptions, 'wrapper'>
): RenderResult {
  return render(ui, { wrapper: AllProviders, ...options })
}

// Re-export everything from testing-library
export * from '@testing-library/react'

// Override the default render
export { renderWithProviders as render }
