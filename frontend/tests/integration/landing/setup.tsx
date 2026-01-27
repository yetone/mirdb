/**
 * Test Setup for Landing Page Integration Tests
 *
 * This file is created by the first scenario builder and
 * provides integration test utilities and setup.
 *
 * Includes:
 * - Full app render with all providers
 * - Router mock setup for navigation tests
 * - Theme context mock for theme tests
 * - Common assertions for integration scenarios
 */

import '@testing-library/jest-dom'
import { ReactElement } from 'react'
import { render, RenderOptions } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import { ThemeProvider } from '../../../src/contexts/ThemeContext'
import { AuthProvider } from '../../../src/contexts/AuthContext'

interface WrapperProps {
  children: React.ReactNode
}

function IntegrationProviders({ children }: WrapperProps) {
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

export function renderForIntegration(
  ui: ReactElement,
  options?: Omit<RenderOptions, 'wrapper'>
) {
  return render(ui, { wrapper: IntegrationProviders, ...options })
}

export * from '@testing-library/react'
