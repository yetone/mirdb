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
import { render as rtlRender, RenderOptions } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { ThemeProvider } from '../../../src/contexts/ThemeContext'
import { AuthProvider } from '../../../src/contexts/AuthContext'

interface WrapperProps {
  children: React.ReactNode
}

interface ExtendedRenderOptions extends Omit<RenderOptions, 'wrapper'> {
  initialEntries?: string[]
}

function IntegrationProviders({ children }: WrapperProps) {
  return (
    <ThemeProvider>
      <AuthProvider>
        {children}
      </AuthProvider>
    </ThemeProvider>
  )
}

export function renderForIntegration(
  ui: ReactElement,
  options?: Omit<RenderOptions, 'wrapper'>
) {
  return rtlRender(ui, { wrapper: IntegrationProviders, ...options })
}

export function render(
  ui: ReactElement,
  { initialEntries = ['/'], ...options }: ExtendedRenderOptions = {}
) {
  function Wrapper({ children }: WrapperProps) {
    return (
      <MemoryRouter initialEntries={initialEntries}>
        <ThemeProvider>
          <AuthProvider>
            {children}
          </AuthProvider>
        </ThemeProvider>
      </MemoryRouter>
    )
  }
  return rtlRender(ui, { wrapper: Wrapper, ...options })
}

export {
  screen,
  fireEvent,
  waitFor,
  cleanup,
  within,
  act,
} from '@testing-library/react'
