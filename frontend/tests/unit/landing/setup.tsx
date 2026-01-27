/**
 * Test Setup for Landing Page Unit Tests
 *
 * This file is created by the first scenario builder and
 * provides shared test utilities and setup.
 *
 * Includes:
 * - Custom render function with providers (Router, Theme)
 * - Mock implementations for context providers
 * - Common test utilities and helpers
 * - Viewport resize utilities for responsive tests
 */

import '@testing-library/jest-dom'
import { ReactElement } from 'react'
import { render as rtlRender, RenderOptions } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import { ThemeProvider } from '../../../src/contexts/ThemeContext'
import { AuthProvider } from '../../../src/contexts/AuthContext'

interface WrapperProps {
  children: React.ReactNode
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

export function render(
  ui: ReactElement,
  options?: Omit<RenderOptions, 'wrapper'>
) {
  return rtlRender(ui, { wrapper: AllProviders, ...options })
}

export function renderWithProviders(
  ui: ReactElement,
  options?: Omit<RenderOptions, 'wrapper'>
) {
  return rtlRender(ui, { wrapper: AllProviders, ...options })
}

export function setViewport(width: number, height: number = 768) {
  Object.defineProperty(window, 'innerWidth', {
    writable: true,
    configurable: true,
    value: width,
  })
  Object.defineProperty(window, 'innerHeight', {
    writable: true,
    configurable: true,
    value: height,
  })
  window.dispatchEvent(new Event('resize'))
}

export {
  screen,
  fireEvent,
  waitFor,
  cleanup,
  within,
  act,
} from '@testing-library/react'
