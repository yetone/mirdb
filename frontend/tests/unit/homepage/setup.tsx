/**
 * Unit Test Setup and Utilities
 * Owner: First builder
 *
 * Common setup, mocks, and utilities for homepage unit tests.
 */

// Mock IntersectionObserver for framer-motion's whileInView
const mockIntersectionObserver = vi.fn()
mockIntersectionObserver.mockReturnValue({
  observe: () => null,
  unobserve: () => null,
  disconnect: () => null,
})
window.IntersectionObserver = mockIntersectionObserver

import { ReactElement, ReactNode } from 'react'
import { render, RenderOptions } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import { ThemeProvider } from '../../../src/contexts/ThemeContext'
import '@testing-library/jest-dom'

interface WrapperProps {
  children: ReactNode
}

function AllTheProviders({ children }: WrapperProps) {
  return (
    <BrowserRouter>
      <ThemeProvider>{children}</ThemeProvider>
    </BrowserRouter>
  )
}

function customRender(
  ui: ReactElement,
  options?: Omit<RenderOptions, 'wrapper'>
) {
  return render(ui, { wrapper: AllTheProviders, ...options })
}

export * from '@testing-library/react'
export { customRender as render }

export const mockAuthContext = {
  user: null,
  isAuthenticated: false,
  login: vi.fn(),
  logout: vi.fn(),
}

export const mockThemeContext = {
  theme: 'dark' as const,
  setTheme: vi.fn(),
}

export const mockNavigate = vi.fn()

vi.mock('react-router-dom', async () => {
  const actual = await vi.importActual('react-router-dom')
  return {
    ...actual,
    useNavigate: () => mockNavigate,
  }
})
