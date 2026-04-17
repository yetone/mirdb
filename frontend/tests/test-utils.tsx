/**
 * Test Utilities
 *
 * Custom render function with providers and helper utilities.
 */

import { ReactElement } from 'react'
import { render, RenderOptions } from '@testing-library/react'
import { BrowserRouter, MemoryRouter } from 'react-router-dom'
import { ThemeProvider } from '../src/contexts/ThemeContext'
import { AuthProvider } from '../src/contexts/AuthContext'

interface AllTheProvidersProps {
  children: React.ReactNode
  initialRoutes?: string[]
}

function AllTheProviders({ children, initialRoutes }: AllTheProvidersProps) {
  const Router = initialRoutes ? MemoryRouter : BrowserRouter
  const routerProps = initialRoutes ? { initialEntries: initialRoutes } : {}

  return (
    <Router {...routerProps}>
      <ThemeProvider>
        <AuthProvider>
          {children}
        </AuthProvider>
      </ThemeProvider>
    </Router>
  )
}

interface CustomRenderOptions extends Omit<RenderOptions, 'wrapper'> {
  initialRoutes?: string[]
}

const customRender = (
  ui: ReactElement,
  options?: CustomRenderOptions
) => {
  const { initialRoutes, ...renderOptions } = options || {}
  return render(ui, {
    wrapper: ({ children }) => (
      <AllTheProviders initialRoutes={initialRoutes}>
        {children}
      </AllTheProviders>
    ),
    ...renderOptions,
  })
}

export * from '@testing-library/react'
export { customRender as render }
