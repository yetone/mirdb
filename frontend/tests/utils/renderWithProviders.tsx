/**
 * Custom Render Utility with Providers
 * Owner: First builder
 *
 * Wraps components with necessary providers for testing:
 * - MemoryRouter for routing
 * - ThemeContext for theming
 */
import { ReactElement } from 'react'
import { render, RenderOptions } from '@testing-library/react'
import { MemoryRouter, MemoryRouterProps } from 'react-router-dom'
import { ThemeProvider } from '@/contexts/ThemeContext'

interface CustomRenderOptions extends Omit<RenderOptions, 'wrapper'> {
  initialEntries?: MemoryRouterProps['initialEntries']
}

export function renderWithProviders(
  ui: ReactElement,
  { initialEntries = ['/'], ...options }: CustomRenderOptions = {}
) {
  function Wrapper({ children }: { children: React.ReactNode }) {
    return (
      <MemoryRouter initialEntries={initialEntries}>
        <ThemeProvider>
          {children}
        </ThemeProvider>
      </MemoryRouter>
    )
  }

  return render(ui, { wrapper: Wrapper, ...options })
}

export * from '@testing-library/react'
