import { ReactElement } from 'react'
import { render, RenderOptions } from '@testing-library/react'
import { BrowserRouter, MemoryRouter } from 'react-router-dom'
import { ThemeProvider } from '../../src/contexts/ThemeContext'
import { AuthProvider } from '../../src/contexts/AuthContext'

interface CustomRenderOptions extends Omit<RenderOptions, 'wrapper'> {
  initialEntries?: string[]
  useMemoryRouter?: boolean
}

function AllTheProviders({ children, useMemoryRouter = false, initialEntries = ['/'] }: {
  children: React.ReactNode
  useMemoryRouter?: boolean
  initialEntries?: string[]
}) {
  const Router = useMemoryRouter ? MemoryRouter : BrowserRouter
  const routerProps = useMemoryRouter ? { initialEntries } : {}

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

export function renderWithProviders(
  ui: ReactElement,
  options: CustomRenderOptions = {}
) {
  const { initialEntries, useMemoryRouter = false, ...renderOptions } = options

  return render(ui, {
    wrapper: ({ children }) => (
      <AllTheProviders useMemoryRouter={useMemoryRouter} initialEntries={initialEntries}>
        {children}
      </AllTheProviders>
    ),
    ...renderOptions,
  })
}

export * from '@testing-library/react'
export { renderWithProviders as render }
