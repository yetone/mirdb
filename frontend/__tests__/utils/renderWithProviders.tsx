/**
 * Test Utility: Render with Providers
 * Owner: First builder (shared utility)
 *
 * Wraps components with necessary providers for testing:
 * - BrowserRouter (react-router-dom)
 * - ThemeContext
 * - AuthContext
 * - QueryClientProvider (react-query)
 */
import { ReactElement } from 'react'
import { render, RenderOptions } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import { QueryClient, QueryClientProvider } from 'react-query'
import { ThemeProvider } from '../../src/contexts/ThemeContext'
import { AuthProvider } from '../../src/contexts/AuthContext'

type Theme = 'light' | 'dark' | 'cyberpunk' | 'synthwave' | 'retro' | 'valentine' | 'night'

const createTestQueryClient = () =>
  new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
      },
    },
  })

interface ProviderProps {
  children: React.ReactNode
}

interface RenderWithProvidersOptions extends Omit<RenderOptions, 'wrapper'> {
  theme?: Theme
}

function AllProviders({ children }: ProviderProps) {
  const queryClient = createTestQueryClient()
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <ThemeProvider>
          <AuthProvider>{children}</AuthProvider>
        </ThemeProvider>
      </BrowserRouter>
    </QueryClientProvider>
  )
}

export function renderWithProviders(
  ui: ReactElement,
  options?: RenderWithProvidersOptions
) {
  const { theme, ...renderOptions } = options || {}

  // Set theme on document element before rendering
  if (theme) {
    document.documentElement.setAttribute('data-theme', theme)
    // Also set in localStorage for ThemeProvider initialization
    localStorage.setItem('theme', theme)
  }

  return render(ui, { wrapper: AllProviders, ...renderOptions })
}

export * from '@testing-library/react'
