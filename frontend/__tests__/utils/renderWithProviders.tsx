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
  options?: Omit<RenderOptions, 'wrapper'>
) {
  return render(ui, { wrapper: AllProviders, ...options })
}

export * from '@testing-library/react'
