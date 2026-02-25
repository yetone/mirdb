/**
 * Custom Test Utilities
 * Owner: First builder to run
 *
 * Custom render function with providers for testing.
 */

import { ReactElement, ReactNode, createContext, useContext, useState } from 'react'
import { render, RenderOptions } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { ThemeProvider } from '../src/contexts/ThemeContext'
import { AuthProvider } from '../src/contexts/AuthContext'

const createTestQueryClient = () =>
  new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
        gcTime: 0,
      },
      mutations: {
        retry: false,
      },
    },
  })

interface WrapperProps {
  children: ReactNode
}

interface User {
  id: string
  username: string
  email: string
}

interface AuthContextType {
  user: User | null
  isAuthenticated: boolean
  login: (user: User) => void
  logout: () => void
}

// Create a test-specific AuthContext that mirrors the real one
export const TestAuthContext = createContext<AuthContextType | undefined>(undefined)

export function useTestAuth() {
  const context = useContext(TestAuthContext)
  if (!context) {
    throw new Error('useTestAuth must be used within a TestAuthProvider')
  }
  return context
}

interface TestAuthProviderProps {
  children: ReactNode
  initialUser?: User | null
}

export function TestAuthProvider({ children, initialUser = null }: TestAuthProviderProps) {
  const [user, setUser] = useState<User | null>(initialUser)

  const login = (newUser: User) => {
    setUser(newUser)
  }

  const logout = () => {
    setUser(null)
  }

  return (
    <TestAuthContext.Provider value={{
      user,
      isAuthenticated: !!user,
      login,
      logout
    }}>
      {children}
    </TestAuthContext.Provider>
  )
}

interface CustomRenderOptions extends Omit<RenderOptions, 'wrapper'> {
  authUser?: User | null
}

function createWrapper(authUser?: User | null) {
  return function Wrapper({ children }: WrapperProps) {
    const queryClient = createTestQueryClient()

    // When authUser is explicitly provided (including null), use TestAuthProvider
    // Otherwise, use the real AuthProvider for default behavior
    if (authUser !== undefined) {
      return (
        <QueryClientProvider client={queryClient}>
          <BrowserRouter>
            <ThemeProvider>
              <TestAuthProvider initialUser={authUser}>
                {children}
              </TestAuthProvider>
            </ThemeProvider>
          </BrowserRouter>
        </QueryClientProvider>
      )
    }

    return (
      <QueryClientProvider client={queryClient}>
        <BrowserRouter>
          <ThemeProvider>
            <AuthProvider>
              {children}
            </AuthProvider>
          </ThemeProvider>
        </BrowserRouter>
      </QueryClientProvider>
    )
  }
}

function customRender(
  ui: ReactElement,
  options?: CustomRenderOptions
) {
  const { authUser, ...renderOptions } = options || {}
  return render(ui, { wrapper: createWrapper(authUser), ...renderOptions })
}

export * from '@testing-library/react'
export { customRender as render }
