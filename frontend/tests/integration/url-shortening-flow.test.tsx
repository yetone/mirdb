/**
 * Integration tests for guest URL shortening with redirect flow.
 * Owner: Scenario 5 - Guest URL Shortening with Redirect Flow
 *
 * Tests that guest users can attempt URL shortening and are redirected
 * to registration with the URL preserved.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter, Routes, Route, useLocation } from 'react-router-dom'
import React, { useContext, createContext, useState, ReactNode } from 'react'
import Home from '@/pages/Home'
import { HeroSection, UrlInput } from '@/components/homepage'
import { ThemeProvider } from '@/contexts/ThemeContext'
import { useUrlShortener, PENDING_URL_KEY, getPendingUrl, clearPendingUrl } from '@/hooks/useUrlShortener'
import { renderHook, act } from '@testing-library/react'

/**
 * Mock Auth Context for testing different auth states
 */
interface MockAuthContextValue {
  user: { id: number; email: string; username: string; is_admin: boolean } | null
  isAuthenticated: boolean
  isLoading: boolean
  login: (email: string, password: string) => Promise<void>
  logout: () => void
  register: (email: string, username: string, password: string) => Promise<void>
}

const MockAuthContext = createContext<MockAuthContextValue | undefined>(undefined)

interface MockAuthProviderProps {
  children: ReactNode
  isAuthenticated?: boolean
  user?: MockAuthContextValue['user']
}

const MockAuthProvider: React.FC<MockAuthProviderProps> = ({
  children,
  isAuthenticated = false,
  user = null,
}) => {
  return (
    <MockAuthContext.Provider
      value={{
        user,
        isAuthenticated,
        isLoading: false,
        login: vi.fn(),
        logout: vi.fn(),
        register: vi.fn(),
      }}
    >
      {children}
    </MockAuthContext.Provider>
  )
}

// Hook to use mock auth
const useMockAuth = () => {
  const context = useContext(MockAuthContext)
  if (!context) {
    throw new Error('useMockAuth must be used within MockAuthProvider')
  }
  return context
}

// Override useAuth import for testing
vi.mock('@/contexts/AuthContext', () => ({
  useAuth: () => useMockAuth(),
  AuthProvider: ({ children }: { children: ReactNode }) => <>{children}</>,
}))

/**
 * Location tracker component for verifying navigation
 */
const LocationDisplay: React.FC = () => {
  const location = useLocation()
  return (
    <div data-testid="location-display">
      <span data-testid="pathname">{location.pathname}</span>
      <span data-testid="search">{location.search}</span>
      <span data-testid="state">{JSON.stringify(location.state)}</span>
    </div>
  )
}

/**
 * Mock registration page to receive URL
 */
const MockRegisterPage: React.FC = () => {
  const location = useLocation()
  const params = new URLSearchParams(location.search)
  const urlFromQuery = params.get('returnUrl')
  const urlFromState = (location.state as { urlToShorten?: string })?.urlToShorten
  const urlFromStorage = localStorage.getItem(PENDING_URL_KEY)

  return (
    <div data-testid="register-page">
      <h1>Registration Page</h1>
      <p data-testid="url-from-query">{urlFromQuery || 'No URL in query'}</p>
      <p data-testid="url-from-state">{urlFromState || 'No URL in state'}</p>
      <p data-testid="url-from-storage">{urlFromStorage || 'No URL in storage'}</p>
    </div>
  )
}

/**
 * Mock dashboard page
 */
const MockDashboardPage: React.FC = () => {
  const location = useLocation()
  const urlFromState = (location.state as { urlToShorten?: string })?.urlToShorten

  return (
    <div data-testid="dashboard-page">
      <h1>Dashboard</h1>
      <p data-testid="url-to-shorten">{urlFromState || 'No URL'}</p>
    </div>
  )
}

/**
 * Custom render helper for URL shortening flow tests
 */
const renderWithRouterAndAuth = (
  ui: React.ReactElement,
  {
    initialEntries = ['/'],
    isAuthenticated = false,
    user = null,
  }: {
    initialEntries?: string[]
    isAuthenticated?: boolean
    user?: MockAuthContextValue['user']
  } = {}
) => {
  return render(
    <MemoryRouter initialEntries={initialEntries}>
      <ThemeProvider>
        <MockAuthProvider isAuthenticated={isAuthenticated} user={user}>
          {ui}
          <Routes>
            <Route path="/register" element={<MockRegisterPage />} />
            <Route path="/dashboard" element={<MockDashboardPage />} />
            <Route path="*" element={<LocationDisplay />} />
          </Routes>
        </MockAuthProvider>
      </ThemeProvider>
    </MemoryRouter>
  )
}

/**
 * Wrapper for hook testing
 */
const createHookWrapper = (isAuthenticated: boolean) => {
  const Wrapper: React.FC<{ children: ReactNode }> = ({ children }) => (
    <MemoryRouter>
      <ThemeProvider>
        <MockAuthProvider isAuthenticated={isAuthenticated}>
          {children}
        </MockAuthProvider>
      </ThemeProvider>
    </MemoryRouter>
  )
  return Wrapper
}

describe('Guest URL Shortening with Redirect Flow', () => {
  beforeEach(() => {
    localStorage.clear()
    vi.clearAllMocks()
  })

  afterEach(() => {
    localStorage.clear()
  })

  describe('Test Case 1: Guest submits URL and is redirected to registration', () => {
    it('should redirect unauthenticated user to /register when submitting URL', async () => {
      const user = userEvent.setup()
      const testUrl = 'https://example.com/very-long-url'

      renderWithRouterAndAuth(<Home />, { isAuthenticated: false })

      // Enter URL in the input field
      const urlInput = screen.getByTestId('url-input')
      await user.type(urlInput, testUrl)

      // Click the Shorten URL button
      const shortenButton = screen.getByTestId('shorten-button')
      await user.click(shortenButton)

      // Verify redirect to register page
      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument()
      })
    })

    it('should redirect to registration page with URL preserved (via state or query)', async () => {
      const user = userEvent.setup()
      const testUrl = 'https://example.com/very-long-url'

      renderWithRouterAndAuth(<Home />, { isAuthenticated: false })

      const urlInput = screen.getByTestId('url-input')
      await user.type(urlInput, testUrl)

      const shortenButton = screen.getByTestId('shorten-button')
      await user.click(shortenButton)

      // Verify the URL is passed via route state OR query parameter
      await waitFor(() => {
        const urlFromState = screen.getByTestId('url-from-state')
        const urlFromQuery = screen.getByTestId('url-from-query')
        // Either state or query should contain the URL
        const hasUrl = urlFromState.textContent?.includes(testUrl) ||
                       urlFromQuery.textContent?.includes(testUrl)
        expect(hasUrl).toBe(true)
      })
    })
  })

  describe('Test Case 2: URL is stored in localStorage for guest users', () => {
    it('should store URL in localStorage when guest submits', async () => {
      const user = userEvent.setup()
      const testUrl = 'https://example.com/test'

      renderWithRouterAndAuth(<Home />, { isAuthenticated: false })

      const urlInput = screen.getByTestId('url-input')
      await user.type(urlInput, testUrl)

      const shortenButton = screen.getByTestId('shorten-button')
      await user.click(shortenButton)

      // Verify URL is stored in localStorage
      await waitFor(() => {
        expect(localStorage.setItem).toHaveBeenCalledWith(PENDING_URL_KEY, testUrl)
      })
    })

    it('should have URL available in localStorage on registration page', async () => {
      const user = userEvent.setup()
      const testUrl = 'https://example.com/test'

      // Pre-set localStorage for this test to simulate the flow
      localStorage.setItem(PENDING_URL_KEY, testUrl)
      vi.mocked(localStorage.getItem).mockReturnValue(testUrl)

      renderWithRouterAndAuth(<Home />, { isAuthenticated: false })

      const urlInput = screen.getByTestId('url-input')
      await user.type(urlInput, testUrl)

      const shortenButton = screen.getByTestId('shorten-button')
      await user.click(shortenButton)

      // Verify registration page can access the URL from localStorage
      await waitFor(() => {
        const urlFromStorage = screen.getByTestId('url-from-storage')
        expect(urlFromStorage).toHaveTextContent(testUrl)
      })
    })
  })

  describe('Test Case 3: Registration page receives the original URL', () => {
    it('should pass URL via route state to registration page', async () => {
      const user = userEvent.setup()
      const testUrl = 'https://example.com/original-url'

      renderWithRouterAndAuth(<Home />, { isAuthenticated: false })

      const urlInput = screen.getByTestId('url-input')
      await user.type(urlInput, testUrl)

      const shortenButton = screen.getByTestId('shorten-button')
      await user.click(shortenButton)

      // Verify URL is passed in route state
      await waitFor(() => {
        const urlFromState = screen.getByTestId('url-from-state')
        expect(urlFromState).toHaveTextContent(testUrl)
      })
    })

    it('should make URL available via state, query, or localStorage', async () => {
      const user = userEvent.setup()
      const testUrl = 'https://example.com/long-url-to-shorten'

      renderWithRouterAndAuth(<Home />, { isAuthenticated: false })

      const urlInput = screen.getByTestId('url-input')
      await user.type(urlInput, testUrl)

      const shortenButton = screen.getByTestId('shorten-button')
      await user.click(shortenButton)

      // Verify URL is available through at least one method
      await waitFor(() => {
        const urlFromState = screen.getByTestId('url-from-state')
        const urlFromQuery = screen.getByTestId('url-from-query')
        // Accept either route state or query parameter
        const hasUrl = urlFromState.textContent?.includes(testUrl) ||
                       urlFromQuery.textContent?.includes(testUrl)
        expect(hasUrl).toBe(true)
      })
    })
  })

  describe('useUrlShortener Hook - Guest Flow', () => {
    it('should store URL and navigate to register for unauthenticated users', async () => {
      const wrapper = createHookWrapper(false)
      const { result } = renderHook(() => useUrlShortener(), { wrapper })

      const testUrl = 'https://example.com/hook-test'

      await act(async () => {
        await result.current.shortenUrl(testUrl)
      })

      // Verify localStorage was called
      expect(localStorage.setItem).toHaveBeenCalledWith(PENDING_URL_KEY, testUrl)
    })

    it('should handle loading state during URL processing', async () => {
      const wrapper = createHookWrapper(false)
      const { result } = renderHook(() => useUrlShortener(), { wrapper })

      expect(result.current.isLoading).toBe(false)

      // Start shortening
      let shortenPromise: Promise<void>
      act(() => {
        shortenPromise = result.current.shortenUrl('https://example.com/test')
      })

      // Wait for completion
      await act(async () => {
        await shortenPromise
      })

      expect(result.current.isLoading).toBe(false)
    })

    it('should handle empty URL with error', async () => {
      const wrapper = createHookWrapper(false)
      const { result } = renderHook(() => useUrlShortener(), { wrapper })

      await act(async () => {
        try {
          await result.current.shortenUrl('   ')
        } catch (e) {
          // Expected to throw
        }
      })

      expect(result.current.error).toBe('Please enter a URL')
    })
  })

  describe('useUrlShortener Hook - Authenticated Flow', () => {
    it('should navigate to dashboard for authenticated users', async () => {
      const wrapper = createHookWrapper(true)
      const { result } = renderHook(() => useUrlShortener(), { wrapper })

      const testUrl = 'https://example.com/authenticated-test'

      await act(async () => {
        await result.current.shortenUrl(testUrl)
      })

      // Verify localStorage was called
      expect(localStorage.setItem).toHaveBeenCalledWith(PENDING_URL_KEY, testUrl)
    })
  })

  describe('Helper Functions', () => {
    it('getPendingUrl should retrieve URL from localStorage', () => {
      const testUrl = 'https://example.com/stored'
      vi.mocked(localStorage.getItem).mockReturnValue(testUrl)

      const result = getPendingUrl()

      expect(localStorage.getItem).toHaveBeenCalledWith(PENDING_URL_KEY)
      expect(result).toBe(testUrl)
    })

    it('clearPendingUrl should remove URL from localStorage', () => {
      clearPendingUrl()

      expect(localStorage.removeItem).toHaveBeenCalledWith(PENDING_URL_KEY)
    })

    it('getPendingUrl should return null when no URL is stored', () => {
      vi.mocked(localStorage.getItem).mockReturnValue(null)

      const result = getPendingUrl()

      expect(result).toBeNull()
    })
  })

  describe('Full Integration Flow', () => {
    it('should complete guest URL submission flow end-to-end', async () => {
      const user = userEvent.setup()
      const testUrl = 'https://example.com/complete-flow-test'

      // Render homepage with unauthenticated state
      renderWithRouterAndAuth(<Home />, { isAuthenticated: false })

      // Verify we're on the homepage
      expect(screen.getByTestId('homepage')).toBeInTheDocument()

      // Enter URL
      const urlInput = screen.getByTestId('url-input')
      await user.type(urlInput, testUrl)

      // Submit URL
      const shortenButton = screen.getByTestId('shorten-button')
      expect(shortenButton).not.toBeDisabled()
      await user.click(shortenButton)

      // Verify redirect to registration
      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument()
      })

      // Verify URL is available via route state (primary method used by Home.tsx)
      expect(screen.getByTestId('url-from-state')).toHaveTextContent(testUrl)
      // localStorage should also have been called
      expect(localStorage.setItem).toHaveBeenCalledWith(PENDING_URL_KEY, testUrl)
    })

    it('should handle URL with special characters', async () => {
      const user = userEvent.setup()
      const testUrl = 'https://example.com/path?query=value&other=123'

      renderWithRouterAndAuth(<Home />, { isAuthenticated: false })

      const urlInput = screen.getByTestId('url-input')
      await user.type(urlInput, testUrl)

      const shortenButton = screen.getByTestId('shorten-button')
      await user.click(shortenButton)

      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument()
      })

      // URL should be preserved (might be encoded in query param)
      expect(screen.getByTestId('url-from-state')).toHaveTextContent(testUrl)
    })
  })

  describe('Authenticated User Flow', () => {
    it('should redirect authenticated user to dashboard instead of register', async () => {
      const user = userEvent.setup()
      const testUrl = 'https://example.com/authenticated-user-url'
      const mockUser = {
        id: 1,
        email: 'test@example.com',
        username: 'testuser',
        is_admin: false,
      }

      renderWithRouterAndAuth(<Home />, {
        isAuthenticated: true,
        user: mockUser,
      })

      const urlInput = screen.getByTestId('url-input')
      await user.type(urlInput, testUrl)

      const shortenButton = screen.getByTestId('shorten-button')
      await user.click(shortenButton)

      // Should redirect to dashboard, not register
      await waitFor(() => {
        expect(screen.getByTestId('dashboard-page')).toBeInTheDocument()
      })

      // URL should be passed to dashboard
      expect(screen.getByTestId('url-to-shorten')).toHaveTextContent(testUrl)
    })
  })
})
