import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import App from './App'

/**
 * Integration tests for Unauthenticated User Access - REQ-6
 * Verifies that the homepage loads and renders content without requiring authentication
 */
describe('Unauthenticated User Access - REQ-6', () => {
  beforeEach(() => {
    // Clear any stored auth state
    localStorage.clear()
    sessionStorage.clear()
    // Clear any cookies (simulated)
    document.cookie.split(';').forEach(cookie => {
      const name = cookie.split('=')[0].trim()
      document.cookie = `${name}=;expires=Thu, 01 Jan 1970 00:00:00 GMT;path=/`
    })
  })

  // Test Case 3: Verify no authentication API calls on initial load
  it('TC3: Page renders without making authentication API calls', async () => {
    // Mock fetch to track any auth-related calls
    const fetchCalls: string[] = []
    const originalFetch = global.fetch

    global.fetch = vi.fn((url: RequestInfo | URL) => {
      const urlString = url.toString().toLowerCase()
      fetchCalls.push(urlString)

      // Check for auth-related calls
      const isAuthCall =
        urlString.includes('/auth') ||
        urlString.includes('/login') ||
        urlString.includes('/session') ||
        urlString.includes('/token') ||
        urlString.includes('/user') ||
        urlString.includes('/me') ||
        urlString.includes('/whoami') ||
        urlString.includes('/verify')

      if (isAuthCall) {
        throw new Error(`Unexpected auth API call: ${urlString}`)
      }

      return Promise.resolve(new Response('{}', { status: 200 }))
    }) as typeof fetch

    render(
      <BrowserRouter>
        <App />
      </BrowserRouter>
    )

    // Verify homepage content renders without auth
    expect(screen.getByText(/Welcome to MirDB/i)).toBeInTheDocument()

    // Verify no auth-related fetch calls were made
    const authCalls = fetchCalls.filter(
      url =>
        url.includes('/auth') ||
        url.includes('/login') ||
        url.includes('/session') ||
        url.includes('/token') ||
        url.includes('/user') ||
        url.includes('/me')
    )
    expect(authCalls).toHaveLength(0)

    global.fetch = originalFetch
  })

  it('Homepage renders all main sections without authentication', () => {
    render(
      <BrowserRouter>
        <App />
      </BrowserRouter>
    )

    // Verify main Navigation is rendered (using aria-label to be specific)
    const mainNav = screen.getByRole('navigation', { name: /main navigation/i })
    expect(mainNav).toBeInTheDocument()

    // Verify Hero section content is rendered
    expect(screen.getByText(/Welcome to MirDB/i)).toBeInTheDocument()
    expect(screen.getByText(/high-performance persistent key-value store/i)).toBeInTheDocument()

    // Verify CTA button is rendered and accessible
    const ctaLink = screen.getByRole('link', { name: /Get Started/i })
    expect(ctaLink).toBeInTheDocument()
    expect(ctaLink).toHaveAttribute('href', '#getting-started')

    // Verify Footer is rendered
    const footer = screen.getByRole('contentinfo')
    expect(footer).toBeInTheDocument()
  })

  it('App does not require authentication context or provider', () => {
    // Render App without any auth providers - should work fine
    const { container } = render(
      <BrowserRouter>
        <App />
      </BrowserRouter>
    )

    // Should render successfully without errors
    expect(container.querySelector('.app')).toBeInTheDocument()

    // Main content should be present
    const main = container.querySelector('main')
    expect(main).toBeInTheDocument()
  })

  it('Homepage loads with cleared localStorage and sessionStorage', () => {
    // Ensure storage is cleared
    localStorage.clear()
    sessionStorage.clear()

    render(
      <BrowserRouter>
        <App />
      </BrowserRouter>
    )

    // Should still render without issues
    expect(screen.getByText(/Welcome to MirDB/i)).toBeInTheDocument()
  })

  it('Homepage is accessible without any tokens in storage', () => {
    // Specifically check for common auth token keys
    localStorage.removeItem('token')
    localStorage.removeItem('authToken')
    localStorage.removeItem('accessToken')
    localStorage.removeItem('refreshToken')
    sessionStorage.removeItem('token')
    sessionStorage.removeItem('authToken')

    render(
      <BrowserRouter>
        <App />
      </BrowserRouter>
    )

    // Homepage should be fully accessible
    const heroHeadline = screen.getByTestId('hero-headline')
    expect(heroHeadline).toBeInTheDocument()
    expect(heroHeadline).toHaveTextContent(/Welcome to MirDB/i)
  })
})
