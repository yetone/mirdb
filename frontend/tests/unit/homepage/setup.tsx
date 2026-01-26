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

/**
 * SEO Meta Tags Setup
 * Owner: Scenario 20 - SEO and Meta Tags
 *
 * Set up meta tags in the JSDOM environment to simulate what the browser sees
 * from index.html. These tags are necessary for SEO testing.
 */
function setupSEOMetaTags() {
  // Set document title
  document.title = 'URL Shortener - Shorten. Share. Track.'

  // Add viewport meta tag
  const viewport = document.createElement('meta')
  viewport.setAttribute('name', 'viewport')
  viewport.setAttribute('content', 'width=device-width, initial-scale=1.0')
  document.head.appendChild(viewport)

  // Add meta description
  const description = document.createElement('meta')
  description.setAttribute('name', 'description')
  description.setAttribute('content', 'Shorten, share, and track your URLs with our powerful URL shortening service')
  document.head.appendChild(description)

  // Add Open Graph title
  const ogTitle = document.createElement('meta')
  ogTitle.setAttribute('property', 'og:title')
  ogTitle.setAttribute('content', 'URL Shortener - Shorten. Share. Track.')
  document.head.appendChild(ogTitle)

  // Add Open Graph description
  const ogDescription = document.createElement('meta')
  ogDescription.setAttribute('property', 'og:description')
  ogDescription.setAttribute('content', 'Shorten, share, and track your URLs with our powerful URL shortening service. Get detailed analytics, manage your links, and share them easily.')
  document.head.appendChild(ogDescription)

  // Add Open Graph type
  const ogType = document.createElement('meta')
  ogType.setAttribute('property', 'og:type')
  ogType.setAttribute('content', 'website')
  document.head.appendChild(ogType)
}

// Run SEO setup once at module load time
setupSEOMetaTags()

import { ReactElement, ReactNode } from 'react'
import { render, RenderOptions } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import { ThemeProvider } from '../../../src/contexts/ThemeContext'
import { AuthProvider } from '../../../src/contexts/AuthContext'
import '@testing-library/jest-dom'

interface WrapperProps {
  children: ReactNode
}

function AllTheProviders({ children }: WrapperProps) {
  return (
    <BrowserRouter>
      <ThemeProvider>
        <AuthProvider>{children}</AuthProvider>
      </ThemeProvider>
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
