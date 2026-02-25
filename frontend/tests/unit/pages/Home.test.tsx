/**
 * Home Page Hero Section Tests
 * Owner: Scenario 1 - Hero Section Display
 *
 * Test cases:
 * 1. Hero section displays with product name and tagline
 * 2. URL input field is present with appropriate placeholder text
 * 3. Primary CTA 'Shorten URL' button is visible
 * 4. Secondary CTAs 'Sign Up Free' and 'Log In' buttons are visible
 *
 * Authenticated User Experience Tests (Scenario 15):
 * 1. Personalized greeting is displayed with username
 * 2. Dashboard link is visible for authenticated users
 * 3. Login/Register buttons are replaced with Dashboard/Logout options
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '../../test-utils'
import Home from '../../../src/pages/Home'
import * as AuthContext from '../../../src/contexts/AuthContext'

// Create a spy for useAuth that we can configure per test
const mockLogout = vi.fn()

vi.spyOn(AuthContext, 'useAuth')

describe('Home Page Hero Section', () => {
  // Set up default unauthenticated state for Hero Section tests
  beforeEach(() => {
    vi.mocked(AuthContext.useAuth).mockReturnValue({
      user: null,
      isAuthenticated: false,
      login: vi.fn(),
      logout: mockLogout
    })
  })

  /**
   * Test Case 1: Hero section displays with product name and tagline
   * Input: Render Home component
   * Expected: Hero section displays with product name 'URL Shortening Service'
   *           and tagline 'Shorten Links. Track Insights. Share Smarter.'
   */
  describe('Test Case 1: Hero section with product name and tagline', () => {
    it('displays the hero section', () => {
      render(<Home />)

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()
    })

    it('displays the product name', () => {
      render(<Home />)

      const productName = screen.getByTestId('product-name')
      expect(productName).toBeInTheDocument()
      expect(productName).toHaveTextContent('URL Shortening Service')
    })

    it('displays the tagline', () => {
      render(<Home />)

      const tagline = screen.getByTestId('tagline')
      expect(tagline).toBeInTheDocument()
      expect(tagline).toHaveTextContent('Shorten Links. Track Insights. Share Smarter.')
    })

    it('hero section has background gradient styling', () => {
      render(<Home />)

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toHaveClass('bg-gradient-to-br')
    })
  })

  /**
   * Test Case 2: URL input field is present with appropriate placeholder text
   * Input: Render Home component
   * Expected: URL input field is present with appropriate placeholder text
   */
  describe('Test Case 2: URL input field with placeholder', () => {
    it('displays URL input field', () => {
      render(<Home />)

      const urlInput = screen.getByTestId('url-input')
      expect(urlInput).toBeInTheDocument()
    })

    it('URL input has appropriate placeholder text', () => {
      render(<Home />)

      const urlInput = screen.getByTestId('url-input')
      expect(urlInput).toHaveAttribute('placeholder', 'Enter your long URL here...')
    })

    it('URL input has accessibility label', () => {
      render(<Home />)

      const urlInput = screen.getByTestId('url-input')
      expect(urlInput).toHaveAttribute('aria-label', 'URL input')
    })
  })

  /**
   * Test Case 3: Primary CTA 'Shorten URL' button is visible
   * Input: Render Home component
   * Expected: Primary CTA 'Shorten URL' button is visible
   */
  describe('Test Case 3: Primary CTA Shorten URL button', () => {
    it('displays Shorten URL button', () => {
      render(<Home />)

      const shortenButton = screen.getByTestId('shorten-url-button')
      expect(shortenButton).toBeInTheDocument()
      expect(shortenButton).toHaveTextContent('Shorten URL')
    })

    it('Shorten URL button is a submit button', () => {
      render(<Home />)

      const shortenButton = screen.getByTestId('shorten-url-button')
      expect(shortenButton).toHaveAttribute('type', 'submit')
    })

    it('Shorten URL button is not disabled initially', () => {
      render(<Home />)

      const shortenButton = screen.getByTestId('shorten-url-button')
      expect(shortenButton).not.toBeDisabled()
    })

    it('Shorten URL button has primary styling', () => {
      render(<Home />)

      const shortenButton = screen.getByTestId('shorten-url-button')
      expect(shortenButton).toHaveClass('btn-primary')
    })
  })

  /**
   * Test Case 4: Secondary CTAs 'Sign Up Free' and 'Log In' buttons are visible
   * Input: Render Home component
   * Expected: Secondary CTAs 'Sign Up Free' and 'Log In' buttons are visible
   */
  describe('Test Case 4: Secondary CTA buttons', () => {
    it('displays Sign Up Free button', () => {
      render(<Home />)

      const signupButton = screen.getByTestId('signup-button')
      expect(signupButton).toBeInTheDocument()
      expect(signupButton).toHaveTextContent('Sign Up Free')
    })

    it('Sign Up Free button links to register page', () => {
      render(<Home />)

      const signupButton = screen.getByTestId('signup-button')
      expect(signupButton).toHaveAttribute('href', '/register')
    })

    it('displays Log In button', () => {
      render(<Home />)

      const loginButton = screen.getByTestId('login-button')
      expect(loginButton).toBeInTheDocument()
      expect(loginButton).toHaveTextContent('Log In')
    })

    it('Log In button links to login page', () => {
      render(<Home />)

      const loginButton = screen.getByTestId('login-button')
      expect(loginButton).toHaveAttribute('href', '/login')
    })
  })

  /**
   * Additional: URL form structure test
   */
  describe('URL Form Structure', () => {
    it('displays the URL shortening form container', () => {
      render(<Home />)

      const formContainer = screen.getByTestId('url-form')
      expect(formContainer).toBeInTheDocument()
    })

    it('form container contains input and button', () => {
      render(<Home />)

      const formContainer = screen.getByTestId('url-form')
      const urlInput = screen.getByTestId('url-input')
      const shortenButton = screen.getByTestId('shorten-url-button')

      expect(formContainer).toContainElement(urlInput)
      expect(formContainer).toContainElement(shortenButton)
    })
  })
})

/**
 * Authenticated User Experience Tests
 * Owner: Scenario 15 - Authenticated User Experience
 *
 * Test cases:
 * 1. Personalized greeting is displayed with username
 * 2. Dashboard link is visible for authenticated users
 * 3. Login/Register buttons are replaced with Dashboard/Logout options
 */
describe('Authenticated User Experience', () => {
  const mockUser = {
    id: '1',
    username: 'testuser',
    email: 'testuser@example.com'
  }

  const setAuthenticatedUser = () => {
    vi.mocked(AuthContext.useAuth).mockReturnValue({
      user: mockUser,
      isAuthenticated: true,
      login: vi.fn(),
      logout: mockLogout
    })
  }

  const setUnauthenticatedUser = () => {
    vi.mocked(AuthContext.useAuth).mockReturnValue({
      user: null,
      isAuthenticated: false,
      login: vi.fn(),
      logout: mockLogout
    })
  }

  /**
   * Test Case 1: Personalized greeting is displayed with username
   * Input: Render HomePage with authenticated user context
   * Expected: Personalized greeting is displayed with username
   */
  describe('Test Case 1: Personalized greeting with username', () => {
    it('displays personalized greeting with username when authenticated', () => {
      setAuthenticatedUser()
      render(<Home />)

      const greeting = screen.getByTestId('user-greeting')
      expect(greeting).toBeInTheDocument()
      expect(greeting).toHaveTextContent(`Welcome back, ${mockUser.username}`)
    })

    it('does not display personalized greeting when not authenticated', () => {
      setUnauthenticatedUser()
      render(<Home />)

      const greeting = screen.queryByTestId('user-greeting')
      expect(greeting).not.toBeInTheDocument()
    })
  })

  /**
   * Test Case 2: Dashboard link is visible for authenticated users
   * Input: Render HomePage with authenticated user context
   * Expected: Dashboard link is visible for authenticated users
   */
  describe('Test Case 2: Dashboard link visibility', () => {
    it('displays dashboard link when authenticated', () => {
      setAuthenticatedUser()
      render(<Home />)

      const dashboardLink = screen.getByTestId('dashboard-link')
      expect(dashboardLink).toBeInTheDocument()
      expect(dashboardLink).toHaveAttribute('href', '/dashboard')
    })

    it('dashboard link has proper text', () => {
      setAuthenticatedUser()
      render(<Home />)

      const dashboardLink = screen.getByTestId('dashboard-link')
      expect(dashboardLink).toHaveTextContent('Go to Dashboard')
    })

    it('does not display dashboard link when not authenticated', () => {
      setUnauthenticatedUser()
      render(<Home />)

      const dashboardLink = screen.queryByTestId('dashboard-link')
      expect(dashboardLink).not.toBeInTheDocument()
    })
  })

  /**
   * Test Case 3: Login/Register buttons replaced with Dashboard/Logout
   * Input: Render HomePage with authenticated user context
   * Expected: Login/Register buttons are replaced with Dashboard/Logout options
   */
  describe('Test Case 3: CTA button replacement for authenticated users', () => {
    it('hides Sign Up Free button when authenticated', () => {
      setAuthenticatedUser()
      render(<Home />)

      const signupButton = screen.queryByTestId('signup-button')
      expect(signupButton).not.toBeInTheDocument()
    })

    it('hides Log In button when authenticated', () => {
      setAuthenticatedUser()
      render(<Home />)

      const loginButton = screen.queryByTestId('login-button')
      expect(loginButton).not.toBeInTheDocument()
    })

    it('displays logout button when authenticated', () => {
      setAuthenticatedUser()
      render(<Home />)

      const logoutButton = screen.getByTestId('logout-button')
      expect(logoutButton).toBeInTheDocument()
      expect(logoutButton).toHaveTextContent('Log Out')
    })

    it('displays dashboard button instead of signup when authenticated', () => {
      setAuthenticatedUser()
      render(<Home />)

      const dashboardButton = screen.getByTestId('dashboard-link')
      expect(dashboardButton).toBeInTheDocument()
    })

    it('shows signup and login buttons when not authenticated', () => {
      setUnauthenticatedUser()
      render(<Home />)

      const signupButton = screen.getByTestId('signup-button')
      const loginButton = screen.getByTestId('login-button')
      expect(signupButton).toBeInTheDocument()
      expect(loginButton).toBeInTheDocument()
    })

    it('hides logout button when not authenticated', () => {
      setUnauthenticatedUser()
      render(<Home />)

      const logoutButton = screen.queryByTestId('logout-button')
      expect(logoutButton).not.toBeInTheDocument()
    })
  })
})
