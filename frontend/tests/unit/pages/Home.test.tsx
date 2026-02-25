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

/**
 * UI Consistency with Design System Tests
 * Owner: Scenario 18 - UI Consistency with Design System
 *
 * Test cases:
 * 1. HomePage uses existing design system components (HeroSection, FeaturesSection, etc.)
 * 2. HomePage uses Tailwind CSS utility classes and DaisyUI components
 * 3. Navigation component is consistent across pages (tested via integration test)
 *
 * This scenario validates NFR-4: Homepage must maintain consistency with
 * existing UI components and design tokens.
 */
describe('UI Consistency with Design System', () => {
  beforeEach(() => {
    vi.mocked(AuthContext.useAuth).mockReturnValue({
      user: null,
      isAuthenticated: false,
      login: vi.fn(),
      logout: vi.fn()
    })
  })

  /**
   * Test Case 1: HomePage uses existing design system components
   * Input: Inspect HomePage component imports
   * Expected: Uses existing design system components (HeroSection, FeaturesSection, etc.)
   */
  describe('Test Case 1: Design system component usage', () => {
    it('renders HeroSection component from homepage components', () => {
      render(<Home />)

      // HeroSection should be present with its characteristic testid
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()
    })

    it('renders FeaturesSection component from homepage components', () => {
      render(<Home />)

      // FeaturesSection should be present with its characteristic testid
      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()
    })

    it('renders HowItWorksSection component from homepage components', () => {
      render(<Home />)

      // HowItWorksSection should be present with its characteristic testid
      const howItWorksSection = screen.getByTestId('how-it-works-section')
      expect(howItWorksSection).toBeInTheDocument()
    })

    it('renders Footer component from shared components', () => {
      render(<Home />)

      // Footer should be present with its characteristic testid
      const footer = screen.getByTestId('footer')
      expect(footer).toBeInTheDocument()
    })

    it('renders UrlShortenForm within HeroSection', () => {
      render(<Home />)

      // UrlShortenForm should be present within the url-form container
      const urlForm = screen.getByTestId('url-form')
      const urlInput = screen.getByTestId('url-input')
      const shortenButton = screen.getByTestId('shorten-url-button')

      expect(urlForm).toBeInTheDocument()
      expect(urlInput).toBeInTheDocument()
      expect(shortenButton).toBeInTheDocument()
    })
  })

  /**
   * Test Case 2: HomePage uses Tailwind CSS utility classes and DaisyUI components
   * Input: Inspect HomePage styling
   * Expected: Uses Tailwind CSS utility classes and DaisyUI components
   */
  describe('Test Case 2: Tailwind CSS and DaisyUI styling', () => {
    it('root container uses Tailwind min-h-screen class', () => {
      render(<Home />)

      // Main wrapper should have min-h-screen for full viewport height
      const mainWrapper = screen.getByTestId('hero-section').closest('div')
      expect(mainWrapper).toHaveClass('min-h-screen')
    })

    it('root container uses DaisyUI base-100 background class', () => {
      render(<Home />)

      // Main wrapper should have bg-base-100 for theme-aware background
      const mainWrapper = screen.getByTestId('hero-section').closest('div')
      expect(mainWrapper).toHaveClass('bg-base-100')
    })

    it('hero section uses DaisyUI hero component classes', () => {
      render(<Home />)

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toHaveClass('hero')
    })

    it('hero section uses Tailwind gradient classes', () => {
      render(<Home />)

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toHaveClass('bg-gradient-to-br')
    })

    it('buttons use DaisyUI btn component classes', () => {
      render(<Home />)

      const shortenButton = screen.getByTestId('shorten-url-button')
      expect(shortenButton).toHaveClass('btn')
      expect(shortenButton).toHaveClass('btn-primary')

      const loginButton = screen.getByTestId('login-button')
      expect(loginButton).toHaveClass('btn')
      expect(loginButton).toHaveClass('btn-outline')
    })

    it('inputs use DaisyUI input component classes', () => {
      render(<Home />)

      const urlInput = screen.getByTestId('url-input')
      expect(urlInput).toHaveClass('input')
      expect(urlInput).toHaveClass('input-bordered')
    })

    it('features section uses Tailwind responsive grid classes', () => {
      render(<Home />)

      const featuresGrid = screen.getByTestId('features-grid')
      expect(featuresGrid).toHaveClass('grid')
      expect(featuresGrid).toHaveClass('grid-cols-1')
      expect(featuresGrid).toHaveClass('md:grid-cols-3')
    })

    it('feature cards use DaisyUI card component classes', () => {
      render(<Home />)

      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards.length).toBeGreaterThan(0)
      featureCards.forEach((card) => {
        expect(card).toHaveClass('card')
        expect(card).toHaveClass('bg-base-100')
        expect(card).toHaveClass('shadow-xl')
      })
    })

    it('footer uses DaisyUI footer component classes', () => {
      render(<Home />)

      const footer = screen.getByTestId('footer')
      expect(footer).toHaveClass('footer')
      expect(footer).toHaveClass('footer-center')
    })

    it('footer uses Tailwind background and text classes', () => {
      render(<Home />)

      const footer = screen.getByTestId('footer')
      expect(footer).toHaveClass('bg-base-200')
      expect(footer).toHaveClass('text-base-content')
    })

    it('sections use consistent padding with Tailwind classes', () => {
      render(<Home />)

      const featuresSection = screen.getByTestId('features-section')
      const howItWorksSection = screen.getByTestId('how-it-works-section')

      // Both sections should use consistent py-16 padding
      expect(featuresSection).toHaveClass('py-16')
      expect(howItWorksSection).toHaveClass('py-16')
    })

    it('text elements use Tailwind typography classes', () => {
      render(<Home />)

      const productName = screen.getByTestId('product-name')
      expect(productName).toHaveClass('font-bold')

      // Check for responsive text sizing
      expect(productName.className).toMatch(/text-\d+xl/)
    })

    it('step cards use Tailwind flex layout classes', () => {
      render(<Home />)

      const stepCards = screen.getAllByTestId('step-card')
      expect(stepCards.length).toBe(3)
      stepCards.forEach((card) => {
        expect(card).toHaveClass('flex')
        expect(card).toHaveClass('flex-col')
        expect(card).toHaveClass('items-center')
      })
    })

    it('step numbers use DaisyUI primary color classes', () => {
      render(<Home />)

      const stepNumbers = screen.getAllByTestId('step-number')
      expect(stepNumbers.length).toBe(3)
      stepNumbers.forEach((num) => {
        expect(num).toHaveClass('bg-primary')
        expect(num).toHaveClass('text-primary-content')
      })
    })

    it('does not use inline styles for layout', () => {
      render(<Home />)

      // Main sections should not have inline styles for layout
      const heroSection = screen.getByTestId('hero-section')
      const featuresSection = screen.getByTestId('features-section')
      const howItWorksSection = screen.getByTestId('how-it-works-section')
      const footer = screen.getByTestId('footer')

      // Check that style attribute is either not present or empty
      expect(heroSection).not.toHaveAttribute('style')
      expect(featuresSection).not.toHaveAttribute('style')
      expect(howItWorksSection).not.toHaveAttribute('style')
      expect(footer).not.toHaveAttribute('style')
    })

    it('uses DaisyUI join class for input-button grouping', () => {
      render(<Home />)

      const urlInput = screen.getByTestId('url-input')
      const shortenButton = screen.getByTestId('shorten-url-button')

      // Both should have join-item class for grouped appearance
      expect(urlInput).toHaveClass('join-item')
      expect(shortenButton).toHaveClass('join-item')
    })

    it('links use DaisyUI link styling classes', () => {
      render(<Home />)

      const footerLinks = screen.getByTestId('footer-links')
      const links = footerLinks.querySelectorAll('a')

      links.forEach((link) => {
        expect(link).toHaveClass('link')
        expect(link).toHaveClass('link-hover')
      })
    })
  })

  /**
   * Test Case 3: Navigation component is consistent across pages
   * Input: Compare header on homepage vs dashboard
   * Expected: Navigation component is consistent across pages
   *
   * Note: Full integration test in Navigation.integration.test.tsx
   * This test validates that HomePage includes proper navigation elements
   */
  describe('Test Case 3: Navigation consistency', () => {
    it('homepage includes navigation links to login and register', () => {
      render(<Home />)

      const loginLink = screen.getByTestId('login-button')
      const signupLink = screen.getByTestId('signup-button')

      expect(loginLink).toHaveAttribute('href', '/login')
      expect(signupLink).toHaveAttribute('href', '/register')
    })

    it('navigation links use consistent DaisyUI btn styling', () => {
      render(<Home />)

      const loginLink = screen.getByTestId('login-button')
      const signupLink = screen.getByTestId('signup-button')

      // Both should be styled as buttons
      expect(loginLink).toHaveClass('btn')
      expect(signupLink).toHaveClass('btn')

      // Should use large button variant for consistency
      expect(loginLink).toHaveClass('btn-lg')
      expect(signupLink).toHaveClass('btn-lg')
    })

    it('dashboard link uses consistent styling when authenticated', () => {
      vi.mocked(AuthContext.useAuth).mockReturnValue({
        user: { id: '1', username: 'test', email: 'test@example.com' },
        isAuthenticated: true,
        login: vi.fn(),
        logout: vi.fn()
      })

      render(<Home />)

      const dashboardLink = screen.getByTestId('dashboard-link')
      expect(dashboardLink).toHaveAttribute('href', '/dashboard')
      expect(dashboardLink).toHaveClass('btn')
      expect(dashboardLink).toHaveClass('btn-lg')
    })

    it('footer navigation uses consistent link styling', () => {
      render(<Home />)

      const footerAbout = screen.getByTestId('footer-link-about')
      const footerPrivacy = screen.getByTestId('footer-link-privacy-policy')
      const footerTerms = screen.getByTestId('footer-link-terms-of-service')

      // All footer links should use consistent styling
      ;[footerAbout, footerPrivacy, footerTerms].forEach((link) => {
        expect(link).toHaveClass('link')
        expect(link).toHaveClass('link-hover')
        expect(link).toHaveClass('hover:text-primary')
        expect(link).toHaveClass('transition-colors')
      })
    })

    it('social links in footer use consistent styling', () => {
      render(<Home />)

      const twitterLink = screen.getByTestId('footer-social-twitter')
      const githubLink = screen.getByTestId('footer-social-github')
      const linkedinLink = screen.getByTestId('footer-social-linkedin')

      ;[twitterLink, githubLink, linkedinLink].forEach((link) => {
        expect(link).toHaveClass('link')
        expect(link).toHaveClass('link-hover')
        expect(link).toHaveClass('hover:text-primary')
      })
    })
  })

  /**
   * Additional Design System Compliance Tests
   */
  describe('Additional Design System Compliance', () => {
    it('uses semantic HTML elements appropriately', () => {
      render(<Home />)

      // Check for semantic main element
      const main = document.querySelector('main')
      expect(main).toBeInTheDocument()

      // Check for semantic footer element
      const footer = screen.getByTestId('footer')
      expect(footer.tagName.toLowerCase()).toBe('footer')

      // Check for semantic section elements
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection.tagName.toLowerCase()).toBe('section')
    })

    it('uses consistent container widths', () => {
      render(<Home />)

      // Features section should have container with max-width
      const featuresSection = screen.getByTestId('features-section')
      const featuresContainer = featuresSection.querySelector('.container')
      expect(featuresContainer).toHaveClass('max-w-6xl')

      // How it works section should have container with max-width
      const howItWorksSection = screen.getByTestId('how-it-works-section')
      const howItWorksContainer = howItWorksSection.querySelector('.container')
      expect(howItWorksContainer).toHaveClass('max-w-4xl')
    })

    it('uses DaisyUI color tokens for theming', () => {
      render(<Home />)

      // Check that primary color is used appropriately
      const shortenButton = screen.getByTestId('shorten-url-button')
      expect(shortenButton).toHaveClass('btn-primary')

      // Check for base color usage
      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toHaveClass('bg-base-200')

      const howItWorksSection = screen.getByTestId('how-it-works-section')
      expect(howItWorksSection).toHaveClass('bg-base-100')
    })

    it('uses responsive classes for mobile-first design', () => {
      render(<Home />)

      // Check for md: breakpoint classes
      const featuresGrid = screen.getByTestId('features-grid')
      expect(featuresGrid.className).toMatch(/md:grid-cols-3/)

      // Check for responsive text sizing on product name
      const productName = screen.getByTestId('product-name')
      expect(productName.className).toMatch(/md:text-6xl/)
    })

    it('uses Tailwind gap classes for consistent spacing', () => {
      render(<Home />)

      const featuresGrid = screen.getByTestId('features-grid')
      expect(featuresGrid).toHaveClass('gap-8')
    })

    it('icons use consistent sizing classes', () => {
      render(<Home />)

      const featureIcons = screen.getAllByTestId('feature-icon')
      featureIcons.forEach((iconContainer) => {
        expect(iconContainer).toHaveClass('text-primary')
        const svg = iconContainer.querySelector('svg')
        expect(svg).toHaveClass('h-12')
        expect(svg).toHaveClass('w-12')
      })
    })
  })
})
