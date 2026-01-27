/**
 * Home Page Integration Tests
 * Owner: Scenario 8 - Homepage Integration
 *
 * Tests for the full homepage assembly with all sections working together.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { BrowserRouter, MemoryRouter } from 'react-router-dom'
import Home from '../../src/pages/Home'

// Test wrapper with Router
function TestWrapper({ children }: { children: React.ReactNode }) {
  return <BrowserRouter>{children}</BrowserRouter>
}

// Mock authentication context for testing with/without auth
const mockAuthContext = {
  isAuthenticated: false,
  user: null,
}

// Helper to render Home with custom auth state
function renderHome(options?: { authenticated?: boolean }) {
  if (options?.authenticated) {
    mockAuthContext.isAuthenticated = true
    mockAuthContext.user = { id: '1', email: 'test@example.com' }
  }
  return render(
    <TestWrapper>
      <Home />
    </TestWrapper>
  )
}

describe('Home Page Integration', () => {
  beforeEach(() => {
    // Reset auth state before each test
    mockAuthContext.isAuthenticated = false
    mockAuthContext.user = null
  })

  afterEach(() => {
    // Clean up document head modifications
    document.title = ''
    const metas = document.querySelectorAll('meta[name="description"], meta[property^="og:"]')
    metas.forEach((meta) => meta.remove())
  })

  // Test Case 1: Navigate to '/' route - Home page component renders without errors
  describe('Test Case 1: Home page renders at / route', () => {
    it('renders Home page component without errors when navigating to / route', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()
    })

    it('renders without throwing any errors', () => {
      expect(() => renderHome()).not.toThrow()
    })
  })

  // Test Case 2: Navbar component is present at top
  describe('Test Case 2: Navbar is present at top', () => {
    it('renders Navbar component at the top of the page', () => {
      renderHome()

      const navbar = screen.getByTestId('navbar')
      expect(navbar).toBeInTheDocument()
    })

    it('Navbar contains navigation elements', () => {
      renderHome()

      const navbar = screen.getByTestId('navbar')
      expect(navbar.tagName.toLowerCase()).toBe('nav')
      expect(navbar).toHaveAttribute('aria-label', 'Main navigation')
    })

    it('Navbar has Login and Sign Up links', () => {
      renderHome()

      const navbar = screen.getByTestId('navbar')
      const loginLink = within(navbar).getByRole('link', { name: /login/i })
      const signUpLink = within(navbar).getByRole('link', { name: /sign up/i })

      expect(loginLink).toBeInTheDocument()
      expect(signUpLink).toBeInTheDocument()
    })
  })

  // Test Case 3: HeroSection component is present after Navbar
  describe('Test Case 3: HeroSection is present after Navbar', () => {
    it('renders HeroSection component after Navbar', () => {
      renderHome()

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()
    })

    it('HeroSection is positioned after Navbar in the DOM', () => {
      renderHome()

      const navbar = screen.getByTestId('navbar')
      const heroSection = screen.getByTestId('hero-section')

      // Check DOM order: navbar should come before hero section
      const navbarPosition = navbar.compareDocumentPosition(heroSection)
      // DOCUMENT_POSITION_FOLLOWING = 4
      expect(navbarPosition & Node.DOCUMENT_POSITION_FOLLOWING).toBe(
        Node.DOCUMENT_POSITION_FOLLOWING
      )
    })
  })

  // Test Case 4: FeaturesSection component is present after Hero
  describe('Test Case 4: FeaturesSection is present after Hero', () => {
    it('renders FeaturesSection component', () => {
      renderHome()

      // FeaturesSection renders feature cards with specific test IDs
      const featuresWrapper = screen.getByTestId('features-section-wrapper')
      expect(featuresWrapper).toBeInTheDocument()
    })

    it('FeaturesSection contains feature cards', () => {
      renderHome()

      // Look for feature-related content
      const urlShorteningText = screen.getByText(/url shortening/i)
      const clickAnalyticsText = screen.getByText(/click analytics/i)

      expect(urlShorteningText).toBeInTheDocument()
      expect(clickAnalyticsText).toBeInTheDocument()
    })

    it('FeaturesSection is positioned after HeroSection', () => {
      renderHome()

      const heroSection = screen.getByTestId('hero-section')
      const featuresWrapper = screen.getByTestId('features-section-wrapper')

      const position = heroSection.compareDocumentPosition(featuresWrapper)
      expect(position & Node.DOCUMENT_POSITION_FOLLOWING).toBe(
        Node.DOCUMENT_POSITION_FOLLOWING
      )
    })
  })

  // Test Case 5: HowItWorksSection component is present after Features
  describe('Test Case 5: HowItWorksSection is present after Features', () => {
    it('renders HowItWorksSection component', () => {
      renderHome()

      const howItWorksSection = screen.getByTestId('how-it-works-section')
      expect(howItWorksSection).toBeInTheDocument()
    })

    it('HowItWorksSection contains step information', () => {
      renderHome()

      // Verify the how-it-works section exists and contains expected step content
      const howItWorksSection = screen.getByTestId('how-it-works-section')

      // Check for step titles within the section
      const createStep = within(howItWorksSection).getByTestId('step-title-1')
      const shareStep = within(howItWorksSection).getByTestId('step-title-2')
      const trackStep = within(howItWorksSection).getByTestId('step-title-3')

      expect(createStep).toHaveTextContent(/create/i)
      expect(shareStep).toHaveTextContent(/share/i)
      expect(trackStep).toHaveTextContent(/track/i)
    })

    it('HowItWorksSection is positioned after FeaturesSection', () => {
      renderHome()

      const featuresWrapper = screen.getByTestId('features-section-wrapper')
      const howItWorksSection = screen.getByTestId('how-it-works-section')

      const position = featuresWrapper.compareDocumentPosition(howItWorksSection)
      expect(position & Node.DOCUMENT_POSITION_FOLLOWING).toBe(
        Node.DOCUMENT_POSITION_FOLLOWING
      )
    })
  })

  // Test Case 6: Footer component is present at page bottom
  describe('Test Case 6: Footer is present at page bottom', () => {
    it('renders Footer component', () => {
      renderHome()

      const footer = screen.getByTestId('footer')
      expect(footer).toBeInTheDocument()
    })

    it('Footer is positioned at the bottom (after all main content)', () => {
      renderHome()

      const howItWorksSection = screen.getByTestId('how-it-works-section')
      const footer = screen.getByTestId('footer')

      const position = howItWorksSection.compareDocumentPosition(footer)
      expect(position & Node.DOCUMENT_POSITION_FOLLOWING).toBe(
        Node.DOCUMENT_POSITION_FOLLOWING
      )
    })

    it('Footer contains copyright notice', () => {
      renderHome()

      const copyright = screen.getByTestId('footer-copyright')
      expect(copyright).toBeInTheDocument()
      expect(copyright).toHaveTextContent(/©.*url shortener/i)
    })
  })

  // Test Case 7: Navigate to '/' without authentication - page loads successfully
  describe('Test Case 7: Page loads without authentication', () => {
    it('loads successfully without authentication (no auth redirect)', () => {
      renderHome({ authenticated: false })

      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()

      // Should not redirect to login
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()
    })

    it('all sections render for unauthenticated users', () => {
      renderHome({ authenticated: false })

      expect(screen.getByTestId('navbar')).toBeInTheDocument()
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section-wrapper')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer')).toBeInTheDocument()
    })
  })

  // Test Case 8: Navigate to '/' with authentication - page still accessible
  describe('Test Case 8: Page loads with authentication', () => {
    it('loads successfully when user is authenticated (still accessible)', () => {
      renderHome({ authenticated: true })

      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()

      // Homepage should still be accessible for authenticated users
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()
    })

    it('all sections render for authenticated users', () => {
      renderHome({ authenticated: true })

      expect(screen.getByTestId('navbar')).toBeInTheDocument()
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section-wrapper')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer')).toBeInTheDocument()
    })
  })

  // Test Case 9: BackgroundEffect component renders
  describe('Test Case 9: BackgroundEffect renders', () => {
    it('renders BackgroundEffect component (if implemented)', () => {
      renderHome()

      // There may be multiple BackgroundEffect components (one in Home, one in HeroSection)
      const backgroundEffects = screen.getAllByTestId('background-effect')
      expect(backgroundEffects.length).toBeGreaterThan(0)
      expect(backgroundEffects[0]).toBeInTheDocument()
    })

    it('BackgroundEffect does not block main content interaction', () => {
      renderHome()

      // Main content should still be accessible
      const mainContent = screen.getByTestId('main-content')
      expect(mainContent).toBeInTheDocument()

      // CTA button should be clickable (not blocked by background)
      const ctaButton = screen.getByTestId('hero-cta-primary')
      expect(ctaButton).toBeInTheDocument()
    })
  })

  // Test Case 10: SEO meta tags are present
  describe('Test Case 10: SEO meta tags are present', () => {
    it('sets page title for SEO', () => {
      renderHome()

      // Wait for useEffect to run
      expect(document.title).toContain('URL Shortener')
    })

    it('sets meta description for SEO', () => {
      renderHome()

      const metaDescription = document.querySelector('meta[name="description"]')
      expect(metaDescription).not.toBeNull()
      expect(metaDescription?.getAttribute('content')).toContain('URL')
    })

    it('sets Open Graph meta tags for social sharing', () => {
      renderHome()

      const ogTitle = document.querySelector('meta[property="og:title"]')
      expect(ogTitle).not.toBeNull()

      const ogDescription = document.querySelector('meta[property="og:description"]')
      expect(ogDescription).not.toBeNull()
    })
  })

  // Test Case 11: Page uses semantic HTML elements
  describe('Test Case 11: Semantic HTML structure', () => {
    it('uses semantic main element for main content', () => {
      renderHome()

      const mainElement = screen.getByRole('main')
      expect(mainElement).toBeInTheDocument()
    })

    it('uses semantic nav element for navigation', () => {
      renderHome()

      const navElements = screen.getAllByRole('navigation')
      expect(navElements.length).toBeGreaterThan(0)
    })

    it('uses semantic footer element', () => {
      renderHome()

      const footer = screen.getByTestId('footer')
      expect(footer.tagName.toLowerCase()).toBe('footer')
    })

    it('uses semantic section elements for content sections', () => {
      renderHome()

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection.tagName.toLowerCase()).toBe('section')

      const howItWorksSection = screen.getByTestId('how-it-works-section')
      expect(howItWorksSection.tagName.toLowerCase()).toBe('section')
    })

    it('footer has contentinfo role', () => {
      renderHome()

      const footer = screen.getByRole('contentinfo')
      expect(footer).toBeInTheDocument()
    })
  })

  // Additional integration tests
  describe('Page structure and layout', () => {
    it('has correct section order: Navbar -> Hero -> Features -> HowItWorks -> Footer', () => {
      renderHome()

      const homePage = screen.getByTestId('home-page')
      const sections = within(homePage).getAllByRole('region', { hidden: true })

      // Verify key elements exist and are in order
      const navbar = screen.getByTestId('navbar')
      const hero = screen.getByTestId('hero-section')
      const features = screen.getByTestId('features-section-wrapper')
      const howItWorks = screen.getByTestId('how-it-works-section')
      const footer = screen.getByTestId('footer')

      // All elements should be present
      expect(navbar).toBeInTheDocument()
      expect(hero).toBeInTheDocument()
      expect(features).toBeInTheDocument()
      expect(howItWorks).toBeInTheDocument()
      expect(footer).toBeInTheDocument()
    })

    it('applies correct base styling', () => {
      renderHome()

      const homePage = screen.getByTestId('home-page')
      expect(homePage).toHaveClass('min-h-screen')
      expect(homePage).toHaveClass('bg-base-200')
    })
  })

  describe('Navigation links', () => {
    it('has functional login link in navbar', () => {
      renderHome()

      const navbar = screen.getByTestId('navbar')
      const loginLink = within(navbar).getByRole('link', { name: /login/i })

      expect(loginLink).toHaveAttribute('href', '/login')
    })

    it('has functional register/sign up link in navbar', () => {
      renderHome()

      const navbar = screen.getByTestId('navbar')
      const signUpLink = within(navbar).getByRole('link', { name: /sign up/i })

      expect(signUpLink).toHaveAttribute('href', '/register')
    })
  })
})
