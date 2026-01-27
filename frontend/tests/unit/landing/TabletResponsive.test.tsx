/**
 * Tablet Responsive Design Tests
 * Owner: Scenario 7 - Responsive Design - Tablet
 *
 * Tests for tablet viewport (768px-1023px):
 * 1. No horizontal overflow on the page at 768px
 * 2. Feature cards display in 2-column grid layout
 * 3. Full navigation is visible without hamburger menu
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { renderWithProviders, screen, within } from './setup.tsx'
import Home from '../../../src/pages/Home'
import FeaturesSection from '../../../src/components/landing/FeaturesSection'

/**
 * Sets viewport dimensions for testing
 * Note: Since JSDOM doesn't fully support CSS media queries,
 * we verify the presence of responsive classes that Tailwind uses
 */
function setTabletViewport() {
  Object.defineProperty(window, 'innerWidth', {
    writable: true,
    configurable: true,
    value: 768,
  })
  Object.defineProperty(window, 'innerHeight', {
    writable: true,
    configurable: true,
    value: 1024,
  })
  window.dispatchEvent(new Event('resize'))
}

describe('Tablet Responsive Design (768px viewport)', () => {
  beforeEach(() => {
    setTabletViewport()
  })

  afterEach(() => {
    // Reset viewport to default
    Object.defineProperty(window, 'innerWidth', {
      writable: true,
      configurable: true,
      value: 1024,
    })
    Object.defineProperty(window, 'innerHeight', {
      writable: true,
      configurable: true,
      value: 768,
    })
  })

  /**
   * Test Case 1: No horizontal overflow at 768px viewport width
   * Verifies that the landing page content fits within the tablet viewport
   */
  describe('Test Case 1: No horizontal overflow at 768px viewport', () => {
    it('renders LandingPage without horizontal overflow', () => {
      const { container } = renderWithProviders(<Home />)

      // The main container should have min-h-screen and relative positioning
      const mainWrapper = container.firstChild as HTMLElement
      expect(mainWrapper).toHaveClass('min-h-screen')
      expect(mainWrapper).toHaveClass('relative')

      // No overflow-x-auto or overflow-x-scroll should be needed
      // Page content should fit within viewport
      expect(mainWrapper).not.toHaveStyle({ overflowX: 'scroll' })
    })

    it('main content areas use responsive padding for tablet', () => {
      renderWithProviders(<Home />)

      // Hero section should have responsive padding classes
      // The hero section is labeled by its heading "Shorten Links. Track Everything."
      const heroSection = screen.getByRole('region', { name: /shorten links/i })
      expect(heroSection).toHaveClass('px-4')

      // Features section should have responsive padding
      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toHaveClass('px-4')
    })

    it('content max-width constraints are applied', () => {
      renderWithProviders(<Home />)

      // Check that the features section has a max-width constraint
      const featuresSection = screen.getByTestId('features-section')
      const innerContainer = featuresSection.querySelector('.max-w-7xl')
      expect(innerContainer).toBeInTheDocument()
    })
  })

  /**
   * Test Case 2: Feature cards display in 2-column grid at tablet viewport
   * Verifies the responsive grid layout adapts for medium screens
   */
  describe('Test Case 2: Feature cards 2-column grid layout', () => {
    it('features grid has responsive column classes for tablet', () => {
      renderWithProviders(<FeaturesSection />)

      const featuresGrid = screen.getByTestId('features-grid')

      // Grid should have md:grid-cols-2 class for tablet (768px+)
      expect(featuresGrid).toHaveClass('grid')
      expect(featuresGrid).toHaveClass('grid-cols-1')
      expect(featuresGrid).toHaveClass('md:grid-cols-2')
    })

    it('feature cards are present and properly structured for grid layout', () => {
      renderWithProviders(<FeaturesSection />)

      const featureCards = screen.getAllByTestId('feature-card')

      // Should have 4 cards that will display in 2-column grid
      expect(featureCards).toHaveLength(4)

      // Each card should be a direct child of the grid container
      const featuresGrid = screen.getByTestId('features-grid')
      const gridChildren = featuresGrid.children
      expect(gridChildren.length).toBe(4)
    })

    it('feature cards have h-full class for equal height in grid', () => {
      const { container } = renderWithProviders(<FeaturesSection />)

      // GlassMorphismCard wrapper should have h-full for equal height rows
      const cardWrappers = container.querySelectorAll('[data-testid="features-grid"] > div')

      cardWrappers.forEach((wrapper) => {
        expect(wrapper).toHaveClass('h-full')
      })
    })

    it('grid gap is appropriate for tablet spacing', () => {
      renderWithProviders(<FeaturesSection />)

      const featuresGrid = screen.getByTestId('features-grid')
      expect(featuresGrid).toHaveClass('gap-6')
    })
  })

  /**
   * Test Case 3: Full navigation visible without hamburger menu
   * Verifies that navigation links are visible at tablet viewport
   */
  describe('Test Case 3: Full navigation without hamburger menu', () => {
    it('navigation header is visible', () => {
      renderWithProviders(<Home />)

      const header = screen.getByRole('banner')
      expect(header).toBeInTheDocument()
      expect(header).toHaveClass('navbar')
    })

    it('login link is directly visible (not in mobile menu)', () => {
      renderWithProviders(<Home />)

      const loginLink = screen.getByRole('link', { name: /login/i })
      expect(loginLink).toBeInTheDocument()
      expect(loginLink).toBeVisible()

      // Login should not be hidden or have mobile-only visibility classes
      expect(loginLink).not.toHaveClass('hidden')
      expect(loginLink).not.toHaveClass('md:hidden')
    })

    it('register/get started button is directly visible', () => {
      renderWithProviders(<Home />)

      const registerLink = screen.getByRole('link', { name: /get started/i })
      expect(registerLink).toBeInTheDocument()
      expect(registerLink).toBeVisible()
      expect(registerLink).toHaveClass('btn-primary')
    })

    it('logo/brand link is visible', () => {
      renderWithProviders(<Home />)

      const logoLink = screen.getByRole('link', { name: /url shortener/i })
      expect(logoLink).toBeInTheDocument()
      expect(logoLink).toBeVisible()
    })

    it('navigation does not use hamburger menu pattern', () => {
      const { container } = renderWithProviders(<Home />)

      // Should not have dropdown or mobile menu toggle elements
      const dropdownToggle = container.querySelector('.dropdown-toggle')
      const hamburgerMenu = container.querySelector('[data-testid="hamburger-menu"]')
      const mobileMenuButton = container.querySelector('button[aria-label*="menu"]')

      expect(dropdownToggle).not.toBeInTheDocument()
      expect(hamburgerMenu).not.toBeInTheDocument()
      expect(mobileMenuButton).not.toBeInTheDocument()
    })

    it('navbar uses flexbox layout for horizontal items', () => {
      renderWithProviders(<Home />)

      const header = screen.getByRole('banner')

      // Navbar should use DaisyUI's navbar class which provides flex layout
      expect(header).toHaveClass('navbar')

      // Check navbar-start and navbar-end for proper layout structure
      const navbarStart = header.querySelector('.navbar-start')
      const navbarEnd = header.querySelector('.navbar-end')

      expect(navbarStart).toBeInTheDocument()
      expect(navbarEnd).toBeInTheDocument()
    })

    it('theme toggle is visible in navigation', () => {
      const { container } = renderWithProviders(<Home />)

      // Theme toggle should be present in the navbar
      const header = screen.getByRole('banner')
      const navbarEnd = header.querySelector('.navbar-end')

      expect(navbarEnd).toBeInTheDocument()

      // Check that the navbar-end area contains the theme toggle and nav links
      const buttons = navbarEnd?.querySelectorAll('.btn, button')
      expect(buttons?.length).toBeGreaterThanOrEqual(2) // Theme toggle + login + get started
    })
  })

  /**
   * Additional tablet-specific layout tests
   */
  describe('Additional tablet layout verification', () => {
    it('hero section text is responsive for tablet', () => {
      renderWithProviders(<Home />)

      const heading = screen.getByRole('heading', { level: 1 })

      // Hero heading should have responsive font size classes
      // text-4xl md:text-5xl lg:text-6xl
      expect(heading).toHaveClass('text-4xl')
      expect(heading).toHaveClass('md:text-5xl')
    })

    it('hero CTA buttons layout adapts for tablet', () => {
      const { container } = renderWithProviders(<Home />)

      // Button container should use flex with sm:flex-row for tablet+
      const ctaContainer = container.querySelector('.flex.flex-col.sm\\:flex-row')
      expect(ctaContainer).toBeInTheDocument()
    })

    it('features section header is centered', () => {
      renderWithProviders(<FeaturesSection />)

      const heading = screen.getByRole('heading', { level: 2, name: /features/i })
      const headingContainer = heading.parentElement

      expect(headingContainer).toHaveClass('text-center')
    })

    it('page footer is visible and properly positioned', () => {
      renderWithProviders(<Home />)

      const footer = screen.getByRole('contentinfo')
      expect(footer).toBeInTheDocument()
      expect(footer).toHaveClass('footer')
    })
  })
})
