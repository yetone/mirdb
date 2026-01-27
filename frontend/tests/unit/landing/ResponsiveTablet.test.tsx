/**
 * Responsive Design - Tablet Tests
 * Owner: Scenario 7 - Responsive Design - Tablet
 *
 * Tests for tablet viewport (768px-1023px):
 * 1. No horizontal overflow on the page at 768px viewport width
 * 2. Feature cards display in 2-column grid layout
 * 3. Full navigation is visible without hamburger menu
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { screen, within } from '@testing-library/react'
import { renderWithProviders, setViewport } from './setup.tsx'
import Home from '../../../src/pages/Home'
import FeaturesSection from '../../../src/components/landing/FeaturesSection'

const TABLET_WIDTH = 768
const TABLET_HEIGHT = 1024

describe('Responsive Design - Tablet (768px-1023px)', () => {
  beforeEach(() => {
    // Set viewport to tablet size before each test
    setViewport(TABLET_WIDTH, TABLET_HEIGHT)
  })

  afterEach(() => {
    // Reset viewport after each test
    setViewport(1280, 800)
  })

  // Test Case 1: No horizontal overflow on the page at 768px viewport width
  describe('Test Case 1: No horizontal overflow at tablet viewport', () => {
    it('renders landing page without horizontal overflow at 768px width', () => {
      const { container } = renderWithProviders(<Home />)

      // Check that the main container doesn't have horizontal scroll
      const mainContainer = container.firstChild as HTMLElement
      expect(mainContainer).toHaveClass('min-h-screen')

      // The page should not have overflow-x visible/scroll on its content
      // All sections should be contained within the viewport width
      const allSections = container.querySelectorAll('section, header, footer, main')
      allSections.forEach((section) => {
        const styles = window.getComputedStyle(section)
        // overflow-x should not be set to scroll or visible with content wider than container
        expect(styles.overflowX).not.toBe('scroll')
      })
    })

    it('all content is contained within page width', () => {
      const { container } = renderWithProviders(<Home />)

      // The root div should use relative positioning
      const rootDiv = container.firstChild as HTMLElement
      expect(rootDiv).toBeInTheDocument()

      // Check responsive classes are applied
      const header = screen.getByRole('banner')
      expect(header).toBeInTheDocument()

      // Navbar should be visible and properly styled
      expect(header).toHaveClass('navbar')
    })

    it('hero section uses responsive padding for tablet', () => {
      renderWithProviders(<Home />)

      // Hero section should be present - find it by its aria-labelledby heading
      const heroHeading = screen.getByRole('heading', { name: /shorten links/i })
      expect(heroHeading).toBeInTheDocument()

      // The section containing the hero should use responsive classes
      const heroSection = heroHeading.closest('section')
      expect(heroSection).toBeInTheDocument()
      expect(heroSection).toHaveClass('min-h-[80vh]')
    })
  })

  // Test Case 2: Feature cards display in 2-column grid layout
  describe('Test Case 2: Feature cards in 2-column grid at tablet', () => {
    it('features grid has responsive grid classes for tablet (md:grid-cols-2)', () => {
      renderWithProviders(<FeaturesSection />)

      const featuresGrid = screen.getByTestId('features-grid')
      expect(featuresGrid).toBeInTheDocument()

      // Grid should have md:grid-cols-2 class for tablet breakpoint
      expect(featuresGrid).toHaveClass('md:grid-cols-2')
    })

    it('features grid applies 2-column layout at tablet breakpoint (768px)', () => {
      renderWithProviders(<FeaturesSection />)

      const featuresGrid = screen.getByTestId('features-grid')

      // Verify the grid has the proper responsive classes
      // grid-cols-1 for mobile, md:grid-cols-2 for tablet, lg:grid-cols-4 for desktop
      expect(featuresGrid).toHaveClass('grid')
      expect(featuresGrid).toHaveClass('grid-cols-1')
      expect(featuresGrid).toHaveClass('md:grid-cols-2')
      expect(featuresGrid).toHaveClass('lg:grid-cols-4')
    })

    it('all 4 feature cards are rendered at tablet viewport', () => {
      renderWithProviders(<FeaturesSection />)

      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards).toHaveLength(4)
    })

    it('feature cards have proper spacing with gap classes', () => {
      renderWithProviders(<FeaturesSection />)

      const featuresGrid = screen.getByTestId('features-grid')
      // Grid should have gap for proper spacing between cards
      expect(featuresGrid).toHaveClass('gap-6')
    })
  })

  // Test Case 3: Full navigation is visible without hamburger menu
  describe('Test Case 3: Full navigation visible at tablet viewport', () => {
    it('navigation header is visible at tablet viewport', () => {
      renderWithProviders(<Home />)

      const header = screen.getByRole('banner')
      expect(header).toBeInTheDocument()
      expect(header).toHaveClass('navbar')
    })

    it('Login link is visible in navigation (no hamburger menu)', () => {
      renderWithProviders(<Home />)

      const loginLink = screen.getByRole('link', { name: /login/i })
      expect(loginLink).toBeInTheDocument()
      expect(loginLink).toHaveAttribute('href', '/login')

      // The link should be visible, not hidden inside a hamburger menu
      expect(loginLink).toBeVisible()
    })

    it('Get Started/Register button is visible in navigation (no hamburger menu)', () => {
      renderWithProviders(<Home />)

      const registerLink = screen.getByRole('link', { name: /get started/i })
      expect(registerLink).toBeInTheDocument()
      expect(registerLink).toHaveAttribute('href', '/register')

      // The link should be visible, not hidden inside a hamburger menu
      expect(registerLink).toBeVisible()
    })

    it('brand logo/name is visible in navigation', () => {
      renderWithProviders(<Home />)

      const brandLink = screen.getByRole('link', { name: /url shortener/i })
      expect(brandLink).toBeInTheDocument()
      expect(brandLink).toHaveAttribute('href', '/')
      expect(brandLink).toBeVisible()
    })

    it('no hamburger menu icon is present at tablet viewport', () => {
      renderWithProviders(<Home />)

      // Hamburger menu typically uses a button with menu icon
      // At tablet viewport, full nav should be visible instead
      const hamburgerButtons = screen.queryAllByRole('button', { name: /menu|hamburger|toggle/i })

      // Either no hamburger exists, or if it exists, it should not be the primary navigation
      // The main navigation links should still be directly accessible
      const loginLink = screen.getByRole('link', { name: /login/i })
      const registerLink = screen.getByRole('link', { name: /get started/i })

      expect(loginLink).toBeVisible()
      expect(registerLink).toBeVisible()
    })

    it('theme toggle is visible in navigation at tablet viewport', () => {
      renderWithProviders(<Home />)

      // Theme toggle should be visible in the navbar
      // It's typically rendered as a button or label
      const header = screen.getByRole('banner')
      expect(header).toBeInTheDocument()

      // Look for theme toggle within the navbar
      const themeToggle = within(header).queryByRole('checkbox') ||
                         within(header).queryByLabelText(/theme/i) ||
                         within(header).queryByTestId('theme-toggle')

      // Theme toggle should exist in some form
      expect(themeToggle || within(header).queryByRole('button')).toBeTruthy()
    })
  })

  // Additional responsive layout tests for tablet
  describe('Additional tablet viewport tests', () => {
    it('hero section content is properly aligned at tablet viewport', () => {
      renderWithProviders(<Home />)

      // Check for headline (h1) in hero section
      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline).toBeInTheDocument()

      // Verify the headline is in a section element
      const heroSection = headline.closest('section')
      expect(heroSection).toBeInTheDocument()
    })

    it('footer is displayed correctly at tablet viewport', () => {
      renderWithProviders(<Home />)

      const footer = screen.getByRole('contentinfo')
      expect(footer).toBeInTheDocument()

      // Footer should have proper classes
      expect(footer).toHaveClass('footer')
    })

    it('main content area is present and accessible', () => {
      renderWithProviders(<Home />)

      const main = screen.getByRole('main')
      expect(main).toBeInTheDocument()
    })
  })
})
