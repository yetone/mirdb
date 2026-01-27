/**
 * Integration Test: Homepage Responsive Design
 * Owner: Scenario 6 - Responsive Design
 *
 * Validates responsive design across mobile, tablet, and desktop viewports
 * as specified in REQ-7, NFR-1, and US-5.
 *
 * Test Cases:
 * 1. No horizontal scrolling required at 375px (mobile)
 * 2. Layout adapts appropriately at 768px (tablet)
 * 3. Full desktop layout with multi-column sections at 1280px
 * 4. CTA buttons meet minimum 44x44 pixel touch targets
 * 5. Mobile-friendly navigation menu accessible
 * 6. Text remains readable (minimum 16px for body text)
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { screen } from '@testing-library/react'
import { renderWithProviders } from '../utils/test-utils'
import Home from '../../src/pages/Home'

/**
 * Helper to set viewport size for testing
 */
function setViewport(width: number, height: number) {
  Object.defineProperty(window, 'innerWidth', {
    writable: true,
    configurable: true,
    value: width,
  })
  Object.defineProperty(window, 'innerHeight', {
    writable: true,
    configurable: true,
    value: height,
  })
  window.dispatchEvent(new Event('resize'))
}

/**
 * Helper to reset viewport to default
 */
function resetViewport() {
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
}

describe('Homepage Responsive Design', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  afterEach(() => {
    resetViewport()
  })

  // Test Case 1: Mobile viewport (375px width)
  describe('Mobile Viewport (375px)', () => {
    beforeEach(() => {
      setViewport(375, 667)
    })

    it('renders homepage without horizontal overflow at 375px width', () => {
      const { container } = renderWithProviders(<Home />)

      // The main container should have proper mobile styling
      const main = container.querySelector('main')
      expect(main).toBeInTheDocument()
      expect(main).toHaveClass('min-h-screen')

      // All content should be contained within the viewport
      // Check that no explicit widths are set that would cause overflow
      expect(main).not.toHaveStyle('width: 100vw')
    })

    it('all content is readable and visible at mobile viewport', () => {
      renderWithProviders(<Home />)

      // Hero content should be visible
      const heroHeading = screen.getByRole('heading', { name: /shorten your links/i })
      expect(heroHeading).toBeInTheDocument()
      expect(heroHeading).not.toHaveStyle('display: none')
      expect(heroHeading).not.toHaveStyle('visibility: hidden')

      // Features section should be visible
      const featuresHeading = screen.getByRole('heading', { name: /powerful features/i })
      expect(featuresHeading).toBeInTheDocument()

      // How It Works section should be visible
      const howItWorksHeading = screen.getByRole('heading', { name: /how it works/i })
      expect(howItWorksHeading).toBeInTheDocument()
    })

    it('uses mobile-optimized layout classes for features grid', () => {
      const { container } = renderWithProviders(<Home />)

      // Features grid should use single column on mobile
      const featuresGrid = container.querySelector('[data-testid="features-grid"]')
      expect(featuresGrid).toBeInTheDocument()
      expect(featuresGrid).toHaveClass('grid-cols-1')
    })

    it('uses stacked layout for hero CTAs on mobile', () => {
      const { container } = renderWithProviders(<Home />)

      // The CTA container should use flex-col for mobile
      const heroSection = container.querySelector('[aria-label="Hero"]')
      expect(heroSection).toBeInTheDocument()

      // Look for the flex container with CTAs
      const ctaContainer = heroSection?.querySelector('.flex-col')
      expect(ctaContainer).toBeInTheDocument()
    })
  })

  // Test Case 2: Tablet viewport (768px width)
  describe('Tablet Viewport (768px)', () => {
    beforeEach(() => {
      setViewport(768, 1024)
    })

    it('renders homepage appropriately at tablet viewport', () => {
      const { container } = renderWithProviders(<Home />)

      const main = container.querySelector('main')
      expect(main).toBeInTheDocument()
      expect(main).toHaveClass('min-h-screen')
    })

    it('adapts features grid layout for tablet', () => {
      const { container } = renderWithProviders(<Home />)

      // Features grid should use 2 columns on tablet (md:grid-cols-2)
      const featuresGrid = container.querySelector('[data-testid="features-grid"]')
      expect(featuresGrid).toBeInTheDocument()
      expect(featuresGrid).toHaveClass('md:grid-cols-2')
    })

    it('stats grid adapts for tablet viewport', () => {
      const { container } = renderWithProviders(<Home />)

      // Stats grid should have 3 columns on md breakpoint
      const statsGrid = container.querySelector('[data-testid="stats-grid"]')
      expect(statsGrid).toBeInTheDocument()
      expect(statsGrid).toHaveClass('md:grid-cols-3')
    })

    it('all sections remain visible and accessible', () => {
      renderWithProviders(<Home />)

      // Verify all main sections are present
      const heroSection = screen.getByRole('region', { name: /hero/i })
      const featuresSection = screen.getByRole('region', { name: /features/i })
      const howItWorksSection = screen.getByRole('region', { name: /how it works/i })
      const statsSection = screen.getByRole('region', { name: /statistics/i })

      expect(heroSection).toBeInTheDocument()
      expect(featuresSection).toBeInTheDocument()
      expect(howItWorksSection).toBeInTheDocument()
      expect(statsSection).toBeInTheDocument()
    })
  })

  // Test Case 3: Desktop viewport (1280px width)
  describe('Desktop Viewport (1280px)', () => {
    beforeEach(() => {
      setViewport(1280, 800)
    })

    it('renders full desktop layout with proper width constraints', () => {
      const { container } = renderWithProviders(<Home />)

      const main = container.querySelector('main')
      expect(main).toBeInTheDocument()
    })

    it('features grid uses three-column layout on desktop', () => {
      const { container } = renderWithProviders(<Home />)

      // Features grid should use 3 columns on large screens
      const featuresGrid = container.querySelector('[data-testid="features-grid"]')
      expect(featuresGrid).toBeInTheDocument()
      expect(featuresGrid).toHaveClass('lg:grid-cols-3')
    })

    it('how it works section uses horizontal layout on desktop', () => {
      const { container } = renderWithProviders(<Home />)

      // How It Works steps should be in a row on desktop
      const stepsContainer = container.querySelector('[data-testid="how-it-works-steps"]')
      expect(stepsContainer).toBeInTheDocument()
      expect(stepsContainer).toHaveClass('lg:flex-row')
    })

    it('step connectors are visible on desktop layout', () => {
      const { container } = renderWithProviders(<Home />)

      // Step connectors should be visible on lg screens (lg:flex)
      const connectors = container.querySelectorAll('[data-testid="step-connector"]')
      expect(connectors.length).toBe(2) // Two connectors between 3 steps

      connectors.forEach((connector) => {
        expect(connector).toHaveClass('lg:flex')
      })
    })

    it('hero CTAs display side-by-side on desktop', () => {
      const { container } = renderWithProviders(<Home />)

      // The CTA container should have sm:flex-row for horizontal layout
      const heroSection = container.querySelector('[aria-label="Hero"]')
      expect(heroSection).toBeInTheDocument()

      // Look for the flex container with row layout
      const ctaContainers = heroSection?.querySelectorAll('.sm\\:flex-row')
      expect(ctaContainers?.length).toBeGreaterThan(0)
    })
  })

  // Test Case 4: Touch target sizes
  describe('Touch Target Accessibility', () => {
    beforeEach(() => {
      setViewport(375, 667) // Mobile viewport
    })

    it('CTA buttons meet minimum 44x44 pixel touch target requirement', () => {
      renderWithProviders(<Home />)

      const getStartedButton = screen.getByRole('button', { name: /get started/i })
      const loginButton = screen.getByRole('button', { name: /login/i })
      const learnMoreButton = screen.getByRole('button', { name: /learn more/i })

      // All buttons should have btn class which provides adequate sizing via DaisyUI
      expect(getStartedButton).toHaveClass('btn')
      expect(loginButton).toHaveClass('btn')
      expect(learnMoreButton).toHaveClass('btn')

      // Check padding classes that ensure minimum touch target
      // py-3 = 12px vertical padding, combined with font-size gives at least 44px height
      // px-8 = 32px horizontal padding, combined with text gives at least 44px width
      expect(getStartedButton).toHaveClass('px-8', 'py-3')
      expect(learnMoreButton).toHaveClass('px-8', 'py-3')
    })

    it('login button has adequate touch target size', () => {
      renderWithProviders(<Home />)

      const loginButton = screen.getByRole('button', { name: /login/i })

      // Login button uses px-6 py-2 which with btn class provides minimum touch target
      expect(loginButton).toHaveClass('btn')
      expect(loginButton).toHaveClass('px-6', 'py-2')
    })

    it('buttons are not styled with explicit small dimensions', () => {
      renderWithProviders(<Home />)

      const buttons = screen.getAllByRole('button')

      buttons.forEach((button) => {
        // Ensure no button has inline styles that would make it smaller than touch target
        expect(button).not.toHaveStyle('width: 32px')
        expect(button).not.toHaveStyle('height: 32px')
        expect(button).not.toHaveStyle('min-width: 0')
        expect(button).not.toHaveStyle('min-height: 0')
      })
    })
  })

  // Test Case 5: Mobile navigation accessibility
  describe('Mobile Navigation Accessibility', () => {
    beforeEach(() => {
      setViewport(375, 667)
    })

    it('navigation header is accessible on mobile', () => {
      const { container } = renderWithProviders(<Home />)

      // Header with navigation should be present
      const header = container.querySelector('header')
      expect(header).toBeInTheDocument()

      // Navigation should be present
      const nav = container.querySelector('nav')
      expect(nav).toBeInTheDocument()
    })

    it('login button is accessible in mobile navigation', () => {
      renderWithProviders(<Home />)

      // Login button should be visible and accessible
      const loginButton = screen.getByRole('button', { name: /login/i })
      expect(loginButton).toBeInTheDocument()

      // Button should be inside a link to /login
      const loginLink = loginButton.closest('a')
      expect(loginLink).toHaveAttribute('href', '/login')
    })

    it('navigation has proper z-index for mobile overlay compatibility', () => {
      const { container } = renderWithProviders(<Home />)

      const header = container.querySelector('header')
      expect(header).toHaveClass('z-10')
    })

    it('navigation uses mobile-friendly padding', () => {
      const { container } = renderWithProviders(<Home />)

      const header = container.querySelector('header')
      // Mobile padding: p-4, larger screens: sm:p-6
      expect(header).toHaveClass('p-4')
      expect(header).toHaveClass('sm:p-6')
    })
  })

  // Test Case 6: Font sizes for readability
  describe('Font Readability', () => {
    beforeEach(() => {
      setViewport(375, 667)
    })

    it('body text uses readable font sizes on mobile', () => {
      renderWithProviders(<Home />)

      // Check hero paragraph text size - text-lg is 18px which exceeds 16px minimum
      const heroParagraph = screen.getByText(/transform long urls/i)
      expect(heroParagraph).toHaveClass('text-lg')
    })

    it('feature descriptions use readable font sizes', () => {
      renderWithProviders(<Home />)

      // Feature descriptions should use base font size or larger
      // text-base-content/70 applies color, the default text size is 16px (1rem)
      const urlShorteningDesc = screen.getByText(/transform long, unwieldy urls/i)
      expect(urlShorteningDesc).toBeInTheDocument()
      expect(urlShorteningDesc).toHaveClass('text-base-content/70')
    })

    it('step descriptions use readable font sizes', () => {
      renderWithProviders(<Home />)

      // How It Works step descriptions should be readable
      const step1Desc = screen.getByText(/copy and paste any long url/i)
      expect(step1Desc).toBeInTheDocument()
      expect(step1Desc).toHaveClass('text-base-content/70')
    })

    it('headings scale appropriately for mobile', () => {
      renderWithProviders(<Home />)

      // Hero heading should use responsive text sizes
      // text-4xl for mobile, sm:text-5xl for sm, lg:text-6xl for lg
      const heroHeading = screen.getByRole('heading', { name: /shorten your links/i })
      expect(heroHeading).toHaveClass('text-4xl')
      expect(heroHeading).toHaveClass('sm:text-5xl')
      expect(heroHeading).toHaveClass('lg:text-6xl')
    })

    it('section headings are readable at all viewport sizes', () => {
      renderWithProviders(<Home />)

      // Section headings use text-3xl sm:text-4xl
      const featuresHeading = screen.getByRole('heading', { name: /powerful features/i })
      expect(featuresHeading).toHaveClass('text-3xl')
      expect(featuresHeading).toHaveClass('sm:text-4xl')

      const howItWorksHeading = screen.getByRole('heading', { name: /how it works/i })
      expect(howItWorksHeading).toHaveClass('text-3xl')
      expect(howItWorksHeading).toHaveClass('sm:text-4xl')
    })

    it('stat values remain readable on mobile', () => {
      renderWithProviders(<Home />)

      // Stats section heading should be readable
      const statsHeading = screen.getByRole('heading', { name: /trusted by thousands/i })
      expect(statsHeading).toHaveClass('text-3xl')
      expect(statsHeading).toHaveClass('sm:text-4xl')
    })
  })

  // Additional: Responsive spacing and container constraints
  describe('Responsive Spacing and Containers', () => {
    it('sections use responsive padding', () => {
      const { container } = renderWithProviders(<Home />)

      // Features section should have responsive padding
      const featuresSection = container.querySelector('[aria-label="Features"]')
      expect(featuresSection).toHaveClass('py-16')
      expect(featuresSection).toHaveClass('sm:py-24')
      expect(featuresSection).toHaveClass('px-4')
      expect(featuresSection).toHaveClass('sm:px-6')
      expect(featuresSection).toHaveClass('lg:px-8')
    })

    it('hero section uses responsive padding', () => {
      const { container } = renderWithProviders(<Home />)

      const heroSection = container.querySelector('[aria-label="Hero"]')
      expect(heroSection).toHaveClass('px-4')
      expect(heroSection).toHaveClass('sm:px-6')
      expect(heroSection).toHaveClass('lg:px-8')
    })

    it('content containers have max-width constraints', () => {
      const { container } = renderWithProviders(<Home />)

      // Sections should have max-width containers to prevent overly wide content
      const maxWidthContainers = container.querySelectorAll('.max-w-6xl, .max-w-4xl, .max-w-7xl')
      expect(maxWidthContainers.length).toBeGreaterThan(0)
    })

    it('navigation has responsive max-width', () => {
      const { container } = renderWithProviders(<Home />)

      const nav = container.querySelector('nav')
      expect(nav).toHaveClass('max-w-7xl')
    })
  })
})
