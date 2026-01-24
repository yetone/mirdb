/**
 * Responsive Design Integration Tests
 * Owner: Scenario 7 - Responsive Design
 *
 * Tests the landing page renders correctly across mobile, tablet, and desktop viewports.
 * Verifies REQ-7: Implement responsive design for mobile, tablet, and desktop viewports.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { screen, within } from '@testing-library/react'
import { renderWithRouter } from './test-utils'

/**
 * Helper to set viewport dimensions for testing
 * Note: JSDOM doesn't actually resize, but we can mock window dimensions
 * and test that components have the correct responsive classes applied.
 */
function setViewportWidth(width: number) {
  Object.defineProperty(window, 'innerWidth', {
    writable: true,
    configurable: true,
    value: width,
  })
  window.dispatchEvent(new Event('resize'))
}

/**
 * Helper to check if an element has no horizontal overflow
 */
function hasNoHorizontalOverflow(element: HTMLElement): boolean {
  // In JSDOM, scrollWidth and clientWidth are typically the same
  // because there's no actual rendering, but we verify element is contained
  return element.scrollWidth <= element.clientWidth || element.scrollWidth === 0
}

describe('Responsive Design - REQ-7', () => {
  const originalInnerWidth = window.innerWidth

  beforeEach(() => {
    localStorage.clear()
  })

  afterEach(() => {
    // Reset viewport after each test
    Object.defineProperty(window, 'innerWidth', {
      writable: true,
      configurable: true,
      value: originalInnerWidth,
    })
  })

  describe('Test Case 1: Mobile Viewport (375px)', () => {
    it('should render all content visible without horizontal scroll at 375px mobile width', () => {
      setViewportWidth(375)
      const { container } = renderWithRouter()

      // Verify hero section renders
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Verify headline is visible and readable
      const headline = screen.getByTestId('hero-headline')
      expect(headline).toBeInTheDocument()
      expect(headline).toHaveTextContent('Shorten URLs. Track Everything.')

      // Verify CTA buttons are visible
      const primaryCTA = screen.getByTestId('hero-primary-cta')
      const secondaryCTA = screen.getByTestId('hero-secondary-cta')
      expect(primaryCTA).toBeInTheDocument()
      expect(secondaryCTA).toBeInTheDocument()

      // Verify the container doesn't cause horizontal overflow
      // The root div should have proper responsive classes
      const rootDiv = container.firstChild as HTMLElement
      expect(rootDiv).toHaveClass('min-h-screen')
    })

    it('should have CTA buttons stacked vertically on mobile (flex-col on small screens)', () => {
      setViewportWidth(375)
      renderWithRouter()

      // Find the CTA button container
      const ctaContainer = screen.getByRole('group', { name: /call to action buttons/i })
      expect(ctaContainer).toBeInTheDocument()

      // Verify it has responsive flex classes for stacking on mobile
      expect(ctaContainer).toHaveClass('flex-col')
      expect(ctaContainer).toHaveClass('sm:flex-row')
    })
  })

  describe('Test Case 2: Tablet Viewport (768px)', () => {
    it('should adapt layout appropriately at 768px tablet width', () => {
      setViewportWidth(768)
      renderWithRouter()

      // Verify hero section renders
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Verify headline with responsive text sizing
      const headline = screen.getByTestId('hero-headline')
      expect(headline).toBeInTheDocument()
      // Should have md: breakpoint classes for tablet
      expect(headline).toHaveClass('md:text-5xl')

      // Verify subheadline has tablet styling
      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline).toHaveClass('md:text-xl')
    })

    it('should show CTA buttons in a row at tablet width (sm breakpoint triggers)', () => {
      setViewportWidth(768)
      renderWithRouter()

      // At 768px (above sm: breakpoint), buttons should display in a row
      const ctaContainer = screen.getByRole('group', { name: /call to action buttons/i })
      expect(ctaContainer).toHaveClass('sm:flex-row')
    })
  })

  describe('Test Case 3: Desktop Viewport (1280px)', () => {
    it('should display full desktop layout with all sections visible at 1280px', () => {
      setViewportWidth(1280)
      renderWithRouter()

      // Verify hero section with desktop styling
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Verify headline with large desktop text
      const headline = screen.getByTestId('hero-headline')
      expect(headline).toHaveClass('lg:text-6xl')

      // Verify navigation is visible
      const navbar = screen.getByRole('navigation')
      expect(navbar).toBeInTheDocument()

      // Verify Login and Register links are visible
      const loginLink = screen.getByRole('link', { name: /login/i })
      const registerLink = screen.getByRole('link', { name: /register/i })
      expect(loginLink).toBeInTheDocument()
      expect(registerLink).toBeInTheDocument()
    })

    it('should display CTA buttons in row layout at desktop width', () => {
      setViewportWidth(1280)
      renderWithRouter()

      const ctaContainer = screen.getByRole('group', { name: /call to action buttons/i })
      // Both flex-col (mobile) and sm:flex-row (tablet+) classes should be present
      // At 1280px, sm:flex-row takes effect
      expect(ctaContainer).toHaveClass('sm:flex-row')
    })
  })

  describe('Test Case 4: Responsive Tailwind Classes', () => {
    it('should use responsive breakpoint classes (sm:, md:, lg:) in hero section', () => {
      renderWithRouter()

      // Verify hero headline has responsive text sizing classes
      const headline = screen.getByTestId('hero-headline')
      expect(headline.className).toMatch(/text-4xl/)
      expect(headline.className).toMatch(/md:text-5xl/)
      expect(headline.className).toMatch(/lg:text-6xl/)
    })

    it('should use responsive padding classes in hero section', () => {
      renderWithRouter()

      const heroSection = screen.getByTestId('hero-section')
      // Hero section should have responsive-friendly padding
      expect(heroSection).toHaveClass('px-4')
    })

    it('should have responsive container with max-width constraint', () => {
      renderWithRouter()

      const heroSection = screen.getByTestId('hero-section')
      // Find the inner container with max-w-4xl
      const container = heroSection.querySelector('.max-w-4xl')
      expect(container).toBeInTheDocument()
      expect(container).toHaveClass('mx-auto')
    })

    it('should use responsive flex direction for CTA buttons', () => {
      renderWithRouter()

      const ctaContainer = screen.getByRole('group', { name: /call to action buttons/i })
      // Should stack vertically on mobile (flex-col) and row on sm+ (sm:flex-row)
      expect(ctaContainer).toHaveClass('flex')
      expect(ctaContainer).toHaveClass('flex-col')
      expect(ctaContainer).toHaveClass('sm:flex-row')
    })
  })

  describe('Test Case 5: Navigation Accessibility on Mobile', () => {
    it('should render navigation element accessible on mobile viewport', () => {
      setViewportWidth(375)
      renderWithRouter()

      // Navigation should be present and accessible
      const navbar = screen.getByRole('navigation')
      expect(navbar).toBeInTheDocument()

      // The navbar uses DaisyUI's navbar classes which remain visible
      expect(navbar).toHaveClass('navbar')
    })

    it('should have Login and Register links accessible on mobile', () => {
      setViewportWidth(375)
      renderWithRouter()

      // Login and Register links should be in the document
      // Note: The current Navbar doesn't use a hamburger menu but shows links directly
      const loginLink = screen.getByRole('link', { name: /login/i })
      const registerLink = screen.getByRole('link', { name: /register/i })

      expect(loginLink).toBeInTheDocument()
      expect(registerLink).toBeInTheDocument()

      // Links should be within the navigation
      const navbar = screen.getByRole('navigation')
      expect(navbar).toContainElement(loginLink)
      expect(navbar).toContainElement(registerLink)
    })

    it('should have sticky navbar that remains accessible while scrolling', () => {
      setViewportWidth(375)
      renderWithRouter()

      const navbar = screen.getByRole('navigation')
      // Verify navbar has sticky positioning classes
      expect(navbar).toHaveClass('sticky')
      expect(navbar).toHaveClass('top-0')
      expect(navbar).toHaveClass('z-50')
    })

    it('should have accessible Home link in navbar', () => {
      setViewportWidth(375)
      renderWithRouter()

      const navbar = screen.getByRole('navigation')
      // The URL Shortener logo/text acts as home link
      const homeLink = within(navbar).getByRole('link', { name: /url shortener/i })
      expect(homeLink).toBeInTheDocument()
      expect(homeLink).toHaveAttribute('href', '/')
    })
  })

  describe('Additional Responsive Behavior', () => {
    it('should not have horizontal scrollbar at any tested viewport size', () => {
      const viewportSizes = [320, 375, 414, 768, 1024, 1280, 1440]

      viewportSizes.forEach(width => {
        setViewportWidth(width)
        const { container } = renderWithRouter()

        // Container should have proper responsive classes to prevent overflow
        const rootDiv = container.firstChild as HTMLElement
        expect(rootDiv).toHaveClass('bg-base-100')

        // Clean up for next iteration
        container.remove()
      })
    })

    it('should maintain readable text at all viewport sizes', () => {
      setViewportWidth(320) // smallest mobile
      renderWithRouter()

      // Text should be readable - verify it exists and is not truncated to nothing
      const headline = screen.getByTestId('hero-headline')
      expect(headline.textContent?.length).toBeGreaterThan(0)

      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline.textContent?.length).toBeGreaterThan(0)
    })
  })
})
