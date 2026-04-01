/**
 * Responsive Design E2E Tests.
 * Primary Owner: Scenario 7 - Responsive Design Desktop
 * Contributors: Scenarios 8, 9
 *
 * Tests:
 * - Desktop layout (1024px+): 3-column features grid
 * - Tablet layout (768-1023px): 2-column features grid (Scenario 8)
 * - Mobile layout (<768px): 1-column stacked layout, touch targets (Scenario 9)
 *
 * Note: These tests use vitest with jsdom for CI environments where
 * Playwright browsers may not be available. The tests verify the
 * responsive CSS classes are correctly applied.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { Features } from '../../src/components/Features'
import { Hero } from '../../src/components/Hero'
import { Navigation } from '../../src/components/Navigation'

describe('Desktop Responsive Design (1024px+)', () => {
  describe('TC1: All content renders without horizontal scrollbar at 1280px', () => {
    it('should have appropriate container constraints to prevent overflow', () => {
      render(<Features />)

      // Verify the features section has max-width container
      const featuresSection = screen.getByRole('region', { name: /features/i })
      expect(featuresSection).toBeInTheDocument()

      // Check for max-w-7xl class which constrains width
      const container = featuresSection.querySelector('.max-w-7xl')
      expect(container).not.toBeNull()
    })

    it('should have proper padding on all sections', () => {
      render(<Features />)

      const featuresSection = screen.getByRole('region', { name: /features/i })
      // px-4 sm:px-6 lg:px-8 provides appropriate padding at all breakpoints
      expect(featuresSection.className).toContain('px-4')
    })
  })

  describe('TC2: Features section displays in 3-column grid layout at 1280px', () => {
    it('should have lg:grid-cols-3 class for desktop 3-column layout', () => {
      render(<Features />)

      // Get the features grid by data-testid
      const featuresGrid = screen.getByTestId('features-grid')
      expect(featuresGrid).toBeInTheDocument()

      // Verify grid classes for responsive columns
      // grid: enables grid layout
      // grid-cols-1: 1 column on mobile
      // md:grid-cols-2: 2 columns on tablet (768px+)
      // lg:grid-cols-3: 3 columns on desktop (1024px+)
      expect(featuresGrid.className).toContain('grid')
      expect(featuresGrid.className).toContain('grid-cols-1')
      expect(featuresGrid.className).toContain('md:grid-cols-2')
      expect(featuresGrid.className).toContain('lg:grid-cols-3')
    })

    it('should have appropriate gap between feature cards', () => {
      render(<Features />)

      const featuresGrid = screen.getByTestId('features-grid')
      // gap-6 provides consistent spacing (1.5rem)
      expect(featuresGrid.className).toContain('gap-6')
    })

    it('should render multiple feature cards', () => {
      render(<Features />)

      const featuresGrid = screen.getByTestId('features-grid')
      // Should have at least 3 feature cards for 3-column display
      const cards = featuresGrid.children
      expect(cards.length).toBeGreaterThanOrEqual(3)
    })
  })

  describe('TC3: Hero section is centered with appropriate padding at desktop width', () => {
    it('should have flexbox centering classes', () => {
      render(<Hero />)

      // Find the hero section
      const heroSection = document.querySelector('section')
      expect(heroSection).not.toBeNull()

      // Verify centering classes
      expect(heroSection!.className).toContain('flex')
      expect(heroSection!.className).toContain('flex-col')
      expect(heroSection!.className).toContain('items-center')
      expect(heroSection!.className).toContain('justify-center')
    })

    it('should have text-center for centered text alignment', () => {
      render(<Hero />)

      const heroSection = document.querySelector('section')
      expect(heroSection!.className).toContain('text-center')
    })

    it('should have horizontal padding for all viewport sizes', () => {
      render(<Hero />)

      const heroSection = document.querySelector('section')
      // px-4 provides 16px padding on all sides
      expect(heroSection!.className).toContain('px-4')
    })

    it('should display MirDB heading', () => {
      render(<Hero />)

      const heading = screen.getByRole('heading', { level: 1 })
      expect(heading).toHaveTextContent('MirDB')
    })

    it('should display tagline', () => {
      render(<Hero />)

      const tagline = screen.getByRole('heading', { level: 2 })
      expect(tagline).toHaveTextContent(/Persistent Key-Value Store/i)
    })
  })

  describe('TC4: All navigation elements are visible without hamburger menu at desktop', () => {
    it('should have desktop navigation visible at md breakpoint (768px+)', () => {
      render(<Navigation />)

      const nav = screen.getByRole('navigation')
      expect(nav).toBeInTheDocument()

      // Desktop nav container uses hidden md:flex - hidden by default, visible at md+
      // This means at desktop (1024px+), the md:flex overrides hidden
      const desktopNavContainer = nav.querySelector('.hidden.md\\:flex')
      expect(desktopNavContainer).not.toBeNull()
    })

    it('should have hamburger button hidden at md breakpoint (768px+)', () => {
      render(<Navigation />)

      // The hamburger button uses md:hidden - visible by default, hidden at md+
      const hamburgerButton = screen.getByRole('button', { name: /menu/i })
      expect(hamburgerButton.className).toContain('md:hidden')
    })

    it('should display all navigation links', () => {
      render(<Navigation />)

      // Verify all navigation links are rendered
      expect(screen.getByText('Features')).toBeInTheDocument()
      expect(screen.getByText('Usage')).toBeInTheDocument()
      expect(screen.getByText('Roadmap')).toBeInTheDocument()
      expect(screen.getByText('GitHub')).toBeInTheDocument()
    })

    it('should have MirDB logo/brand link', () => {
      render(<Navigation />)

      const brandLink = screen.getByRole('link', { name: /mirdb/i })
      expect(brandLink).toBeInTheDocument()
      expect(brandLink).toHaveAttribute('href', '/')
    })
  })

  describe('Page layout utilizes available desktop space appropriately', () => {
    it('should have max-width container in features section', () => {
      render(<Features />)

      const featuresSection = screen.getByRole('region', { name: /features/i })
      const maxWidthContainer = featuresSection.querySelector('.max-w-7xl')

      // max-w-7xl = 80rem = 1280px which matches desktop viewport
      expect(maxWidthContainer).not.toBeNull()
    })

    it('should have mx-auto for horizontal centering of containers', () => {
      render(<Features />)

      const featuresSection = screen.getByRole('region', { name: /features/i })
      const container = featuresSection.querySelector('.max-w-7xl')

      // mx-auto centers the container horizontally
      expect(container!.className).toContain('mx-auto')
    })
  })
})
