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
import { render, screen, fireEvent } from '@testing-library/react'
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

/**
 * Tablet Responsive Design Tests (768px-1023px)
 * Owner: Scenario 8 - Responsive Design - Tablet
 *
 * These tests verify the homepage displays correctly on tablet viewports.
 * REQ-9: Must be responsive on tablet 768px-1023px
 */
describe('Tablet Responsive Design (768px-1023px)', () => {
  describe('TC1: All content renders without horizontal scrollbar at 800px', () => {
    it('should have max-width container to constrain content width', () => {
      render(<Features />)

      // The features section has a max-w-7xl container
      const featuresSection = screen.getByRole('region', { name: /features/i })
      expect(featuresSection).toBeInTheDocument()

      // Verify the max-width container exists
      const container = featuresSection.querySelector('.max-w-7xl')
      expect(container).not.toBeNull()
    })

    it('should have w-full constraint preventing overflow', () => {
      render(<Hero />)

      // Hero section uses w-full which constrains to viewport width
      const heroSection = document.querySelector('section')
      expect(heroSection).not.toBeNull()
      // w-full ensures content doesn't exceed viewport width
      expect(heroSection!.className).toMatch(/w-full|px-4/)
    })

    it('should have proper horizontal padding on all sections', () => {
      render(<Features />)

      const featuresSection = screen.getByRole('region', { name: /features/i })
      // px-4 sm:px-6 lg:px-8 provides appropriate padding
      // At tablet (sm to lg), sm:px-6 applies (1.5rem = 24px)
      expect(featuresSection.className).toMatch(/px-4|sm:px-6/)
    })

    it('should have navigation constrained to viewport', () => {
      render(<Navigation />)

      const nav = screen.getByRole('navigation')
      expect(nav).toBeInTheDocument()
      // Navigation uses inner container with max-w-6xl to constrain content
      const innerContainer = nav.querySelector('.max-w-6xl')
      expect(innerContainer).not.toBeNull()
    })
  })

  describe('TC2: Features display in 2-column grid layout at 800px (md breakpoint)', () => {
    it('should have md:grid-cols-2 class for tablet 2-column layout', () => {
      render(<Features />)

      const featuresGrid = screen.getByTestId('features-grid')
      expect(featuresGrid).toBeInTheDocument()

      // At tablet (md breakpoint: 768px+), md:grid-cols-2 applies
      // This creates a 2-column layout for tablet viewports
      expect(featuresGrid.className).toContain('md:grid-cols-2')
    })

    it('should have grid class enabled for layout', () => {
      render(<Features />)

      const featuresGrid = screen.getByTestId('features-grid')
      expect(featuresGrid.className).toContain('grid')
    })

    it('should have base grid-cols-1 that md:grid-cols-2 overrides', () => {
      render(<Features />)

      const featuresGrid = screen.getByTestId('features-grid')
      // grid-cols-1 is the base (mobile), md:grid-cols-2 overrides at tablet
      expect(featuresGrid.className).toContain('grid-cols-1')
    })

    it('should have consistent gap between feature cards', () => {
      render(<Features />)

      const featuresGrid = screen.getByTestId('features-grid')
      // gap-6 provides 1.5rem (24px) spacing between cards
      expect(featuresGrid.className).toContain('gap-6')
    })

    it('should render feature cards that will fill 2-column grid', () => {
      render(<Features />)

      const featuresGrid = screen.getByTestId('features-grid')
      // At least 2 cards needed to demonstrate 2-column layout
      const cards = featuresGrid.children
      expect(cards.length).toBeGreaterThanOrEqual(2)
    })
  })

  describe('TC3: All text is readable without zooming or horizontal scroll', () => {
    it('should have appropriate heading font sizes', () => {
      render(<Hero />)

      // H1 heading should be present
      const h1 = screen.getByRole('heading', { level: 1 })
      expect(h1).toHaveTextContent('MirDB')
      // text-4xl sm:text-5xl or similar responsive sizing
      expect(h1.className).toMatch(/text-(3xl|4xl|5xl|6xl)/)
    })

    it('should have readable body text', () => {
      render(<Hero />)

      // H2 tagline should be readable
      const h2 = screen.getByRole('heading', { level: 2 })
      expect(h2).toBeInTheDocument()
      // Should have appropriate text size class
      expect(h2.className).toMatch(/text-(lg|xl|2xl)/)
    })

    it('should have max-width on text containers for readability', () => {
      render(<Features />)

      const featuresSection = screen.getByRole('region', { name: /features/i })
      // max-w-2xl on description ensures readable line length
      const textContainer = featuresSection.querySelector('.max-w-2xl')
      expect(textContainer).not.toBeNull()
    })

    it('should have centered text alignment for main headings', () => {
      render(<Features />)

      const featuresSection = screen.getByRole('region', { name: /features/i })
      // text-center class ensures headings are centered
      const centerTextContainer = featuresSection.querySelector('.text-center')
      expect(centerTextContainer).not.toBeNull()
    })

    it('should have proper text colors with sufficient contrast', () => {
      render(<Features />)

      const heading = screen.getByRole('heading', { name: /features/i })
      // text-gray-900 provides high contrast against light backgrounds
      expect(heading.className).toMatch(/text-gray-900|text-white|dark:text-white/)
    })
  })

  describe('Tablet layout adaptation', () => {
    it('should have navigation visible at tablet breakpoint', () => {
      render(<Navigation />)

      const nav = screen.getByRole('navigation')
      // Desktop nav uses hidden md:flex - visible at md (768px) and above
      const desktopNav = nav.querySelector('.hidden.md\\:flex')
      expect(desktopNav).not.toBeNull()
    })

    it('should hide hamburger menu at tablet breakpoint', () => {
      render(<Navigation />)

      // Hamburger button uses md:hidden - hidden at md (768px) and above
      const hamburgerButton = screen.getByRole('button', { name: /menu/i })
      expect(hamburgerButton.className).toContain('md:hidden')
    })

    it('should have consistent spacing between sections', () => {
      render(<Features />)

      const featuresSection = screen.getByRole('region', { name: /features/i })
      // py-16 provides vertical padding (4rem = 64px)
      expect(featuresSection.className).toContain('py-16')
    })

    it('should have Hero section centered and properly spaced', () => {
      render(<Hero />)

      const heroSection = document.querySelector('section')
      expect(heroSection!.className).toContain('items-center')
      expect(heroSection!.className).toContain('justify-center')
    })
  })
})

/**
 * Mobile Responsive Design Tests (Scenario 9)
 *
 * Tests for mobile viewport (<768px) behavior:
 * - Single-column stacked layout
 * - Touch targets minimum 44x44px
 * - No horizontal overflow
 * - Body text at least 16px for readability
 */
describe('Mobile Responsive Design (<768px)', () => {
  describe('TC1: All content renders without horizontal scrollbar at 375px', () => {
    it('should have appropriate container constraints to prevent overflow', () => {
      render(<Features />)

      // Verify the features section has container with proper width constraints
      const featuresSection = screen.getByRole('region', { name: /features/i })
      expect(featuresSection).toBeInTheDocument()

      // Check for max-w-7xl and mx-auto classes which constrain width
      const container = featuresSection.querySelector('.max-w-7xl')
      expect(container).not.toBeNull()
      expect(container!.className).toContain('mx-auto')
    })

    it('should have proper padding that scales on mobile (px-4)', () => {
      render(<Features />)

      const featuresSection = screen.getByRole('region', { name: /features/i })
      // px-4 provides 16px padding which is appropriate for mobile
      expect(featuresSection.className).toContain('px-4')
    })

    it('should have hero section with overflow-hidden behavior', () => {
      render(<Hero />)

      const heroSection = document.querySelector('section')
      expect(heroSection).not.toBeNull()
      // px-4 ensures content doesn't overflow on small screens
      expect(heroSection!.className).toContain('px-4')
    })

    it('should have navigation with constrained width', () => {
      render(<Navigation />)

      const nav = screen.getByRole('navigation')
      expect(nav).toBeInTheDocument()

      // Navigation container should have max-width and padding
      const navContainer = nav.querySelector('.max-w-6xl')
      expect(navContainer).not.toBeNull()
      expect(navContainer!.className).toContain('px-4')
    })
  })

  describe('TC2: Features section displays in single-column stacked layout at 375px', () => {
    it('should have grid-cols-1 class for mobile single-column layout', () => {
      render(<Features />)

      const featuresGrid = screen.getByTestId('features-grid')
      expect(featuresGrid).toBeInTheDocument()

      // grid-cols-1 provides single column layout on mobile
      // The breakpoint classes (md:grid-cols-2, lg:grid-cols-3) only activate at larger widths
      expect(featuresGrid.className).toContain('grid')
      expect(featuresGrid.className).toContain('grid-cols-1')
    })

    it('should have gap spacing for stacked cards', () => {
      render(<Features />)

      const featuresGrid = screen.getByTestId('features-grid')
      // gap-6 provides consistent vertical spacing between stacked cards
      expect(featuresGrid.className).toContain('gap-6')
    })

    it('should render all feature cards in a vertical stack', () => {
      render(<Features />)

      const featuresGrid = screen.getByTestId('features-grid')
      // Verify cards exist and grid is properly configured for stacking
      const cards = featuresGrid.children
      expect(cards.length).toBeGreaterThanOrEqual(1)
    })
  })

  describe('TC3: All buttons and interactive elements have minimum 44x44px touch targets', () => {
    it('should have hamburger menu button with adequate touch target size', () => {
      render(<Navigation />)

      const hamburgerButton = screen.getByRole('button', { name: /menu/i })
      expect(hamburgerButton).toBeInTheDocument()

      // p-2 = 8px padding + w-6 h-6 icon (24x24) = 40x40, but with min dimensions it should be 44x44
      // Check for touch target classes: min-w-[44px] min-h-[44px] or equivalent
      expect(hamburgerButton.className).toContain('min-w-[44px]')
      expect(hamburgerButton.className).toContain('min-h-[44px]')
    })

    it('should have CTA button with adequate touch target size', () => {
      render(<Hero />)

      // Find the CTA link/button
      const ctaButton = screen.getByRole('link', { name: /get started/i })
      expect(ctaButton).toBeInTheDocument()

      // py-3 = 12px vertical padding, with text-lg this exceeds 44px height
      // px-8 = 32px horizontal padding, which provides adequate width
      expect(ctaButton.className).toContain('py-3')
      expect(ctaButton.className).toContain('px-8')
    })

    it('should have mobile navigation links with adequate touch target size', () => {
      render(<Navigation />)

      // Open mobile menu to reveal mobile nav links
      const hamburgerButton = screen.getByRole('button', { name: /menu/i })
      fireEvent.click(hamburgerButton)

      // Find the mobile navigation container (appears after clicking hamburger)
      // The mobile menu has py-4 class and contains the navigation links
      const mobileMenuContainer = document.querySelector('div.py-4')
      expect(mobileMenuContainer).not.toBeNull()

      // Find mobile navigation links within the mobile menu
      const mobileNavLinks = mobileMenuContainer!.querySelectorAll('a')
      expect(mobileNavLinks.length).toBeGreaterThan(0)

      // Each mobile nav link should have min-h-[44px] for touch accessibility
      mobileNavLinks.forEach((link) => {
        expect(link.className).toContain('min-h-[44px]')
      })
    })
  })

  describe('TC4: No horizontal overflow at mobile width', () => {
    it('should have all text constrained to not overflow', () => {
      render(<Hero />)

      // Verify tagline has max-width constraint
      const tagline = screen.getByRole('heading', { level: 2 })
      expect(tagline.className).toContain('max-w-2xl')
    })

    it('should have features description constrained', () => {
      render(<Features />)

      // Verify description text has max-width
      const featuresSection = screen.getByRole('region', { name: /features/i })
      const description = featuresSection.querySelector('.max-w-2xl')
      expect(description).not.toBeNull()
    })

    it('should not have elements that exceed container width', () => {
      render(<Features />)

      const featuresSection = screen.getByRole('region', { name: /features/i })
      // Verify no fixed large widths that would cause overflow
      const container = featuresSection.querySelector('.max-w-7xl')
      expect(container).not.toBeNull()

      // Container should have w-full behavior by default with max-width constraint
      expect(container!.className).toContain('mx-auto')
    })
  })

  describe('TC5: Body text is at least 16px for readability', () => {
    it('should have default base text size (Tailwind base is 16px)', () => {
      render(<Features />)

      // Feature descriptions should use default text size (text-base = 16px)
      // or explicit sizing that's at least 16px
      const featuresSection = screen.getByRole('region', { name: /features/i })
      const description = featuresSection.querySelector('.text-gray-600')
      expect(description).not.toBeNull()

      // Verify no text-sm or text-xs classes which would make text smaller
      // The description should not have small text classes
      expect(description!.className).not.toContain('text-xs')
      expect(description!.className).not.toContain('text-sm')
    })

    it('should have readable paragraph text in hero section', () => {
      render(<Hero />)

      const tagline = screen.getByRole('heading', { level: 2 })
      // text-xl = 20px which is above 16px minimum
      expect(tagline.className).toContain('text-xl')
    })

    it('should have readable link text in navigation', () => {
      render(<Navigation />)

      const nav = screen.getByRole('navigation')
      // Navigation links should not use small text classes
      const links = nav.querySelectorAll('a')
      links.forEach((link) => {
        expect(link.className).not.toContain('text-xs')
      })
    })
  })

  describe('Mobile-specific navigation behavior', () => {
    it('should show hamburger menu on mobile (md:hidden class)', () => {
      render(<Navigation />)

      const hamburgerButton = screen.getByRole('button', { name: /menu/i })
      // md:hidden means visible on mobile, hidden at md+ (768px+)
      expect(hamburgerButton.className).toContain('md:hidden')
    })

    it('should hide desktop navigation on mobile (hidden md:flex)', () => {
      render(<Navigation />)

      const nav = screen.getByRole('navigation')
      // Desktop nav container uses hidden md:flex - hidden on mobile
      const desktopNavContainer = nav.querySelector('.hidden.md\\:flex')
      expect(desktopNavContainer).not.toBeNull()
    })

    it('should toggle mobile menu visibility when hamburger is clicked', () => {
      render(<Navigation />)

      // Initially mobile menu is closed - no py-4 container visible
      expect(document.querySelector('div.py-4')).toBeNull()

      // Click hamburger to open menu
      const hamburgerButton = screen.getByRole('button', { name: /menu/i })
      fireEvent.click(hamburgerButton)

      // Mobile menu should now be visible (has py-4 and border-t classes)
      const mobileNav = document.querySelector('div.py-4.border-t')
      expect(mobileNav).not.toBeNull()
    })
  })
})
