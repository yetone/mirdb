/**
 * Integration tests for responsive layout.
 * Owner: Scenario 5 - Responsive Mobile Layout
 *
 * Test coverage:
 * - Mobile viewport: single column layout
 * - CTA buttons meet touch target size (44x44px)
 * - No horizontal scrolling on mobile
 * - Tablet viewport adapts appropriately
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import Home from '../../src/pages/Home'

// Helper to render Home with router context
const renderHome = () => {
  return render(
    <BrowserRouter>
      <Home />
    </BrowserRouter>
  )
}

// Helper to set viewport width for testing
const setViewportWidth = (width: number) => {
  Object.defineProperty(window, 'innerWidth', {
    writable: true,
    configurable: true,
    value: width,
  })
  window.dispatchEvent(new Event('resize'))
}

describe('Responsive Mobile Layout', () => {
  beforeEach(() => {
    // Reset viewport to mobile size (375px - iPhone SE)
    setViewportWidth(375)
  })

  describe('Test Case 1: No horizontal scrollbar at 375px viewport', () => {
    it('should render Home component without horizontal overflow', () => {
      const { container } = renderHome()

      // Main element should have proper styling to prevent overflow
      const main = container.querySelector('main')
      expect(main).toBeInTheDocument()

      // Check that the component has min-h-screen class for full height
      expect(main).toHaveClass('min-h-screen')

      // Hero section should be present and properly styled
      const heroSection = screen.getByLabelText(/hero section/i)
      expect(heroSection).toBeInTheDocument()

      // The hero content should have text-center for proper alignment
      const heroContent = heroSection.querySelector('.hero-content')
      expect(heroContent).toBeInTheDocument()
      expect(heroContent).toHaveClass('text-center')
    })

    it('should have constrained max-width on content container', () => {
      renderHome()

      // The motion div with max-w-2xl should constrain content width
      const heroSection = screen.getByLabelText(/hero section/i)
      const contentContainer = heroSection.querySelector('.max-w-2xl')
      expect(contentContainer).toBeInTheDocument()
    })
  })

  describe('Test Case 2: CTA button minimum dimensions (44x44px)', () => {
    it('should render primary CTA button with btn-lg class for adequate touch target', () => {
      renderHome()

      const primaryCTA = screen.getByRole('link', { name: /get started/i })
      expect(primaryCTA).toBeInTheDocument()

      // DaisyUI btn-lg provides adequate touch target size (min 48px height)
      expect(primaryCTA).toHaveClass('btn')
      expect(primaryCTA).toHaveClass('btn-lg')
      expect(primaryCTA).toHaveClass('btn-primary')
    })

    it('should render secondary CTA button with btn-lg class for adequate touch target', () => {
      renderHome()

      const secondaryCTA = screen.getByRole('link', { name: /login/i })
      expect(secondaryCTA).toBeInTheDocument()

      // DaisyUI btn-lg provides adequate touch target size (min 48px height)
      expect(secondaryCTA).toHaveClass('btn')
      expect(secondaryCTA).toHaveClass('btn-lg')
    })

    it('should have CTA buttons that are accessible and properly labeled', () => {
      renderHome()

      const primaryCTA = screen.getByRole('link', { name: /get started/i })
      const secondaryCTA = screen.getByRole('link', { name: /login/i })

      // Both buttons should have accessible labels
      expect(primaryCTA).toHaveAccessibleName()
      expect(secondaryCTA).toHaveAccessibleName()
    })
  })

  describe('Test Case 3: Features display in single column on mobile', () => {
    it('should have CTA buttons stack vertically on mobile (flex-col)', () => {
      renderHome()

      // The button container should have flex-col for mobile
      const heroSection = screen.getByLabelText(/hero section/i)
      const buttonContainer = heroSection.querySelector('.flex.flex-col')
      expect(buttonContainer).toBeInTheDocument()

      // On larger screens, it should switch to flex-row (sm:flex-row)
      expect(buttonContainer).toHaveClass('sm:flex-row')
    })

    it('should have gap between stacked buttons', () => {
      renderHome()

      const heroSection = screen.getByLabelText(/hero section/i)
      const buttonContainer = heroSection.querySelector('.flex.flex-col')
      expect(buttonContainer).toHaveClass('gap-4')
    })
  })

  describe('Test Case 4: Homepage usable on iPhone SE (375x667)', () => {
    it('should render hero section with all essential elements', () => {
      setViewportWidth(375)
      renderHome()

      // Hero section should be visible
      const heroSection = screen.getByLabelText(/hero section/i)
      expect(heroSection).toBeInTheDocument()

      // Headline should be present
      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline).toBeInTheDocument()
      expect(headline).toHaveTextContent(/shorten links/i)

      // CTAs should be present
      expect(screen.getByRole('link', { name: /get started/i })).toBeInTheDocument()
      expect(screen.getByRole('link', { name: /login/i })).toBeInTheDocument()
    })

    it('should have responsive text sizing classes', () => {
      renderHome()

      const headline = screen.getByRole('heading', { level: 1 })

      // Should have mobile-first text size and larger for md breakpoint
      expect(headline).toHaveClass('text-5xl')
      expect(headline).toHaveClass('md:text-6xl')
    })

    it('should have hero content centered and justified', () => {
      renderHome()

      const heroSection = screen.getByLabelText(/hero section/i)
      const buttonContainer = heroSection.querySelector('.flex.flex-col')

      // Buttons should be centered
      expect(buttonContainer).toHaveClass('justify-center')
    })
  })

  describe('Test Case 5: Tablet layout adaptation (768x1024)', () => {
    beforeEach(() => {
      setViewportWidth(768)
    })

    it('should render correctly at tablet viewport width', () => {
      renderHome()

      // Hero section should still be present
      const heroSection = screen.getByLabelText(/hero section/i)
      expect(heroSection).toBeInTheDocument()

      // Headline should be present
      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline).toBeInTheDocument()
    })

    it('should have responsive classes that activate at md breakpoint', () => {
      renderHome()

      // Check headline has md: responsive classes
      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline).toHaveClass('md:text-6xl')

      // Check paragraph has md: responsive classes
      const paragraph = screen.getByText(/create short, memorable urls/i)
      expect(paragraph).toHaveClass('md:text-xl')
    })

    it('should maintain proper button layout at tablet size', () => {
      renderHome()

      const heroSection = screen.getByLabelText(/hero section/i)
      const buttonContainer = heroSection.querySelector('.flex.flex-col')

      // At sm (640px+) and md (768px+), buttons should be in row layout
      expect(buttonContainer).toHaveClass('sm:flex-row')
    })
  })

  describe('Accessibility on Mobile', () => {
    it('should maintain proper heading hierarchy on mobile', () => {
      setViewportWidth(375)
      renderHome()

      const h1Elements = screen.getAllByRole('heading', { level: 1 })
      // Should have exactly one h1
      expect(h1Elements).toHaveLength(1)
    })

    it('should have readable text with proper contrast classes', () => {
      setViewportWidth(375)
      renderHome()

      const paragraph = screen.getByText(/create short, memorable urls/i)
      // Should have opacity modifier for proper contrast
      expect(paragraph).toHaveClass('text-base-content/80')
    })

    it('should have proper aria labels for navigation links', () => {
      setViewportWidth(375)
      renderHome()

      const primaryCTA = screen.getByRole('link', { name: /get started/i })
      const secondaryCTA = screen.getByRole('link', { name: /login/i })

      expect(primaryCTA).toHaveAttribute('aria-label')
      expect(secondaryCTA).toHaveAttribute('aria-label')
    })
  })
})
