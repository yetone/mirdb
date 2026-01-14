import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import Home from '../pages/Home'
import HeroSection from '../components/HeroSection'
import FeaturesSection from '../components/FeaturesSection'

/**
 * Tablet Viewport Responsive Design Tests
 *
 * These tests verify that the homepage displays correctly on tablet devices
 * at 768px viewport width (iPad size).
 *
 * Requirements tested: REQ-8
 */

// Tablet viewport dimensions (iPad portrait)
const TABLET_VIEWPORT_WIDTH = 768
const TABLET_VIEWPORT_HEIGHT = 1024

// Helper to render components with router
const renderWithRouter = (component: React.ReactElement) => {
  return render(<BrowserRouter>{component}</BrowserRouter>)
}

// Helper to set viewport width
const setViewportWidth = (width: number) => {
  Object.defineProperty(window, 'innerWidth', {
    writable: true,
    configurable: true,
    value: width,
  })
  window.dispatchEvent(new Event('resize'))
}

describe('Responsive Design - Tablet Viewport (768px)', () => {
  beforeEach(() => {
    // Set tablet viewport width before each test
    setViewportWidth(TABLET_VIEWPORT_WIDTH)
  })

  afterEach(() => {
    // Reset viewport width after tests
    setViewportWidth(1024)
  })

  describe('Test Case 1: Layout adapts to tablet size with appropriate spacing', () => {
    it('should render homepage at 768px viewport width without errors', () => {
      renderWithRouter(<Home />)

      // Verify main container exists
      const main = screen.getByRole('main')
      expect(main).toBeInTheDocument()

      // Verify all sections are present
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      const featuresSection = screen.getByRole('region', { name: /features/i })
      expect(featuresSection).toBeInTheDocument()

      const demoSection = screen.getByTestId('demo-section')
      expect(demoSection).toBeInTheDocument()

      const footerSection = screen.getByTestId('footer-section')
      expect(footerSection).toBeInTheDocument()
    })

    it('should have proper tablet padding classes on sections', () => {
      renderWithRouter(<Home />)

      // Hero section should have px-4 base padding
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection.className).toContain('px-4')

      // Features section should have responsive padding (px-4 base, sm:px-6 at 640px+)
      const featuresSection = screen.getByRole('region', { name: /features/i })
      expect(featuresSection.className).toMatch(/px-4/)
      // At 768px, sm:px-6 should be active
      expect(featuresSection.className).toMatch(/sm:px-6/)

      // Demo section should have responsive padding
      const demoSection = screen.getByTestId('demo-section')
      expect(demoSection.className).toMatch(/px-4/)
      expect(demoSection.className).toMatch(/sm:px-6/)
    })

    it('should have max-width containers for proper content width on tablet', () => {
      renderWithRouter(<Home />)

      // Hero section should have max-w-4xl container
      const heroSection = screen.getByTestId('hero-section')
      const heroContainer = heroSection.querySelector('.max-w-4xl')
      expect(heroContainer).toBeInTheDocument()

      // Features section should have max-w-7xl container
      const featuresSection = screen.getByRole('region', { name: /features/i })
      const featuresContainer = featuresSection.querySelector('.max-w-7xl')
      expect(featuresContainer).toBeInTheDocument()

      // Demo section should have max-width container
      const demoSection = screen.getByTestId('demo-section')
      const demoContainer = demoSection.querySelector('[class*="max-w-"]')
      expect(demoContainer).toBeInTheDocument()
    })

    it('should have centered content containers with mx-auto', () => {
      renderWithRouter(<Home />)

      // All sections should have mx-auto for centering
      const heroSection = screen.getByTestId('hero-section')
      const heroContainer = heroSection.querySelector('.mx-auto')
      expect(heroContainer).toBeInTheDocument()

      const featuresSection = screen.getByRole('region', { name: /features/i })
      const featuresContainer = featuresSection.querySelector('.mx-auto')
      expect(featuresContainer).toBeInTheDocument()

      const demoSection = screen.getByTestId('demo-section')
      const demoContainer = demoSection.querySelector('.mx-auto')
      expect(demoContainer).toBeInTheDocument()
    })

    it('should use full viewport height for hero section', () => {
      renderWithRouter(<Home />)

      const heroSection = screen.getByTestId('hero-section')
      // min-h-screen ensures hero takes full viewport height
      expect(heroSection.className).toContain('min-h-screen')
    })
  })

  describe('Test Case 2: Feature cards display in 2-column grid layout', () => {
    it('should have grid container with md:grid-cols-2 for tablet (768px+)', () => {
      renderWithRouter(<FeaturesSection />)

      const featuresSection = screen.getByRole('region', { name: /features/i })
      const gridContainer = featuresSection.querySelector('.grid')

      expect(gridContainer).toBeInTheDocument()
      // At 768px (md breakpoint), should use 2 columns
      expect(gridContainer?.classList.contains('md:grid-cols-2')).toBe(true)
    })

    it('should transition from 1-column (mobile) to 2-column (tablet) layout', () => {
      renderWithRouter(<FeaturesSection />)

      const featuresSection = screen.getByRole('region', { name: /features/i })
      const gridContainer = featuresSection.querySelector('.grid')

      // Mobile: 1 column (base)
      expect(gridContainer?.classList.contains('grid-cols-1')).toBe(true)
      // Tablet (768px+): 2 columns
      expect(gridContainer?.classList.contains('md:grid-cols-2')).toBe(true)
      // Desktop (1024px+): 3 columns
      expect(gridContainer?.classList.contains('lg:grid-cols-3')).toBe(true)
    })

    it('should display all feature cards visible on tablet', () => {
      renderWithRouter(<FeaturesSection />)

      const featureCards = screen.getAllByTestId('feature-card')

      // All 3 feature cards should be present
      expect(featureCards).toHaveLength(3)

      // Verify card content exists
      featureCards.forEach((card) => {
        const title = within(card).getByRole('heading', { level: 3 })
        expect(title).toBeInTheDocument()

        const description = card.querySelector('.text-base-content\\/70')
        expect(description).toBeInTheDocument()
      })
    })

    it('should have appropriate gap spacing between grid items on tablet', () => {
      renderWithRouter(<FeaturesSection />)

      const featuresSection = screen.getByRole('region', { name: /features/i })
      const gridContainer = featuresSection.querySelector('.grid')

      // gap-8 (2rem = 32px) spacing between cards
      expect(gridContainer?.classList.contains('gap-8')).toBe(true)
    })

    it('should have feature cards with shadow styling for visual separation', () => {
      renderWithRouter(<FeaturesSection />)

      const featureCards = screen.getAllByTestId('feature-card')

      featureCards.forEach((card) => {
        // DaisyUI card with shadow
        expect(card.className).toContain('card')
        expect(card.className).toContain('shadow-xl')
      })
    })
  })

  describe('Test Case 3: Hero section text and buttons are appropriately sized', () => {
    it('should have h1 headline with responsive text size (text-4xl base, md:text-6xl)', () => {
      renderWithRouter(<HeroSection />)

      const headline = screen.getByRole('heading', { level: 1 })

      // Mobile base: text-4xl (36px)
      expect(headline.className).toContain('text-4xl')
      // Tablet and up: md:text-6xl (60px) - larger for more screen real estate
      expect(headline.className).toContain('md:text-6xl')
    })

    it('should have subheadline with responsive text size (text-lg base, md:text-xl)', () => {
      renderWithRouter(<HeroSection />)

      const subheadline = screen.getByTestId('hero-subheadline')

      // Mobile base: text-lg (18px)
      expect(subheadline.className).toContain('text-lg')
      // Tablet and up: md:text-xl (20px)
      expect(subheadline.className).toContain('md:text-xl')
    })

    it('should have CTA buttons using btn-lg for adequate size on tablet', () => {
      renderWithRouter(<HeroSection />)

      const getStartedButton = screen.getByTestId('cta-get-started')
      const loginButton = screen.getByTestId('cta-login')

      // Both buttons should use btn-lg for larger touch target
      expect(getStartedButton).toHaveClass('btn-lg')
      expect(loginButton).toHaveClass('btn-lg')
    })

    it('should have CTA buttons using horizontal layout on tablet (sm:flex-row)', () => {
      renderWithRouter(<HeroSection />)

      const getStartedButton = screen.getByTestId('cta-get-started')
      const ctaContainer = getStartedButton.parentElement

      // At tablet (768px), buttons should be horizontal (sm breakpoint is 640px)
      // flex-col for mobile, sm:flex-row for 640px+
      expect(ctaContainer?.className).toContain('flex-col')
      expect(ctaContainer?.className).toContain('sm:flex-row')
    })

    it('should have proper button styling for primary and outline variants', () => {
      renderWithRouter(<HeroSection />)

      const getStartedButton = screen.getByTestId('cta-get-started')
      const loginButton = screen.getByTestId('cta-login')

      // Primary CTA button
      expect(getStartedButton).toHaveClass('btn')
      expect(getStartedButton).toHaveClass('btn-primary')

      // Secondary/outline button
      expect(loginButton).toHaveClass('btn')
      expect(loginButton).toHaveClass('btn-outline')
    })

    it('should have appropriate gap between CTA buttons', () => {
      renderWithRouter(<HeroSection />)

      const getStartedButton = screen.getByTestId('cta-get-started')
      const ctaContainer = getStartedButton.parentElement

      // gap-4 (1rem = 16px) between buttons
      expect(ctaContainer?.className).toContain('gap-4')
    })

    it('should have subheadline with max-width constraint for readability', () => {
      renderWithRouter(<HeroSection />)

      const subheadline = screen.getByTestId('hero-subheadline')

      // max-w-2xl limits line length for optimal readability
      expect(subheadline.className).toContain('max-w-2xl')
    })
  })

  describe('Additional tablet layout verifications', () => {
    it('should have features section heading with responsive size', () => {
      renderWithRouter(<Home />)

      const featuresHeading = screen.getByRole('heading', { name: /powerful features/i })

      // text-3xl base (30px), sm:text-4xl (36px) for larger screens
      expect(featuresHeading.className).toContain('text-3xl')
      expect(featuresHeading.className).toContain('sm:text-4xl')
    })

    it('should have demo section with proper tablet layout', () => {
      renderWithRouter(<Home />)

      const demoSection = screen.getByTestId('demo-section')
      const submitButton = screen.getByTestId('demo-submit-button')
      const form = submitButton.closest('form')

      // Form should use horizontal layout on tablet (sm:flex-row at 640px+)
      expect(form?.className).toContain('flex-col')
      expect(form?.className).toContain('sm:flex-row')
    })

    it('should have footer with proper tablet layout', () => {
      renderWithRouter(<Home />)

      const footerSection = screen.getByTestId('footer-section')

      // Footer navigation should wrap properly
      const navElements = footerSection.querySelectorAll('nav')
      navElements.forEach((nav) => {
        expect(nav.className).toContain('flex-wrap')
      })
    })

    it('should maintain proper heading hierarchy on tablet', () => {
      renderWithRouter(<Home />)

      // h1 should exist (hero headline)
      const h1 = screen.getByRole('heading', { level: 1 })
      expect(h1).toBeInTheDocument()
      expect(h1.textContent).toContain('Shorten URLs')

      // h2 headings for sections
      const h2s = screen.getAllByRole('heading', { level: 2 })
      expect(h2s.length).toBeGreaterThanOrEqual(2)

      // h3 headings for feature cards
      const h3s = screen.getAllByRole('heading', { level: 3 })
      expect(h3s.length).toBeGreaterThanOrEqual(3)
    })
  })
})
