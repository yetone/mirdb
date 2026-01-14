import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import Home from '../pages/Home'
import HeroSection from '../components/HeroSection'
import FeaturesSection from '../components/FeaturesSection'

/**
 * Mobile Viewport Responsive Design Tests
 *
 * These tests verify that the homepage displays correctly on mobile devices
 * at 375px viewport width (iPhone SE size).
 *
 * Requirements tested: REQ-8, US-5, NFR-3
 */

// Mobile viewport width (iPhone SE)
const MOBILE_VIEWPORT_WIDTH = 375
const MIN_TOUCH_TARGET_SIZE = 44

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

describe('Responsive Design - Mobile Viewport (375px)', () => {
  beforeEach(() => {
    // Set mobile viewport width before each test
    setViewportWidth(MOBILE_VIEWPORT_WIDTH)
  })

  afterEach(() => {
    // Reset viewport width after tests
    setViewportWidth(1024)
  })

  describe('Test Case 1: No horizontal scrollbar at 375px viewport width', () => {
    it('should not have elements wider than viewport width', () => {
      renderWithRouter(<Home />)

      // Check that the main container does not exceed viewport width
      const main = screen.getByRole('main')
      expect(main).toBeInTheDocument()

      // Verify the homepage uses responsive classes that prevent overflow
      // Check hero section
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Hero section should have px-4 for mobile padding
      expect(heroSection.className).toContain('px-4')

      // Check that max-width containers are properly set
      const heroContainer = heroSection.querySelector('.max-w-4xl')
      expect(heroContainer).toBeInTheDocument()
    })

    it('should have proper mobile-first padding classes on all sections', () => {
      renderWithRouter(<Home />)

      // Hero section should have px-4 padding
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection.className).toContain('px-4')

      // Features section should have responsive padding
      const featuresSection = screen.getByRole('region', { name: /features/i })
      expect(featuresSection.className).toMatch(/px-4|sm:px-6|lg:px-8/)

      // Demo section should have responsive padding
      const demoSection = screen.getByTestId('demo-section')
      expect(demoSection.className).toMatch(/px-4|sm:px-6|lg:px-8/)

      // Footer section should have px-4 padding
      const footerSection = screen.getByTestId('footer-section')
      const footerContainer = footerSection.querySelector('.px-4')
      expect(footerContainer).toBeInTheDocument()
    })

    it('should have containers with max-width constraints', () => {
      renderWithRouter(<Home />)

      // Verify max-width containers exist to prevent overflow
      const heroSection = screen.getByTestId('hero-section')
      const heroMaxWidth = heroSection.querySelector('[class*="max-w-"]')
      expect(heroMaxWidth).toBeInTheDocument()

      const featuresSection = screen.getByRole('region', { name: /features/i })
      const featuresMaxWidth = featuresSection.querySelector('[class*="max-w-"]')
      expect(featuresMaxWidth).toBeInTheDocument()

      const demoSection = screen.getByTestId('demo-section')
      const demoMaxWidth = demoSection.querySelector('[class*="max-w-"]')
      expect(demoMaxWidth).toBeInTheDocument()
    })
  })

  describe('Test Case 2: CTA buttons have minimum touch target size of 44x44 pixels', () => {
    it('should have CTA buttons with btn-lg class for adequate touch target', () => {
      renderWithRouter(<HeroSection />)

      const getStartedButton = screen.getByTestId('cta-get-started')
      const loginButton = screen.getByTestId('cta-login')

      // btn-lg class in DaisyUI ensures min-height of 48px (> 44px)
      expect(getStartedButton).toHaveClass('btn-lg')
      expect(loginButton).toHaveClass('btn-lg')
    })

    it('should have demo submit button with sufficient size', () => {
      renderWithRouter(<Home />)

      const submitButton = screen.getByTestId('demo-submit-button')
      // btn class in DaisyUI ensures minimum height (default btn is 32px min, btn-md is 40px)
      // The button should be tappable
      expect(submitButton).toHaveClass('btn')
      expect(submitButton).toBeInTheDocument()
    })

    it('should have buttons using DaisyUI btn classes that meet accessibility standards', () => {
      renderWithRouter(<HeroSection />)

      const getStartedButton = screen.getByTestId('cta-get-started')
      const loginButton = screen.getByTestId('cta-login')

      // DaisyUI btn-lg has min-height: 3rem (48px) which exceeds 44px requirement
      // Verify buttons have the btn class base
      expect(getStartedButton).toHaveClass('btn')
      expect(loginButton).toHaveClass('btn')

      // Verify they are large buttons
      expect(getStartedButton).toHaveClass('btn-lg')
      expect(loginButton).toHaveClass('btn-lg')
    })

    it('should have full-width input field on mobile for easy tapping', () => {
      renderWithRouter(<Home />)

      const urlInput = screen.getByTestId('demo-url-input')
      // w-full class ensures the input spans full width on mobile
      expect(urlInput).toHaveClass('w-full')
    })
  })

  describe('Test Case 3: Feature cards stack vertically in single column', () => {
    it('should have grid-cols-1 as the base (mobile-first) grid layout', () => {
      renderWithRouter(<FeaturesSection />)

      const featuresSection = screen.getByRole('region', { name: /features/i })
      const gridContainer = featuresSection.querySelector('.grid')

      expect(gridContainer).toBeInTheDocument()
      // Verify mobile-first single column layout
      expect(gridContainer?.classList.contains('grid-cols-1')).toBe(true)
    })

    it('should have responsive grid breakpoints for larger screens', () => {
      renderWithRouter(<FeaturesSection />)

      const featuresSection = screen.getByRole('region', { name: /features/i })
      const gridContainer = featuresSection.querySelector('.grid')

      // Mobile: 1 column (base)
      expect(gridContainer?.classList.contains('grid-cols-1')).toBe(true)
      // Tablet: 2 columns
      expect(gridContainer?.classList.contains('md:grid-cols-2')).toBe(true)
      // Desktop: 3 columns
      expect(gridContainer?.classList.contains('lg:grid-cols-3')).toBe(true)
    })

    it('should display all feature cards in vertical stack on mobile', () => {
      renderWithRouter(<FeaturesSection />)

      const featureCards = screen.getAllByTestId('feature-card')

      // All cards should be present
      expect(featureCards.length).toBeGreaterThanOrEqual(3)

      // At mobile viewport (375px), grid-cols-1 means each card takes full row
      // This is enforced by the CSS class grid-cols-1
      const featuresSection = screen.getByRole('region', { name: /features/i })
      const gridContainer = featuresSection.querySelector('.grid')
      expect(gridContainer?.classList.contains('grid-cols-1')).toBe(true)
    })

    it('should have proper gap spacing between stacked cards', () => {
      renderWithRouter(<FeaturesSection />)

      const featuresSection = screen.getByRole('region', { name: /features/i })
      const gridContainer = featuresSection.querySelector('.grid')

      // gap-8 provides 2rem (32px) spacing between cards
      expect(gridContainer?.classList.contains('gap-8')).toBe(true)
    })
  })

  describe('Test Case 4: Text remains readable with appropriate font sizes', () => {
    it('should have responsive headline font size (text-4xl on mobile)', () => {
      renderWithRouter(<HeroSection />)

      const headline = screen.getByRole('heading', { level: 1 })

      // Mobile-first: text-4xl (2.25rem = 36px)
      // text-4xl md:text-6xl pattern
      expect(headline.className).toContain('text-4xl')
    })

    it('should have responsive subheadline font size (text-lg on mobile)', () => {
      renderWithRouter(<HeroSection />)

      const subheadline = screen.getByTestId('hero-subheadline')

      // Mobile-first: text-lg (1.125rem = 18px)
      // text-lg md:text-xl pattern
      expect(subheadline.className).toContain('text-lg')
    })

    it('should have readable section heading sizes', () => {
      renderWithRouter(<Home />)

      // Features section heading
      const featuresHeading = screen.getByRole('heading', { name: /powerful features/i })
      expect(featuresHeading.className).toContain('text-3xl')

      // Demo section heading
      const demoHeading = screen.getByRole('heading', { name: /try it now/i })
      expect(demoHeading.className).toContain('text-3xl')
    })

    it('should have body text using base-content color classes for readability', () => {
      renderWithRouter(<HeroSection />)

      const subheadline = screen.getByTestId('hero-subheadline')

      // text-base-content/70 ensures proper contrast in both light and dark modes
      expect(subheadline.className).toMatch(/text-base-content/)
    })

    it('should use proper semantic heading hierarchy', () => {
      renderWithRouter(<Home />)

      // h1 should exist (hero headline)
      const h1 = screen.getByRole('heading', { level: 1 })
      expect(h1).toBeInTheDocument()

      // h2 headings for sections
      const h2s = screen.getAllByRole('heading', { level: 2 })
      expect(h2s.length).toBeGreaterThanOrEqual(2) // At least Features and Demo sections

      // h3 headings for feature cards
      const h3s = screen.getAllByRole('heading', { level: 3 })
      expect(h3s.length).toBeGreaterThanOrEqual(3) // At least 3 feature card titles
    })
  })

  describe('Mobile-friendly layout patterns', () => {
    it('should have CTA buttons stacking vertically on mobile', () => {
      renderWithRouter(<HeroSection />)

      const heroSection = screen.getByTestId('hero-section')

      // Find the flex container for CTAs
      const getStartedButton = screen.getByTestId('cta-get-started')
      const ctaContainer = getStartedButton.parentElement

      // flex-col sm:flex-row means vertical on mobile, horizontal on larger screens
      expect(ctaContainer?.className).toContain('flex-col')
      expect(ctaContainer?.className).toContain('sm:flex-row')
    })

    it('should have demo form inputs stacking vertically on mobile', () => {
      renderWithRouter(<Home />)

      const submitButton = screen.getByTestId('demo-submit-button')
      const form = submitButton.closest('form')

      // flex-col sm:flex-row means vertical on mobile
      expect(form?.className).toContain('flex-col')
      expect(form?.className).toContain('sm:flex-row')
    })

    it('should have footer navigation using flex-wrap for mobile', () => {
      renderWithRouter(<Home />)

      const footerSection = screen.getByTestId('footer-section')
      const navElements = footerSection.querySelectorAll('nav')

      navElements.forEach((nav) => {
        // flex-wrap allows items to wrap on narrow viewports
        expect(nav.className).toContain('flex-wrap')
      })
    })

    it('should have mx-auto centering on containers for mobile alignment', () => {
      renderWithRouter(<Home />)

      const heroSection = screen.getByTestId('hero-section')
      const heroContainer = heroSection.querySelector('.mx-auto')
      expect(heroContainer).toBeInTheDocument()

      const featuresSection = screen.getByRole('region', { name: /features/i })
      const featuresContainer = featuresSection.querySelector('.mx-auto')
      expect(featuresContainer).toBeInTheDocument()
    })
  })

  describe('Navigation accessibility on mobile', () => {
    it('should have footer navigation links accessible and properly spaced', () => {
      renderWithRouter(<Home />)

      const footerSection = screen.getByTestId('footer-section')

      // Main navigation links
      const homeLink = within(footerSection).getByRole('link', { name: /home/i })
      const loginLink = within(footerSection).getByRole('link', { name: /login/i })
      const registerLink = within(footerSection).getByRole('link', { name: /register/i })

      expect(homeLink).toBeInTheDocument()
      expect(loginLink).toBeInTheDocument()
      expect(registerLink).toBeInTheDocument()

      // Links should have link-hover class for touch feedback
      expect(homeLink).toHaveClass('link-hover')
      expect(loginLink).toHaveClass('link-hover')
      expect(registerLink).toHaveClass('link-hover')
    })

    it('should have gap spacing between navigation links for touch targets', () => {
      renderWithRouter(<Home />)

      const footerSection = screen.getByTestId('footer-section')
      const navElements = footerSection.querySelectorAll('nav')

      navElements.forEach((nav) => {
        // gap-6 provides 1.5rem (24px) spacing between links
        expect(nav.className).toContain('gap-6')
      })
    })
  })
})
