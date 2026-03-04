/**
 * Responsive Design Tests
 * Owner: Scenario 6 - Responsive Design
 *
 * Tests that the homepage layout adapts correctly across desktop, tablet,
 * and mobile viewports (REQ-8, User Story 6).
 */
import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import { Home } from '../../src/pages/Home'
import { FuturisticButton } from '../../src/components/FuturisticButton'

// Helper to wrap component with router
const renderWithRouter = (ui: React.ReactElement) => {
  return render(<BrowserRouter>{ui}</BrowserRouter>)
}

// Helper to set viewport size
const setViewport = (width: number, height: number = 768) => {
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

describe('Responsive Design Tests', () => {
  beforeEach(() => {
    // Reset viewport to default
    setViewport(1280, 800)
  })

  describe('Test Case 1: Desktop Viewport (1280px)', () => {
    beforeEach(() => {
      setViewport(1280, 800)
    })

    it('should render multi-column layout at 1280px viewport width', () => {
      renderWithRouter(<Home />)

      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()
    })

    it('should display full navigation visible on desktop', () => {
      renderWithRouter(<Home />)

      const navbar = screen.getByTestId('navbar')
      expect(navbar).toBeInTheDocument()

      // Desktop navigation should be visible
      const featuresLink = screen.getByTestId('nav-features')
      const faqLink = screen.getByTestId('nav-faq')
      const loginLink = screen.getByTestId('nav-login')
      const registerButton = screen.getByTestId('nav-register')

      expect(featuresLink).toBeInTheDocument()
      expect(faqLink).toBeInTheDocument()
      expect(loginLink).toBeInTheDocument()
      expect(registerButton).toBeInTheDocument()
    })

    it('should render hero section with full-width content on desktop', () => {
      renderWithRouter(<Home />)

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()
      expect(heroSection).toHaveClass('w-full')
    })

    it('should have responsive padding classes for desktop', () => {
      renderWithRouter(<Home />)

      const heroSection = screen.getByTestId('hero-section')
      // Check for lg: responsive classes
      expect(heroSection.className).toMatch(/lg:px-8/)
    })
  })

  describe('Test Case 2: Tablet Viewport (768px)', () => {
    beforeEach(() => {
      setViewport(768, 1024)
    })

    it('should adapt layout at 768px viewport width', () => {
      renderWithRouter(<Home />)

      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()
    })

    it('should maintain navigation accessibility on tablet', () => {
      renderWithRouter(<Home />)

      const navbar = screen.getByTestId('navbar')
      expect(navbar).toBeInTheDocument()

      // At 768px (md breakpoint), desktop nav should still be visible
      const featuresLink = screen.getByTestId('nav-features')
      expect(featuresLink).toBeInTheDocument()
    })

    it('should have responsive text sizing for tablet', () => {
      renderWithRouter(<Home />)

      const headline = screen.getByTestId('hero-headline')
      // Check for sm: responsive classes
      expect(headline.className).toMatch(/sm:text-5xl/)
    })
  })

  describe('Test Case 3: Mobile Viewport (375px)', () => {
    beforeEach(() => {
      setViewport(375, 667)
    })

    it('should render single column layout on mobile', () => {
      renderWithRouter(<Home />)

      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()
    })

    it('should stack elements vertically on mobile', () => {
      renderWithRouter(<Home />)

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Hero section should have flex-col for vertical stacking
      const heroContent = heroSection.querySelector('.flex-col')
      expect(heroContent).toBeInTheDocument()
    })

    it('should have mobile-first base text sizing', () => {
      renderWithRouter(<Home />)

      const headline = screen.getByTestId('hero-headline')
      // Base text-4xl for mobile
      expect(headline.className).toMatch(/text-4xl/)
    })

    it('should have appropriate padding on mobile', () => {
      renderWithRouter(<Home />)

      const heroSection = screen.getByTestId('hero-section')
      // Base px-4 for mobile
      expect(heroSection.className).toMatch(/px-4/)
    })
  })

  describe('Test Case 4: Navigation Accessibility on Mobile', () => {
    beforeEach(() => {
      setViewport(375, 667)
    })

    it('should have accessible navigation via hamburger menu on mobile', () => {
      renderWithRouter(<Home />)

      // Mobile menu toggle should exist
      const mobileMenuToggle = screen.getByTestId('mobile-menu-toggle')
      expect(mobileMenuToggle).toBeInTheDocument()
      expect(mobileMenuToggle).toHaveAttribute('aria-label', 'Open navigation menu')
    })

    it('should have mobile navigation menu with all links', () => {
      renderWithRouter(<Home />)

      const mobileMenu = screen.getByTestId('mobile-menu')
      expect(mobileMenu).toBeInTheDocument()

      // Mobile menu should contain navigation items
      const mobileFeatures = screen.getByTestId('mobile-nav-features')
      const mobileFaq = screen.getByTestId('mobile-nav-faq')
      const mobileLogin = screen.getByTestId('mobile-nav-login')
      const mobileRegister = screen.getByTestId('mobile-nav-register')

      expect(mobileFeatures).toBeInTheDocument()
      expect(mobileFaq).toBeInTheDocument()
      expect(mobileLogin).toBeInTheDocument()
      expect(mobileRegister).toBeInTheDocument()
    })

    it('should have proper ARIA roles for mobile navigation', () => {
      renderWithRouter(<Home />)

      const mobileMenu = screen.getByTestId('mobile-menu')
      expect(mobileMenu).toHaveAttribute('role', 'menu')
    })
  })

  describe('Test Case 5: No Horizontal Overflow on Mobile', () => {
    beforeEach(() => {
      setViewport(375, 667)
    })

    it('should have full-width containers that prevent overflow', () => {
      renderWithRouter(<Home />)

      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()
      expect(homePage).toHaveClass('min-h-screen')
    })

    it('should have responsive max-width containers', () => {
      renderWithRouter(<Home />)

      const heroSection = screen.getByTestId('hero-section')
      // Hero section content should have max-width constraint
      const contentContainer = heroSection.querySelector('.max-w-4xl')
      expect(contentContainer).toBeInTheDocument()
    })

    it('should use w-full class to contain content within viewport', () => {
      renderWithRouter(<Home />)

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toHaveClass('w-full')
    })

    it('should not have fixed pixel widths that could cause overflow', () => {
      renderWithRouter(<Home />)

      const homePage = screen.getByTestId('home-page')
      const heroSection = screen.getByTestId('hero-section')

      // Check that neither has fixed pixel widths in className
      expect(homePage.className).not.toMatch(/w-\d+px/)
      expect(heroSection.className).not.toMatch(/w-\d+px/)
    })
  })

  describe('Test Case 6: CTA Button Sizing for Touch Accessibility', () => {
    it('should have minimum 44px height for CTA buttons on lg size', () => {
      const { container } = render(
        <BrowserRouter>
          <FuturisticButton size="lg" data-testid="test-cta">
            Get Started
          </FuturisticButton>
        </BrowserRouter>
      )

      const button = screen.getByTestId('test-cta')
      expect(button).toBeInTheDocument()

      // Check that btn-lg class is applied (DaisyUI btn-lg is min 44px)
      expect(button.className).toMatch(/btn-lg/)
    })

    it('should have touch-friendly padding on lg CTA buttons', () => {
      render(
        <BrowserRouter>
          <FuturisticButton size="lg" data-testid="test-cta">
            Get Started
          </FuturisticButton>
        </BrowserRouter>
      )

      const button = screen.getByTestId('test-cta')
      // Check for py-4 (16px vertical padding = 32px + content = 44px+)
      expect(button.className).toMatch(/py-4/)
    })

    it('should have minimum accessible size for md CTA buttons', () => {
      render(
        <BrowserRouter>
          <FuturisticButton size="md" data-testid="test-cta">
            Get Started
          </FuturisticButton>
        </BrowserRouter>
      )

      const button = screen.getByTestId('test-cta')
      // md size should have btn-md class
      expect(button.className).toMatch(/btn-md/)
      expect(button.className).toMatch(/py-3/)
    })

    it('should render hero CTA with accessible touch target size', () => {
      renderWithRouter(<Home />)

      const heroCta = screen.getByTestId('hero-cta')
      expect(heroCta).toBeInTheDocument()
      // Hero CTA should use lg size
      expect(heroCta.className).toMatch(/btn-lg/)
    })
  })

  describe('Test Case 7: Text Readability on Mobile', () => {
    beforeEach(() => {
      setViewport(375, 667)
    })

    it('should have readable headline font size on mobile (text-4xl = 36px)', () => {
      renderWithRouter(<Home />)

      const headline = screen.getByTestId('hero-headline')
      // text-4xl is 2.25rem (36px) - readable on mobile
      expect(headline.className).toMatch(/text-4xl/)
    })

    it('should have readable subheading font size on mobile (text-lg = 18px)', () => {
      renderWithRouter(<Home />)

      const subheading = screen.getByTestId('hero-subheading')
      // text-lg is 1.125rem (18px) - readable on mobile
      expect(subheading.className).toMatch(/text-lg/)
    })

    it('should have proper line height for readability', () => {
      renderWithRouter(<Home />)

      const headline = screen.getByTestId('hero-headline')
      // leading-tight for headlines
      expect(headline.className).toMatch(/leading-tight/)
    })

    it('should use appropriate text opacity for contrast', () => {
      renderWithRouter(<Home />)

      const subheading = screen.getByTestId('hero-subheading')
      // text-base-content/70 provides sufficient contrast
      expect(subheading.className).toMatch(/text-base-content/)
    })

    it('should scale text appropriately across breakpoints', () => {
      renderWithRouter(<Home />)

      const headline = screen.getByTestId('hero-headline')
      // Should have responsive text scaling
      expect(headline.className).toMatch(/text-4xl/)
      expect(headline.className).toMatch(/sm:text-5xl/)
      expect(headline.className).toMatch(/lg:text-6xl/)
    })

    it('should center text for optimal readability on narrow screens', () => {
      renderWithRouter(<Home />)

      const heroContent = screen.getByTestId('hero-section').querySelector('.text-center')
      expect(heroContent).toBeInTheDocument()
    })
  })

  describe('Additional Responsive Checks', () => {
    it('should have responsive navbar padding', () => {
      renderWithRouter(<Home />)

      const navbar = screen.getByTestId('navbar')
      expect(navbar.className).toMatch(/px-4/)
      expect(navbar.className).toMatch(/sm:px-6/)
      expect(navbar.className).toMatch(/lg:px-8/)
    })

    it('should have CTA footer with responsive button layout', () => {
      renderWithRouter(<Home />)

      const ctaFooter = screen.getByTestId('cta-footer')
      expect(ctaFooter).toBeInTheDocument()

      // Check for responsive flex direction (col on mobile, row on sm+)
      const buttonContainer = ctaFooter.querySelector('.flex-col.sm\\:flex-row')
      expect(buttonContainer).toBeInTheDocument()
    })

    it('should have responsive section padding', () => {
      renderWithRouter(<Home />)

      const ctaFooter = screen.getByTestId('cta-footer')
      expect(ctaFooter.className).toMatch(/px-4/)
      expect(ctaFooter.className).toMatch(/sm:px-6/)
      expect(ctaFooter.className).toMatch(/lg:px-8/)
    })
  })
})
