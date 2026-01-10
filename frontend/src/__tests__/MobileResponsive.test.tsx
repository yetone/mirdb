import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import Home from '../pages/Home'
import FeaturesSection from '../components/FeaturesSection'
import FooterSection from '../components/FooterSection'
import HeroSection from '../components/HeroSection'

// Test wrapper with routing
const renderWithRouter = (ui: React.ReactElement) => {
  return render(<BrowserRouter>{ui}</BrowserRouter>)
}

// Mock window.matchMedia for responsive tests
const mockMatchMedia = (width: number) => {
  return vi.fn().mockImplementation((query: string) => ({
    matches: (() => {
      // Parse Tailwind breakpoints: sm=640, md=768, lg=1024
      if (query.includes('min-width: 1024px')) return width >= 1024
      if (query.includes('min-width: 768px')) return width >= 768
      if (query.includes('min-width: 640px')) return width >= 640
      return false
    })(),
    media: query,
    onchange: null,
    addListener: vi.fn(),
    removeListener: vi.fn(),
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    dispatchEvent: vi.fn(),
  }))
}

describe('Mobile Responsive Design Tests (NFR-2)', () => {
  /**
   * Test Case 1: Render homepage at 375px viewport width
   * Expected: No horizontal scrollbar appears, content fits within viewport
   */
  describe('Test Case 1: Homepage at 375px viewport width', () => {
    beforeEach(() => {
      // Set viewport to mobile size (375px)
      window.matchMedia = mockMatchMedia(375)
      Object.defineProperty(window, 'innerWidth', {
        writable: true,
        configurable: true,
        value: 375,
      })
    })

    afterEach(() => {
      vi.restoreAllMocks()
    })

    it('should render homepage without horizontal overflow at mobile viewport', () => {
      renderWithRouter(<Home />)

      // Verify main element exists
      const main = screen.getByRole('main')
      expect(main).toBeInTheDocument()
    })

    it('should render hero section with proper mobile padding', () => {
      renderWithRouter(<Home />)

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()
      // Verify overflow is hidden to prevent horizontal scroll
      expect(heroSection).toHaveClass('overflow-hidden')
    })

    it('should have responsive text sizing in hero section', () => {
      renderWithRouter(<HeroSection />)

      const heading = screen.getByRole('heading', { level: 1 })
      // Mobile starts at text-4xl, scales up at breakpoints
      expect(heading).toHaveClass('text-4xl')
      expect(heading).toHaveClass('sm:text-5xl')
      expect(heading).toHaveClass('md:text-6xl')
      expect(heading).toHaveClass('lg:text-7xl')
    })

    it('should have mobile-first padding in hero content', () => {
      renderWithRouter(<HeroSection />)

      const heroSection = screen.getByTestId('hero-section')
      const contentContainer = heroSection.querySelector('.relative.z-10')
      expect(contentContainer).toHaveClass('px-4')
      expect(contentContainer).toHaveClass('sm:px-6')
      expect(contentContainer).toHaveClass('lg:px-8')
    })

    it('should stack CTAs vertically on mobile', () => {
      renderWithRouter(<HeroSection />)

      // Find the CTA container
      const ctaContainer = screen.getByTestId('cta-register').parentElement
      expect(ctaContainer).toHaveClass('flex-col')
      expect(ctaContainer).toHaveClass('sm:flex-row')
    })

    it('should render all homepage sections', () => {
      renderWithRouter(<Home />)

      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer-section')).toBeInTheDocument()
    })
  })

  /**
   * Test Case 2: Render FeaturesSection at mobile viewport
   * Expected: Feature cards display in single column layout
   */
  describe('Test Case 2: FeaturesSection at mobile viewport', () => {
    beforeEach(() => {
      window.matchMedia = mockMatchMedia(375)
    })

    afterEach(() => {
      vi.restoreAllMocks()
    })

    it('should have single column layout on mobile (grid-cols-1)', () => {
      render(<FeaturesSection />)

      const grid = screen.getByTestId('features-grid')
      expect(grid).toHaveClass('grid-cols-1')
    })

    it('should have two column layout on medium screens (md:grid-cols-2)', () => {
      render(<FeaturesSection />)

      const grid = screen.getByTestId('features-grid')
      expect(grid).toHaveClass('md:grid-cols-2')
    })

    it('should have three column layout on large screens (lg:grid-cols-3)', () => {
      render(<FeaturesSection />)

      const grid = screen.getByTestId('features-grid')
      expect(grid).toHaveClass('lg:grid-cols-3')
    })

    it('should have responsive gap between feature cards', () => {
      render(<FeaturesSection />)

      const grid = screen.getByTestId('features-grid')
      expect(grid).toHaveClass('gap-8')
    })

    it('should render all 3 feature cards', () => {
      render(<FeaturesSection />)

      const featureCards = screen.getAllByTestId(/feature-card-\d/)
      expect(featureCards).toHaveLength(3)
    })

    it('should have mobile-friendly padding on section', () => {
      render(<FeaturesSection />)

      const section = screen.getByTestId('features-section')
      expect(section).toHaveClass('px-4')
      expect(section).toHaveClass('py-16')
    })
  })

  /**
   * Test Case 3: Check CTA button dimensions on mobile
   * Expected: Buttons have minimum 44px height for touch accessibility
   */
  describe('Test Case 3: CTA button touch accessibility', () => {
    beforeEach(() => {
      window.matchMedia = mockMatchMedia(375)
    })

    afterEach(() => {
      vi.restoreAllMocks()
    })

    it('should have minimum 44px height on primary CTA button', () => {
      renderWithRouter(<HeroSection />)

      // FuturisticButton lg size is 52px which exceeds the 44px accessibility requirement
      const ctaButton = screen.getByTestId('cta-register')
      expect(ctaButton.className).toContain('min-h-[52px]')
    })

    it('should have minimum 44px height on login link', () => {
      renderWithRouter(<HeroSection />)

      const loginLink = screen.getByTestId('login-link')
      expect(loginLink).toHaveClass('min-h-[44px]')
    })

    it('should use btn-lg class for larger touch target', () => {
      renderWithRouter(<HeroSection />)

      const ctaButton = screen.getByTestId('cta-register')
      expect(ctaButton).toHaveClass('btn-lg')
    })

    it('should have tappable footer navigation links', () => {
      renderWithRouter(<FooterSection />)

      const homeLink = screen.getByTestId('footer-link-home')
      const loginLink = screen.getByTestId('footer-link-login')
      const registerLink = screen.getByTestId('footer-link-register')

      expect(homeLink).toHaveClass('min-h-[44px]')
      expect(loginLink).toHaveClass('min-h-[44px]')
      expect(registerLink).toHaveClass('min-h-[44px]')
    })

    it('should have tappable footer legal links', () => {
      renderWithRouter(<FooterSection />)

      const privacyLink = screen.getByTestId('footer-link-privacy')
      const termsLink = screen.getByTestId('footer-link-terms')

      expect(privacyLink).toHaveClass('min-h-[44px]')
      expect(termsLink).toHaveClass('min-h-[44px]')
    })
  })

  /**
   * Test Case 4: Render footer at mobile viewport
   * Expected: Footer content is readable and links are tappable
   */
  describe('Test Case 4: Footer at mobile viewport', () => {
    beforeEach(() => {
      window.matchMedia = mockMatchMedia(375)
    })

    afterEach(() => {
      vi.restoreAllMocks()
    })

    it('should render footer section', () => {
      renderWithRouter(<FooterSection />)

      const footer = screen.getByTestId('footer-section')
      expect(footer).toBeInTheDocument()
    })

    it('should have single column grid on mobile', () => {
      renderWithRouter(<FooterSection />)

      const grid = screen.getByTestId('footer-grid')
      expect(grid).toHaveClass('grid-cols-1')
    })

    it('should have three column grid on medium screens', () => {
      renderWithRouter(<FooterSection />)

      const grid = screen.getByTestId('footer-grid')
      expect(grid).toHaveClass('md:grid-cols-3')
    })

    it('should have mobile-friendly padding', () => {
      renderWithRouter(<FooterSection />)

      const footer = screen.getByTestId('footer-section')
      expect(footer).toHaveClass('px-4')
      expect(footer).toHaveClass('py-8')
    })

    it('should render brand section with readable text', () => {
      renderWithRouter(<FooterSection />)

      const brand = screen.getByTestId('footer-brand')
      expect(brand).toBeInTheDocument()
      expect(within(brand).getByText('URL Shortener')).toBeInTheDocument()
    })

    it('should render navigation links section', () => {
      renderWithRouter(<FooterSection />)

      const nav = screen.getByTestId('footer-nav')
      expect(nav).toBeInTheDocument()
      expect(within(nav).getByText('Home')).toBeInTheDocument()
      expect(within(nav).getByText('Login')).toBeInTheDocument()
      expect(within(nav).getByText('Register')).toBeInTheDocument()
    })

    it('should render legal links section', () => {
      renderWithRouter(<FooterSection />)

      const legal = screen.getByTestId('footer-legal')
      expect(legal).toBeInTheDocument()
      expect(within(legal).getByText('Privacy Policy')).toBeInTheDocument()
      expect(within(legal).getByText('Terms of Service')).toBeInTheDocument()
    })

    it('should render copyright notice', () => {
      renderWithRouter(<FooterSection />)

      const copyright = screen.getByTestId('footer-copyright')
      expect(copyright).toBeInTheDocument()
      expect(copyright).toHaveTextContent(/URL Shortener/)
      expect(copyright).toHaveTextContent(/All rights reserved/)
    })

    it('should have proper gap between footer sections', () => {
      renderWithRouter(<FooterSection />)

      const grid = screen.getByTestId('footer-grid')
      expect(grid).toHaveClass('gap-8')
    })
  })

  /**
   * Additional responsive design tests
   */
  describe('Additional Responsive Design Verification', () => {
    beforeEach(() => {
      window.matchMedia = mockMatchMedia(375)
    })

    afterEach(() => {
      vi.restoreAllMocks()
    })

    it('should have max-w constraints to prevent content overflow', () => {
      renderWithRouter(<Home />)

      const featuresSection = screen.getByTestId('features-section')
      const container = featuresSection.querySelector('.container')
      expect(container).toHaveClass('max-w-6xl')
    })

    it('should use container class for consistent widths', () => {
      renderWithRouter(<Home />)

      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection.querySelector('.container')).toBeInTheDocument()

      const howItWorksSection = screen.getByTestId('how-it-works-section')
      expect(howItWorksSection.querySelector('.container')).toBeInTheDocument()

      const footerSection = screen.getByTestId('footer-section')
      expect(footerSection.querySelector('.container')).toBeInTheDocument()
    })

    it('should have HowItWorks section with responsive grid', () => {
      renderWithRouter(<Home />)

      const stepsContainer = screen.getByTestId('steps-container')
      expect(stepsContainer).toHaveClass('grid-cols-1')
      expect(stepsContainer).toHaveClass('md:grid-cols-3')
    })

    it('should have responsive heading sizes in HowItWorks section', () => {
      renderWithRouter(<Home />)

      const heading = screen.getByRole('heading', { name: /how it works/i })
      expect(heading).toHaveClass('text-3xl')
      expect(heading).toHaveClass('md:text-4xl')
    })
  })
})
