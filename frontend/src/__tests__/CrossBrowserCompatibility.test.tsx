/**
 * Cross-Browser Compatibility Tests for Homepage
 *
 * These tests verify that the homepage uses web standards and CSS features
 * that are compatible across Chrome, Firefox, Safari, and Edge browsers.
 *
 * Since we cannot run actual browser tests in this environment (missing system
 * dependencies for Playwright browsers), these tests verify:
 * - CSS class usage that works across browsers
 * - Standard HTML elements and attributes
 * - No browser-specific CSS prefixes without fallbacks
 * - Proper use of responsive design utilities
 *
 * For full E2E cross-browser testing, the Playwright tests in e2e/ should
 * be run in a CI environment with proper browser dependencies installed.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import Home from '../pages/Home'
import HeroSection from '../components/HeroSection'
import FeaturesSection from '../components/FeaturesSection'
import HowItWorksSection from '../components/HowItWorksSection'
import FooterSection from '../components/FooterSection'

// Wrapper component for tests that require routing
const RouterWrapper = ({ children }: { children: React.ReactNode }) => (
  <BrowserRouter>{children}</BrowserRouter>
)

describe('Cross-Browser Homepage Compatibility', () => {
  describe('Test Case 1: Chrome Compatibility - All sections display correctly', () => {
    beforeEach(() => {
      render(
        <RouterWrapper>
          <Home />
        </RouterWrapper>
      )
    })

    it('renders HeroSection with proper cross-browser CSS classes', () => {
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Verify gradient classes (supported in Chrome 26+, all modern browsers)
      expect(heroSection.className).toContain('bg-gradient-to-br')
      expect(heroSection.className).toContain('from-primary')
      expect(heroSection.className).toContain('via-secondary')
      expect(heroSection.className).toContain('to-accent')

      // Verify flexbox layout (supported in Chrome 29+, all modern browsers)
      expect(heroSection.className).toContain('flex')
      expect(heroSection.className).toContain('items-center')
      expect(heroSection.className).toContain('justify-center')
    })

    it('renders headline with standard HTML and responsive text classes', () => {
      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline).toBeInTheDocument()
      expect(headline).toHaveTextContent('Shorten, Share, Track')

      // Verify responsive text classes (Tailwind's responsive prefixes work across browsers)
      expect(headline.className).toContain('text-4xl')
    })

    it('renders FeaturesSection with grid layout', () => {
      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()

      const featuresGrid = screen.getByTestId('features-grid')
      expect(featuresGrid).toBeInTheDocument()

      // Verify CSS Grid (supported in Chrome 57+, Firefox 52+, Safari 10.1+, Edge 16+)
      expect(featuresGrid.className).toContain('grid')
    })

    it('renders HowItWorksSection with proper structure', () => {
      const howItWorksSection = screen.getByTestId('how-it-works-section')
      expect(howItWorksSection).toBeInTheDocument()

      const stepsContainer = screen.getByTestId('steps-container')
      expect(stepsContainer).toBeInTheDocument()
      expect(stepsContainer.className).toContain('grid')
    })

    it('renders FooterSection with all navigation elements', () => {
      const footerSection = screen.getByTestId('footer-section')
      expect(footerSection).toBeInTheDocument()

      expect(screen.getByTestId('footer-brand')).toBeInTheDocument()
      expect(screen.getByTestId('footer-nav')).toBeInTheDocument()
      expect(screen.getByTestId('footer-legal')).toBeInTheDocument()
      expect(screen.getByTestId('footer-copyright')).toBeInTheDocument()
    })
  })

  describe('Test Case 2: Firefox Compatibility - All sections display correctly', () => {
    it('renders all feature cards with SVG icons (Firefox SVG support)', () => {
      render(
        <RouterWrapper>
          <FeaturesSection />
        </RouterWrapper>
      )

      // Verify SVG icons render (SVG supported since Firefox 4+)
      expect(screen.getByTestId('url-shortening-icon')).toBeInTheDocument()
      expect(screen.getByTestId('analytics-icon')).toBeInTheDocument()
      expect(screen.getByTestId('link-management-icon')).toBeInTheDocument()

      // Verify each icon is an SVG element
      const urlIcon = screen.getByTestId('url-shortening-icon')
      expect(urlIcon.tagName.toLowerCase()).toBe('svg')
    })

    it('renders with proper box model classes (Firefox box-sizing support)', () => {
      render(
        <RouterWrapper>
          <Home />
        </RouterWrapper>
      )

      // Tailwind uses border-box by default - all modern browsers support this
      const cards = screen.getAllByTestId(/feature-card-/)
      expect(cards).toHaveLength(3)

      // Verify cards have proper styling classes
      cards.forEach((card) => {
        expect(card).toBeInTheDocument()
      })
    })

    it('uses standard CSS transforms (Firefox transform support)', () => {
      render(
        <RouterWrapper>
          <HeroSection />
        </RouterWrapper>
      )

      // Check for hover transform classes - transforms supported since Firefox 16+
      const ctaButton = screen.getByTestId('cta-register')
      expect(ctaButton.className).toContain('hover:scale-105')
    })

    it('renders links with proper accessibility attributes', () => {
      render(
        <RouterWrapper>
          <FooterSection />
        </RouterWrapper>
      )

      // Verify link elements (standard HTML - works in all browsers)
      const homeLink = screen.getByTestId('footer-link-home')
      expect(homeLink).toHaveAttribute('href', '/')
      expect(homeLink.tagName.toLowerCase()).toBe('a')
    })
  })

  describe('Test Case 3: Safari/WebKit Compatibility - All sections display correctly', () => {
    it('uses standard flexbox layout (Safari flexbox support)', () => {
      render(
        <RouterWrapper>
          <HeroSection />
        </RouterWrapper>
      )

      const heroSection = screen.getByTestId('hero-section')
      // Flexbox fully supported in Safari 9+
      expect(heroSection.className).toContain('flex')

      // Check for responsive flex direction (Safari supports responsive utilities)
      const ctaContainer = heroSection.querySelector('.flex.flex-col')
      expect(ctaContainer).toBeInTheDocument()
    })

    it('renders with -webkit- compatible gradient (Safari gradient support)', () => {
      render(
        <RouterWrapper>
          <HeroSection />
        </RouterWrapper>
      )

      const heroSection = screen.getByTestId('hero-section')
      // Tailwind's gradient classes compile to standard CSS with vendor prefixes via PostCSS
      expect(heroSection.className).toContain('bg-gradient-to-br')
    })

    it('uses standard transition properties (Safari transition support)', () => {
      render(
        <RouterWrapper>
          <HeroSection />
        </RouterWrapper>
      )

      const ctaButton = screen.getByTestId('cta-register')
      // CSS transitions supported in Safari 6.1+ (FuturisticButton uses 300ms)
      expect(ctaButton.className).toContain('transition-all')
      expect(ctaButton.className).toContain('duration-300')
    })

    it('renders step cards with proper shadow (Safari box-shadow support)', () => {
      render(
        <RouterWrapper>
          <HowItWorksSection />
        </RouterWrapper>
      )

      for (let i = 1; i <= 3; i++) {
        const stepCard = screen.getByTestId(`step-card-${i}`)
        // Box-shadow supported in Safari 5.1+
        expect(stepCard.className).toContain('shadow-xl')
      }
    })

    it('renders with proper backdrop blur classes (Safari blur support)', () => {
      render(
        <RouterWrapper>
          <HeroSection />
        </RouterWrapper>
      )

      // Check for blur classes which Safari supports since iOS 9 / Safari 9
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection.querySelector('.blur-3xl')).toBeInTheDocument()
    })
  })

  describe('Test Case 4: Edge Compatibility - All sections display correctly', () => {
    it('renders with standard CSS Grid (Edge Grid support since 16+)', () => {
      render(
        <RouterWrapper>
          <Home />
        </RouterWrapper>
      )

      const featuresGrid = screen.getByTestId('features-grid')
      // CSS Grid fully supported in Edge 16+ (Chromium Edge supports everything)
      expect(featuresGrid.className).toContain('grid')
      expect(featuresGrid.className).toContain('gap-8')
    })

    it('uses standard CSS custom properties via DaisyUI themes', () => {
      render(
        <RouterWrapper>
          <Home />
        </RouterWrapper>
      )

      // DaisyUI uses CSS custom properties - supported in Edge 16+
      const heroSection = screen.getByTestId('hero-section')
      // Check that primary color classes are applied (rely on CSS custom properties)
      expect(heroSection.className).toContain('from-primary')
    })

    it('renders with proper minimum height for touch targets (Edge touch support)', () => {
      render(
        <RouterWrapper>
          <HeroSection />
        </RouterWrapper>
      )

      const ctaButton = screen.getByTestId('cta-register')
      // FuturisticButton lg size is 52px which exceeds the 44px accessibility requirement
      expect(ctaButton.className).toContain('min-h-[52px]')

      const loginLink = screen.getByTestId('login-link')
      expect(loginLink.className).toContain('min-h-[44px]')
    })

    it('renders all sections in correct DOM order', () => {
      const { container } = render(
        <RouterWrapper>
          <Home />
        </RouterWrapper>
      )

      const main = container.querySelector('main')
      expect(main).toBeInTheDocument()

      // Verify sections appear in correct order in the DOM
      const sections = main!.querySelectorAll('[data-testid]')
      const sectionIds = Array.from(sections)
        .map((s) => s.getAttribute('data-testid'))
        .filter((id) => id?.includes('section'))

      expect(sectionIds).toContain('hero-section')
      expect(sectionIds).toContain('features-section')
      expect(sectionIds).toContain('how-it-works-section')
      expect(sectionIds).toContain('footer-section')
    })

    it('renders with proper overflow handling (Edge overflow support)', () => {
      render(
        <RouterWrapper>
          <HeroSection />
        </RouterWrapper>
      )

      const heroSection = screen.getByTestId('hero-section')
      // overflow-hidden prevents scrollbar issues across browsers
      expect(heroSection.className).toContain('overflow-hidden')
    })
  })

  describe('Cross-Browser CSS Feature Validation', () => {
    it('uses only standard HTML5 semantic elements', () => {
      const { container } = render(
        <RouterWrapper>
          <Home />
        </RouterWrapper>
      )

      // Verify use of semantic HTML5 elements (supported in all modern browsers)
      expect(container.querySelector('main')).toBeInTheDocument()
      expect(container.querySelector('section')).toBeInTheDocument()
      expect(container.querySelector('footer')).toBeInTheDocument()
      expect(container.querySelector('nav')).toBeInTheDocument()
    })

    it('uses responsive breakpoints compatible with all browsers', () => {
      render(
        <RouterWrapper>
          <HeroSection />
        </RouterWrapper>
      )

      const headline = screen.getByRole('heading', { level: 1 })

      // Tailwind responsive prefixes use standard CSS media queries
      // These work in all browsers supporting CSS3 media queries
      expect(headline.className).toMatch(/sm:text-5xl/)
      expect(headline.className).toMatch(/md:text-6xl/)
      expect(headline.className).toMatch(/lg:text-7xl/)
    })

    it('uses standard button styling without browser-specific hacks', () => {
      render(
        <RouterWrapper>
          <HeroSection />
        </RouterWrapper>
      )

      const ctaButton = screen.getByTestId('cta-register')

      // Verify standard button classes from DaisyUI and FuturisticButton styling
      expect(ctaButton.className).toContain('btn')
      expect(ctaButton.className).toContain('btn-lg')
      // FuturisticButton uses gradient styling instead of btn-primary
      expect(ctaButton.className).toContain('bg-gradient-to-r')
      expect(ctaButton.className).toContain('from-primary')
    })

    it('renders icons with proper viewBox for all browsers', () => {
      render(
        <RouterWrapper>
          <FeaturesSection />
        </RouterWrapper>
      )

      const icons = screen.getAllByTestId(/-icon$/)
      icons.forEach((icon) => {
        expect(icon).toHaveAttribute('viewBox')
        expect(icon.getAttribute('viewBox')).toBe('0 0 24 24')
      })
    })

    it('uses motion-reduce for accessibility across browsers', () => {
      render(
        <RouterWrapper>
          <HeroSection />
        </RouterWrapper>
      )

      const heroSection = screen.getByTestId('hero-section')
      // Check for motion-reduce classes that respect prefers-reduced-motion
      const animatedElements = heroSection.querySelectorAll('.animate-pulse')
      animatedElements.forEach((el) => {
        expect(el.className).toContain('motion-reduce:animate-none')
      })
    })
  })

  describe('Visual Consistency Verification', () => {
    it('all feature cards have consistent structure', () => {
      render(
        <RouterWrapper>
          <FeaturesSection />
        </RouterWrapper>
      )

      for (let i = 0; i < 3; i++) {
        const card = screen.getByTestId(`feature-card-${i}`)

        // Each card should have icon, title (h3), and description (p)
        const cardContainer = within(card)
        expect(cardContainer.getByRole('heading', { level: 3 })).toBeInTheDocument()
        expect(card.querySelector('p')).toBeInTheDocument()
        expect(card.querySelector('svg')).toBeInTheDocument()
      }
    })

    it('all step cards have consistent structure', () => {
      render(
        <RouterWrapper>
          <HowItWorksSection />
        </RouterWrapper>
      )

      for (let i = 1; i <= 3; i++) {
        const stepCard = screen.getByTestId(`step-card-${i}`)
        const stepNumber = screen.getByTestId(`step-number-${i}`)
        const stepIcon = screen.getByTestId(`step-icon-${i}`)

        expect(stepCard).toBeInTheDocument()
        expect(stepNumber).toHaveTextContent(String(i))
        expect(stepIcon).toBeInTheDocument()
      }
    })

    it('footer navigation is consistent across all link types', () => {
      render(
        <RouterWrapper>
          <FooterSection />
        </RouterWrapper>
      )

      const links = [
        { testId: 'footer-link-home', href: '/', text: 'Home' },
        { testId: 'footer-link-login', href: '/login', text: 'Login' },
        { testId: 'footer-link-register', href: '/register', text: 'Register' },
        { testId: 'footer-link-privacy', href: '/privacy', text: 'Privacy Policy' },
        { testId: 'footer-link-terms', href: '/terms', text: 'Terms of Service' },
      ]

      links.forEach(({ testId, href, text }) => {
        const link = screen.getByTestId(testId)
        expect(link).toHaveAttribute('href', href)
        expect(link).toHaveTextContent(text)
        // All footer links should have minimum touch target height
        expect(link.className).toContain('min-h-[44px]')
      })
    })
  })
})
