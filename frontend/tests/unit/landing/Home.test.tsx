/**
 * Home Page Integration Tests
 * Owner: Scenario 14 - Integration with Existing Components
 *
 * Tests that verify the Home page properly integrates with existing
 * application components (Navbar, GlassMorphismCard, FuturisticButton, BackgroundEffect)
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { screen, within } from '@testing-library/react'
import { renderWithProviders } from './test-utils'
import Home from '../../../src/pages/Home'

// Mock matchMedia for responsive tests
beforeEach(() => {
  Object.defineProperty(window, 'matchMedia', {
    writable: true,
    value: vi.fn().mockImplementation((query: string) => ({
      matches: false,
      media: query,
      onchange: null,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      dispatchEvent: vi.fn(),
    })),
  })

  // Clear localStorage before each test
  localStorage.clear()
})

afterEach(() => {
  vi.restoreAllMocks()
})

describe('Home Page - Integration with Existing Components', () => {
  describe('Test Case 1: Navbar Integration', () => {
    it('renders Navbar component at top of page', () => {
      renderWithProviders(<Home />)

      // Verify Navbar is present - look for nav element with navbar class
      const navbar = document.querySelector('nav.navbar')
      expect(navbar).toBeTruthy()

      // Verify Navbar contains expected navigation elements
      expect(screen.getByText('URL Shortener')).toBeInTheDocument()

      // Verify Login and Register links are present when not authenticated
      // Use getAllByRole since there are multiple links (navbar + footer)
      const loginLinks = screen.getAllByRole('link', { name: /login/i })
      const registerLinks = screen.getAllByRole('link', { name: /register/i })
      expect(loginLinks.length).toBeGreaterThan(0)
      expect(registerLinks.length).toBeGreaterThan(0)
    })

    it('Navbar provides consistent navigation across the app', () => {
      renderWithProviders(<Home />)

      // Get the navbar element
      const navbar = document.querySelector('nav.navbar')
      expect(navbar).toBeTruthy()

      // Use within to scope queries to the navbar
      const navbarElement = navbar as HTMLElement
      const loginLink = within(navbarElement).getByRole('link', { name: /login/i })
      expect(loginLink).toHaveAttribute('href', '/login')

      // Verify register link points to /register
      const registerLink = within(navbarElement).getByRole('link', { name: /register/i })
      expect(registerLink).toHaveAttribute('href', '/register')
    })
  })

  describe('Test Case 2: GlassMorphismCard Integration', () => {
    it('feature cards use GlassMorphismCard component', () => {
      renderWithProviders(<Home />)

      // Verify FeaturesSection is rendered
      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()

      // Verify GlassMorphismCard components are used (they have the card class with backdrop-blur)
      const featureCards = featuresSection.querySelectorAll('.card.backdrop-blur-lg')
      expect(featureCards.length).toBeGreaterThan(0)

      // Verify at least 3 feature cards are rendered
      expect(featureCards.length).toBeGreaterThanOrEqual(3)
    })

    it('GlassMorphismCard provides consistent visual design', () => {
      renderWithProviders(<Home />)

      const featuresSection = screen.getByTestId('features-section')
      const cards = featuresSection.querySelectorAll('.card')

      // Each card should have the glassmorphism styling
      cards.forEach((card) => {
        expect(card.classList.contains('backdrop-blur-lg')).toBe(true)
        expect(card.classList.contains('bg-base-200/50')).toBe(true)
      })
    })
  })

  describe('Test Case 3: FuturisticButton Integration', () => {
    it('primary and secondary CTAs use FuturisticButton component', () => {
      renderWithProviders(<Home />)

      // Check Hero section CTAs
      const heroSection = screen.getByTestId('hero-section')

      // Primary CTA should be present
      const primaryCTA = screen.getByTestId('hero-primary-cta')
      expect(primaryCTA).toBeInTheDocument()
      expect(primaryCTA.classList.contains('btn')).toBe(true)
      expect(primaryCTA.classList.contains('btn-primary')).toBe(true)

      // Secondary CTA should be present
      const secondaryCTA = screen.getByTestId('hero-secondary-cta')
      expect(secondaryCTA).toBeInTheDocument()
      expect(secondaryCTA.classList.contains('btn')).toBe(true)
      expect(secondaryCTA.classList.contains('btn-secondary')).toBe(true)
    })

    it('CTA buttons have FuturisticButton hover effects', () => {
      renderWithProviders(<Home />)

      const primaryCTA = screen.getByTestId('hero-primary-cta')
      const secondaryCTA = screen.getByTestId('hero-secondary-cta')

      // FuturisticButton adds hover:scale-105 class for transform effect
      expect(primaryCTA.classList.contains('hover:scale-105')).toBe(true)
      expect(secondaryCTA.classList.contains('hover:scale-105')).toBe(true)
    })

    it('secondary CTA section also uses FuturisticButton', () => {
      renderWithProviders(<Home />)

      const ctaSection = screen.getByTestId('cta-section')
      const ctaButton = screen.getByTestId('cta-button')

      expect(ctaButton).toBeInTheDocument()
      expect(ctaButton.classList.contains('btn')).toBe(true)
      expect(ctaButton.classList.contains('btn-primary')).toBe(true)
    })
  })

  describe('Test Case 4: BackgroundEffect Integration', () => {
    it('BackgroundEffect component provides visual backdrop', () => {
      renderWithProviders(<Home />)

      // BackgroundEffect renders a fixed div with -z-10 and animated blur elements
      const bgEffect = document.querySelector('.fixed.inset-0.-z-10')
      expect(bgEffect).toBeTruthy()

      // Verify animated elements are present
      const animatedElements = document.querySelectorAll('.animate-pulse')
      expect(animatedElements.length).toBeGreaterThan(0)
    })

    it('BackgroundEffect uses theme-aware colors', () => {
      renderWithProviders(<Home />)

      // BackgroundEffect should use primary and secondary theme colors
      const primaryBg = document.querySelector('.bg-primary\\/20')
      const secondaryBg = document.querySelector('.bg-secondary\\/20')

      expect(primaryBg).toBeTruthy()
      expect(secondaryBg).toBeTruthy()
    })
  })

  describe('Test Case 5: ThemeContext Integration', () => {
    it('landing page components respond to ThemeContext', () => {
      renderWithProviders(<Home />)

      // Verify theme-aware classes are applied
      const mainContainer = document.querySelector('.bg-base-100')
      expect(mainContainer).toBeTruthy()

      // Navbar should use theme-aware colors
      const navbar = document.querySelector('.bg-base-100\\/80')
      expect(navbar).toBeTruthy()

      // Headlines should use theme-aware text colors
      const headline = screen.getByTestId('hero-headline')
      expect(headline.classList.contains('text-base-content')).toBe(true)
    })

    it('theme classes work with DaisyUI theme system', () => {
      renderWithProviders(<Home />)

      // Verify various theme-aware classes are used throughout
      const themeAwareElements = document.querySelectorAll('[class*="base-"]')
      expect(themeAwareElements.length).toBeGreaterThan(0)

      // Check for text-base-content usage
      const textContentElements = document.querySelectorAll('.text-base-content')
      expect(textContentElements.length).toBeGreaterThan(0)
    })

    it('all landing sections use theme-compatible styling', () => {
      renderWithProviders(<Home />)

      // Check each section for theme-aware styling
      const heroSection = screen.getByTestId('hero-section')
      const featuresSection = screen.getByTestId('features-section')
      const howItWorksSection = screen.getByTestId('how-it-works-section')
      const socialProofSection = screen.getByTestId('social-proof-section')
      const ctaSection = screen.getByTestId('cta-section')
      const footer = screen.getByTestId('footer')

      // All sections should be present
      expect(heroSection).toBeInTheDocument()
      expect(featuresSection).toBeInTheDocument()
      expect(howItWorksSection).toBeInTheDocument()
      expect(socialProofSection).toBeInTheDocument()
      expect(ctaSection).toBeInTheDocument()
      expect(footer).toBeInTheDocument()

      // Verify theme-aware background classes on sections
      expect(howItWorksSection.classList.contains('bg-base-200/50')).toBe(true)
      expect(ctaSection.classList.contains('bg-base-200')).toBe(true)
      expect(footer.classList.contains('bg-base-300')).toBe(true)
    })
  })

  describe('Complete Integration - All Sections Present', () => {
    it('Home page assembles all required landing components', () => {
      renderWithProviders(<Home />)

      // Verify all sections are rendered in the correct order
      const main = document.querySelector('main')
      expect(main).toBeTruthy()

      // All sections should be present
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
      expect(screen.getByTestId('social-proof-section')).toBeInTheDocument()
      expect(screen.getByTestId('cta-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer')).toBeInTheDocument()
    })

    it('SocialProof section uses GlassMorphismCard for statistics', () => {
      renderWithProviders(<Home />)

      const socialProofSection = screen.getByTestId('social-proof-section')
      const statisticsGrid = within(socialProofSection).getByTestId('statistics-grid')

      // Statistics should be wrapped in GlassMorphismCard
      const statCards = statisticsGrid.querySelectorAll('.card.backdrop-blur-lg')
      expect(statCards.length).toBeGreaterThan(0)
    })

    it('trust indicators use GlassMorphismCard', () => {
      renderWithProviders(<Home />)

      const socialProofSection = screen.getByTestId('social-proof-section')
      const trustIndicators = within(socialProofSection).getByTestId('trust-indicators')

      // Trust indicators should be wrapped in GlassMorphismCard
      const trustCards = trustIndicators.querySelectorAll('.card.backdrop-blur-lg')
      expect(trustCards.length).toBe(2) // Security and Uptime cards
    })
  })
})
