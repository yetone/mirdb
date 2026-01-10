import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import Home from '../pages/Home'
import HeroSection from '../components/HeroSection'
import FeaturesSection from '../components/FeaturesSection'

/**
 * Theme Support - Synthwave Theme Tests (NFR-3)
 *
 * Verifies the homepage renders correctly in synthwave theme with:
 * - Proper DaisyUI semantic color tokens that adapt to synthwave theme
 * - Appropriate contrast for text and backgrounds (neon colors on dark backgrounds)
 * - Visual consistency across all sections without color clashing or illegible text
 */

// Helper to set up synthwave theme before tests
const setupSynthwaveTheme = () => {
  document.documentElement.setAttribute('data-theme', 'synthwave')
}

// Helper to clean up theme after tests
const cleanupTheme = () => {
  document.documentElement.removeAttribute('data-theme')
}

describe('Theme Support - Synthwave Theme', () => {
  beforeEach(() => {
    setupSynthwaveTheme()
  })

  afterEach(() => {
    cleanupTheme()
  })

  // Test Case 1: Integration test - Render homepage with synthwave theme
  describe('Homepage Synthwave Theme Integration', () => {
    it('should render homepage with all sections when synthwave theme is active', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      // Verify synthwave theme is set
      expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave')

      // Verify all main sections render
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer-section')).toBeInTheDocument()
    })

    it('should use DaisyUI semantic color tokens that adapt to synthwave theme', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      // Features section uses bg-base-200 which adapts to synthwave dark background
      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toHaveClass('bg-base-200')

      // How It Works section uses bg-base-200
      const howItWorksSection = screen.getByTestId('how-it-works-section')
      expect(howItWorksSection).toHaveClass('bg-base-200')

      // Footer section uses bg-base-200
      const footerSection = screen.getByTestId('footer-section')
      expect(footerSection).toHaveClass('bg-base-200')
    })

    it('should display readable text with proper contrast in synthwave theme', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      // All headings should be rendered and visible
      const powerfulFeaturesHeading = screen.getByRole('heading', { name: /powerful features/i })
      expect(powerfulFeaturesHeading).toBeInTheDocument()

      const howItWorksHeading = screen.getByRole('heading', { name: /how it works/i })
      expect(howItWorksHeading).toBeInTheDocument()

      // Main headline in hero section
      const mainHeadline = screen.getByRole('heading', { level: 1 })
      expect(mainHeadline).toBeInTheDocument()
      expect(mainHeadline).toHaveTextContent('Shorten, Share, Track')
    })
  })

  // Test Case 2: Unit test - Text contrast verification in synthwave theme
  describe('Text Contrast in Synthwave Theme', () => {
    it('should have hero section text with high contrast (white on gradient)', () => {
      render(
        <MemoryRouter>
          <HeroSection />
        </MemoryRouter>
      )

      // Main headline should have white text for contrast on gradient
      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline).toHaveClass('text-white')

      // Subheadline uses semi-transparent white for readable contrast
      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline).toHaveClass('text-white/90')
    })

    it('should have feature card descriptions with readable text using base-content color', () => {
      render(<FeaturesSection />)

      // Feature descriptions use text-base-content/70 for proper contrast in synthwave theme
      const urlDescription = screen.getByText(/create memorable, short links instantly/i)
      expect(urlDescription).toHaveClass('text-base-content/70')

      const analyticsDescription = screen.getByText(/track clicks, locations, and referrers/i)
      expect(analyticsDescription).toHaveClass('text-base-content/70')

      const linkDescription = screen.getByText(/organize and manage all your links/i)
      expect(linkDescription).toHaveClass('text-base-content/70')
    })

    it('should have step card descriptions with readable text in synthwave theme', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      // Step card descriptions should use text-base-content/70 for readability
      const stepDescriptions = [
        screen.getByText(/copy your long url and paste/i),
        screen.getByText(/instantly receive a compact/i),
        screen.getByText(/share your link and monitor/i),
      ]

      stepDescriptions.forEach((description) => {
        expect(description).toHaveClass('text-base-content/70')
      })
    })

    it('should have footer text with proper contrast using base-content colors', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      // Footer brand description uses text-base-content/70
      // Use the footer-brand testid to scope the search
      const footerBrand = screen.getByTestId('footer-brand')
      const brandDescription = footerBrand.querySelector('p')
      expect(brandDescription).toHaveClass('text-base-content/70')

      // Copyright text uses text-base-content/60 for subdued appearance
      const copyright = screen.getByTestId('footer-copyright')
      expect(copyright).toHaveClass('text-base-content/60')
    })
  })

  // Test Case 3: Hero section visual consistency in synthwave theme
  describe('Hero Section Synthwave Theme', () => {
    it('should render hero section with gradient background using DaisyUI semantic colors', () => {
      render(
        <MemoryRouter>
          <HeroSection />
        </MemoryRouter>
      )

      const heroSection = screen.getByTestId('hero-section')

      // Hero uses gradient with DaisyUI primary/secondary/accent colors
      // These colors adapt to synthwave theme (neon pink, purple, cyan)
      expect(heroSection).toHaveClass('bg-gradient-to-br')
      expect(heroSection).toHaveClass('from-primary')
      expect(heroSection).toHaveClass('via-secondary')
      expect(heroSection).toHaveClass('to-accent')
    })

    it('should have visible CTA buttons with proper styling in synthwave theme', () => {
      render(
        <MemoryRouter>
          <HeroSection />
        </MemoryRouter>
      )

      // Primary CTA button should be visible with proper styling
      const primaryCTA = screen.getByRole('link', { name: /get started free/i })
      expect(primaryCTA).toBeInTheDocument()
      expect(primaryCTA).toHaveClass('btn')
      expect(primaryCTA).toHaveClass('btn-lg')
      expect(primaryCTA).toHaveClass('btn-primary')
      expect(primaryCTA).toHaveClass('bg-white')
      expect(primaryCTA).toHaveClass('text-primary')

      // Secondary CTA link should be visible
      const loginLink = screen.getByTestId('login-link')
      expect(loginLink).toBeInTheDocument()
      expect(loginLink).toHaveClass('text-white')
    })

    it('should have animated background elements with proper opacity', () => {
      render(
        <MemoryRouter>
          <HeroSection />
        </MemoryRouter>
      )

      const heroSection = screen.getByTestId('hero-section')

      // Background animation container should have overflow-hidden to contain effects
      expect(heroSection).toHaveClass('overflow-hidden')
    })
  })

  // Test Case 4: Feature cards visual consistency in synthwave theme
  describe('Feature Cards Synthwave Theme', () => {
    it('should render feature cards with GlassMorphism styling that works in synthwave theme', () => {
      render(<FeaturesSection />)

      const cards = screen.getAllByTestId('glassmorphism-card')
      expect(cards).toHaveLength(3)

      cards.forEach((card) => {
        // GlassMorphism uses semi-transparent bg-base-100
        expect(card).toHaveClass('bg-base-100/30')
        // Border uses base-content for theme adaptability
        expect(card).toHaveClass('border-base-content/10')
        // Blur effect for glass look
        expect(card).toHaveClass('backdrop-blur-md')
      })
    })

    it('should have feature card icons with primary color that adapts to synthwave theme', () => {
      render(<FeaturesSection />)

      const urlShorteningIcon = screen.getByTestId('url-shortening-icon')
      const analyticsIcon = screen.getByTestId('analytics-icon')
      const linkManagementIcon = screen.getByTestId('link-management-icon')

      // Icons should be inside containers with text-primary class
      // In synthwave theme, primary is a neon pink/magenta color
      expect(urlShorteningIcon.closest('.text-primary')).toBeInTheDocument()
      expect(analyticsIcon.closest('.text-primary')).toBeInTheDocument()
      expect(linkManagementIcon.closest('.text-primary')).toBeInTheDocument()
    })

    it('should have feature card titles rendered correctly in synthwave theme', () => {
      render(<FeaturesSection />)

      const urlTitle = screen.getByText('URL Shortening')
      const analyticsTitle = screen.getByText('Analytics Dashboard')
      const linkTitle = screen.getByText('Link Management')

      // Titles should be present and styled
      expect(urlTitle).toBeInTheDocument()
      expect(analyticsTitle).toBeInTheDocument()
      expect(linkTitle).toBeInTheDocument()

      // Titles should have font-semibold styling
      expect(urlTitle).toHaveClass('font-semibold')
      expect(analyticsTitle).toHaveClass('font-semibold')
      expect(linkTitle).toHaveClass('font-semibold')
    })
  })

  // Test Case 5: Section background consistency in synthwave theme
  describe('Section Background Consistency', () => {
    it('should have consistent background colors across sections that adapt to synthwave theme', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      // Hero section uses gradient (primary/secondary/accent adapt to synthwave neon colors)
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toHaveClass('from-primary')
      expect(heroSection).toHaveClass('via-secondary')
      expect(heroSection).toHaveClass('to-accent')

      // Features and How It Works sections use base-200 for subtle contrast
      const featuresSection = screen.getByTestId('features-section')
      const howItWorksSection = screen.getByTestId('how-it-works-section')

      expect(featuresSection).toHaveClass('bg-base-200')
      expect(howItWorksSection).toHaveClass('bg-base-200')
    })

    it('should have step cards with proper synthwave theme styling', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      // Step cards use bg-base-100 which adapts to synthwave dark background
      const stepCards = screen.getAllByTestId(/step-card-\d/)
      expect(stepCards).toHaveLength(3)

      stepCards.forEach((card) => {
        expect(card).toHaveClass('bg-base-100')
        expect(card).toHaveClass('shadow-xl')
      })
    })

    it('should have step numbers with primary color background in synthwave theme', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      const stepNumber1 = screen.getByTestId('step-number-1')
      const stepNumber2 = screen.getByTestId('step-number-2')
      const stepNumber3 = screen.getByTestId('step-number-3')

      // Step numbers use bg-primary and text-primary-content for theme adaptability
      // In synthwave theme, primary is neon pink/magenta
      expect(stepNumber1).toHaveClass('bg-primary')
      expect(stepNumber1).toHaveClass('text-primary-content')

      expect(stepNumber2).toHaveClass('bg-primary')
      expect(stepNumber2).toHaveClass('text-primary-content')

      expect(stepNumber3).toHaveClass('bg-primary')
      expect(stepNumber3).toHaveClass('text-primary-content')
    })
  })

  // Test Case 6: Footer section in synthwave theme
  describe('Footer Section Synthwave Theme', () => {
    it('should have footer with base-200 background for synthwave theme', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      const footer = screen.getByTestId('footer-section')
      expect(footer).toHaveClass('bg-base-200')
    })

    it('should have footer links with proper text colors for synthwave theme', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      // Footer navigation links use text-base-content/70
      const footerNav = screen.getByTestId('footer-nav')
      const homeLink = footerNav.querySelector('a')
      expect(homeLink).toHaveClass('text-base-content/70')
    })

    it('should have copyright text with subdued color for synthwave theme', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      const copyright = screen.getByTestId('footer-copyright')
      expect(copyright).toHaveClass('text-base-content/60')
    })

    it('should have footer border with proper opacity for synthwave theme', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      const copyright = screen.getByTestId('footer-copyright')
      expect(copyright).toHaveClass('border-base-content/10')
    })
  })

  // Test Case 7: Accessibility - touch targets in synthwave theme
  describe('Accessibility in Synthwave Theme', () => {
    it('should have minimum touch target sizes for interactive elements', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      // Primary CTA should have minimum height for touch targets
      const primaryCTA = screen.getByRole('link', { name: /get started free/i })
      expect(primaryCTA).toHaveClass('min-h-[44px]')

      // Login link should have minimum height
      const loginLink = screen.getByTestId('login-link')
      expect(loginLink).toHaveClass('min-h-[44px]')

      // Footer links should have minimum height
      const footerHomeLink = screen.getByTestId('footer-link-home')
      expect(footerHomeLink).toHaveClass('min-h-[44px]')
    })
  })
})
