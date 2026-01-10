import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import Home from '../pages/Home'
import HeroSection from '../components/HeroSection'
import FeaturesSection from '../components/FeaturesSection'

/**
 * Theme Support - Cyberpunk Theme Tests (NFR-3)
 *
 * Verifies the homepage renders correctly with cyberpunk DaisyUI theme:
 * - All sections display with cyberpunk theme styling without visual issues
 * - Proper DaisyUI semantic color tokens that adapt to cyberpunk theme
 * - Appropriate contrast for text and backgrounds
 * - Visual consistency across all sections
 * - CTA buttons maintain visibility and readability
 */

// Helper to set up cyberpunk theme before tests
const setupCyberpunkTheme = () => {
  document.documentElement.setAttribute('data-theme', 'cyberpunk')
}

// Helper to clean up theme after tests
const cleanupTheme = () => {
  document.documentElement.removeAttribute('data-theme')
}

describe('Theme Support - Cyberpunk Theme', () => {
  beforeEach(() => {
    setupCyberpunkTheme()
  })

  afterEach(() => {
    cleanupTheme()
  })

  // Test Case 1: Integration test - Render homepage with cyberpunk theme
  describe('Homepage Cyberpunk Theme Integration', () => {
    it('should render homepage with all sections when cyberpunk theme is active', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      // Verify cyberpunk theme is set
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')

      // Verify all main sections render
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer-section')).toBeInTheDocument()
    })

    it('should use DaisyUI semantic color tokens that adapt to cyberpunk theme', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      // Features section uses bg-base-200 which adapts to cyberpunk theme
      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toHaveClass('bg-base-200')

      // How It Works section uses bg-base-200
      const howItWorksSection = screen.getByTestId('how-it-works-section')
      expect(howItWorksSection).toHaveClass('bg-base-200')

      // Footer section uses bg-base-200
      const footerSection = screen.getByTestId('footer-section')
      expect(footerSection).toHaveClass('bg-base-200')
    })

    it('should display readable text with proper contrast in cyberpunk theme', () => {
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

    it('should render all sections without visual artifacts in cyberpunk theme', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      // Verify theme is active
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')

      // Hero section should have proper styling
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toHaveClass('bg-gradient-to-br')
      expect(heroSection).toHaveClass('from-primary')
      expect(heroSection).toHaveClass('via-secondary')
      expect(heroSection).toHaveClass('to-accent')

      // Features grid should be present with proper layout
      const featuresGrid = screen.getByTestId('features-grid')
      expect(featuresGrid).toHaveClass('grid')

      // Steps container should be present
      const stepsContainer = screen.getByTestId('steps-container')
      expect(stepsContainer).toHaveClass('grid')

      // Footer grid should be present
      const footerGrid = screen.getByTestId('footer-grid')
      expect(footerGrid).toHaveClass('grid')
    })
  })

  // Test Case 2: Unit test - CTA buttons in cyberpunk theme
  describe('CTA Buttons Cyberpunk Theme', () => {
    it('should render primary CTA button with proper styling for visibility', () => {
      render(
        <MemoryRouter>
          <HeroSection />
        </MemoryRouter>
      )

      // Primary CTA button should be visible and accessible with FuturisticButton styling
      const primaryCTA = screen.getByRole('link', { name: /get started free/i })
      expect(primaryCTA).toBeInTheDocument()

      // Button should have proper DaisyUI btn classes
      expect(primaryCTA).toHaveClass('btn')
      expect(primaryCTA).toHaveClass('btn-lg')
      // FuturisticButton uses gradient styling instead of btn-primary
      expect(primaryCTA.className).toContain('bg-gradient-to-r')
      expect(primaryCTA.className).toContain('from-primary')
      expect(primaryCTA.className).toContain('to-secondary')
      expect(primaryCTA.className).toContain('text-primary-content')

      // Button should have shadow for visibility
      expect(primaryCTA.className).toContain('shadow')
    })

    it('should render secondary CTA link with proper styling', () => {
      render(
        <MemoryRouter>
          <HeroSection />
        </MemoryRouter>
      )

      // Secondary CTA link should be visible
      const loginLink = screen.getByTestId('login-link')
      expect(loginLink).toBeInTheDocument()
      expect(loginLink).toHaveClass('text-white')
      expect(loginLink).toHaveClass('font-semibold')
    })

    it('should have accessible CTA buttons with minimum touch target size', () => {
      render(
        <MemoryRouter>
          <HeroSection />
        </MemoryRouter>
      )

      // Primary CTA should have minimum height for touch targets (FuturisticButton lg size is 52px)
      const primaryCTA = screen.getByRole('link', { name: /get started free/i })
      expect(primaryCTA.className).toContain('min-h-[52px]')

      // Login link should have minimum height
      const loginLink = screen.getByTestId('login-link')
      expect(loginLink).toHaveClass('min-h-[44px]')
    })

    it('should maintain button readability with cyberpunk theme colors', () => {
      render(
        <MemoryRouter>
          <HeroSection />
        </MemoryRouter>
      )

      // Verify cyberpunk theme is active
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')

      // Primary CTA uses gradient background with theme-aware text color
      const primaryCTA = screen.getByTestId('cta-register')
      expect(primaryCTA.className).toContain('bg-gradient-to-r')
      expect(primaryCTA.className).toContain('from-primary')
      expect(primaryCTA.className).toContain('text-primary-content')

      // This ensures the button remains readable regardless of the
      // cyberpunk theme's primary color (typically cyan/yellow)
    })
  })

  // Test Case: Hero section cyberpunk theme specific tests
  describe('Hero Section Cyberpunk Theme', () => {
    it('should render hero section with gradient background that uses theme colors', () => {
      render(
        <MemoryRouter>
          <HeroSection />
        </MemoryRouter>
      )

      const heroSection = screen.getByTestId('hero-section')

      // Hero uses gradient with DaisyUI primary/secondary/accent colors
      // In cyberpunk theme, these are typically cyan, magenta, yellow
      expect(heroSection).toHaveClass('bg-gradient-to-br')
      expect(heroSection).toHaveClass('from-primary')
      expect(heroSection).toHaveClass('via-secondary')
      expect(heroSection).toHaveClass('to-accent')
    })

    it('should have white text for contrast on cyberpunk gradient background', () => {
      render(
        <MemoryRouter>
          <HeroSection />
        </MemoryRouter>
      )

      // Main headline should have white text for contrast on gradient
      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline).toHaveClass('text-white')

      // Subheadline uses semi-transparent white
      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline).toHaveClass('text-white/90')
    })

    it('should have animated background elements with proper styling', () => {
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

  // Test: Feature cards in cyberpunk theme
  describe('Feature Cards Cyberpunk Theme', () => {
    it('should render feature cards with GlassMorphism styling that works in cyberpunk theme', () => {
      render(<FeaturesSection />)

      const cards = screen.getAllByTestId('glassmorphism-card')
      expect(cards).toHaveLength(3)

      cards.forEach((card) => {
        // GlassMorphism uses semi-transparent bg-base-100 which adapts to cyberpunk
        expect(card).toHaveClass('bg-base-100/30')
        // Border uses base-content for theme adaptability
        expect(card).toHaveClass('border-base-content/10')
        // Blur effect for glass look
        expect(card).toHaveClass('backdrop-blur-md')
      })
    })

    it('should have feature card icons with primary color that adapts to cyberpunk theme', () => {
      render(<FeaturesSection />)

      const urlShorteningIcon = screen.getByTestId('url-shortening-icon')
      const analyticsIcon = screen.getByTestId('analytics-icon')
      const linkManagementIcon = screen.getByTestId('link-management-icon')

      // Icons should be inside containers with text-primary class
      // In cyberpunk theme, primary is typically cyan
      expect(urlShorteningIcon.closest('.text-primary')).toBeInTheDocument()
      expect(analyticsIcon.closest('.text-primary')).toBeInTheDocument()
      expect(linkManagementIcon.closest('.text-primary')).toBeInTheDocument()
    })

    it('should have feature card descriptions with readable text using base-content color', () => {
      render(<FeaturesSection />)

      // Feature descriptions use text-base-content/70 for proper contrast
      const urlDescription = screen.getByText(/create memorable, short links instantly/i)
      expect(urlDescription).toHaveClass('text-base-content/70')

      const analyticsDescription = screen.getByText(/track clicks, locations, and referrers/i)
      expect(analyticsDescription).toHaveClass('text-base-content/70')

      const linkDescription = screen.getByText(/organize and manage all your links/i)
      expect(linkDescription).toHaveClass('text-base-content/70')
    })

    it('should have feature card titles rendered correctly in cyberpunk theme', () => {
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

  // Test: Section background consistency in cyberpunk theme
  describe('Section Background Consistency Cyberpunk', () => {
    it('should have consistent background colors across sections that adapt to cyberpunk theme', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      // Hero section uses gradient (primary/secondary/accent adapt to cyberpunk theme)
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

    it('should have step cards with proper cyberpunk theme styling', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      // Step cards use bg-base-100 which adapts to cyberpunk theme
      const stepCards = screen.getAllByTestId(/step-card-\d/)
      expect(stepCards).toHaveLength(3)

      stepCards.forEach((card) => {
        expect(card).toHaveClass('bg-base-100')
        expect(card).toHaveClass('shadow-xl')
      })
    })

    it('should have step numbers with primary color background in cyberpunk theme', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      const stepNumber1 = screen.getByTestId('step-number-1')
      const stepNumber2 = screen.getByTestId('step-number-2')
      const stepNumber3 = screen.getByTestId('step-number-3')

      // Step numbers use bg-primary and text-primary-content for theme adaptability
      // In cyberpunk theme, this creates the characteristic neon look
      expect(stepNumber1).toHaveClass('bg-primary')
      expect(stepNumber1).toHaveClass('text-primary-content')

      expect(stepNumber2).toHaveClass('bg-primary')
      expect(stepNumber2).toHaveClass('text-primary-content')

      expect(stepNumber3).toHaveClass('bg-primary')
      expect(stepNumber3).toHaveClass('text-primary-content')
    })
  })

  // Test: Footer section in cyberpunk theme
  describe('Footer Section Cyberpunk Theme', () => {
    it('should have footer with base-200 background for cyberpunk theme', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      const footer = screen.getByTestId('footer-section')
      expect(footer).toHaveClass('bg-base-200')
    })

    it('should have footer links with proper text colors for cyberpunk theme', () => {
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

    it('should have copyright text with subdued color for cyberpunk theme', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      const copyright = screen.getByTestId('footer-copyright')
      expect(copyright).toHaveClass('text-base-content/60')
    })

    it('should have footer brand section properly styled in cyberpunk theme', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      const footerBrand = screen.getByTestId('footer-brand')
      expect(footerBrand).toBeInTheDocument()

      // Brand description uses subdued text color - select within footer brand
      const brandDescription = footerBrand.querySelector('p')
      expect(brandDescription).toHaveClass('text-base-content/70')
    })
  })

  // Test: Visual consistency without artifacts
  describe('Visual Consistency Check', () => {
    it('should have no clashing colors - all elements use theme-aware tokens', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      // Verify cyberpunk theme is active
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')

      // All sections should use DaisyUI semantic tokens that adapt to theme
      // bg-base-100, bg-base-200, text-base-content, text-primary, etc.

      // Features section uses theme-aware background
      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toHaveClass('bg-base-200')

      // All feature cards use theme-aware styling
      const cards = screen.getAllByTestId('glassmorphism-card')
      cards.forEach((card) => {
        expect(card).toHaveClass('bg-base-100/30')
        expect(card).toHaveClass('border-base-content/10')
      })

      // Step cards use theme-aware styling
      const stepCards = screen.getAllByTestId(/step-card-\d/)
      stepCards.forEach((card) => {
        expect(card).toHaveClass('bg-base-100')
      })
    })

    it('should have legible text throughout the page in cyberpunk theme', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      // All text content should be present and readable
      expect(screen.getByText('Shorten, Share, Track')).toBeInTheDocument()
      expect(screen.getByText('Powerful Features')).toBeInTheDocument()
      expect(screen.getByText('How It Works')).toBeInTheDocument()
      expect(screen.getByText('URL Shortening')).toBeInTheDocument()
      expect(screen.getByText('Analytics Dashboard')).toBeInTheDocument()
      expect(screen.getByText('Link Management')).toBeInTheDocument()
      expect(screen.getByText('URL Shortener')).toBeInTheDocument()

      // Descriptions should be present - use getAllByText since text appears in multiple places
      const transformTexts = screen.getAllByText(/transform your long urls into short/i)
      expect(transformTexts.length).toBeGreaterThan(0)
    })
  })
})
