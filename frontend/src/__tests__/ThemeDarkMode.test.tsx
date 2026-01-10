import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import Home from '../pages/Home'
import HeroSection from '../components/HeroSection'
import FeaturesSection from '../components/FeaturesSection'

/**
 * Theme Support - Dark Mode Tests (NFR-3)
 *
 * Verifies the homepage renders correctly in dark theme with:
 * - Proper DaisyUI semantic color tokens that adapt to dark theme
 * - Appropriate contrast for text and backgrounds
 * - Visual consistency across all sections
 * - Glassmorphism effects work correctly in dark mode
 */

// Helper to set up dark theme before tests
const setupDarkTheme = () => {
  document.documentElement.setAttribute('data-theme', 'dark')
}

// Helper to clean up theme after tests
const cleanupTheme = () => {
  document.documentElement.removeAttribute('data-theme')
}

describe('Theme Support - Dark Mode', () => {
  beforeEach(() => {
    setupDarkTheme()
  })

  afterEach(() => {
    cleanupTheme()
  })

  // Test Case 1: Integration test - Render homepage with dark theme
  describe('Homepage Dark Theme Integration', () => {
    it('should render homepage with all sections when dark theme is active', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      // Verify dark theme is set
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')

      // Verify all main sections render
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer-section')).toBeInTheDocument()
    })

    it('should use DaisyUI semantic color tokens that adapt to dark theme', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      // Features section uses bg-base-200 which adapts to dark theme
      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toHaveClass('bg-base-200')

      // How It Works section uses bg-base-200
      const howItWorksSection = screen.getByTestId('how-it-works-section')
      expect(howItWorksSection).toHaveClass('bg-base-200')

      // Footer section uses bg-base-200
      const footerSection = screen.getByTestId('footer-section')
      expect(footerSection).toHaveClass('bg-base-200')
    })

    it('should display readable text with proper contrast in dark theme', () => {
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

    it('should maintain proper section structure in dark theme', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      // Verify dark theme is active
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')

      // All sections should render with their proper semantic classes
      const heroSection = screen.getByTestId('hero-section')
      const featuresSection = screen.getByTestId('features-section')
      const howItWorksSection = screen.getByTestId('how-it-works-section')
      const footerSection = screen.getByTestId('footer-section')

      expect(heroSection).toBeInTheDocument()
      expect(featuresSection).toBeInTheDocument()
      expect(howItWorksSection).toBeInTheDocument()
      expect(footerSection).toBeInTheDocument()
    })
  })

  // Test Case 2: Unit test - Hero section in dark theme
  describe('Hero Section Dark Theme', () => {
    it('should render hero section with gradient background appropriate for dark mode', () => {
      render(
        <MemoryRouter>
          <HeroSection />
        </MemoryRouter>
      )

      const heroSection = screen.getByTestId('hero-section')

      // Hero uses gradient with DaisyUI primary/secondary/accent colors that adapt to dark theme
      expect(heroSection).toHaveClass('bg-gradient-to-br')
      expect(heroSection).toHaveClass('from-primary')
      expect(heroSection).toHaveClass('via-secondary')
      expect(heroSection).toHaveClass('to-accent')
    })

    it('should have appropriate text colors for dark mode hero content', () => {
      render(
        <MemoryRouter>
          <HeroSection />
        </MemoryRouter>
      )

      // Main headline should have white text for contrast on gradient
      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline).toHaveClass('text-white')

      // Subheadline uses semi-transparent white which works in dark mode
      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline).toHaveClass('text-white/90')
    })

    it('should have visible CTA buttons with proper styling for dark mode', () => {
      render(
        <MemoryRouter>
          <HeroSection />
        </MemoryRouter>
      )

      // Verify dark theme is active
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')

      // Primary CTA button should be visible and styled properly
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

    it('should have animated background elements that work in dark mode', () => {
      render(
        <MemoryRouter>
          <HeroSection />
        </MemoryRouter>
      )

      const heroSection = screen.getByTestId('hero-section')

      // Background animation container should have overflow-hidden to contain effects
      expect(heroSection).toHaveClass('overflow-hidden')

      // Verify hero section has relative positioning for absolute children
      expect(heroSection).toHaveClass('relative')
    })

    it('should have proper minimum height for hero section in dark mode', () => {
      render(
        <MemoryRouter>
          <HeroSection />
        </MemoryRouter>
      )

      const heroSection = screen.getByTestId('hero-section')

      // Hero section should have full viewport height
      expect(heroSection).toHaveClass('min-h-screen')
      expect(heroSection).toHaveClass('flex')
      expect(heroSection).toHaveClass('items-center')
      expect(heroSection).toHaveClass('justify-center')
    })
  })

  // Test Case 3: Unit test - Feature cards in dark theme
  describe('Feature Cards Dark Theme', () => {
    it('should render feature cards with GlassMorphism styling appropriate for dark mode', () => {
      render(<FeaturesSection />)

      // Verify dark theme is active
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')

      const cards = screen.getAllByTestId('glassmorphism-card')
      expect(cards).toHaveLength(3)

      cards.forEach((card) => {
        // GlassMorphism uses semi-transparent bg-base-100 which adapts to dark theme
        expect(card).toHaveClass('bg-base-100/30')
        // Border uses base-content for theme adaptability
        expect(card).toHaveClass('border-base-content/10')
        // Blur effect for glass look (works in both light and dark)
        expect(card).toHaveClass('backdrop-blur-md')
        // Rounded corners and shadow
        expect(card).toHaveClass('rounded-2xl')
        expect(card).toHaveClass('shadow-xl')
      })
    })

    it('should have feature card icons with primary color that adapts to dark theme', () => {
      render(<FeaturesSection />)

      const urlShorteningIcon = screen.getByTestId('url-shortening-icon')
      const analyticsIcon = screen.getByTestId('analytics-icon')
      const linkManagementIcon = screen.getByTestId('link-management-icon')

      // Icons should be inside containers with text-primary class
      expect(urlShorteningIcon.closest('.text-primary')).toBeInTheDocument()
      expect(analyticsIcon.closest('.text-primary')).toBeInTheDocument()
      expect(linkManagementIcon.closest('.text-primary')).toBeInTheDocument()
    })

    it('should have feature card descriptions with readable text using base-content color in dark mode', () => {
      render(<FeaturesSection />)

      // Feature descriptions use text-base-content/70 for proper contrast in dark mode
      const urlDescription = screen.getByText(/create memorable, short links instantly/i)
      expect(urlDescription).toHaveClass('text-base-content/70')

      const analyticsDescription = screen.getByText(/track clicks, locations, and referrers/i)
      expect(analyticsDescription).toHaveClass('text-base-content/70')

      const linkDescription = screen.getByText(/organize and manage all your links/i)
      expect(linkDescription).toHaveClass('text-base-content/70')
    })

    it('should have feature card titles styled correctly in dark theme', () => {
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

    it('should have features section header visible in dark mode', () => {
      render(<FeaturesSection />)

      const heading = screen.getByRole('heading', { name: /powerful features/i })
      expect(heading).toBeInTheDocument()
      expect(heading).toHaveClass('text-3xl')
      expect(heading).toHaveClass('font-bold')
      expect(heading).toHaveClass('text-center')
    })
  })

  // Test dark theme section background consistency
  describe('Section Background Consistency in Dark Mode', () => {
    it('should have consistent background colors across sections that adapt to dark theme', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      // Verify dark theme is active
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')

      // Hero section uses gradient (primary/secondary/accent adapt to dark theme)
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

    it('should have step cards with proper dark theme styling', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      // Step cards use bg-base-100 which adapts to dark theme
      const stepCards = screen.getAllByTestId(/step-card-\d/)
      expect(stepCards).toHaveLength(3)

      stepCards.forEach((card) => {
        expect(card).toHaveClass('bg-base-100')
        expect(card).toHaveClass('shadow-xl')
      })
    })

    it('should have step numbers with primary color background in dark mode', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      const stepNumber1 = screen.getByTestId('step-number-1')
      const stepNumber2 = screen.getByTestId('step-number-2')
      const stepNumber3 = screen.getByTestId('step-number-3')

      // Step numbers use bg-primary and text-primary-content for theme adaptability
      expect(stepNumber1).toHaveClass('bg-primary')
      expect(stepNumber1).toHaveClass('text-primary-content')

      expect(stepNumber2).toHaveClass('bg-primary')
      expect(stepNumber2).toHaveClass('text-primary-content')

      expect(stepNumber3).toHaveClass('bg-primary')
      expect(stepNumber3).toHaveClass('text-primary-content')
    })
  })

  // Test footer section in dark theme
  describe('Footer Section Dark Theme', () => {
    it('should have footer with base-200 background for dark theme', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      const footer = screen.getByTestId('footer-section')
      expect(footer).toHaveClass('bg-base-200')
    })

    it('should have footer links with proper text colors for dark theme', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      // Footer navigation links use text-base-content/70 which adapts to dark theme
      const footerNav = screen.getByTestId('footer-nav')
      const homeLink = footerNav.querySelector('a')
      expect(homeLink).toHaveClass('text-base-content/70')
    })

    it('should have copyright text with subdued color for dark theme', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      const copyright = screen.getByTestId('footer-copyright')
      expect(copyright).toHaveClass('text-base-content/60')
    })

    it('should have footer brand section properly visible in dark mode', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      // Brand section should be visible using testId
      const footerBrand = screen.getByTestId('footer-brand')
      expect(footerBrand).toBeInTheDocument()

      // Brand name heading should be visible within the brand section
      const brandHeading = footerBrand.querySelector('h3')
      expect(brandHeading).toBeInTheDocument()
      expect(brandHeading).toHaveTextContent('URL Shortener')
    })
  })

  // Additional dark theme specific tests
  describe('Dark Theme Visual Artifacts Check', () => {
    it('should not have any hardcoded light-only colors', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      // Verify we're testing with dark theme
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')

      // All sections should use DaisyUI semantic color classes, not hardcoded colors
      const featuresSection = screen.getByTestId('features-section')
      const howItWorksSection = screen.getByTestId('how-it-works-section')
      const footerSection = screen.getByTestId('footer-section')

      // These sections should use base-200 (which adapts to dark theme)
      // not hardcoded colors like bg-gray-100 or bg-white
      expect(featuresSection).toHaveClass('bg-base-200')
      expect(howItWorksSection).toHaveClass('bg-base-200')
      expect(footerSection).toHaveClass('bg-base-200')
    })

    it('should have all glassmorphism cards rendering properly in dark mode', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      // Verify dark theme is active
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')

      // Get all glassmorphism cards
      const cards = screen.getAllByTestId('glassmorphism-card')

      // Should have 3 cards (one for each feature)
      expect(cards.length).toBeGreaterThanOrEqual(3)

      // Each card should have the proper glassmorphism styling
      cards.forEach((card) => {
        expect(card).toHaveClass('backdrop-blur-md')
        expect(card).toHaveClass('bg-base-100/30')
        expect(card).toHaveClass('border-base-content/10')
      })
    })

    it('should maintain proper text contrast in dark mode', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      // Verify dark theme is active
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')

      // Check that text uses semantic colors that adapt to dark theme
      const featureDescriptions = [
        screen.getByText(/create memorable, short links instantly/i),
        screen.getByText(/track clicks, locations, and referrers/i),
        screen.getByText(/organize and manage all your links/i),
      ]

      featureDescriptions.forEach((description) => {
        // text-base-content/70 adapts to dark theme for proper contrast
        expect(description).toHaveClass('text-base-content/70')
      })
    })
  })
})
