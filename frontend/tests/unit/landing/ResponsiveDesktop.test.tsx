/**
 * Responsive Design - Desktop Tests
 * Owner: Scenario 8 - Responsive Design - Desktop
 *
 * Tests for verifying the landing page displays correctly on desktop (1024px+)
 *
 * Test Cases:
 * 1. Full desktop layout is rendered at 1280px viewport
 * 2. Feature cards display in multi-column grid (3-4 columns)
 * 3. Hero section uses side-by-side layout for text and visual
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { screen, within } from '@testing-library/react'
import { renderWithProviders, setViewport } from './setup'
import Home from '../../../src/pages/Home'
import HeroSection from '../../../src/components/landing/HeroSection'
import FeaturesSection from '../../../src/components/landing/FeaturesSection'

describe('Responsive Design - Desktop (1280px)', () => {
  const DESKTOP_WIDTH = 1280
  const DESKTOP_HEIGHT = 800

  beforeEach(() => {
    setViewport(DESKTOP_WIDTH, DESKTOP_HEIGHT)
  })

  afterEach(() => {
    // Reset viewport to default
    setViewport(1024, 768)
  })

  /**
   * Test Case 1: Full desktop layout is rendered at 1280px viewport
   * Input: Render LandingPage at 1280px viewport width
   * Expected: Full desktop layout is rendered
   */
  describe('Test Case 1: Full desktop layout at 1280px', () => {
    it('renders the landing page with all main sections visible', () => {
      renderWithProviders(<Home />)

      // Verify main sections exist
      const header = screen.getByRole('banner')
      expect(header).toBeInTheDocument()

      const main = screen.getByRole('main')
      expect(main).toBeInTheDocument()

      const footer = screen.getByRole('contentinfo')
      expect(footer).toBeInTheDocument()
    })

    it('renders full navigation bar at desktop width', () => {
      renderWithProviders(<Home />)

      // Navigation should display all items inline at desktop
      const header = screen.getByRole('banner')
      expect(header).toBeInTheDocument()

      // All nav items should be visible
      const loginLink = screen.getByRole('link', { name: /login/i })
      const getStartedLink = screen.getByRole('link', { name: /get started/i })
      const logoLink = screen.getByRole('link', { name: /url shortener/i })

      expect(loginLink).toBeInTheDocument()
      expect(getStartedLink).toBeInTheDocument()
      expect(logoLink).toBeInTheDocument()
    })

    it('renders hero section at desktop viewport', () => {
      renderWithProviders(<Home />)

      // Hero section content should be visible
      const heroHeading = screen.getByRole('heading', { level: 1 })
      expect(heroHeading).toBeInTheDocument()
      expect(heroHeading).toHaveTextContent(/shorten links/i)
    })

    it('renders features section at desktop viewport', () => {
      renderWithProviders(<Home />)

      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()
    })

    it('renders footer at desktop viewport', () => {
      renderWithProviders(<Home />)

      const footer = screen.getByRole('contentinfo')
      expect(footer).toBeInTheDocument()
      expect(footer).toHaveTextContent(/url shortener/i)
    })
  })

  /**
   * Test Case 2: Feature cards display in multi-column grid (3-4 columns)
   * Input: Check feature cards at desktop viewport
   * Expected: Feature cards display in multi-column grid (3-4 columns)
   */
  describe('Test Case 2: Feature cards multi-column grid', () => {
    it('features grid uses lg:grid-cols-4 class for desktop layout', () => {
      renderWithProviders(<FeaturesSection />)

      const grid = screen.getByTestId('features-grid')
      expect(grid).toBeInTheDocument()

      // Check for Tailwind grid classes that enable 4-column layout on large screens
      expect(grid).toHaveClass('lg:grid-cols-4')
    })

    it('renders all 4 feature cards in the grid', () => {
      renderWithProviders(<FeaturesSection />)

      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards).toHaveLength(4)
    })

    it('feature cards are rendered as grid items', () => {
      renderWithProviders(<FeaturesSection />)

      const grid = screen.getByTestId('features-grid')
      expect(grid).toHaveClass('grid')

      // Verify grid has proper column classes for responsive layout
      expect(grid).toHaveClass('grid-cols-1') // Mobile
      expect(grid).toHaveClass('md:grid-cols-2') // Tablet
      expect(grid).toHaveClass('lg:grid-cols-4') // Desktop
    })

    it('feature cards have consistent height for grid alignment', () => {
      renderWithProviders(<FeaturesSection />)

      const featureCards = screen.getAllByTestId('feature-card')

      // Each card should have full height class for consistent grid alignment
      featureCards.forEach((card) => {
        // The parent GlassMorphismCard has h-full class
        const cardWrapper = card.closest('[class*="h-full"]')
        expect(cardWrapper).toBeInTheDocument()
      })
    })

    it('feature cards display all content at desktop viewport', () => {
      renderWithProviders(<FeaturesSection />)

      // Verify each card has title, description, and icon
      const featureCards = screen.getAllByTestId('feature-card')

      featureCards.forEach((card) => {
        const title = within(card).getByTestId('feature-title')
        const description = within(card).getByTestId('feature-description')
        const icon = within(card).getByTestId('feature-icon')

        expect(title).toBeInTheDocument()
        expect(description).toBeInTheDocument()
        expect(icon).toBeInTheDocument()
      })
    })
  })

  /**
   * Test Case 3: Hero section uses side-by-side layout for text and visual
   * Input: Check hero section at desktop viewport
   * Expected: Hero section uses side-by-side layout for text and visual
   */
  describe('Test Case 3: Hero section desktop layout', () => {
    it('hero section renders headline with responsive font sizes', () => {
      renderWithProviders(<HeroSection />)

      const heading = screen.getByRole('heading', { level: 1 })
      expect(heading).toBeInTheDocument()

      // Check for responsive font size classes
      // Desktop should use lg:text-6xl
      expect(heading).toHaveClass('lg:text-6xl')
    })

    it('hero section renders subheadline with responsive text size', () => {
      renderWithProviders(<HeroSection />)

      // Find the subheadline paragraph
      const subheadline = screen.getByText(/create short, powerful links/i)
      expect(subheadline).toBeInTheDocument()

      // Should have responsive text size classes
      expect(subheadline).toHaveClass('md:text-xl')
    })

    it('hero CTA buttons use horizontal layout on desktop (sm breakpoint and up)', () => {
      renderWithProviders(<HeroSection />)

      // Find the container with CTA buttons
      const getStartedButton = screen.getByRole('button', { name: /get started/i })
      const ctaContainer = getStartedButton.closest('div')

      expect(ctaContainer).toBeInTheDocument()
      // Check for responsive flex layout - changes from column to row at sm breakpoint
      expect(ctaContainer).toHaveClass('sm:flex-row')
    })

    it('hero section has adequate padding for desktop viewport', () => {
      renderWithProviders(<HeroSection />)

      // Hero section is labeled by hero-heading via aria-labelledby
      const heroSection = document.querySelector('section[aria-labelledby="hero-heading"]')
      expect(heroSection).toBeInTheDocument()

      // Should have padding classes
      expect(heroSection).toHaveClass('px-4')
      expect(heroSection).toHaveClass('py-16')
    })

    it('hero visual element (mockup window) is rendered', () => {
      renderWithProviders(<HeroSection />)

      // The mockup window containing the dashboard preview
      const mockupWindow = document.querySelector('.mockup-window')
      expect(mockupWindow).toBeInTheDocument()
    })

    it('hero mockup window has appropriate max width for desktop', () => {
      renderWithProviders(<HeroSection />)

      const mockupWindow = document.querySelector('.mockup-window')
      expect(mockupWindow).toBeInTheDocument()
      expect(mockupWindow).toHaveClass('max-w-3xl')
    })

    it('hero content container has centered text alignment', () => {
      renderWithProviders(<HeroSection />)

      const heading = screen.getByRole('heading', { level: 1 })
      const contentContainer = heading.closest('div')

      expect(contentContainer).toHaveClass('text-center')
    })

    it('hero stats grid displays 3 columns on desktop', () => {
      renderWithProviders(<HeroSection />)

      // Find the stats grid inside the mockup
      const statsGrid = document.querySelector('.grid-cols-3')
      expect(statsGrid).toBeInTheDocument()

      // Should have 3 stat items
      const statItems = statsGrid?.querySelectorAll('.stat')
      expect(statItems?.length).toBe(3)
    })
  })

  /**
   * Additional desktop layout tests
   */
  describe('Additional desktop layout verifications', () => {
    it('landing page uses full viewport height', () => {
      renderWithProviders(<Home />)

      const container = document.querySelector('.min-h-screen')
      expect(container).toBeInTheDocument()
    })

    it('navigation is sticky at top of page', () => {
      renderWithProviders(<Home />)

      const header = screen.getByRole('banner')
      expect(header).toHaveClass('sticky')
      expect(header).toHaveClass('top-0')
    })

    it('navigation has blur backdrop effect', () => {
      renderWithProviders(<Home />)

      const header = screen.getByRole('banner')
      expect(header).toHaveClass('backdrop-blur-md')
    })

    it('features section has adequate max-width constraint', () => {
      renderWithProviders(<FeaturesSection />)

      const container = document.querySelector('.max-w-7xl')
      expect(container).toBeInTheDocument()
    })

    it('features section has responsive padding', () => {
      renderWithProviders(<FeaturesSection />)

      const section = screen.getByTestId('features-section')
      expect(section).toHaveClass('px-4')
      expect(section).toHaveClass('sm:px-6')
      expect(section).toHaveClass('lg:px-8')
    })
  })
})
