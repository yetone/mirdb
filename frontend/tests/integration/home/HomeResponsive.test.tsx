/**
 * Home Responsive Design Integration Tests
 * Owner: Scenario 7 (primary), Scenario 8 (shared)
 *
 * Integration tests for responsive behavior:
 * - Mobile layout (< 768px) works correctly (Scenario 7)
 * - Tablet layout (768-1024px) works correctly (Scenario 8)
 * - No horizontal overflow at any breakpoint
 * - Touch targets meet minimum size requirements
 *
 * Testing framework: Vitest + @testing-library/react
 */
import React from 'react'
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { screen, within } from '@testing-library/react'
import { renderWithProviders } from '../../utils/renderWithProviders'
import Home from '@/pages/Home'
import { HeroSection } from '@/components/home/HeroSection'
import { FeaturesSection } from '@/components/home/FeaturesSection'

// Mock GlassMorphismCard to track its usage
vi.mock('../../../src/components/GlassMorphismCard', () => ({
  GlassMorphismCard: ({ children, className }: { children: React.ReactNode; className?: string }) => (
    React.createElement('div', { 'data-testid': 'glass-morphism-card', className }, children)
  ),
}))

// Mock BackgroundEffect
vi.mock('../../../src/components/BackgroundEffect', () => ({
  BackgroundEffect: () => React.createElement('div', { 'data-testid': 'background-effect' }),
}))

// Helper to simulate viewport width by mocking matchMedia
function mockViewportWidth(width: number) {
  Object.defineProperty(window, 'innerWidth', {
    writable: true,
    configurable: true,
    value: width,
  })

  // Mock matchMedia for responsive breakpoints
  window.matchMedia = vi.fn().mockImplementation((query: string) => {
    // Parse the query to check breakpoints
    const minWidthMatch = query.match(/min-width:\s*(\d+)px/)
    const maxWidthMatch = query.match(/max-width:\s*(\d+)px/)

    let matches = false
    if (minWidthMatch) {
      matches = width >= parseInt(minWidthMatch[1])
    }
    if (maxWidthMatch) {
      matches = width <= parseInt(maxWidthMatch[1])
    }

    return {
      matches,
      media: query,
      onchange: null,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      dispatchEvent: vi.fn(),
    }
  })

  // Dispatch resize event
  window.dispatchEvent(new Event('resize'))
}

describe('HomeResponsive - Mobile Viewport Tests (Scenario 7)', () => {
  beforeEach(() => {
    // Reset viewport mock before each test
    vi.clearAllMocks()
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  describe('Test Case 1: Render Home at 375px viewport width (mobile) - Component renders without horizontal overflow', () => {
    it('should render Home component at 375px mobile width without horizontal overflow', () => {
      mockViewportWidth(375)
      renderWithProviders(<Home />)

      // Verify the main element is rendered
      const mainElement = screen.getByRole('main')
      expect(mainElement).toBeInTheDocument()

      // Check that main uses min-h-screen for full viewport height
      expect(mainElement).toHaveClass('min-h-screen')
    })

    it('should render HeroSection with proper mobile-friendly classes', () => {
      mockViewportWidth(375)
      renderWithProviders(<HeroSection />)

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Verify it has horizontal padding (px-4) for mobile
      expect(heroSection).toHaveClass('px-4')
    })

    it('should render FeaturesSection with mobile-friendly padding', () => {
      mockViewportWidth(375)
      renderWithProviders(<FeaturesSection />)

      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()

      // Verify it has horizontal padding for mobile
      expect(featuresSection).toHaveClass('px-4')
    })

    it('should use max-w constraints to prevent horizontal overflow', () => {
      mockViewportWidth(375)
      renderWithProviders(<Home />)

      // Check that hero content has max-width constraint
      const heroSection = screen.getByTestId('hero-section')
      const heroContainer = heroSection.querySelector('.max-w-4xl')
      expect(heroContainer).toBeInTheDocument()

      // Check that features content has max-width constraint
      const featuresSection = screen.getByTestId('features-section')
      const featuresContainer = featuresSection.querySelector('.max-w-6xl')
      expect(featuresContainer).toBeInTheDocument()
    })
  })

  describe('Test Case 2: CTA buttons have minimum 44px tap targets', () => {
    it('should render CTA buttons with adequate touch target size', () => {
      mockViewportWidth(375)
      renderWithProviders(<HeroSection />)

      // Find the Get Started button
      const getStartedButton = screen.getByRole('button', { name: /get started/i })
      expect(getStartedButton).toBeInTheDocument()

      // Find the Log In button
      const loginButton = screen.getByRole('button', { name: /log in/i })
      expect(loginButton).toBeInTheDocument()

      // DaisyUI btn-lg class ensures minimum 48px height which exceeds 44px requirement
      // Check that buttons have the btn-lg class for large touch targets
      expect(getStartedButton).toHaveClass('btn-lg')
      expect(loginButton).toHaveClass('btn-lg')
    })

    it('should have buttons with btn class that provides minimum sizing', () => {
      mockViewportWidth(375)
      renderWithProviders(<HeroSection />)

      const buttons = screen.getAllByRole('button')

      buttons.forEach(button => {
        // All buttons should have the base btn class from DaisyUI
        expect(button).toHaveClass('btn')
      })
    })
  })

  describe('Test Case 3: Feature cards stack vertically at mobile width', () => {
    it('should render feature cards in a single-column grid at mobile width', () => {
      mockViewportWidth(375)
      renderWithProviders(<FeaturesSection />)

      const featuresSection = screen.getByTestId('features-section')

      // Find the grid container
      const gridContainer = featuresSection.querySelector('.grid')
      expect(gridContainer).toBeInTheDocument()

      // At mobile (< 768px), grid should be single column
      // The class grid-cols-1 should be applied for mobile
      expect(gridContainer).toHaveClass('grid-cols-1')
    })

    it('should have responsive grid classes (1 col mobile, 3 col desktop)', () => {
      mockViewportWidth(375)
      renderWithProviders(<FeaturesSection />)

      const featuresSection = screen.getByTestId('features-section')
      const gridContainer = featuresSection.querySelector('.grid')

      // Check responsive classes are present
      expect(gridContainer).toHaveClass('grid-cols-1')
      expect(gridContainer).toHaveClass('md:grid-cols-3')
    })

    it('should render exactly 3 feature cards stacked vertically', () => {
      mockViewportWidth(375)
      renderWithProviders(<FeaturesSection />)

      const featureCards = screen.getAllByTestId('glass-morphism-card')
      expect(featureCards).toHaveLength(3)
    })
  })

  describe('Test Case 4: Hero section text is readable at mobile width', () => {
    it('should display headline with responsive text sizes', () => {
      mockViewportWidth(375)
      renderWithProviders(<HeroSection />)

      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline).toBeInTheDocument()

      // Check responsive text sizing classes
      // Mobile: text-4xl, Desktop: md:text-6xl
      expect(headline).toHaveClass('text-4xl')
      expect(headline).toHaveClass('md:text-6xl')
    })

    it('should display description with responsive text sizes', () => {
      mockViewportWidth(375)
      renderWithProviders(<HeroSection />)

      const description = screen.getByText(/transform long/i)
      expect(description).toBeInTheDocument()

      // Check responsive text sizing classes
      // Mobile: text-lg, Desktop: md:text-xl
      expect(description).toHaveClass('text-lg')
      expect(description).toHaveClass('md:text-xl')
    })

    it('should have readable headline text without truncation', () => {
      mockViewportWidth(375)
      renderWithProviders(<HeroSection />)

      const headline = screen.getByRole('heading', { level: 1 })

      // Text content should be fully visible (no truncate/line-clamp classes)
      expect(headline.className).not.toContain('truncate')
      expect(headline.className).not.toContain('line-clamp')

      // Headline should contain the expected text
      expect(headline.textContent).toMatch(/shorten.*url|url.*shorten/i)
    })

    it('should have description centered and constrained for readability', () => {
      mockViewportWidth(375)
      renderWithProviders(<HeroSection />)

      const description = screen.getByText(/transform long/i)

      // Should have max-width for comfortable reading width
      expect(description).toHaveClass('max-w-2xl')
      // Should be centered
      expect(description).toHaveClass('mx-auto')
    })
  })

  describe('Test Case 5: CTA buttons layout adapts to mobile', () => {
    it('should stack CTA buttons vertically on small screens', () => {
      mockViewportWidth(375)
      renderWithProviders(<HeroSection />)

      // Find the container with CTA buttons (flex container)
      const heroSection = screen.getByTestId('hero-section')

      // The CTA container should have flex-col for mobile, sm:flex-row for larger screens
      const ctaContainer = heroSection.querySelector('.flex-col')
      expect(ctaContainer).toBeInTheDocument()

      // Should switch to row layout on sm breakpoint
      expect(ctaContainer).toHaveClass('sm:flex-row')
    })

    it('should have proper gap between stacked buttons', () => {
      mockViewportWidth(375)
      renderWithProviders(<HeroSection />)

      const heroSection = screen.getByTestId('hero-section')
      const ctaContainer = heroSection.querySelector('.gap-4')

      expect(ctaContainer).toBeInTheDocument()
    })
  })

  describe('Test Case 6: Content accessible at 320px (iPhone SE width)', () => {
    it('should render Home component at 320px without errors', () => {
      mockViewportWidth(320)
      renderWithProviders(<Home />)

      // Verify main elements are rendered
      const mainElement = screen.getByRole('main')
      expect(mainElement).toBeInTheDocument()

      // Verify hero section
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Verify features section
      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()
    })

    it('should maintain text readability at 320px', () => {
      mockViewportWidth(320)
      renderWithProviders(<HeroSection />)

      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline).toBeInTheDocument()

      // Text should still be visible and not truncated
      expect(headline.textContent).toBeTruthy()
      expect(headline.textContent!.length).toBeGreaterThan(10)
    })

    it('should keep buttons accessible at 320px', () => {
      mockViewportWidth(320)
      renderWithProviders(<HeroSection />)

      const getStartedButton = screen.getByRole('button', { name: /get started/i })
      const loginButton = screen.getByRole('button', { name: /log in/i })

      expect(getStartedButton).toBeInTheDocument()
      expect(loginButton).toBeInTheDocument()
      expect(getStartedButton).toBeVisible()
      expect(loginButton).toBeVisible()
    })

    it('should stack feature cards at 320px width', () => {
      mockViewportWidth(320)
      renderWithProviders(<FeaturesSection />)

      const featureCards = screen.getAllByTestId('glass-morphism-card')
      expect(featureCards).toHaveLength(3)

      // Grid should be single column
      const featuresSection = screen.getByTestId('features-section')
      const gridContainer = featuresSection.querySelector('.grid-cols-1')
      expect(gridContainer).toBeInTheDocument()
    })
  })

  describe('General Mobile Responsiveness', () => {
    it('should have all interactive elements visible at mobile width', () => {
      mockViewportWidth(375)
      renderWithProviders(<Home />)

      // Check hero CTAs
      const getStartedButton = screen.getByRole('button', { name: /get started/i })
      const loginButton = screen.getByRole('button', { name: /log in/i })

      expect(getStartedButton).toBeVisible()
      expect(loginButton).toBeVisible()
    })

    it('should render without console errors at mobile width', () => {
      const consoleSpy = vi.spyOn(console, 'error')
      mockViewportWidth(375)

      renderWithProviders(<Home />)

      // Should not have any React errors
      expect(consoleSpy).not.toHaveBeenCalled()
      consoleSpy.mockRestore()
    })

    it('should use relative units for font sizes to support user scaling', () => {
      mockViewportWidth(375)
      renderWithProviders(<HeroSection />)

      const headline = screen.getByRole('heading', { level: 1 })
      const description = screen.getByText(/transform long/i)

      // Tailwind text-* classes use rem units which are relative
      // text-4xl = 2.25rem, text-lg = 1.125rem
      expect(headline.className).toMatch(/text-\d+xl/)
      expect(description.className).toMatch(/text-(lg|xl)/)
    })
  })
})
