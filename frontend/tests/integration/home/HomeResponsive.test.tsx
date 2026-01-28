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

      // Check that main uses flex-grow to take available space
      expect(mainElement).toHaveClass('flex-grow')
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

/**
 * Tablet Viewport Tests (Scenario 8)
 *
 * Tests for tablet responsive design at 768px-1024px viewport widths.
 * Verifies layout adaptation, section visibility, and proper styling.
 */
import { HowItWorks } from '@/components/home/HowItWorks'
import { Footer } from '@/components/home/Footer'

describe('HomeResponsive - Tablet Viewport Tests (Scenario 8)', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  describe('Test Case 1: Render Home at 768px viewport width (tablet) - Component renders with appropriate tablet layout', () => {
    it('should render Home component at 768px tablet width', () => {
      mockViewportWidth(768)
      renderWithProviders(<Home />)

      // Verify main element is rendered
      const mainElement = screen.getByRole('main')
      expect(mainElement).toBeInTheDocument()

      // Verify all major sections are present
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()

      const howItWorksSection = screen.getByTestId('how-it-works-section')
      expect(howItWorksSection).toBeInTheDocument()

      const footer = screen.getByTestId('footer')
      expect(footer).toBeInTheDocument()
    })

    it('should apply md: breakpoint styles at 768px width', () => {
      mockViewportWidth(768)
      renderWithProviders(<Home />)

      // At 768px (md breakpoint), responsive classes should switch
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Hero headline should have md:text-6xl class
      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline).toHaveClass('md:text-6xl')
    })

    it('should render HeroSection with tablet-appropriate layout', () => {
      mockViewportWidth(768)
      renderWithProviders(<HeroSection />)

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Verify CTAs are in row layout at tablet size (sm:flex-row applies)
      const ctaContainer = heroSection.querySelector('.sm\\:flex-row')
      expect(ctaContainer).toBeInTheDocument()

      // Both buttons should be visible
      const getStartedButton = screen.getByRole('button', { name: /get started/i })
      const loginButton = screen.getByRole('button', { name: /log in/i })
      expect(getStartedButton).toBeVisible()
      expect(loginButton).toBeVisible()
    })
  })

  describe('Test Case 2: Check feature cards layout at 768px - Feature cards may display in 2-column or adaptive grid', () => {
    it('should render feature cards in 3-column grid at tablet width (md:grid-cols-3)', () => {
      mockViewportWidth(768)
      renderWithProviders(<FeaturesSection />)

      const featuresSection = screen.getByTestId('features-section')
      const gridContainer = featuresSection.querySelector('.grid')

      expect(gridContainer).toBeInTheDocument()
      // At md breakpoint (768px), the grid transitions to 3 columns
      expect(gridContainer).toHaveClass('md:grid-cols-3')
    })

    it('should render exactly 3 feature cards at tablet width', () => {
      mockViewportWidth(768)
      renderWithProviders(<FeaturesSection />)

      const featureCards = screen.getAllByTestId('glass-morphism-card')
      expect(featureCards).toHaveLength(3)
    })

    it('should maintain proper gap between feature cards at tablet width', () => {
      mockViewportWidth(768)
      renderWithProviders(<FeaturesSection />)

      const featuresSection = screen.getByTestId('features-section')
      const gridContainer = featuresSection.querySelector('.grid')

      // Grid should have gap classes
      expect(gridContainer).toHaveClass('gap-6')
      expect(gridContainer).toHaveClass('md:gap-8')
    })

    it('should display each feature card with icon, title, and description', () => {
      mockViewportWidth(768)
      renderWithProviders(<FeaturesSection />)

      // Check URL Shortening feature
      const urlShorteningCard = screen.getByTestId('feature-card-url-shortening')
      expect(urlShorteningCard).toBeInTheDocument()
      expect(within(urlShorteningCard).getByText('URL Shortening')).toBeInTheDocument()
      expect(screen.getByTestId('feature-icon-url-shortening')).toBeInTheDocument()

      // Check Analytics feature
      const analyticsCard = screen.getByTestId('feature-card-analytics')
      expect(analyticsCard).toBeInTheDocument()
      expect(within(analyticsCard).getByText('Analytics Dashboard')).toBeInTheDocument()

      // Check Link Management feature
      const linkManagementCard = screen.getByTestId('feature-card-link-management')
      expect(linkManagementCard).toBeInTheDocument()
      expect(within(linkManagementCard).getByText('Link Management')).toBeInTheDocument()
    })
  })

  describe('Test Case 3: Render at 1024px viewport width - Layout transitions toward desktop appearance', () => {
    it('should render Home component at 1024px width', () => {
      mockViewportWidth(1024)
      renderWithProviders(<Home />)

      const mainElement = screen.getByRole('main')
      expect(mainElement).toBeInTheDocument()

      // All sections should be rendered
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer')).toBeInTheDocument()
    })

    it('should apply desktop-like styling at 1024px (above md breakpoint)', () => {
      mockViewportWidth(1024)
      renderWithProviders(<HeroSection />)

      const headline = screen.getByRole('heading', { level: 1 })
      // At 1024px, md: responsive classes are active
      expect(headline).toHaveClass('md:text-6xl')

      const description = screen.getByText(/transform long/i)
      expect(description).toHaveClass('md:text-xl')
    })

    it('should render feature cards in 3-column grid at 1024px', () => {
      mockViewportWidth(1024)
      renderWithProviders(<FeaturesSection />)

      const featuresSection = screen.getByTestId('features-section')
      const gridContainer = featuresSection.querySelector('.grid')

      expect(gridContainer).toHaveClass('md:grid-cols-3')
    })

    it('should render How It Works steps in 3-column layout at 1024px', () => {
      mockViewportWidth(1024)
      renderWithProviders(<HowItWorks />)

      const howItWorksSection = screen.getByTestId('how-it-works-section')
      const gridContainer = howItWorksSection.querySelector('ol.grid')

      expect(gridContainer).toBeInTheDocument()
      expect(gridContainer).toHaveClass('md:grid-cols-3')
    })

    it('should maintain CTA buttons in row layout at 1024px', () => {
      mockViewportWidth(1024)
      renderWithProviders(<HeroSection />)

      const heroSection = screen.getByTestId('hero-section')
      const ctaContainer = heroSection.querySelector('.sm\\:flex-row')

      expect(ctaContainer).toBeInTheDocument()
    })
  })

  describe('Test Case 4: E2E Visual inspection at tablet breakpoints - No layout breaks or overlapping elements', () => {
    it('should render all sections without errors at 768px', () => {
      const consoleSpy = vi.spyOn(console, 'error')
      mockViewportWidth(768)

      renderWithProviders(<Home />)

      // Should not have any React errors
      expect(consoleSpy).not.toHaveBeenCalled()
      consoleSpy.mockRestore()
    })

    it('should render all sections without errors at 1024px', () => {
      const consoleSpy = vi.spyOn(console, 'error')
      mockViewportWidth(1024)

      renderWithProviders(<Home />)

      expect(consoleSpy).not.toHaveBeenCalled()
      consoleSpy.mockRestore()
    })

    it('should have proper max-width constraints at tablet widths to prevent overflow', () => {
      mockViewportWidth(768)
      renderWithProviders(<Home />)

      // Hero section has max-w-4xl
      const heroSection = screen.getByTestId('hero-section')
      const heroContainer = heroSection.querySelector('.max-w-4xl')
      expect(heroContainer).toBeInTheDocument()

      // Features section has max-w-6xl
      const featuresSection = screen.getByTestId('features-section')
      const featuresContainer = featuresSection.querySelector('.max-w-6xl')
      expect(featuresContainer).toBeInTheDocument()

      // How It Works section has max-w-6xl
      const howItWorksSection = screen.getByTestId('how-it-works-section')
      const howItWorksContainer = howItWorksSection.querySelector('.max-w-6xl')
      expect(howItWorksContainer).toBeInTheDocument()

      // Footer has max-w-4xl
      const footer = screen.getByTestId('footer')
      const footerContainer = footer.querySelector('.max-w-4xl')
      expect(footerContainer).toBeInTheDocument()
    })

    it('should have proper padding on all sections at tablet width', () => {
      mockViewportWidth(768)
      renderWithProviders(<Home />)

      // Hero section has px-4
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toHaveClass('px-4')

      // Features section has px-4
      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toHaveClass('px-4')

      // How It Works section has px-4
      const howItWorksSection = screen.getByTestId('how-it-works-section')
      expect(howItWorksSection).toHaveClass('px-4')

      // Footer has px-4
      const footer = screen.getByTestId('footer')
      expect(footer).toHaveClass('px-4')
    })

    it('should maintain readable text at tablet widths', () => {
      mockViewportWidth(768)
      renderWithProviders(<Home />)

      // Hero headline
      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline.textContent).toBeTruthy()
      expect(headline.className).not.toContain('truncate')

      // Features heading
      const featuresHeading = screen.getByRole('heading', { name: /powerful features/i })
      expect(featuresHeading).toBeInTheDocument()

      // How It Works heading
      const howItWorksHeading = screen.getByRole('heading', { name: /how it works/i })
      expect(howItWorksHeading).toBeInTheDocument()
    })

    it('should render Footer correctly at tablet widths', () => {
      mockViewportWidth(768)
      renderWithProviders(<Footer />)

      const footer = screen.getByTestId('footer')
      expect(footer).toBeInTheDocument()

      // Should have navigation links
      expect(screen.getByRole('link', { name: /home/i })).toBeInTheDocument()
      expect(screen.getByRole('link', { name: /login/i })).toBeInTheDocument()
      expect(screen.getByRole('link', { name: /register/i })).toBeInTheDocument()

      // Should have copyright text
      expect(screen.getByText(/url shortener/i)).toBeInTheDocument()
      expect(screen.getByText(/all rights reserved/i)).toBeInTheDocument()
    })

    it('should have all interactive elements accessible at tablet widths', () => {
      mockViewportWidth(768)
      renderWithProviders(<Home />)

      // CTA buttons
      const getStartedButton = screen.getByRole('button', { name: /get started/i })
      const loginButton = screen.getByRole('button', { name: /log in/i })
      expect(getStartedButton).toBeVisible()
      expect(loginButton).toBeVisible()

      // Footer links
      const homeLink = screen.getByRole('link', { name: /^home$/i })
      const loginLink = screen.getByRole('link', { name: /login/i })
      const registerLink = screen.getByRole('link', { name: /register/i })
      expect(homeLink).toBeVisible()
      expect(loginLink).toBeVisible()
      expect(registerLink).toBeVisible()
    })
  })

  describe('General Tablet Responsiveness', () => {
    it('should render How It Works steps in proper layout at tablet width', () => {
      mockViewportWidth(768)
      renderWithProviders(<HowItWorks />)

      // All 3 steps should be visible
      expect(screen.getByTestId('step-1')).toBeInTheDocument()
      expect(screen.getByTestId('step-2')).toBeInTheDocument()
      expect(screen.getByTestId('step-3')).toBeInTheDocument()

      // Step numbers should be visible
      expect(screen.getByTestId('step-number-1')).toHaveTextContent('1')
      expect(screen.getByTestId('step-number-2')).toHaveTextContent('2')
      expect(screen.getByTestId('step-number-3')).toHaveTextContent('3')
    })

    it('should maintain semantic structure at tablet widths', () => {
      mockViewportWidth(768)
      renderWithProviders(<Home />)

      // Main element exists
      const main = screen.getByRole('main')
      expect(main).toBeInTheDocument()

      // Footer element exists
      const footer = screen.getByRole('contentinfo')
      expect(footer).toBeInTheDocument()

      // Proper heading hierarchy
      const h1 = screen.getByRole('heading', { level: 1 })
      expect(h1).toBeInTheDocument()

      const h2s = screen.getAllByRole('heading', { level: 2 })
      expect(h2s.length).toBeGreaterThanOrEqual(2) // Features and How It Works headings
    })

    it('should render at 900px (mid-tablet) without issues', () => {
      mockViewportWidth(900)
      renderWithProviders(<Home />)

      // All sections should render
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer')).toBeInTheDocument()

      // Feature cards should be in 3-column grid
      const featuresSection = screen.getByTestId('features-section')
      const gridContainer = featuresSection.querySelector('.grid')
      expect(gridContainer).toHaveClass('md:grid-cols-3')
    })
  })
})
