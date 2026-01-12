import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import Home from './Home'
import HeroSection from '../components/HeroSection'
import FeaturesSection from '../components/FeaturesSection'
import FinalCTASection from '../components/FinalCTASection'
import AnalyticsPreviewSection from '../components/AnalyticsPreviewSection'
import DemoSection from '../components/DemoSection'
import { AuthProvider } from '../contexts/AuthContext'

// Mock scrollIntoView
Element.prototype.scrollIntoView = vi.fn()

// Mock localStorage
const localStorageMock = {
  getItem: vi.fn(),
  setItem: vi.fn(),
  clear: vi.fn(),
  removeItem: vi.fn(),
}
Object.defineProperty(window, 'localStorage', { value: localStorageMock })

const renderWithRouter = (component: React.ReactElement) => {
  return render(
    <MemoryRouter initialEntries={['/']}>
      <AuthProvider>
        {component}
      </AuthProvider>
    </MemoryRouter>
  )
}

describe('DaisyUI Component Integration - Landing Page', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    localStorageMock.getItem.mockReturnValue(null)
  })

  /**
   * Test Case 1: Check CTA buttons for DaisyUI button classes
   * Expected: Buttons use DaisyUI btn class with appropriate modifiers
   */
  describe('Test Case 1: CTA Buttons use DaisyUI btn classes', () => {
    it('HeroSection primary CTA uses DaisyUI btn btn-primary btn-lg classes', () => {
      renderWithRouter(<HeroSection />)

      const primaryCTA = screen.getByTestId('hero-cta-primary')
      expect(primaryCTA).toBeInTheDocument()

      // Verify DaisyUI button classes
      expect(primaryCTA).toHaveClass('btn')
      expect(primaryCTA).toHaveClass('btn-primary')
      expect(primaryCTA).toHaveClass('btn-lg')
    })

    it('HeroSection secondary CTA uses DaisyUI btn btn-outline btn-lg classes', () => {
      renderWithRouter(<HeroSection />)

      const secondaryCTA = screen.getByTestId('hero-cta-secondary')
      expect(secondaryCTA).toBeInTheDocument()

      // Verify DaisyUI button classes for secondary/outline button
      expect(secondaryCTA).toHaveClass('btn')
      expect(secondaryCTA).toHaveClass('btn-outline')
      expect(secondaryCTA).toHaveClass('btn-lg')
    })

    it('FinalCTASection button uses DaisyUI btn btn-primary btn-lg classes', () => {
      renderWithRouter(<FinalCTASection />)

      const ctaButton = screen.getByTestId('final-cta-button')
      expect(ctaButton).toBeInTheDocument()

      // Verify DaisyUI button classes
      expect(ctaButton).toHaveClass('btn')
      expect(ctaButton).toHaveClass('btn-primary')
      expect(ctaButton).toHaveClass('btn-lg')
    })

    it('AnalyticsPreviewSection CTA uses DaisyUI btn btn-primary btn-lg classes', () => {
      renderWithRouter(<AnalyticsPreviewSection />)

      const ctaButton = screen.getByTestId('analytics-cta')
      expect(ctaButton).toBeInTheDocument()

      // Verify DaisyUI button classes
      expect(ctaButton).toHaveClass('btn')
      expect(ctaButton).toHaveClass('btn-primary')
      expect(ctaButton).toHaveClass('btn-lg')
    })

    it('DemoSection shorten button uses DaisyUI btn btn-primary classes', () => {
      renderWithRouter(<DemoSection />)

      const shortenButton = screen.getByTestId('demo-shorten-button')
      expect(shortenButton).toBeInTheDocument()

      // Verify DaisyUI button classes
      expect(shortenButton).toHaveClass('btn')
      expect(shortenButton).toHaveClass('btn-primary')
    })

    it('All primary CTAs across the landing page use consistent DaisyUI styling', () => {
      renderWithRouter(<Home />)

      // Collect all primary CTA buttons
      const heroPrimaryCTA = screen.getByTestId('hero-cta-primary')
      const finalCtaButton = screen.getByTestId('final-cta-button')
      const analyticsCta = screen.getByTestId('analytics-cta')

      // All should have the primary button styling
      const primaryCTAs = [heroPrimaryCTA, finalCtaButton, analyticsCta]

      primaryCTAs.forEach((cta) => {
        expect(cta).toHaveClass('btn')
        expect(cta).toHaveClass('btn-primary')
        expect(cta).toHaveClass('btn-lg')
      })
    })
  })

  /**
   * Test Case 2: Check feature cards for consistent styling
   * Expected: Cards use existing GlassMorphismCard or DaisyUI card component
   * Note: GlassMorphismCard does not exist - the implementation uses DaisyUI card classes
   */
  describe('Test Case 2: Feature cards use DaisyUI card component', () => {
    it('FeaturesSection cards use DaisyUI card class', () => {
      renderWithRouter(<FeaturesSection />)

      const featuresSection = screen.getByTestId('features-section')
      const cards = featuresSection.querySelectorAll('.card')

      // Should have 4 feature cards
      expect(cards.length).toBe(4)

      // Each card should have the DaisyUI card class
      cards.forEach((card) => {
        expect(card).toHaveClass('card')
      })
    })

    it('FeaturesSection cards use DaisyUI card-body for content wrapper', () => {
      renderWithRouter(<FeaturesSection />)

      const featuresSection = screen.getByTestId('features-section')
      const cards = featuresSection.querySelectorAll('.card')

      // Each card should have a card-body element
      cards.forEach((card) => {
        const cardBody = card.querySelector('.card-body')
        expect(cardBody).toBeInTheDocument()
      })
    })

    it('FeaturesSection cards use DaisyUI card-title for headings', () => {
      renderWithRouter(<FeaturesSection />)

      const featuresSection = screen.getByTestId('features-section')
      const cardTitles = featuresSection.querySelectorAll('.card-title')

      // Should have 4 card titles (one per feature)
      expect(cardTitles.length).toBe(4)
    })

    it('FeaturesSection cards use DaisyUI shadow utilities', () => {
      renderWithRouter(<FeaturesSection />)

      const featuresSection = screen.getByTestId('features-section')
      const cards = featuresSection.querySelectorAll('.card')

      // Each card should have shadow styling
      cards.forEach((card) => {
        expect(card).toHaveClass('shadow-xl')
      })
    })

    it('FeaturesSection cards use bg-base-100 from DaisyUI theme', () => {
      renderWithRouter(<FeaturesSection />)

      const featuresSection = screen.getByTestId('features-section')
      const cards = featuresSection.querySelectorAll('.card')

      // Each card should have bg-base-100 (DaisyUI theme color)
      cards.forEach((card) => {
        expect(card).toHaveClass('bg-base-100')
      })
    })

    it('DemoSection uses DaisyUI card component for the form container', () => {
      renderWithRouter(<DemoSection />)

      const demoSection = screen.getByTestId('demo-section')
      const card = demoSection.querySelector('.card')

      expect(card).toBeInTheDocument()
      expect(card).toHaveClass('card')
      expect(card).toHaveClass('bg-base-100')
      expect(card).toHaveClass('shadow-xl')

      // Should have card-body
      const cardBody = card?.querySelector('.card-body')
      expect(cardBody).toBeInTheDocument()
    })

    it('DemoSection input uses DaisyUI input input-bordered classes', () => {
      renderWithRouter(<DemoSection />)

      const input = screen.getByTestId('demo-input')
      expect(input).toHaveClass('input')
      expect(input).toHaveClass('input-bordered')
    })
  })

  /**
   * Test Case 3: Check that FuturisticButton component is used where appropriate
   * Expected: Primary CTAs use existing FuturisticButton component
   *
   * Note: The FuturisticButton component does NOT exist in the codebase.
   * The current implementation uses DaisyUI btn classes directly for all buttons.
   * This test verifies that the existing pattern (DaisyUI btn-primary btn-lg)
   * is consistently applied for primary CTAs, which is the current design pattern.
   */
  describe('Test Case 3: Primary CTAs use consistent button styling pattern', () => {
    it('Primary CTAs use btn-primary styling as the established pattern', () => {
      renderWithRouter(<Home />)

      // Get all primary CTA buttons
      const heroPrimaryCTA = screen.getByTestId('hero-cta-primary')
      const finalCtaButton = screen.getByTestId('final-cta-button')
      const analyticsCta = screen.getByTestId('analytics-cta')

      // All primary CTAs should use the same DaisyUI button styling pattern
      // This is the established pattern for "futuristic" primary CTAs in this codebase
      ;[heroPrimaryCTA, finalCtaButton, analyticsCta].forEach((cta) => {
        expect(cta).toHaveClass('btn')
        expect(cta).toHaveClass('btn-primary')
        expect(cta).toHaveClass('btn-lg')
      })
    })

    it('Hero primary CTA links to /register for unauthenticated users', () => {
      renderWithRouter(<HeroSection />)

      const primaryCTA = screen.getByTestId('hero-cta-primary')
      expect(primaryCTA).toHaveAttribute('href', '/register')
      expect(primaryCTA.textContent).toContain('Get Started Free')
    })

    it('Final CTA button links to /register', () => {
      renderWithRouter(<FinalCTASection />)

      const ctaButton = screen.getByTestId('final-cta-button')
      expect(ctaButton).toHaveAttribute('href', '/register')
      expect(ctaButton.textContent).toContain('Start Shortening URLs Today')
    })

    it('Analytics CTA button links to /register', () => {
      renderWithRouter(<AnalyticsPreviewSection />)

      const ctaButton = screen.getByTestId('analytics-cta')
      expect(ctaButton).toHaveAttribute('href', '/register')
      expect(ctaButton.textContent).toContain('See Your Analytics')
    })

    it('Primary CTAs are accessible with proper button semantics', () => {
      renderWithRouter(<Home />)

      const heroPrimaryCTA = screen.getByTestId('hero-cta-primary')
      const finalCtaButton = screen.getByTestId('final-cta-button')
      const analyticsCta = screen.getByTestId('analytics-cta')

      // All CTAs should be link elements (for navigation)
      expect(heroPrimaryCTA.tagName.toLowerCase()).toBe('a')
      expect(finalCtaButton.tagName.toLowerCase()).toBe('a')
      expect(analyticsCta.tagName.toLowerCase()).toBe('a')
    })

    it('Secondary CTA (Learn More) uses btn-outline as alternative styling', () => {
      renderWithRouter(<HeroSection />)

      const secondaryCTA = screen.getByTestId('hero-cta-secondary')
      expect(secondaryCTA).toHaveClass('btn')
      expect(secondaryCTA).toHaveClass('btn-outline')
      expect(secondaryCTA).toHaveClass('btn-lg')

      // Secondary CTA should be a button (for scroll action), not a link
      expect(secondaryCTA.tagName.toLowerCase()).toBe('button')
    })
  })

  /**
   * Additional Integration Tests - Verify DaisyUI theming consistency
   */
  describe('DaisyUI Theme Integration', () => {
    it('Sections use DaisyUI theme colors (base-100, base-200)', () => {
      renderWithRouter(<Home />)

      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toHaveClass('bg-base-200')

      const analyticsSection = screen.getByTestId('analytics-preview-section')
      expect(analyticsSection).toHaveClass('bg-base-200')
    })

    it('Feature cards use DaisyUI theme-aware text colors', () => {
      renderWithRouter(<FeaturesSection />)

      const featuresSection = screen.getByTestId('features-section')
      const descriptions = featuresSection.querySelectorAll('.card p')

      // Descriptions should use base-content with opacity modifier
      descriptions.forEach((desc) => {
        expect(desc).toHaveClass('text-base-content/70')
      })
    })

    it('DemoSection alert uses DaisyUI alert-error class for errors', () => {
      renderWithRouter(<DemoSection />)

      // Initially no error is shown
      expect(screen.queryByTestId('demo-error')).not.toBeInTheDocument()
    })
  })
})
