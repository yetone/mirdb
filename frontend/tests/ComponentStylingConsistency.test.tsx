import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import Home from '../src/pages/Home'
import HeroSection from '../src/components/HeroSection'
import FeaturesSection from '../src/components/FeaturesSection'
import HowItWorks from '../src/components/HowItWorks'

const renderWithRouter = (component: React.ReactElement) => {
  return render(<BrowserRouter>{component}</BrowserRouter>)
}

/**
 * Test Case 1: Primary CTA Button Styling Consistency
 * Verifies that CTA buttons use consistent DaisyUI btn classes or FuturisticButton component
 */
describe('Test Case 1: Primary CTA Button Styling Consistency', () => {
  it('should use DaisyUI btn classes for the "Get Started Free" CTA button', () => {
    renderWithRouter(<HeroSection />)

    const getStartedButton = screen.getByRole('link', { name: /get started free/i })
    expect(getStartedButton).toBeInTheDocument()

    // Verify DaisyUI btn class is present
    expect(getStartedButton.className).toMatch(/\bbtn\b/)
    // Verify primary variant for CTA
    expect(getStartedButton.className).toMatch(/\bbtn-primary\b/)
    // Verify consistent size class
    expect(getStartedButton.className).toMatch(/\bbtn-lg\b/)
  })

  it('should use DaisyUI btn classes for the "Sign In" secondary CTA button', () => {
    renderWithRouter(<HeroSection />)

    const signInButton = screen.getByRole('link', { name: /sign in/i })
    expect(signInButton).toBeInTheDocument()

    // Verify DaisyUI btn class is present
    expect(signInButton.className).toMatch(/\bbtn\b/)
    // Verify outline variant for secondary CTA
    expect(signInButton.className).toMatch(/\bbtn-outline\b/)
    // Verify consistent size class
    expect(signInButton.className).toMatch(/\bbtn-lg\b/)
  })

  it('should have consistent button styling pattern across both CTAs', () => {
    renderWithRouter(<HeroSection />)

    const getStartedButton = screen.getByRole('link', { name: /get started free/i })
    const signInButton = screen.getByRole('link', { name: /sign in/i })

    // Both should have base btn class
    expect(getStartedButton.className).toMatch(/\bbtn\b/)
    expect(signInButton.className).toMatch(/\bbtn\b/)

    // Both should have consistent size
    expect(getStartedButton.className).toMatch(/\bbtn-lg\b/)
    expect(signInButton.className).toMatch(/\bbtn-lg\b/)
  })
})

/**
 * Test Case 2: Feature Card Styling Consistency
 * Verifies that feature cards use GlassMorphismCard or consistent DaisyUI card styling
 */
describe('Test Case 2: Feature Card Styling Consistency', () => {
  it('should use DaisyUI card classes for feature cards', () => {
    renderWithRouter(<FeaturesSection />)

    const featureCards = screen.getAllByTestId('feature-card')
    expect(featureCards.length).toBeGreaterThanOrEqual(3)

    featureCards.forEach((card) => {
      // Verify DaisyUI card class is present
      expect(card.className).toMatch(/\bcard\b/)
    })
  })

  it('should use consistent shadow styling on feature cards', () => {
    renderWithRouter(<FeaturesSection />)

    const featureCards = screen.getAllByTestId('feature-card')

    featureCards.forEach((card) => {
      // Verify shadow class for card elevation
      expect(card.className).toMatch(/\bshadow-xl\b/)
    })
  })

  it('should use consistent hover effects on feature cards', () => {
    renderWithRouter(<FeaturesSection />)

    const featureCards = screen.getAllByTestId('feature-card')

    featureCards.forEach((card) => {
      // Verify hover shadow enhancement
      expect(card.className).toMatch(/\bhover:shadow-2xl\b/)
      // Verify transition for smooth hover effect
      expect(card.className).toMatch(/\btransition-shadow\b/)
    })
  })

  it('should use card-body for consistent card content layout', () => {
    renderWithRouter(<FeaturesSection />)

    const featureCards = screen.getAllByTestId('feature-card')

    featureCards.forEach((card) => {
      // Each card should have a card-body element
      const cardBody = card.querySelector('.card-body')
      expect(cardBody).toBeInTheDocument()
    })
  })

  it('should use consistent background colors on feature cards', () => {
    renderWithRouter(<FeaturesSection />)

    const featureCards = screen.getAllByTestId('feature-card')

    featureCards.forEach((card) => {
      // Verify base-100 background for theme consistency
      expect(card.className).toMatch(/\bbg-base-100\b/)
    })
  })
})

/**
 * Test Case 3: Tailwind Class Usage Patterns
 * Verifies that homepage components use Tailwind utility classes consistent with project patterns
 */
describe('Test Case 3: Tailwind Class Usage Patterns', () => {
  describe('Hero Section Tailwind Patterns', () => {
    it('should use consistent responsive padding classes', () => {
      renderWithRouter(<HeroSection />)

      const heroSection = screen.getByTestId('hero-section')

      // Verify responsive padding pattern (px-4 sm:px-6 lg:px-8)
      expect(heroSection.className).toMatch(/\bpx-4\b/)
      expect(heroSection.className).toMatch(/\bsm:px-6\b/)
      expect(heroSection.className).toMatch(/\blg:px-8\b/)
    })

    it('should use DaisyUI theme-aware color classes', () => {
      renderWithRouter(<HeroSection />)

      const heroSection = screen.getByTestId('hero-section')

      // Verify theme-aware base colors
      expect(heroSection.className).toMatch(/\bbase-100\b/)
      expect(heroSection.className).toMatch(/\bbase-200\b/)
    })

    it('should use consistent flex layout classes', () => {
      renderWithRouter(<HeroSection />)

      const heroSection = screen.getByTestId('hero-section')

      // Verify flex centering pattern
      expect(heroSection.className).toMatch(/\bflex\b/)
      expect(heroSection.className).toMatch(/\bitems-center\b/)
      expect(heroSection.className).toMatch(/\bjustify-center\b/)
    })

    it('should use consistent text color classes with theme support', () => {
      renderWithRouter(<HeroSection />)

      // Check headline uses base-content
      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline.className).toMatch(/\btext-base-content\b/)

      // Check accent color for primary text
      expect(headline.textContent).toContain('Amplify Reach')
    })
  })

  describe('Features Section Tailwind Patterns', () => {
    it('should use consistent section background colors', () => {
      renderWithRouter(<FeaturesSection />)

      const featuresSection = screen.getByTestId('features-section')

      // Verify theme-aware background
      expect(featuresSection.className).toMatch(/\bbg-base-200\b/)
    })

    it('should use consistent responsive grid layout', () => {
      renderWithRouter(<FeaturesSection />)

      const featuresGrid = screen.getByTestId('features-grid')

      // Verify grid class
      expect(featuresGrid.className).toMatch(/\bgrid\b/)
      // Verify responsive column pattern
      expect(featuresGrid.className).toMatch(/\bgrid-cols-1\b/)
      expect(featuresGrid.className).toMatch(/\bmd:grid-cols-2\b/)
      expect(featuresGrid.className).toMatch(/\blg:grid-cols-4\b/)
    })

    it('should use consistent spacing classes', () => {
      renderWithRouter(<FeaturesSection />)

      const featuresGrid = screen.getByTestId('features-grid')

      // Verify gap class for consistent spacing
      expect(featuresGrid.className).toMatch(/\bgap-8\b/)
    })

    it('should use primary color for icons consistently', () => {
      renderWithRouter(<FeaturesSection />)

      const featureCards = screen.getAllByTestId('feature-card')

      featureCards.forEach((card) => {
        // Check for primary color icon container
        const iconContainer = card.querySelector('.text-primary')
        expect(iconContainer).toBeInTheDocument()
      })
    })
  })

  describe('HowItWorks Section Tailwind Patterns', () => {
    it('should use consistent DaisyUI card styling for step cards', () => {
      renderWithRouter(<HowItWorks />)

      // Find step cards
      const step1 = screen.getByTestId('step-1')
      const cardElement = step1.querySelector('.card')

      expect(cardElement).toBeInTheDocument()
      expect(cardElement?.className).toMatch(/\bcard\b/)
      expect(cardElement?.className).toMatch(/\bbg-base-100\b/)
      expect(cardElement?.className).toMatch(/\bshadow-xl\b/)
    })

    it('should use DaisyUI badge component for step numbers', () => {
      renderWithRouter(<HowItWorks />)

      const step1 = screen.getByTestId('step-1')
      const badge = step1.querySelector('.badge')

      expect(badge).toBeInTheDocument()
      expect(badge?.className).toMatch(/\bbadge\b/)
      expect(badge?.className).toMatch(/\bbadge-primary\b/)
    })
  })

  describe('Full Homepage Component Integration', () => {
    it('should render complete homepage with consistent styling throughout', () => {
      renderWithRouter(<Home />)

      // Verify all major sections are present
      expect(screen.getByTestId('homepage')).toBeInTheDocument()
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer')).toBeInTheDocument()
    })

    it('should use consistent max-width container pattern across sections', () => {
      renderWithRouter(<Home />)

      const featuresSection = screen.getByTestId('features-section')
      const howItWorksSection = screen.getByTestId('how-it-works-section')

      // Both should use max-w container
      const featuresContainer = featuresSection.querySelector('.max-w-7xl, .max-w-6xl')
      const howItWorksContainer = howItWorksSection.querySelector('.max-w-7xl, .max-w-6xl')

      expect(featuresContainer).toBeInTheDocument()
      expect(howItWorksContainer).toBeInTheDocument()
    })
  })
})
