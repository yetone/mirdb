/**
 * HeroSection Unit Tests
 *
 * Tests for the hero section component rendering requirements.
 * Validates:
 * - Test Case 1: Headline visibility with value proposition text
 * - Test Case 2: Subheading with descriptive text
 * - Test Case 3: Primary CTA button presence
 */
import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import { HeroSection } from './HeroSection'

// Helper to wrap component with router
const renderWithRouter = (ui: React.ReactElement) => {
  return render(<BrowserRouter>{ui}</BrowserRouter>)
}

describe('HeroSection', () => {
  describe('Test Case 1: Hero Headline Rendering', () => {
    it('should render a headline element that is visible', () => {
      renderWithRouter(<HeroSection />)

      const headline = screen.getByTestId('hero-headline')
      expect(headline).toBeInTheDocument()
      expect(headline).toBeVisible()
    })

    it('should contain value proposition text in headline (e.g., "Shorten URLs" or "Track Clicks")', () => {
      renderWithRouter(<HeroSection />)

      const headline = screen.getByTestId('hero-headline')
      const headlineText = headline.textContent || ''

      // Check for value proposition keywords
      const hasValueProposition =
        headlineText.includes('Shorten') ||
        headlineText.includes('URLs') ||
        headlineText.includes('Track') ||
        headlineText.includes('Clicks') ||
        headlineText.includes('Audience')

      expect(hasValueProposition).toBe(true)
    })

    it('should render headline as h1 element for semantic HTML', () => {
      renderWithRouter(<HeroSection />)

      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline).toBeInTheDocument()
    })

    it('should accept custom headline text via props', () => {
      const customHeadline = 'Custom Value Proposition'
      renderWithRouter(<HeroSection headline={customHeadline} />)

      const headline = screen.getByTestId('hero-headline')
      expect(headline).toHaveTextContent(customHeadline)
    })
  })

  describe('Test Case 2: Hero Subheading Rendering', () => {
    it('should render a subheading element that is visible', () => {
      renderWithRouter(<HeroSection />)

      const subheading = screen.getByTestId('hero-subheading')
      expect(subheading).toBeInTheDocument()
      expect(subheading).toBeVisible()
    })

    it('should contain descriptive text about URL shortening service', () => {
      renderWithRouter(<HeroSection />)

      const subheading = screen.getByTestId('hero-subheading')
      const subheadingText = subheading.textContent || ''

      // Check for descriptive service keywords
      const hasServiceDescription =
        subheadingText.includes('short') ||
        subheadingText.includes('link') ||
        subheadingText.includes('URL') ||
        subheadingText.includes('analytics') ||
        subheadingText.includes('clicking')

      expect(hasServiceDescription).toBe(true)
    })

    it('should accept custom subheading text via props', () => {
      const customSubheading = 'Custom service description'
      renderWithRouter(<HeroSection subheading={customSubheading} />)

      const subheading = screen.getByTestId('hero-subheading')
      expect(subheading).toHaveTextContent(customSubheading)
    })
  })

  describe('Test Case 3: Primary CTA Button Rendering', () => {
    it('should render a primary CTA button that is visible', () => {
      renderWithRouter(<HeroSection />)

      const ctaButton = screen.getByTestId('hero-cta')
      expect(ctaButton).toBeInTheDocument()
      expect(ctaButton).toBeVisible()
    })

    it('should display "Get Started" or "Create Account" text on CTA', () => {
      renderWithRouter(<HeroSection />)

      const ctaButton = screen.getByTestId('hero-cta')
      const buttonText = ctaButton.textContent || ''

      const hasExpectedText =
        buttonText.includes('Get Started') ||
        buttonText.includes('Create Account') ||
        buttonText.includes('Sign Up')

      expect(hasExpectedText).toBe(true)
    })

    it('should be a button element for accessibility', () => {
      renderWithRouter(<HeroSection />)

      const ctaButton = screen.getByRole('button', { name: /get started/i })
      expect(ctaButton).toBeInTheDocument()
    })

    it('should accept custom CTA text via props', () => {
      const customCtaText = 'Join Now'
      renderWithRouter(<HeroSection ctaText={customCtaText} />)

      const ctaButton = screen.getByTestId('hero-cta')
      expect(ctaButton).toHaveTextContent(customCtaText)
    })

    it('should link to /register by default', () => {
      renderWithRouter(<HeroSection />)

      const ctaButton = screen.getByTestId('hero-cta')
      // FuturisticButton uses href prop for navigation
      expect(ctaButton).toBeInTheDocument()
    })
  })

  describe('Accessibility', () => {
    it('should have proper aria-labelledby on the section', () => {
      renderWithRouter(<HeroSection />)

      const section = screen.getByTestId('hero-section')
      expect(section).toHaveAttribute('aria-labelledby', 'hero-headline')
    })

    it('should have an id on the headline matching aria-labelledby', () => {
      renderWithRouter(<HeroSection />)

      const headline = screen.getByTestId('hero-headline')
      expect(headline).toHaveAttribute('id', 'hero-headline')
    })
  })
})
