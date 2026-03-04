/**
 * Hero Section Tests
 * Owner: Scenario 1 - Hero Section Rendering
 *
 * Test coverage:
 * - Hero headline visibility and content
 * - Hero subheading presence
 * - Primary CTA button rendering
 * - Value proposition verification
 */

import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import { HeroSection } from './HeroSection'

const renderWithRouter = (ui: React.ReactElement) => {
  return render(<BrowserRouter>{ui}</BrowserRouter>)
}

describe('HeroSection', () => {
  describe('Test Case 1: Hero Headline', () => {
    it('renders headline element with value proposition text containing "Shorten URLs"', () => {
      renderWithRouter(<HeroSection />)

      const headline = screen.getByTestId('hero-headline')
      expect(headline).toBeInTheDocument()
      expect(headline.textContent).toContain('Shorten URLs')
    })

    it('renders headline element with value proposition text containing "Track Clicks"', () => {
      renderWithRouter(<HeroSection />)

      const headline = screen.getByTestId('hero-headline')
      expect(headline).toBeInTheDocument()
      expect(headline.textContent).toContain('Track Clicks')
    })

    it('renders headline as an h1 element for proper semantic structure', () => {
      renderWithRouter(<HeroSection />)

      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline).toBeInTheDocument()
    })

    it('accepts custom headline prop', () => {
      const customHeadline = 'Custom Headline Text'
      renderWithRouter(<HeroSection headline={customHeadline} />)

      const headline = screen.getByTestId('hero-headline')
      expect(headline.textContent).toBe(customHeadline)
    })
  })

  describe('Test Case 2: Hero Subheading', () => {
    it('renders subheading element with descriptive text about URL shortening', () => {
      renderWithRouter(<HeroSection />)

      const subheading = screen.getByTestId('hero-subheading')
      expect(subheading).toBeInTheDocument()
    })

    it('subheading contains text about short links or URLs', () => {
      renderWithRouter(<HeroSection />)

      const subheading = screen.getByTestId('hero-subheading')
      expect(subheading.textContent?.toLowerCase()).toMatch(/short|url|link/i)
    })

    it('subheading mentions analytics or tracking', () => {
      renderWithRouter(<HeroSection />)

      const subheading = screen.getByTestId('hero-subheading')
      expect(subheading.textContent?.toLowerCase()).toMatch(
        /analytics|clicking|track/i
      )
    })

    it('accepts custom subheading prop', () => {
      const customSubheading = 'Custom subheading description'
      renderWithRouter(<HeroSection subheading={customSubheading} />)

      const subheading = screen.getByTestId('hero-subheading')
      expect(subheading.textContent).toBe(customSubheading)
    })
  })

  describe('Test Case 3: Primary CTA Button', () => {
    it('renders CTA button with "Get Started" text', () => {
      renderWithRouter(<HeroSection />)

      const ctaButton = screen.getByTestId('hero-cta-button')
      expect(ctaButton).toBeInTheDocument()
      expect(ctaButton.textContent).toMatch(/get started/i)
    })

    it('CTA button is rendered and not disabled', () => {
      renderWithRouter(<HeroSection />)

      const ctaButton = screen.getByTestId('hero-cta-button')
      expect(ctaButton).toBeInTheDocument()
      expect(ctaButton).not.toBeDisabled()
      // Verify button is not hidden via aria-hidden attribute
      expect(ctaButton).not.toHaveAttribute('aria-hidden', 'true')
    })

    it('CTA button has proper aria-label for accessibility', () => {
      renderWithRouter(<HeroSection />)

      const ctaButton = screen.getByTestId('hero-cta-button')
      expect(ctaButton).toHaveAttribute('aria-label')
    })

    it('accepts custom CTA text prop', () => {
      const customCtaText = 'Create Account'
      renderWithRouter(<HeroSection ctaText={customCtaText} />)

      const ctaButton = screen.getByTestId('hero-cta-button')
      expect(ctaButton.textContent).toBe(customCtaText)
    })

    it('CTA links to register page by default', () => {
      renderWithRouter(<HeroSection />)

      const link = screen.getByRole('link')
      expect(link).toHaveAttribute('href', '/register')
    })

    it('accepts custom CTA link prop', () => {
      renderWithRouter(<HeroSection ctaLink="/signup" />)

      const link = screen.getByRole('link')
      expect(link).toHaveAttribute('href', '/signup')
    })
  })

  describe('Test Case 4: Hero Section Structure', () => {
    it('renders hero section with proper test id', () => {
      renderWithRouter(<HeroSection />)

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()
    })

    it('hero section has proper aria-label for accessibility', () => {
      renderWithRouter(<HeroSection />)

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toHaveAttribute('aria-label', 'Hero section')
    })

    it('all hero elements (headline, subheading, CTA) are present', () => {
      renderWithRouter(<HeroSection />)

      expect(screen.getByTestId('hero-headline')).toBeInTheDocument()
      expect(screen.getByTestId('hero-subheading')).toBeInTheDocument()
      expect(screen.getByTestId('hero-cta-button')).toBeInTheDocument()
    })
  })
})
