/**
 * Unit tests for HeroSection component.
 * Owner: Scenario 1 - Hero Section Value Proposition Display
 *
 * Test coverage:
 * - Headline renders with correct content and h1 tag
 * - Subheadline renders with benefit text
 * - Primary CTA button is visible and has correct label
 * - Secondary login option is visible
 * - Accessibility: proper heading hierarchy
 */

import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import HeroSection from '../../src/components/home/HeroSection'

const renderHeroSection = () => {
  return render(
    <BrowserRouter>
      <HeroSection />
    </BrowserRouter>
  )
}

describe('HeroSection', () => {
  describe('Test Case 1: Headline with role and aria-level', () => {
    it('should render headline element with role="heading" and aria-level="1"', () => {
      renderHeroSection()

      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline).toBeInTheDocument()
      expect(headline).toHaveAttribute('aria-level', '1')
    })

    it('should display action-oriented headline text', () => {
      renderHeroSection()

      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline).toHaveTextContent(/shorten links/i)
      expect(headline).toHaveTextContent(/track results/i)
    })
  })

  describe('Test Case 2: Subheadline mentioning URL shortening and analytics', () => {
    it('should display subheadline mentioning URL shortening', () => {
      renderHeroSection()

      const subheadline = screen.getByText(/short.*url|url.*short|create short/i)
      expect(subheadline).toBeInTheDocument()
    })

    it('should display subheadline mentioning analytics', () => {
      renderHeroSection()

      const analyticsText = screen.getByText(/analytics/i)
      expect(analyticsText).toBeInTheDocument()
    })

    it('should have subheadline describing key benefits', () => {
      renderHeroSection()

      // Check for tracking-related content
      expect(screen.getByText(/track clicks/i)).toBeInTheDocument()
    })
  })

  describe('Hero section structure', () => {
    it('should have a hero section with proper aria-label', () => {
      renderHeroSection()

      const heroSection = screen.getByLabelText(/hero section/i)
      expect(heroSection).toBeInTheDocument()
    })

    it('should render primary CTA button for registration', () => {
      renderHeroSection()

      const ctaButton = screen.getByRole('link', { name: /get started/i })
      expect(ctaButton).toBeInTheDocument()
      expect(ctaButton).toHaveAttribute('href', '/register')
    })

    it('should render secondary CTA button for login', () => {
      renderHeroSection()

      const loginButton = screen.getByRole('link', { name: /login/i })
      expect(loginButton).toBeInTheDocument()
      expect(loginButton).toHaveAttribute('href', '/login')
    })
  })

  describe('Accessibility', () => {
    it('should have exactly one h1 heading on the page', () => {
      renderHeroSection()

      const h1Elements = screen.getAllByRole('heading', { level: 1 })
      expect(h1Elements).toHaveLength(1)
    })

    it('should have accessible link labels for CTAs', () => {
      renderHeroSection()

      const getStartedLink = screen.getByLabelText(/get started with registration/i)
      expect(getStartedLink).toBeInTheDocument()

      const loginLink = screen.getByLabelText(/login to your account/i)
      expect(loginLink).toBeInTheDocument()
    })
  })

  /**
   * Scenario 2: Primary Registration CTA Button
   * Owner: Scenario 2
   */
  describe('Scenario 2: Primary Registration CTA Button', () => {
    describe('Test Case 1: Primary CTA button renders with proper text', () => {
      it('should render primary CTA button with text containing "Get Started", "Sign Up", or similar', () => {
        renderHeroSection()

        // Look for primary CTA button with expected text
        const ctaButton = screen.getByRole('link', { name: /get started/i })
        expect(ctaButton).toBeInTheDocument()
        expect(ctaButton.textContent).toMatch(/get started|sign up/i)
      })

      it('should have primary CTA linking to /register', () => {
        renderHeroSection()

        const ctaButton = screen.getByRole('link', { name: /get started/i })
        expect(ctaButton).toHaveAttribute('href', '/register')
      })
    })

    describe('Test Case 4: Primary CTA has distinct styling', () => {
      it('should have btn-primary class for primary styling', () => {
        renderHeroSection()

        const ctaButton = screen.getByRole('link', { name: /get started/i })
        expect(ctaButton).toHaveClass('btn-primary')
      })

      it('should have btn-lg class for larger size', () => {
        renderHeroSection()

        const ctaButton = screen.getByRole('link', { name: /get started/i })
        expect(ctaButton).toHaveClass('btn-lg')
      })

      it('should have distinct styling compared to secondary login button', () => {
        renderHeroSection()

        const primaryCta = screen.getByRole('link', { name: /get started/i })
        const secondaryCta = screen.getByRole('link', { name: /login/i })

        // Primary has btn-primary, secondary has btn-outline
        expect(primaryCta).toHaveClass('btn-primary')
        expect(secondaryCta).toHaveClass('btn-outline')
        expect(secondaryCta).not.toHaveClass('btn-primary')
      })
    })
  })
})
