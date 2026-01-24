/**
 * HeroSection Unit Tests
 * Owner: Scenario 1 - Hero Section Display
 *
 * Tests for the HeroSection component verifying:
 * - Headline renders with value proposition text
 * - Subheadline summarizes key features
 * - Primary CTA button exists with correct text
 * - Secondary CTA button exists with correct text
 *
 * Requirements: REQ-1
 */

import { describe, it, expect } from 'vitest'
import { screen } from '@testing-library/react'
import { renderWithProviders } from './test-utils'
import { HeroSection } from '../../../src/components/landing/HeroSection'

describe('HeroSection', () => {
  describe('Test Case 1: Hero section renders with headline containing value proposition', () => {
    it('renders the hero section with default headline', () => {
      renderWithProviders(<HeroSection />)

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      const headline = screen.getByTestId('hero-headline')
      expect(headline).toBeInTheDocument()
      expect(headline.textContent).toContain('Shorten')
      expect(headline.textContent).toContain('Track')
    })

    it('renders with custom headline when provided', () => {
      const customHeadline = 'Custom Value Proposition'
      renderWithProviders(<HeroSection headline={customHeadline} />)

      const headline = screen.getByTestId('hero-headline')
      expect(headline).toHaveTextContent(customHeadline)
    })

    it('headline is an h1 element for proper SEO', () => {
      renderWithProviders(<HeroSection />)

      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline).toBeInTheDocument()
      expect(headline).toHaveTextContent('Shorten URLs. Track Everything.')
    })
  })

  describe('Test Case 2: Primary CTA button exists', () => {
    it('renders primary CTA button with default text', () => {
      renderWithProviders(<HeroSection />)

      const primaryCTA = screen.getByTestId('hero-primary-cta')
      expect(primaryCTA).toBeInTheDocument()
      expect(primaryCTA).toHaveTextContent('Get Started Free')
    })

    it('primary CTA links to registration page by default', () => {
      renderWithProviders(<HeroSection />)

      const primaryCTA = screen.getByTestId('hero-primary-cta')
      expect(primaryCTA).toHaveAttribute('href', '/register')
    })

    it('renders with custom primary CTA text when provided', () => {
      const customCTA = { text: 'Create Your First Short Link', href: '/signup' }
      renderWithProviders(<HeroSection primaryCTA={customCTA} />)

      const primaryCTA = screen.getByTestId('hero-primary-cta')
      expect(primaryCTA).toHaveTextContent('Create Your First Short Link')
      expect(primaryCTA).toHaveAttribute('href', '/signup')
    })
  })

  describe('Test Case 3: Secondary CTA button exists with Sign In text', () => {
    it('renders secondary CTA button with Sign In text', () => {
      renderWithProviders(<HeroSection />)

      const secondaryCTA = screen.getByTestId('hero-secondary-cta')
      expect(secondaryCTA).toBeInTheDocument()
      expect(secondaryCTA).toHaveTextContent('Sign In')
    })

    it('secondary CTA links to login page by default', () => {
      renderWithProviders(<HeroSection />)

      const secondaryCTA = screen.getByTestId('hero-secondary-cta')
      expect(secondaryCTA).toHaveAttribute('href', '/login')
    })

    it('renders with custom secondary CTA when provided', () => {
      const customCTA = { text: 'Already a member?', href: '/signin' }
      renderWithProviders(<HeroSection secondaryCTA={customCTA} />)

      const secondaryCTA = screen.getByTestId('hero-secondary-cta')
      expect(secondaryCTA).toHaveTextContent('Already a member?')
      expect(secondaryCTA).toHaveAttribute('href', '/signin')
    })
  })

  describe('Test Case 4: Subheadline summarizes key features', () => {
    it('renders subheadline with default text about URL shortening and analytics', () => {
      renderWithProviders(<HeroSection />)

      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline).toBeInTheDocument()

      // Verify subheadline mentions key features
      const text = subheadline.textContent?.toLowerCase() || ''
      expect(text).toContain('short')
      expect(text).toContain('analytics')
    })

    it('renders with custom subheadline when provided', () => {
      const customSubheadline = 'Custom subheadline with URL shortening and analytics features'
      renderWithProviders(<HeroSection subheadline={customSubheadline} />)

      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline).toHaveTextContent(customSubheadline)
    })

    it('subheadline is not empty', () => {
      renderWithProviders(<HeroSection />)

      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline.textContent?.trim().length).toBeGreaterThan(0)
    })
  })

  describe('Accessibility', () => {
    it('hero section has proper aria-labelledby for screen readers', () => {
      renderWithProviders(<HeroSection />)

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toHaveAttribute('aria-labelledby', 'hero-headline')
    })

    it('CTA buttons group has proper aria-label', () => {
      renderWithProviders(<HeroSection />)

      const ctaGroup = screen.getByRole('group', { name: /call to action/i })
      expect(ctaGroup).toBeInTheDocument()
    })
  })
})
