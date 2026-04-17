/**
 * HeroSection Component Tests
 * Owner: Scenario 1 - Hero Section Value Proposition
 *
 * Tests for the hero section including:
 * - Headline visibility and content
 * - Subheadline presence
 * - CTA button functionality
 * - Theme compatibility
 * - Viewport coverage (above the fold)
 */

import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent, waitFor } from '../../test-utils'
import { HeroSection } from '../../../src/components/homepage/HeroSection'

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: React.HTMLAttributes<HTMLDivElement>) => (
      <div {...props}>{children}</div>
    ),
    p: ({ children, ...props }: React.HTMLAttributes<HTMLParagraphElement>) => (
      <p {...props}>{children}</p>
    ),
  },
}))

describe('HeroSection', () => {
  // Test Case 1: Hero section renders with gradient background
  describe('Test Case 1: Hero section renders with gradient background', () => {
    it('renders hero section with gradient background compatible with theme system', () => {
      render(<HeroSection />)

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      const gradient = screen.getByTestId('hero-gradient')
      expect(gradient).toBeInTheDocument()
      expect(gradient).toHaveClass('bg-gradient-to-br')
      expect(gradient).toHaveClass('from-primary/20')
      expect(gradient).toHaveClass('to-secondary/20')
    })
  })

  // Test Case 2: Headline h1 element exists and is visible
  describe('Test Case 2: Headline visibility', () => {
    it('displays headline "Shorten Links. Track Success." as h1 element', () => {
      render(<HeroSection />)

      const headline = screen.getByTestId('hero-headline')
      expect(headline).toBeInTheDocument()
      expect(headline.tagName).toBe('H1')
      expect(headline).toHaveTextContent('Shorten Links. Track Success.')
      expect(headline).toBeVisible()
    })
  })

  // Test Case 3: Subheadline text is present
  describe('Test Case 3: Subheadline presence', () => {
    it('displays subheadline explaining URL shortening service value', () => {
      render(<HeroSection />)

      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline).toBeInTheDocument()
      expect(subheadline).toHaveTextContent(/Transform long URLs into powerful short links/)
      expect(subheadline).toHaveTextContent(/analytics/)
      expect(subheadline).toHaveTextContent(/track/)
    })
  })

  // Test Case 4: CTA button navigates to /register
  describe('Test Case 4: CTA button navigation', () => {
    it('renders "Get Started Free" button that links to /register', () => {
      render(<HeroSection />)

      const ctaButton = screen.getByTestId('hero-cta')
      expect(ctaButton).toBeInTheDocument()
      expect(ctaButton).toHaveTextContent('Get Started Free')
      expect(ctaButton).toHaveAttribute('href', '/register')
    })

    it('CTA button is clickable and has correct styles', () => {
      render(<HeroSection />)

      const ctaButton = screen.getByTestId('hero-cta')
      expect(ctaButton).toHaveClass('btn')
      expect(ctaButton).toHaveClass('btn-primary')
      expect(ctaButton).toHaveClass('btn-lg')
    })
  })

  // Test Case 5: All hero elements are visible without scrolling (viewport coverage)
  describe('Test Case 5: Viewport coverage - above the fold', () => {
    it('hero section has minimum height for above-the-fold visibility', () => {
      render(<HeroSection />)

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toHaveClass('min-h-[80vh]')
    })

    it('all primary elements are rendered in the hero section', () => {
      render(<HeroSection />)

      // All elements should be present and visible
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('hero-headline')).toBeInTheDocument()
      expect(screen.getByTestId('hero-subheadline')).toBeInTheDocument()
      expect(screen.getByTestId('hero-cta')).toBeInTheDocument()
    })

    it('content is centered for optimal viewing', () => {
      render(<HeroSection />)

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toHaveClass('flex')
      expect(heroSection).toHaveClass('items-center')
      expect(heroSection).toHaveClass('justify-center')
    })
  })

  // Additional tests for theme compatibility
  describe('Theme Compatibility', () => {
    it('uses theme-aware color classes', () => {
      render(<HeroSection />)

      const headline = screen.getByTestId('hero-headline')
      expect(headline).toHaveClass('text-base-content')

      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline).toHaveClass('text-base-content/70')
    })
  })

  // Accessibility tests
  describe('Accessibility', () => {
    it('headline has proper heading structure', () => {
      render(<HeroSection />)

      const h1 = screen.getByRole('heading', { level: 1 })
      expect(h1).toBeInTheDocument()
      expect(h1).toHaveTextContent('Shorten Links. Track Success.')
    })

    it('CTA button is accessible as a link', () => {
      render(<HeroSection />)

      const ctaLink = screen.getByRole('link', { name: /Get Started Free/i })
      expect(ctaLink).toBeInTheDocument()
    })
  })

  // Responsive design
  describe('Responsive Design', () => {
    it('uses responsive text sizing', () => {
      render(<HeroSection />)

      const headline = screen.getByTestId('hero-headline')
      expect(headline).toHaveClass('text-4xl')
      expect(headline).toHaveClass('md:text-5xl')
      expect(headline).toHaveClass('lg:text-6xl')
    })
  })
})
