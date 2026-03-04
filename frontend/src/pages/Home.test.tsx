/**
 * Home Page Tests
 * Owner: Scenario 1 - Hero Section Rendering
 *
 * Integration tests for homepage component.
 * Validates that hero section renders correctly within the homepage context.
 */

import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import Home from './Home'

const renderWithRouter = (ui: React.ReactElement) => {
  return render(<BrowserRouter>{ui}</BrowserRouter>)
}

describe('Home Page', () => {
  describe('Page Structure', () => {
    it('renders home page with proper test id', () => {
      renderWithRouter(<Home />)

      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()
    })

    it('home page has main element for accessibility', () => {
      renderWithRouter(<Home />)

      const main = screen.getByRole('main')
      expect(main).toBeInTheDocument()
    })
  })

  describe('Hero Section Integration', () => {
    it('renders hero section within home page', () => {
      renderWithRouter(<Home />)

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()
    })

    it('hero headline is rendered on home page', () => {
      renderWithRouter(<Home />)

      const headline = screen.getByTestId('hero-headline')
      expect(headline).toBeInTheDocument()
      // Verify not hidden via aria attribute
      expect(headline).not.toHaveAttribute('aria-hidden', 'true')
    })

    it('hero subheading is rendered on home page', () => {
      renderWithRouter(<Home />)

      const subheading = screen.getByTestId('hero-subheading')
      expect(subheading).toBeInTheDocument()
      // Verify not hidden via aria attribute
      expect(subheading).not.toHaveAttribute('aria-hidden', 'true')
    })

    it('hero CTA button is rendered on home page', () => {
      renderWithRouter(<Home />)

      const ctaButton = screen.getByTestId('hero-cta-button')
      expect(ctaButton).toBeInTheDocument()
      expect(ctaButton).not.toBeDisabled()
    })
  })

  describe('Test Case 4: First Viewport Visibility', () => {
    it('hero content renders without requiring scroll', () => {
      // This test validates that all essential hero elements
      // are rendered in the DOM and would be visible in the first viewport
      // The actual viewport check is done via integration/E2E testing
      renderWithRouter(<Home />)

      // All essential hero elements should be present
      const headline = screen.getByTestId('hero-headline')
      const subheading = screen.getByTestId('hero-subheading')
      const ctaButton = screen.getByTestId('hero-cta-button')

      expect(headline).toBeInTheDocument()
      expect(subheading).toBeInTheDocument()
      expect(ctaButton).toBeInTheDocument()

      // Verify elements are not hidden via aria attributes
      expect(headline).not.toHaveAttribute('aria-hidden', 'true')
      expect(subheading).not.toHaveAttribute('aria-hidden', 'true')
      expect(ctaButton).not.toHaveAttribute('aria-hidden', 'true')
    })

    it('hero section uses min-height that fills viewport', () => {
      renderWithRouter(<Home />)

      const heroSection = screen.getByTestId('hero-section')
      // The hero section should have classes that ensure it fills the viewport
      expect(heroSection.className).toMatch(/min-h-\[80vh\]|min-h-screen/)
    })
  })

  describe('Accessibility', () => {
    it('page has proper landmark structure', () => {
      renderWithRouter(<Home />)

      // Should have a main landmark
      const main = screen.getByRole('main')
      expect(main).toBeInTheDocument()
    })

    it('headline uses proper heading level (h1)', () => {
      renderWithRouter(<Home />)

      const h1 = screen.getByRole('heading', { level: 1 })
      expect(h1).toBeInTheDocument()
    })
  })
})
