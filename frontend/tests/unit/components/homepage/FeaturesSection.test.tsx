/**
 * FeaturesSection Component Tests
 * Owner: Scenario 3 - Features Section Display
 *
 * Test cases:
 * 1. Three feature cards are displayed with correct titles
 * 2. Each feature card has an icon, title, and description
 * 3. Desktop: 3-column grid layout
 * 4. Mobile: Single column stack
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { render, screen, within } from '../../../test-utils'
import FeaturesSection from '../../../../src/components/homepage/FeaturesSection'

describe('FeaturesSection', () => {
  describe('Test Case 1: Three feature cards with correct titles', () => {
    it('renders three feature cards with the expected titles', () => {
      render(<FeaturesSection />)

      // Verify all three feature cards are displayed
      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards).toHaveLength(3)

      // Verify specific feature titles are present
      expect(screen.getByText('Lightning URL Shortening')).toBeInTheDocument()
      expect(screen.getByText('Powerful Analytics')).toBeInTheDocument()
      expect(screen.getByText('Secure & Reliable')).toBeInTheDocument()
    })
  })

  describe('Test Case 2: Each feature card has icon, title, and description', () => {
    it('renders each feature card with an icon element', () => {
      render(<FeaturesSection />)

      const icons = screen.getAllByTestId('feature-icon')
      expect(icons).toHaveLength(3)

      // Each icon should contain an SVG
      icons.forEach((icon) => {
        const svg = icon.querySelector('svg')
        expect(svg).toBeInTheDocument()
      })
    })

    it('renders each feature card with a title element', () => {
      render(<FeaturesSection />)

      const titles = screen.getAllByTestId('feature-title')
      expect(titles).toHaveLength(3)

      // Each title should have text content
      titles.forEach((title) => {
        expect(title.textContent).not.toBe('')
      })
    })

    it('renders each feature card with a description element', () => {
      render(<FeaturesSection />)

      const descriptions = screen.getAllByTestId('feature-description')
      expect(descriptions).toHaveLength(3)

      // Each description should have text content
      descriptions.forEach((description) => {
        expect(description.textContent).not.toBe('')
      })
    })

    it('each feature card has complete structure with icon, title, and description', () => {
      render(<FeaturesSection />)

      const featureCards = screen.getAllByTestId('feature-card')

      featureCards.forEach((card) => {
        const cardScope = within(card)

        // Each card should have exactly one icon, title, and description
        expect(cardScope.getByTestId('feature-icon')).toBeInTheDocument()
        expect(cardScope.getByTestId('feature-title')).toBeInTheDocument()
        expect(cardScope.getByTestId('feature-description')).toBeInTheDocument()
      })
    })
  })

  describe('Test Case 3: Desktop viewport - 3-column grid layout', () => {
    it('features grid has the correct CSS classes for 3-column layout on desktop', () => {
      render(<FeaturesSection />)

      const grid = screen.getByTestId('features-grid')

      // Check that the grid has the responsive class for 3 columns on medium+ screens
      expect(grid).toHaveClass('grid')
      expect(grid).toHaveClass('md:grid-cols-3')
    })

    it('renders all three features within the grid container', () => {
      render(<FeaturesSection />)

      const grid = screen.getByTestId('features-grid')
      const gridScope = within(grid)

      const featureCards = gridScope.getAllByTestId('feature-card')
      expect(featureCards).toHaveLength(3)
    })
  })

  describe('Test Case 4: Mobile viewport - single column stack', () => {
    it('features grid has grid-cols-1 as the default (mobile) layout', () => {
      render(<FeaturesSection />)

      const grid = screen.getByTestId('features-grid')

      // The grid should have grid-cols-1 as default (mobile-first)
      expect(grid).toHaveClass('grid-cols-1')
    })

    it('features section is accessible on mobile with proper container', () => {
      render(<FeaturesSection />)

      const section = screen.getByTestId('features-section')
      expect(section).toBeInTheDocument()

      // Section should have padding for mobile
      expect(section).toHaveClass('px-4')
    })
  })

  describe('Accessibility', () => {
    it('has a proper heading for the section', () => {
      render(<FeaturesSection />)

      const heading = screen.getByRole('heading', { level: 2 })
      expect(heading).toHaveTextContent('Why Choose Our Service?')
    })

    it('section has aria-labelledby pointing to the heading', () => {
      render(<FeaturesSection />)

      const section = screen.getByTestId('features-section')
      expect(section).toHaveAttribute('aria-labelledby', 'features-heading')

      const heading = document.getElementById('features-heading')
      expect(heading).toBeInTheDocument()
    })

    it('icons have aria-hidden attribute to hide from screen readers', () => {
      render(<FeaturesSection />)

      const icons = screen.getAllByTestId('feature-icon')
      icons.forEach((icon) => {
        const svg = icon.querySelector('svg')
        expect(svg).toHaveAttribute('aria-hidden', 'true')
      })
    })
  })

  describe('Feature content verification', () => {
    it('Lightning URL Shortening feature has correct description', () => {
      render(<FeaturesSection />)

      expect(screen.getByText(/Transform long URLs into short/)).toBeInTheDocument()
    })

    it('Powerful Analytics feature has correct description', () => {
      render(<FeaturesSection />)

      expect(screen.getByText(/Track clicks, geographic data/)).toBeInTheDocument()
    })

    it('Secure & Reliable feature has correct description', () => {
      render(<FeaturesSection />)

      expect(screen.getByText(/Enterprise-grade security/)).toBeInTheDocument()
    })
  })
})
