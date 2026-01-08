import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import Home from './Home'

const renderHome = () => {
  return render(
    <BrowserRouter>
      <Home />
    </BrowserRouter>
  )
}

describe('Home - Feature Cards Display', () => {
  describe('Test Case 1: At least 3 GlassMorphismCard components are rendered', () => {
    it('renders at least 3 feature cards using GlassMorphismCard components', () => {
      renderHome()

      // Get all feature cards
      const featureCards = screen.getAllByTestId(/^feature-card-/)

      // Verify at least 3 cards are rendered
      expect(featureCards.length).toBeGreaterThanOrEqual(3)
    })

    it('renders the features section', () => {
      renderHome()

      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()
    })

    it('renders the features grid', () => {
      renderHome()

      const featuresGrid = screen.getByTestId('features-grid')
      expect(featuresGrid).toBeInTheDocument()
    })
  })

  describe('Test Case 2: URL Shortening feature card with appropriate icon and description', () => {
    it('renders URL Shortening feature card', () => {
      renderHome()

      const urlShorteningCard = screen.getByTestId('feature-card-url-shortening')
      expect(urlShorteningCard).toBeInTheDocument()
    })

    it('URL Shortening card has correct title', () => {
      renderHome()

      const urlShorteningCard = screen.getByTestId('feature-card-url-shortening')
      const title = urlShorteningCard.querySelector('[data-testid="card-title"]')

      expect(title).toBeInTheDocument()
      expect(title?.textContent).toBe('URL Shortening')
    })

    it('URL Shortening card has description', () => {
      renderHome()

      const urlShorteningCard = screen.getByTestId('feature-card-url-shortening')
      const description = urlShorteningCard.querySelector('[data-testid="card-description"]')

      expect(description).toBeInTheDocument()
      expect(description?.textContent).toContain('short links')
    })

    it('URL Shortening card has icon', () => {
      renderHome()

      const urlShorteningCard = screen.getByTestId('feature-card-url-shortening')
      const icon = urlShorteningCard.querySelector('[data-testid="card-icon"]')

      expect(icon).toBeInTheDocument()
    })
  })

  describe('Test Case 3: Analytics Dashboard feature card with chart/stats icon', () => {
    it('renders Analytics Dashboard feature card', () => {
      renderHome()

      const analyticsCard = screen.getByTestId('feature-card-analytics-dashboard')
      expect(analyticsCard).toBeInTheDocument()
    })

    it('Analytics Dashboard card has correct title', () => {
      renderHome()

      const analyticsCard = screen.getByTestId('feature-card-analytics-dashboard')
      const title = analyticsCard.querySelector('[data-testid="card-title"]')

      expect(title).toBeInTheDocument()
      expect(title?.textContent).toBe('Analytics Dashboard')
    })

    it('Analytics Dashboard card has description about tracking', () => {
      renderHome()

      const analyticsCard = screen.getByTestId('feature-card-analytics-dashboard')
      const description = analyticsCard.querySelector('[data-testid="card-description"]')

      expect(description).toBeInTheDocument()
      expect(description?.textContent).toContain('Track')
    })

    it('Analytics Dashboard card has chart/stats icon', () => {
      renderHome()

      const analyticsCard = screen.getByTestId('feature-card-analytics-dashboard')
      const icon = analyticsCard.querySelector('[data-testid="card-icon"]')

      expect(icon).toBeInTheDocument()
      // Icon should contain an SVG element (the BarChart3 icon)
      const svgElement = icon?.querySelector('svg')
      expect(svgElement).toBeInTheDocument()
    })
  })

  describe('Test Case 4: Geographic Insights feature card with location/globe icon', () => {
    it('renders Geographic Insights feature card', () => {
      renderHome()

      const geoCard = screen.getByTestId('feature-card-geographic-insights')
      expect(geoCard).toBeInTheDocument()
    })

    it('Geographic Insights card has correct title', () => {
      renderHome()

      const geoCard = screen.getByTestId('feature-card-geographic-insights')
      const title = geoCard.querySelector('[data-testid="card-title"]')

      expect(title).toBeInTheDocument()
      expect(title?.textContent).toBe('Geographic Insights')
    })

    it('Geographic Insights card has description about location/audience', () => {
      renderHome()

      const geoCard = screen.getByTestId('feature-card-geographic-insights')
      const description = geoCard.querySelector('[data-testid="card-description"]')

      expect(description).toBeInTheDocument()
      expect(description?.textContent).toContain('audience')
    })

    it('Geographic Insights card has globe/location icon', () => {
      renderHome()

      const geoCard = screen.getByTestId('feature-card-geographic-insights')
      const icon = geoCard.querySelector('[data-testid="card-icon"]')

      expect(icon).toBeInTheDocument()
      // Icon should contain an SVG element (the Globe icon)
      const svgElement = icon?.querySelector('svg')
      expect(svgElement).toBeInTheDocument()
    })
  })

  describe('Test Case 5: Each feature card has title, description, and visual icon element', () => {
    it('all feature cards have a title', () => {
      renderHome()

      const featureCards = screen.getAllByTestId(/^feature-card-/)

      featureCards.forEach((card) => {
        const title = card.querySelector('[data-testid="card-title"]')
        expect(title).toBeInTheDocument()
        expect(title?.textContent).not.toBe('')
      })
    })

    it('all feature cards have a description', () => {
      renderHome()

      const featureCards = screen.getAllByTestId(/^feature-card-/)

      featureCards.forEach((card) => {
        const description = card.querySelector('[data-testid="card-description"]')
        expect(description).toBeInTheDocument()
        expect(description?.textContent).not.toBe('')
      })
    })

    it('all feature cards have a visual icon element', () => {
      renderHome()

      const featureCards = screen.getAllByTestId(/^feature-card-/)

      featureCards.forEach((card) => {
        const icon = card.querySelector('[data-testid="card-icon"]')
        expect(icon).toBeInTheDocument()

        // Each icon should contain an SVG
        const svgElement = icon?.querySelector('svg')
        expect(svgElement).toBeInTheDocument()
      })
    })

    it('renders exactly 4 feature cards', () => {
      renderHome()

      const featureCards = screen.getAllByTestId(/^feature-card-/)
      expect(featureCards).toHaveLength(4)
    })
  })
})
