/**
 * Unit tests for FeaturesSection component.
 * Owner: Scenario 4 - Features Section Display
 *
 * Test coverage:
 * - Renders 4 feature cards
 * - Each feature (URL shortening, analytics, geo, dashboard) is present
 * - Grid layout structure is correct
 */

import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import FeaturesSection from '../../src/components/home/FeaturesSection'
import Home from '../../src/pages/Home'

const renderFeaturesSection = () => {
  return render(
    <BrowserRouter>
      <FeaturesSection />
    </BrowserRouter>
  )
}

const renderHome = () => {
  return render(
    <BrowserRouter>
      <Home />
    </BrowserRouter>
  )
}

describe('FeaturesSection', () => {
  /**
   * Test Case 1: Features section container with 4 feature cards is rendered
   */
  describe('Test Case 1: Features section container with 4 feature cards', () => {
    it('should render the features section container', () => {
      renderHome()

      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()
    })

    it('should render exactly 4 feature cards', () => {
      renderHome()

      const featureCards = screen.getAllByRole('article')
      expect(featureCards).toHaveLength(4)
    })

    it('should render features grid container', () => {
      renderFeaturesSection()

      const grid = screen.getByTestId('features-grid')
      expect(grid).toBeInTheDocument()
    })
  })

  /**
   * Test Case 2: URL Shortening feature card with icon, title, and description exists
   */
  describe('Test Case 2: URL Shortening feature card', () => {
    it('should render URL Shortening feature card with title', () => {
      renderHome()

      expect(screen.getByText('URL Shortening')).toBeInTheDocument()
    })

    it('should render URL Shortening feature card with description', () => {
      renderHome()

      expect(screen.getByText(/Transform long, unwieldy URLs into clean/i)).toBeInTheDocument()
    })

    it('should render URL Shortening feature card with icon', () => {
      renderHome()

      const urlCard = screen.getByLabelText('Feature: URL Shortening')
      const svg = urlCard.querySelector('svg')
      expect(svg).toBeInTheDocument()
    })
  })

  /**
   * Test Case 3: Click Analytics feature card with icon, title, and description exists
   */
  describe('Test Case 3: Click Analytics feature card', () => {
    it('should render Click Analytics feature card with title', () => {
      renderHome()

      expect(screen.getByText('Click Analytics')).toBeInTheDocument()
    })

    it('should render Click Analytics feature card with description', () => {
      renderHome()

      expect(screen.getByText(/Track every click with detailed analytics/i)).toBeInTheDocument()
    })

    it('should render Click Analytics feature card with icon', () => {
      renderHome()

      const analyticsCard = screen.getByLabelText('Feature: Click Analytics')
      const svg = analyticsCard.querySelector('svg')
      expect(svg).toBeInTheDocument()
    })
  })

  /**
   * Test Case 4: Geo-location Tracking feature card with icon, title, and description exists
   */
  describe('Test Case 4: Geo-location Tracking feature card', () => {
    it('should render Geo-location Tracking feature card with title', () => {
      renderHome()

      expect(screen.getByText('Geo-location Tracking')).toBeInTheDocument()
    })

    it('should render Geo-location Tracking feature card with description', () => {
      renderHome()

      expect(screen.getByText(/Understand your audience with geographic insights/i)).toBeInTheDocument()
    })

    it('should render Geo-location Tracking feature card with icon', () => {
      renderHome()

      const geoCard = screen.getByLabelText('Feature: Geo-location Tracking')
      const svg = geoCard.querySelector('svg')
      expect(svg).toBeInTheDocument()
    })
  })

  /**
   * Test Case 5: Dashboard feature card with icon, title, and description exists
   */
  describe('Test Case 5: Dashboard feature card', () => {
    it('should render Dashboard feature card with title', () => {
      renderHome()

      expect(screen.getByText('User Dashboard')).toBeInTheDocument()
    })

    it('should render Dashboard feature card with description', () => {
      renderHome()

      expect(screen.getByText(/Manage all your links in one place/i)).toBeInTheDocument()
    })

    it('should render Dashboard feature card with icon', () => {
      renderHome()

      const dashboardCard = screen.getByLabelText('Feature: User Dashboard')
      const svg = dashboardCard.querySelector('svg')
      expect(svg).toBeInTheDocument()
    })
  })

  /**
   * Test Case 6: Each feature card contains an SVG icon or image element
   */
  describe('Test Case 6: Each feature card contains an SVG icon', () => {
    it('should have SVG icons in all feature cards', () => {
      renderHome()

      const featureCards = screen.getAllByRole('article')
      expect(featureCards).toHaveLength(4)

      featureCards.forEach((card) => {
        const svg = card.querySelector('svg')
        expect(svg).toBeInTheDocument()
      })
    })

    it('should have icons with aria-hidden for decorative purposes', () => {
      renderFeaturesSection()

      const svgIcons = document.querySelectorAll('svg[aria-hidden="true"]')
      expect(svgIcons.length).toBeGreaterThanOrEqual(4)
    })
  })

  describe('Layout and Structure', () => {
    it('should have a grid layout for feature cards', () => {
      renderFeaturesSection()

      const grid = screen.getByTestId('features-grid')
      expect(grid).toHaveClass('grid')
      expect(grid).toHaveClass('grid-cols-1')
      expect(grid).toHaveClass('md:grid-cols-2')
    })

    it('should render section heading', () => {
      renderFeaturesSection()

      expect(screen.getByRole('heading', { level: 2 })).toHaveTextContent('Powerful Features')
    })

    it('should have aria-label on the section', () => {
      renderFeaturesSection()

      const section = screen.getByLabelText(/features section/i)
      expect(section).toBeInTheDocument()
    })
  })
})
