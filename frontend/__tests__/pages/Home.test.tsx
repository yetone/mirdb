import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import Home from '../../src/pages/Home'
import { FEATURES } from '../../src/constants/features'

/**
 * Homepage Hero Section Tests
 * Scenario 1: Validates hero section, headline, subheadline, and features
 */

const renderHome = () => {
  return render(
    <BrowserRouter>
      <Home />
    </BrowserRouter>
  )
}

describe('Home Page', () => {
  describe('Hero Section', () => {
    /**
     * Test Case 2: Headline element exists with role='heading' and level 1
     */
    it('should render headline with h1 element containing URL or Shorten', () => {
      renderHome()

      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline).toBeInTheDocument()

      const headlineText = headline.textContent || ''
      const containsUrlOrShorten =
        headlineText.toLowerCase().includes('url') ||
        headlineText.toLowerCase().includes('shorten')
      expect(containsUrlOrShorten).toBe(true)
    })

    it('should render subheadline text', () => {
      renderHome()

      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline).toBeInTheDocument()
      expect(subheadline.textContent).toBeTruthy()
    })

    it('should have accessible hero section with proper labeling', () => {
      renderHome()

      const heroSection = document.querySelector('[aria-labelledby="hero-headline"]')
      expect(heroSection).toBeInTheDocument()
    })
  })

  describe('Features Section', () => {
    /**
     * Test Case 3: Between 3 and 5 FeatureCard components are rendered
     */
    it('should render between 3 and 5 feature cards', () => {
      renderHome()

      const featureList = screen.getByRole('list', { name: /product features/i })
      expect(featureList).toBeInTheDocument()

      const featureItems = screen.getAllByRole('listitem')
      expect(featureItems.length).toBeGreaterThanOrEqual(3)
      expect(featureItems.length).toBeLessThanOrEqual(5)
    })

    it('should render features from FEATURES constant', () => {
      renderHome()

      FEATURES.forEach((feature) => {
        expect(screen.getByText(feature.title)).toBeInTheDocument()
        expect(screen.getByText(feature.description)).toBeInTheDocument()
      })
    })

    /**
     * Test Case 4: Each feature displays an icon element
     */
    it('should render icon for each feature', () => {
      renderHome()

      const icons = screen.getAllByTestId('feature-icon')
      expect(icons.length).toBe(FEATURES.length)

      icons.forEach((iconContainer) => {
        const svgElement = iconContainer.querySelector('svg')
        expect(svgElement).toBeInTheDocument()
      })
    })

    it('should have features section with proper heading', () => {
      renderHome()

      const featuresHeading = screen.getByRole('heading', {
        name: /why choose us/i,
      })
      expect(featuresHeading).toBeInTheDocument()
    })
  })

  describe('Accessibility', () => {
    it('should have skip to main content link', () => {
      renderHome()

      const skipLink = screen.getByText(/skip to main content/i)
      expect(skipLink).toBeInTheDocument()
      expect(skipLink).toHaveAttribute('href', '#main-content')
    })

    it('should have main content with id for skip link', () => {
      renderHome()

      const mainContent = document.getElementById('main-content')
      expect(mainContent).toBeInTheDocument()
    })
  })
})
