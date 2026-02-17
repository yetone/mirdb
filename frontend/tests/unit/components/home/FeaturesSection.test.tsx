/**
 * Tests for FeaturesSection component.
 * Owner: Scenario 2 - Features Section Display
 *
 * Test cases:
 * 1. Render features section - 3-5 feature cards using GlassMorphismCard
 * 2. Check feature content - icon, title, and description
 * 3. Verify URL shortening feature
 * 4. Verify analytics feature
 * 5. Verify secure management feature
 */

import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { FeaturesSection } from '../../../../src/components/home/FeaturesSection'

describe('FeaturesSection', () => {
  // Test case 1: Render features section - 3-5 feature cards displayed using GlassMorphismCard
  it('should render 3-5 feature cards using GlassMorphismCard component', () => {
    render(<FeaturesSection />)

    // Find all feature cards by test ID
    const featureCards = screen.getAllByTestId(/^feature-card-/)

    // Verify there are 3-5 feature cards
    expect(featureCards.length).toBeGreaterThanOrEqual(3)
    expect(featureCards.length).toBeLessThanOrEqual(5)
  })

  // Test case 2: Each feature card has an icon, title, and 1-2 sentence description
  it('should display an icon, title, and description for each feature card', () => {
    render(<FeaturesSection />)

    // Get all feature cards
    const featureCards = screen.getAllByTestId(/^feature-card-/)

    featureCards.forEach((card) => {
      // Check for icon (SVG or icon element)
      const icon = card.querySelector('[data-testid="feature-icon"]')
      expect(icon).toBeInTheDocument()

      // Check for title
      const title = card.querySelector('[data-testid="feature-title"]')
      expect(title).toBeInTheDocument()
      expect(title?.textContent).toBeTruthy()

      // Check for description
      const description = card.querySelector('[data-testid="feature-description"]')
      expect(description).toBeInTheDocument()
      expect(description?.textContent).toBeTruthy()
      // Description should be 1-2 sentences (reasonable length, not too long)
      expect(description?.textContent?.length).toBeGreaterThan(20)
      expect(description?.textContent?.length).toBeLessThan(300)
    })
  })

  // Test case 3: Verify URL shortening feature is present
  it('should display a feature describing URL shortening capability', () => {
    render(<FeaturesSection />)

    // Look for the URL Shortening feature card specifically
    const urlShorteningCard = screen.getByTestId('feature-card-url-shortening')
    expect(urlShorteningCard).toBeInTheDocument()

    // Verify it has URL shortening related content in title or description
    const title = urlShorteningCard.querySelector('[data-testid="feature-title"]')
    expect(title?.textContent).toMatch(/url shortening/i)
  })

  // Test case 4: Verify analytics feature is present
  it('should display a feature describing click analytics capability', () => {
    render(<FeaturesSection />)

    // Look for the Analytics feature card specifically
    const analyticsCard = screen.getByTestId('feature-card-analytics')
    expect(analyticsCard).toBeInTheDocument()

    // Verify it has analytics related content
    const title = analyticsCard.querySelector('[data-testid="feature-title"]')
    expect(title?.textContent).toMatch(/analytics/i)
  })

  // Test case 5: Verify secure management feature is present
  it('should display a feature describing secure URL management', () => {
    render(<FeaturesSection />)

    // Look for the Secure Management feature card specifically
    const secureCard = screen.getByTestId('feature-card-secure-management')
    expect(secureCard).toBeInTheDocument()

    // Verify it has secure management related content
    const title = secureCard.querySelector('[data-testid="feature-title"]')
    expect(title?.textContent).toMatch(/secure|management/i)
  })

  // Additional test: Verify section has appropriate heading
  it('should have a section heading', () => {
    render(<FeaturesSection />)

    const heading = screen.getByRole('heading', { level: 2 })
    expect(heading).toBeInTheDocument()
  })

  // Additional test: Verify features are rendered in a grid layout
  it('should render features in a grid container', () => {
    render(<FeaturesSection />)

    const gridContainer = screen.getByTestId('features-grid')
    expect(gridContainer).toBeInTheDocument()
    expect(gridContainer.className).toMatch(/grid/i)
  })
})
