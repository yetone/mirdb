/**
 * Unit Tests for FeaturesSection Component
 * Owner: Scenario 5 - Features Section Display
 *
 * Tests:
 * - Section renders with at least 3 feature cards
 * - Each card has icon, title, and description
 * - Analytics feature is included as differentiator (US-3)
 * - Grid layout uses responsive classes
 * - Feature icons render correctly with alt text
 *
 * Requirements: REQ-4, US-3
 */

import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { FeaturesSection } from '../../../src/components/homepage/FeaturesSection'

describe('FeaturesSection', () => {
  it('renders features section with at least 3 feature cards', () => {
    render(<FeaturesSection />)

    const section = screen.getByTestId('features-section')
    expect(section).toBeInTheDocument()

    // Check for at least 3 feature cards
    const featureCards = screen.getAllByTestId('feature-card')
    expect(featureCards.length).toBeGreaterThanOrEqual(3)
  })

  it('each feature card has icon, title, and 1-2 sentence description', () => {
    render(<FeaturesSection />)

    const featureCards = screen.getAllByTestId('feature-card')

    featureCards.forEach((card) => {
      // Each card should have an icon
      const icon = card.querySelector('[data-testid="feature-icon"]')
      expect(icon).toBeInTheDocument()
      expect(icon?.textContent?.length).toBeGreaterThan(0)

      // Each card should have a title
      const title = card.querySelector('[data-testid="feature-title"]')
      expect(title).toBeInTheDocument()
      expect(title?.textContent?.length).toBeGreaterThan(0)

      // Each card should have a description
      const description = card.querySelector('[data-testid="feature-description"]')
      expect(description).toBeInTheDocument()

      const descText = description?.textContent ?? ''
      expect(descText.length).toBeGreaterThan(10)

      // Description should be 1-2 sentences
      const sentenceEndings = (descText.match(/[.!?]/g) ?? []).length
      expect(sentenceEndings).toBeGreaterThanOrEqual(1)
      expect(sentenceEndings).toBeLessThanOrEqual(2)
    })
  })

  it('includes at least one feature mentioning analytics as differentiator (US-3)', () => {
    render(<FeaturesSection />)

    const section = screen.getByTestId('features-section')
    const sectionText = section.textContent?.toLowerCase() ?? ''

    // Check that at least one feature mentions 'analytics'
    expect(sectionText).toContain('analytics')
  })

  it('renders grid layout with responsive classes', () => {
    render(<FeaturesSection />)

    const grid = screen.getByTestId('features-grid')

    // Check for responsive grid classes (1 col mobile, 2 tablet, 3 desktop)
    expect(grid.className).toContain('grid-cols-1')
    expect(grid.className).toContain('md:grid-cols-2')
    expect(grid.className).toContain('lg:grid-cols-3')
  })

  it('all feature icons render correctly with proper alt text', () => {
    render(<FeaturesSection />)

    const icons = screen.getAllByTestId('feature-icon')

    icons.forEach((icon) => {
      // Icon should have role="img" for accessibility
      expect(icon.getAttribute('role')).toBe('img')

      // Icon should have aria-label
      const ariaLabel = icon.getAttribute('aria-label')
      expect(ariaLabel).toBeTruthy()
      expect(ariaLabel?.length).toBeGreaterThan(0)

      // Icon content should be present
      expect(icon.textContent?.length).toBeGreaterThan(0)
    })
  })

  it('renders section with heading', () => {
    render(<FeaturesSection />)

    const heading = screen.getByTestId('features-heading')
    expect(heading).toBeInTheDocument()
    expect(heading.tagName.toLowerCase()).toBe('h2')
  })

  it('has proper section ID for anchor links', () => {
    render(<FeaturesSection />)

    const section = screen.getByTestId('features-section')
    expect(section.id).toBe('features-section')
  })

  it('accepts custom features array', () => {
    const customFeatures = [
      { icon: '🚀', title: 'Custom Feature 1', description: 'Custom description one.' },
      { icon: '🎉', title: 'Custom Feature 2', description: 'Custom description two.' },
      { icon: '⭐', title: 'Custom Feature 3', description: 'Custom description three.' },
    ]

    render(<FeaturesSection features={customFeatures} />)

    const featureCards = screen.getAllByTestId('feature-card')
    expect(featureCards.length).toBe(3)

    // Verify custom content is rendered
    expect(screen.getByText('Custom Feature 1')).toBeInTheDocument()
    expect(screen.getByText('Custom Feature 2')).toBeInTheDocument()
    expect(screen.getByText('Custom Feature 3')).toBeInTheDocument()
  })

  it('renders 3-5 feature cards as specified in REQ-4', () => {
    render(<FeaturesSection />)

    const featureCards = screen.getAllByTestId('feature-card')
    expect(featureCards.length).toBeGreaterThanOrEqual(3)
    expect(featureCards.length).toBeLessThanOrEqual(5)
  })
})
