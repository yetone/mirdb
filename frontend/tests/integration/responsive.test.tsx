/**
 * Responsive Layout Integration Tests
 * Owner: Scenario 2 & 4 - Features Section and Responsive Design
 *
 * Tests for responsive layout behavior:
 * - TC6: Feature cards display in horizontal layout on desktop viewport (1024px+)
 */
import { render, screen } from '@testing-library/react'
import { describe, it, expect } from 'vitest'
import { FeaturesSection } from '@/components/homepage/FeaturesSection'

describe('FeaturesSection Responsive Layout', () => {
  it('features grid has correct classes for horizontal layout on desktop (TC6)', () => {
    render(<FeaturesSection />)

    const featuresGrid = screen.getByTestId('features-grid')

    // Verify the grid has the responsive classes that create horizontal layout on desktop
    // md:grid-cols-3 means 3 columns on medium screens and above (768px+)
    expect(featuresGrid).toHaveClass('grid')
    expect(featuresGrid).toHaveClass('grid-cols-1')
    expect(featuresGrid).toHaveClass('md:grid-cols-3')
  })

  it('features grid has gap styling for proper spacing', () => {
    render(<FeaturesSection />)

    const featuresGrid = screen.getByTestId('features-grid')

    // Verify gap classes for spacing between cards
    expect(featuresGrid).toHaveClass('gap-6')
    expect(featuresGrid).toHaveClass('lg:gap-8')
  })

  it('renders three feature cards in the grid', () => {
    render(<FeaturesSection />)

    const featuresGrid = screen.getByTestId('features-grid')
    const featureCards = featuresGrid.querySelectorAll('[data-testid="feature-card"]')

    expect(featureCards).toHaveLength(3)
  })

  it('section has proper responsive padding', () => {
    render(<FeaturesSection />)

    const section = screen.getByTestId('features-section')

    // Verify responsive padding classes
    expect(section).toHaveClass('px-4')
    expect(section).toHaveClass('md:px-8')
    expect(section).toHaveClass('lg:px-16')
  })

  it('content container has max-width constraint for desktop', () => {
    render(<FeaturesSection />)

    const section = screen.getByTestId('features-section')
    const container = section.firstChild as HTMLElement

    expect(container).toHaveClass('max-w-6xl')
    expect(container).toHaveClass('mx-auto')
  })
})
