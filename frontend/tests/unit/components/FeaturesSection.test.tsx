/**
 * FeaturesSection Unit Tests
 * Owner: Scenario 2 - Features Section with Three Core Features
 *
 * Tests for the FeaturesSection component covering all test cases:
 * - TC1: Exactly three feature cards are rendered
 * - TC2: Feature card for 'Smart Shortening' exists with icon and description
 * - TC3: Feature card for 'Real-time Analytics' exists with icon and description
 * - TC4: Feature card for 'Link Dashboard' exists with icon and description
 * - TC5: Each feature card has an icon/illustration element
 */
import { render, screen } from '@testing-library/react'
import { describe, it, expect } from 'vitest'
import { FeaturesSection } from '@/components/homepage/FeaturesSection'

describe('FeaturesSection', () => {
  it('renders the features section', () => {
    render(<FeaturesSection />)
    expect(screen.getByTestId('features-section')).toBeInTheDocument()
  })

  it('renders exactly three feature cards (TC1)', () => {
    render(<FeaturesSection />)
    const featureCards = screen.getAllByTestId('feature-card')
    expect(featureCards).toHaveLength(3)
  })

  it('renders Smart Shortening feature card with icon and description (TC2)', () => {
    render(<FeaturesSection />)

    // Check for Smart Shortening title
    const smartShorteningTitle = screen.getByText('Smart Shortening')
    expect(smartShorteningTitle).toBeInTheDocument()

    // Check that the card has a description
    const description = screen.getByText(/Create short, memorable links instantly/i)
    expect(description).toBeInTheDocument()
  })

  it('renders Real-time Analytics feature card with icon and description (TC3)', () => {
    render(<FeaturesSection />)

    // Check for Real-time Analytics title
    const analyticsTitle = screen.getByText('Real-time Analytics')
    expect(analyticsTitle).toBeInTheDocument()

    // Check that the card has a description
    const description = screen.getByText(/Track every click with detailed analytics/i)
    expect(description).toBeInTheDocument()
  })

  it('renders Link Dashboard feature card with icon and description (TC4)', () => {
    render(<FeaturesSection />)

    // Check for Link Dashboard title
    const dashboardTitle = screen.getByText('Link Dashboard')
    expect(dashboardTitle).toBeInTheDocument()

    // Check that the card has a description
    const description = screen.getByText(/Manage all your links in one place/i)
    expect(description).toBeInTheDocument()
  })

  it('each feature card has an icon/illustration element (TC5)', () => {
    render(<FeaturesSection />)

    // Get all feature icons
    const featureIcons = screen.getAllByTestId('feature-icon')
    expect(featureIcons).toHaveLength(3)

    // Each icon container should have an SVG element
    featureIcons.forEach((iconContainer) => {
      const svg = iconContainer.querySelector('svg')
      expect(svg).toBeInTheDocument()
    })
  })

  it('renders features grid container', () => {
    render(<FeaturesSection />)
    const featuresGrid = screen.getByTestId('features-grid')
    expect(featuresGrid).toBeInTheDocument()
  })

  it('displays section heading "Powerful Features"', () => {
    render(<FeaturesSection />)
    expect(screen.getByText('Powerful Features')).toBeInTheDocument()
  })

  it('has section id for anchor navigation', () => {
    render(<FeaturesSection />)
    const section = screen.getByTestId('features-section')
    expect(section).toHaveAttribute('id', 'features')
  })
})
