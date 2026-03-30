/**
 * Unit tests for FeaturesSection component
 * Owner: Scenario 3 - Features Section Display
 *
 * Tests:
 * 1. Features section container exists with at least 3 feature cards
 * 2. Feature card for 'URL Shortening' is present with icon and description
 * 3. Feature card for 'Analytics Dashboard' is present with icon and description
 * 4. Feature card for 'Click Tracking' is present with icon and description
 */

import { describe, it, expect } from 'vitest'
import { screen } from '@testing-library/react'
import { renderWithProviders } from '../../../test-utils'
import { FeaturesSection } from '@/components/homepage'

describe('FeaturesSection', () => {
  it('renders features section container with at least 3 feature cards', () => {
    renderWithProviders(<FeaturesSection />)

    // Check features section exists
    const featuresSection = screen.getByTestId('features-section')
    expect(featuresSection).toBeInTheDocument()

    // Check for features grid
    const featuresGrid = screen.getByTestId('features-grid')
    expect(featuresGrid).toBeInTheDocument()

    // Check for at least 3 feature cards
    const featureCards = featuresGrid.querySelectorAll('[data-testid^="feature-card-"]')
    expect(featureCards.length).toBeGreaterThanOrEqual(3)
  })

  it('renders URL Shortening feature card with icon and description', () => {
    renderWithProviders(<FeaturesSection />)

    // Check for URL Shortening feature card
    const urlShorteningCard = screen.getByTestId('feature-card-url-shortening')
    expect(urlShorteningCard).toBeInTheDocument()

    // Check for title
    expect(screen.getByText('URL Shortening')).toBeInTheDocument()

    // Check for description (partial match)
    expect(
      screen.getByText(/Transform long, unwieldy URLs into short, memorable links/)
    ).toBeInTheDocument()

    // Check for icon (the card should have an SVG icon)
    const iconContainer = urlShorteningCard.querySelector('.rounded-full')
    expect(iconContainer).toBeInTheDocument()
    const icon = iconContainer?.querySelector('svg')
    expect(icon).toBeInTheDocument()
  })

  it('renders Analytics Dashboard feature card with icon and description', () => {
    renderWithProviders(<FeaturesSection />)

    // Check for Analytics Dashboard feature card
    const analyticsCard = screen.getByTestId('feature-card-analytics-dashboard')
    expect(analyticsCard).toBeInTheDocument()

    // Check for title
    expect(screen.getByText('Analytics Dashboard')).toBeInTheDocument()

    // Check for description (partial match)
    expect(
      screen.getByText(/Gain deep insights into your link performance/)
    ).toBeInTheDocument()

    // Check for icon
    const iconContainer = analyticsCard.querySelector('.rounded-full')
    expect(iconContainer).toBeInTheDocument()
    const icon = iconContainer?.querySelector('svg')
    expect(icon).toBeInTheDocument()
  })

  it('renders Click Tracking feature card with icon and description', () => {
    renderWithProviders(<FeaturesSection />)

    // Check for Click Tracking feature card
    const clickTrackingCard = screen.getByTestId('feature-card-click-tracking')
    expect(clickTrackingCard).toBeInTheDocument()

    // Check for title
    expect(screen.getByText('Click Tracking')).toBeInTheDocument()

    // Check for description (partial match)
    expect(
      screen.getByText(/Monitor every click on your shortened links/)
    ).toBeInTheDocument()

    // Check for icon
    const iconContainer = clickTrackingCard.querySelector('.rounded-full')
    expect(iconContainer).toBeInTheDocument()
    const icon = iconContainer?.querySelector('svg')
    expect(icon).toBeInTheDocument()
  })

  it('renders Secure & Reliable feature card with icon and description', () => {
    renderWithProviders(<FeaturesSection />)

    // Check for Secure & Reliable feature card
    const secureCard = screen.getByTestId('feature-card-secure-reliable')
    expect(secureCard).toBeInTheDocument()

    // Check for title
    expect(screen.getByText('Secure & Reliable')).toBeInTheDocument()

    // Check for description (partial match)
    expect(
      screen.getByText(/Your links are protected with enterprise-grade security/)
    ).toBeInTheDocument()

    // Check for icon
    const iconContainer = secureCard.querySelector('.rounded-full')
    expect(iconContainer).toBeInTheDocument()
    const icon = iconContainer?.querySelector('svg')
    expect(icon).toBeInTheDocument()
  })

  it('displays feature cards in a responsive grid layout', () => {
    renderWithProviders(<FeaturesSection />)

    const featuresGrid = screen.getByTestId('features-grid')
    expect(featuresGrid).toHaveClass('grid')
    expect(featuresGrid).toHaveClass('grid-cols-1')
    expect(featuresGrid).toHaveClass('md:grid-cols-2')
    expect(featuresGrid).toHaveClass('lg:grid-cols-4')
  })

  it('has proper accessibility attributes', () => {
    renderWithProviders(<FeaturesSection />)

    const featuresSection = screen.getByTestId('features-section')
    expect(featuresSection).toHaveAttribute('aria-label', 'Features')
    expect(featuresSection.tagName).toBe('SECTION')
  })
})
