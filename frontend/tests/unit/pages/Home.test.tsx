/**
 * Unit tests for Home page component
 * Tests that verify FeaturesSection is rendered on the homepage
 */

import { describe, it, expect } from 'vitest'
import { screen } from '@testing-library/react'
import { renderWithProviders } from '../../test-utils'
import Home from '@/pages/Home'

describe('Home Page - Features Section Display', () => {
  it('renders the homepage with features section', () => {
    renderWithProviders(<Home />)

    // Check homepage exists
    const homepage = screen.getByTestId('homepage')
    expect(homepage).toBeInTheDocument()

    // Check features section is rendered
    const featuresSection = screen.getByTestId('features-section')
    expect(featuresSection).toBeInTheDocument()
  })

  it('renders features section container with at least 3 feature cards', () => {
    renderWithProviders(<Home />)

    // Check for features grid with at least 3 cards
    const featuresGrid = screen.getByTestId('features-grid')
    const featureCards = featuresGrid.querySelectorAll('[data-testid^="feature-card-"]')
    expect(featureCards.length).toBeGreaterThanOrEqual(3)
  })

  it('renders URL Shortening feature card with icon and description', () => {
    renderWithProviders(<Home />)

    const urlShorteningCard = screen.getByTestId('feature-card-url-shortening')
    expect(urlShorteningCard).toBeInTheDocument()

    expect(screen.getByText('URL Shortening')).toBeInTheDocument()
    expect(
      screen.getByText(/Transform long, unwieldy URLs into short, memorable links/)
    ).toBeInTheDocument()

    // Verify icon is present
    const iconContainer = urlShorteningCard.querySelector('.rounded-full')
    expect(iconContainer?.querySelector('svg')).toBeInTheDocument()
  })

  it('renders Analytics Dashboard feature card with icon and description', () => {
    renderWithProviders(<Home />)

    const analyticsCard = screen.getByTestId('feature-card-analytics-dashboard')
    expect(analyticsCard).toBeInTheDocument()

    expect(screen.getByText('Analytics Dashboard')).toBeInTheDocument()
    expect(
      screen.getByText(/Gain deep insights into your link performance/)
    ).toBeInTheDocument()

    // Verify icon is present
    const iconContainer = analyticsCard.querySelector('.rounded-full')
    expect(iconContainer?.querySelector('svg')).toBeInTheDocument()
  })

  it('renders Click Tracking feature card with icon and description', () => {
    renderWithProviders(<Home />)

    const clickTrackingCard = screen.getByTestId('feature-card-click-tracking')
    expect(clickTrackingCard).toBeInTheDocument()

    expect(screen.getByText('Click Tracking')).toBeInTheDocument()
    expect(
      screen.getByText(/Monitor every click on your shortened links/)
    ).toBeInTheDocument()

    // Verify icon is present
    const iconContainer = clickTrackingCard.querySelector('.rounded-full')
    expect(iconContainer?.querySelector('svg')).toBeInTheDocument()
  })
})
