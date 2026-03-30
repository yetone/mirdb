/**
 * Unit tests for Home page component
 * Scenario 1: Homepage Public Access and Value Proposition Display
 *
 * Tests:
 * - Hero section headline contains value proposition
 * - Sub-headline with service benefits description
 * - Features section is rendered
 */

import { describe, it, expect } from 'vitest'
import { screen } from '@testing-library/react'
import { renderWithProviders } from '../../test-utils'
import Home from '@/pages/Home'

describe('Home Page - Value Proposition Display (Scenario 1)', () => {
  it('renders hero section headline with value proposition text', () => {
    renderWithProviders(<Home />)

    // Check that hero section exists
    const heroSection = screen.getByTestId('hero-section')
    expect(heroSection).toBeInTheDocument()

    // Check headline contains value proposition about URL shortening
    const headline = screen.getByRole('heading', { level: 1 })
    expect(headline).toBeInTheDocument()
    expect(headline.textContent).toMatch(/shorten\s*url|short.*link|track\s*insight/i)
  })

  it('renders sub-headline with service benefits description', () => {
    renderWithProviders(<Home />)

    // Check that sub-headline paragraph exists with descriptive text
    const heroSection = screen.getByTestId('hero-section')
    const subHeadline = heroSection.querySelector('p')
    expect(subHeadline).toBeInTheDocument()

    // Sub-headline should describe service benefits (analytics, tracking, etc.)
    const subHeadlineText = subHeadline?.textContent || ''
    expect(subHeadlineText).toMatch(/transform|track|analyz|performance|click|reach/i)
  })

  it('renders homepage without requiring authentication', () => {
    // Render homepage without any auth token
    renderWithProviders(<Home />)

    // Homepage should render successfully
    const homepage = screen.getByTestId('homepage')
    expect(homepage).toBeInTheDocument()

    // Should show unauthenticated user headline (not personalized)
    const headline = screen.getByRole('heading', { level: 1 })
    expect(headline.textContent).not.toMatch(/welcome back/i)
  })

  it('displays call-to-action buttons for registration and learning more', () => {
    renderWithProviders(<Home />)

    // Check for registration CTA
    const getStartedLink = screen.getByRole('link', { name: /get started/i })
    expect(getStartedLink).toBeInTheDocument()
    expect(getStartedLink).toHaveAttribute('href', '/register')

    // Check for Learn More CTA
    const learnMoreLink = screen.getByRole('link', { name: /learn more/i })
    expect(learnMoreLink).toBeInTheDocument()
  })
})

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
