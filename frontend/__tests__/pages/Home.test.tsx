/**
 * Homepage Component Tests
 * Owner: All scenarios contribute test cases
 *
 * Test coverage for:
 * - Hero section rendering (Scenario 1)
 * - Features section rendering (Scenario 2)
 * - CTA navigation (Scenario 3)
 * - Responsive layouts (Scenarios 4-5)
 * - Theme support (Scenarios 6-8)
 * - Footer rendering (Scenario 9)
 * - Accessibility (Scenarios 10-11)
 * - Public access (Scenario 12)
 * - Component integration (Scenario 13)
 * - Animations (Scenario 14)
 * - Route integration (Scenario 15)
 */
import { describe, it, expect } from 'vitest'
import { screen } from '@testing-library/react'
import { renderWithProviders } from '../utils/renderWithProviders'
import Home from '../../src/pages/Home'

describe('Home Page - Hero Section Display (Scenario 1)', () => {
  it('should render h1 heading with product value proposition', () => {
    renderWithProviders(<Home />)

    const heading = screen.getByRole('heading', { level: 1 })
    expect(heading).toBeInTheDocument()
    expect(heading.textContent).toContain('Shorten URLs')
    expect(heading.textContent).toContain('Track Clicks')
    expect(heading.textContent).toContain('Grow Your Reach')
  })

  it('should render primary CTA button with Get Started text', () => {
    renderWithProviders(<Home />)

    const getStartedButton = screen.getByRole('button', { name: /get started/i })
    expect(getStartedButton).toBeInTheDocument()
    expect(getStartedButton).toBeVisible()
  })

  it('should render secondary CTA button with Login text', () => {
    renderWithProviders(<Home />)

    const loginButton = screen.getByRole('button', { name: /login/i })
    expect(loginButton).toBeInTheDocument()
    expect(loginButton).toBeVisible()
  })

  it('should render BackgroundEffect component in the DOM', () => {
    renderWithProviders(<Home />)

    const backgroundEffect = screen.getByTestId('background-effect')
    expect(backgroundEffect).toBeInTheDocument()
  })

  it('should render subheadline text below the main headline', () => {
    renderWithProviders(<Home />)

    const subheadline = screen.getByTestId('subheadline')
    expect(subheadline).toBeInTheDocument()
    expect(subheadline).toBeVisible()
    expect(subheadline.textContent).toContain('Create short, memorable links')
    expect(subheadline.textContent).toContain('Track performance')
    expect(subheadline.textContent).toContain('dashboard')
  })

  it('should have Get Started button linked to /register', () => {
    renderWithProviders(<Home />)

    const getStartedLink = screen.getByRole('link', { name: /get started/i })
    expect(getStartedLink).toHaveAttribute('href', '/register')
  })

  it('should have Login button linked to /login', () => {
    renderWithProviders(<Home />)

    const loginLink = screen.getByRole('link', { name: /login/i })
    expect(loginLink).toHaveAttribute('href', '/login')
  })

  it('should display product name', () => {
    renderWithProviders(<Home />)

    expect(screen.getByText('URL Shortener')).toBeInTheDocument()
  })
})

describe('Home Page - Features Section Display (Scenario 2)', () => {
  it('should render features section with at least 3 GlassMorphismCard components', () => {
    renderWithProviders(<Home />)

    const featuresSection = screen.getByTestId('features-section')
    expect(featuresSection).toBeInTheDocument()

    // Verify at least 3 feature cards exist
    const urlShorteningCard = screen.getByTestId('feature-card-url-shortening')
    const clickAnalyticsCard = screen.getByTestId('feature-card-click-analytics')
    const dashboardCard = screen.getByTestId('feature-card-dashboard')

    expect(urlShorteningCard).toBeInTheDocument()
    expect(clickAnalyticsCard).toBeInTheDocument()
    expect(dashboardCard).toBeInTheDocument()
  })

  it('should render URL Shortening feature card with appropriate title and description', () => {
    renderWithProviders(<Home />)

    const urlShorteningCard = screen.getByTestId('feature-card-url-shortening')
    expect(urlShorteningCard).toBeInTheDocument()

    expect(screen.getByText('URL Shortening')).toBeInTheDocument()
    expect(screen.getByText('Create short, memorable links in seconds')).toBeInTheDocument()
  })

  it('should render Click Analytics feature card with appropriate title and description', () => {
    renderWithProviders(<Home />)

    const clickAnalyticsCard = screen.getByTestId('feature-card-click-analytics')
    expect(clickAnalyticsCard).toBeInTheDocument()

    expect(screen.getByText('Click Analytics')).toBeInTheDocument()
    expect(screen.getByText('Track performance with detailed analytics')).toBeInTheDocument()
  })

  it('should render Dashboard feature card with appropriate title and description', () => {
    renderWithProviders(<Home />)

    const dashboardCard = screen.getByTestId('feature-card-dashboard')
    expect(dashboardCard).toBeInTheDocument()

    expect(screen.getByText('Dashboard')).toBeInTheDocument()
    expect(screen.getByText('Manage all your URLs in one place')).toBeInTheDocument()
  })

  it('should render each feature card with an icon element', () => {
    renderWithProviders(<Home />)

    // Check that each feature card has an icon
    const urlShorteningIcon = screen.getByTestId('feature-icon-url-shortening')
    const clickAnalyticsIcon = screen.getByTestId('feature-icon-click-analytics')
    const dashboardIcon = screen.getByTestId('feature-icon-dashboard')
    const shareStatsIcon = screen.getByTestId('feature-icon-share-stats')

    expect(urlShorteningIcon).toBeInTheDocument()
    expect(clickAnalyticsIcon).toBeInTheDocument()
    expect(dashboardIcon).toBeInTheDocument()
    expect(shareStatsIcon).toBeInTheDocument()

    // Each icon container should have an SVG element inside
    expect(urlShorteningIcon.querySelector('svg')).toBeInTheDocument()
    expect(clickAnalyticsIcon.querySelector('svg')).toBeInTheDocument()
    expect(dashboardIcon.querySelector('svg')).toBeInTheDocument()
    expect(shareStatsIcon.querySelector('svg')).toBeInTheDocument()
  })

  it('should render Share Stats feature card (4th feature)', () => {
    renderWithProviders(<Home />)

    const shareStatsCard = screen.getByTestId('feature-card-share-stats')
    expect(shareStatsCard).toBeInTheDocument()

    expect(screen.getByText('Share Stats')).toBeInTheDocument()
    expect(screen.getByText('Share public analytics with stakeholders')).toBeInTheDocument()
  })

  it('should render features section heading', () => {
    renderWithProviders(<Home />)

    const heading = screen.getByRole('heading', { name: /powerful features/i })
    expect(heading).toBeInTheDocument()
  })

  it('should have proper accessibility attributes on features section', () => {
    renderWithProviders(<Home />)

    const featuresSection = screen.getByTestId('features-section')
    expect(featuresSection).toHaveAttribute('aria-labelledby', 'features-heading')

    const heading = screen.getByRole('heading', { name: /powerful features/i })
    expect(heading).toHaveAttribute('id', 'features-heading')
  })
})
