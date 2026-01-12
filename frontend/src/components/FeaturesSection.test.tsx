import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import FeaturesSection from './FeaturesSection'

const renderWithRouter = (component: React.ReactElement) => {
  return render(
    <MemoryRouter initialEntries={['/']}>
      {component}
    </MemoryRouter>
  )
}

describe('FeaturesSection', () => {
  // Test Case 1: Features section exists with section heading
  it('renders features section with section heading', () => {
    renderWithRouter(<FeaturesSection />)

    const featuresSection = screen.getByTestId('features-section')
    expect(featuresSection).toBeInTheDocument()

    // Check for section heading
    const heading = screen.getByRole('heading', { level: 2 })
    expect(heading).toBeInTheDocument()
    expect(heading.textContent).toMatch(/features/i)
  })

  // Test Case 2: URL Shortening feature card exists with icon and description
  it('renders URL Shortening feature card with icon and description', () => {
    renderWithRouter(<FeaturesSection />)

    // Find the URL Shortening card title
    const urlShorteningTitle = screen.getByText('URL Shortening')
    expect(urlShorteningTitle).toBeInTheDocument()

    // Get the parent card element
    const card = urlShorteningTitle.closest('.card')
    expect(card).toBeInTheDocument()

    // Check for icon (SVG) in the card
    const icon = card?.querySelector('svg')
    expect(icon).toBeInTheDocument()

    // Check for description about creating short links (get the p element specifically)
    const description = card?.querySelector('p')
    expect(description).toBeInTheDocument()
    expect(description?.textContent).toMatch(/short.*link|memorable.*link|create/i)
  })

  // Test Case 3: Click Analytics feature card exists with icon and description
  it('renders Click Analytics feature card with icon and description', () => {
    renderWithRouter(<FeaturesSection />)

    // Find the Click Analytics card title
    const clickAnalyticsTitle = screen.getByText('Click Analytics')
    expect(clickAnalyticsTitle).toBeInTheDocument()

    // Get the parent card element
    const card = clickAnalyticsTitle.closest('.card')
    expect(card).toBeInTheDocument()

    // Check for icon (SVG) in the card
    const icon = card?.querySelector('svg')
    expect(icon).toBeInTheDocument()

    // Check for description about tracking clicks (get the p element specifically)
    const description = card?.querySelector('p')
    expect(description).toBeInTheDocument()
    expect(description?.textContent).toMatch(/track.*click|click.*insight/i)
  })

  // Test Case 4: GeoIP Tracking feature card exists with icon and description
  it('renders GeoIP Tracking feature card with icon and description', () => {
    renderWithRouter(<FeaturesSection />)

    // Find the GeoIP Tracking card title
    const geoIPTitle = screen.getByText('GeoIP Tracking')
    expect(geoIPTitle).toBeInTheDocument()

    // Get the parent card element
    const card = geoIPTitle.closest('.card')
    expect(card).toBeInTheDocument()

    // Check for icon (SVG) in the card
    const icon = card?.querySelector('svg')
    expect(icon).toBeInTheDocument()

    // Check for description about location tracking (get the p element specifically)
    const description = card?.querySelector('p')
    expect(description).toBeInTheDocument()
    expect(description?.textContent).toMatch(/location|geographic|where.*audience/i)
  })

  // Test Case 5: Link Management feature card exists with icon and description
  it('renders Link Management feature card with icon and description', () => {
    renderWithRouter(<FeaturesSection />)

    // Find the Link Management card title
    const linkManagementTitle = screen.getByText('Link Management')
    expect(linkManagementTitle).toBeInTheDocument()

    // Get the parent card element
    const card = linkManagementTitle.closest('.card')
    expect(card).toBeInTheDocument()

    // Check for icon (SVG) in the card
    const icon = card?.querySelector('svg')
    expect(icon).toBeInTheDocument()

    // Check for description about organizing links (get the p element specifically)
    const description = card?.querySelector('p')
    expect(description).toBeInTheDocument()
    expect(description?.textContent).toMatch(/organize|manage.*link|dashboard/i)
  })

  // Test Case 6: At least 3 feature cards are displayed
  it('displays at least 3 feature cards', () => {
    renderWithRouter(<FeaturesSection />)

    const featuresSection = screen.getByTestId('features-section')

    // Find all card elements within the features section
    const cards = featuresSection.querySelectorAll('.card')

    // Verify at least 3 cards exist
    expect(cards.length).toBeGreaterThanOrEqual(3)

    // Also verify we have exactly 4 cards (as per implementation)
    expect(cards.length).toBe(4)
  })

  // Additional test: Each feature card has an icon
  it('renders all feature cards with icons', () => {
    renderWithRouter(<FeaturesSection />)

    const featuresSection = screen.getByTestId('features-section')
    const cards = featuresSection.querySelectorAll('.card')

    // Each card should have an SVG icon
    cards.forEach((card) => {
      const icon = card.querySelector('svg')
      expect(icon).toBeInTheDocument()
    })
  })
})
