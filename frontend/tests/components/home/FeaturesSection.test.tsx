import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { FeaturesSection } from '../../../src/components/home/FeaturesSection'
import { FeatureCard } from '../../../src/components/shared/FeatureCard'

describe('FeaturesSection', () => {
  // Test Case 1: Component renders without errors
  it('renders without errors', () => {
    expect(() => render(<FeaturesSection />)).not.toThrow()
  })

  // Test Case 2: At least 3 feature cards are displayed
  it('displays at least 3 feature cards', () => {
    render(<FeaturesSection />)
    const featureCards = screen.getAllByRole('article')
    expect(featureCards.length).toBeGreaterThanOrEqual(3)
  })

  // Test Case 3: URL Shortening feature card is present with icon, title, description
  it('displays URL Shortening feature card with icon, title, and description', () => {
    render(<FeaturesSection />)
    expect(screen.getByText('URL Shortening')).toBeInTheDocument()
    expect(screen.getByText(/Create short, memorable links instantly/i)).toBeInTheDocument()
    // Icon should be present (checking for svg element within the card)
    const urlShorteningCard = screen.getByText('URL Shortening').closest('article')
    expect(urlShorteningCard?.querySelector('svg')).toBeInTheDocument()
  })

  // Test Case 4: Click Analytics feature card is present with icon, title, description
  it('displays Click Analytics feature card with icon, title, and description', () => {
    render(<FeaturesSection />)
    expect(screen.getByText('Click Analytics')).toBeInTheDocument()
    expect(screen.getByText(/Track every click with detailed insights/i)).toBeInTheDocument()
    const analyticsCard = screen.getByText('Click Analytics').closest('article')
    expect(analyticsCard?.querySelector('svg')).toBeInTheDocument()
  })

  // Test Case 5: Referrer Tracking feature card is present with icon, title, description
  it('displays Referrer Tracking feature card with icon, title, and description', () => {
    render(<FeaturesSection />)
    expect(screen.getByText('Referrer Tracking')).toBeInTheDocument()
    expect(screen.getByText(/Know where your traffic comes from/i)).toBeInTheDocument()
    const referrerCard = screen.getByText('Referrer Tracking').closest('article')
    expect(referrerCard?.querySelector('svg')).toBeInTheDocument()
  })

  // Test Case 6: GeoIP Location feature card is present (optional)
  it('displays GeoIP Location feature card (optional feature)', () => {
    render(<FeaturesSection />)
    expect(screen.getByText('GeoIP Location')).toBeInTheDocument()
    expect(screen.getByText(/See geographic distribution of clicks/i)).toBeInTheDocument()
    const geoipCard = screen.getByText('GeoIP Location').closest('article')
    expect(geoipCard?.querySelector('svg')).toBeInTheDocument()
  })

  // Test Case 9: Cards use GlassMorphismCard styling consistently
  it('uses GlassMorphismCard styling for all feature cards', () => {
    render(<FeaturesSection />)
    const featureCards = screen.getAllByRole('article')
    featureCards.forEach((card) => {
      // GlassMorphismCard should have backdrop-blur-md class
      expect(card.className).toMatch(/backdrop-blur/)
    })
  })
})

describe('FeatureCard', () => {
  const mockIcon = (
    <svg data-testid="mock-icon" width="24" height="24">
      <circle cx="12" cy="12" r="10" />
    </svg>
  )

  // Test Case 7: Card displays all provided props correctly
  it('displays all provided props correctly', () => {
    render(
      <FeatureCard
        icon={mockIcon}
        title="Test Feature"
        description="This is a test description for the feature card."
      />
    )

    expect(screen.getByText('Test Feature')).toBeInTheDocument()
    expect(screen.getByText('This is a test description for the feature card.')).toBeInTheDocument()
    expect(screen.getByTestId('mock-icon')).toBeInTheDocument()
  })

  // Test Case 8: Visual hover state feedback is displayed
  it('has hover state styling for visual feedback', async () => {
    const user = userEvent.setup()
    const { container } = render(
      <FeatureCard
        icon={mockIcon}
        title="Test Feature"
        description="Test description"
      />
    )

    const card = container.querySelector('article')
    expect(card).toBeInTheDocument()

    // Check that the card has transition classes for hover effects
    expect(card?.className).toMatch(/transition/)
    expect(card?.className).toMatch(/hover:/)

    // Hover over the card
    await user.hover(card!)

    // After hovering, the card should still have the hover classes available
    expect(card?.className).toMatch(/hover:/)
  })

  it('renders within GlassMorphismCard styling', () => {
    const { container } = render(
      <FeatureCard
        icon={mockIcon}
        title="Test Feature"
        description="Test description"
      />
    )

    const card = container.querySelector('article')
    // Should have GlassMorphismCard styling (backdrop-blur)
    expect(card?.className).toMatch(/backdrop-blur/)
  })
})
