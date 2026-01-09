import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import FeaturesSection, { features } from '../src/components/FeaturesSection'

describe('FeaturesSection', () => {
  // Test Case 1: Component renders with at least 3 feature cards visible
  it('renders with at least 3 feature cards visible', () => {
    render(<FeaturesSection />)

    const featuresGrid = screen.getByTestId('features-grid')
    expect(featuresGrid).toBeInTheDocument()

    // Check that we have at least 3 feature cards
    const featureCards = screen.getAllByTestId(/^feature-card-/)
    expect(featureCards.length).toBeGreaterThanOrEqual(3)
  })

  // Test Case 2: Each feature card contains an icon element, title text, and description text
  it('each feature card contains an icon element, title text, and description text', () => {
    render(<FeaturesSection />)

    features.forEach((feature) => {
      // Check icon exists
      const icon = screen.getByTestId(`feature-icon-${feature.id}`)
      expect(icon).toBeInTheDocument()
      expect(icon.querySelector('svg')).toBeInTheDocument()

      // Check title exists and has correct text
      const title = screen.getByTestId(`feature-title-${feature.id}`)
      expect(title).toBeInTheDocument()
      expect(title).toHaveTextContent(feature.title)

      // Check description exists and has correct text
      const description = screen.getByTestId(`feature-description-${feature.id}`)
      expect(description).toBeInTheDocument()
      expect(description).toHaveTextContent(feature.description)
    })
  })

  // Test Case 3: Verify 'Quick URL Shortening' feature card
  it("displays 'Quick URL Shortening' feature card with appropriate icon and description", () => {
    render(<FeaturesSection />)

    const card = screen.getByTestId('feature-card-url-shortening')
    expect(card).toBeInTheDocument()

    const title = screen.getByTestId('feature-title-url-shortening')
    expect(title).toHaveTextContent('Quick URL Shortening')

    const description = screen.getByTestId('feature-description-url-shortening')
    expect(description).toHaveTextContent(/short/i)
    expect(description).toHaveTextContent(/link/i)

    const icon = screen.getByTestId('feature-icon-url-shortening')
    expect(icon.querySelector('svg')).toBeInTheDocument()
  })

  // Test Case 4: Verify 'Detailed Analytics' feature card
  it("displays 'Detailed Analytics' feature card with appropriate icon and description", () => {
    render(<FeaturesSection />)

    const card = screen.getByTestId('feature-card-analytics')
    expect(card).toBeInTheDocument()

    const title = screen.getByTestId('feature-title-analytics')
    expect(title).toHaveTextContent('Detailed Analytics')

    const description = screen.getByTestId('feature-description-analytics')
    expect(description).toHaveTextContent(/track/i)
    expect(description).toHaveTextContent(/click/i)

    const icon = screen.getByTestId('feature-icon-analytics')
    expect(icon.querySelector('svg')).toBeInTheDocument()
  })

  // Test Case 5: Verify 'Easy Link Management' feature card
  it("displays 'Easy Link Management' feature card with appropriate icon and description", () => {
    render(<FeaturesSection />)

    const card = screen.getByTestId('feature-card-link-management')
    expect(card).toBeInTheDocument()

    const title = screen.getByTestId('feature-title-link-management')
    expect(title).toHaveTextContent('Easy Link Management')

    const description = screen.getByTestId('feature-description-link-management')
    expect(description).toHaveTextContent(/manage/i)
    expect(description).toHaveTextContent(/URL/i)

    const icon = screen.getByTestId('feature-icon-link-management')
    expect(icon.querySelector('svg')).toBeInTheDocument()
  })

  // Additional: Verify section has proper accessibility attributes
  it('has proper accessibility attributes', () => {
    render(<FeaturesSection />)

    const section = screen.getByRole('region', { name: /features/i })
    expect(section).toBeInTheDocument()

    const heading = screen.getByRole('heading', { level: 2, name: /powerful features/i })
    expect(heading).toBeInTheDocument()
  })

  // Additional: Verify all 4 features are rendered
  it('renders all 4 feature cards including Secure & Reliable', () => {
    render(<FeaturesSection />)

    const featureCards = screen.getAllByTestId(/^feature-card-/)
    expect(featureCards).toHaveLength(4)

    // Verify the 4th feature card
    const securityCard = screen.getByTestId('feature-card-security')
    expect(securityCard).toBeInTheDocument()

    const securityTitle = screen.getByTestId('feature-title-security')
    expect(securityTitle).toHaveTextContent('Secure & Reliable')
  })
})
