import { describe, it, expect } from 'vitest'
import { render, screen } from './test-utils'
import { FeaturesSection } from '../../components/homepage/FeaturesSection'

describe('FeaturesSection', () => {
  // Test case 1: Component renders with section heading and 4 feature cards
  it('renders with section heading and 4 feature cards', () => {
    render(<FeaturesSection />)

    // Check for section heading
    expect(screen.getByRole('heading', { level: 2 })).toBeInTheDocument()

    // Check for 4 feature cards
    const cards = screen.getAllByTestId('glassmorphism-card')
    expect(cards).toHaveLength(4)
  })

  // Test case 2: Check for URL Shortening feature
  it('displays URL Shortening feature card', () => {
    render(<FeaturesSection />)

    expect(screen.getByText('URL Shortening')).toBeInTheDocument()
    expect(screen.getByText('Create short, memorable links')).toBeInTheDocument()
  })

  // Test case 3: Check for Click Analytics feature
  it('displays Click Analytics feature card', () => {
    render(<FeaturesSection />)

    expect(screen.getByText('Click Analytics')).toBeInTheDocument()
    expect(screen.getByText('Track engagement in real-time')).toBeInTheDocument()
  })

  // Test case 4: Check for Geographic Insights feature
  it('displays Geographic Insights feature card', () => {
    render(<FeaturesSection />)

    expect(screen.getByText('Geographic Insights')).toBeInTheDocument()
    expect(screen.getByText('See where your audience is located')).toBeInTheDocument()
  })

  // Test case 5: Check for Share Stats feature
  it('displays Share Stats feature card', () => {
    render(<FeaturesSection />)

    expect(screen.getByText('Share Stats')).toBeInTheDocument()
    expect(screen.getByText('Generate public links to share analytics')).toBeInTheDocument()
  })

  // Test case 6: Verify GlassMorphismCard usage
  it('uses GlassMorphismCard component for all feature cards', () => {
    render(<FeaturesSection />)

    const cards = screen.getAllByTestId('glassmorphism-card')
    expect(cards).toHaveLength(4)

    // Verify each card has the glassmorphism styling
    cards.forEach((card) => {
      expect(card).toHaveClass('backdrop-blur-md')
    })
  })

  // Test case 7: Verify icons are present
  it('displays an icon for each feature card', () => {
    render(<FeaturesSection />)

    // Check for each feature icon by test id
    expect(screen.getByTestId('icon-url-shortening')).toBeInTheDocument()
    expect(screen.getByTestId('icon-click-analytics')).toBeInTheDocument()
    expect(screen.getByTestId('icon-geographic-insights')).toBeInTheDocument()
    expect(screen.getByTestId('icon-share-stats')).toBeInTheDocument()
  })

  // Test case 8: Check section heading
  it('displays "Key Features" section heading', () => {
    render(<FeaturesSection />)

    const heading = screen.getByRole('heading', { level: 2 })
    expect(heading).toHaveTextContent(/features/i)
  })
})
