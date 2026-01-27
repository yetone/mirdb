/**
 * FeaturesSection Unit Tests
 * Owner: Scenario 2 - Features Section Display
 *
 * Tests:
 * 1. Features section is present in the DOM
 * 2. At least 3 feature cards are rendered
 * 3. URL Shortening feature card is present with description
 * 4. Analytics feature card is present with description
 * 5. Dashboard feature card is present with description
 * 6. Each feature card has an icon
 */

import { describe, it, expect } from 'vitest'
import { screen, within } from '@testing-library/react'
import { renderWithProviders } from './setup.tsx'
import FeaturesSection from '../../../src/components/landing/FeaturesSection'
import Home from '../../../src/pages/Home'

describe('FeaturesSection', () => {
  // Test Case 1: Features section is present in the DOM
  describe('Test Case 1: Features section presence', () => {
    it('renders the features section in the DOM', () => {
      renderWithProviders(<Home />)

      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()
    })

    it('features section has proper aria label', () => {
      renderWithProviders(<Home />)

      const section = screen.getByRole('region', { name: /features/i })
      expect(section).toBeInTheDocument()
    })
  })

  // Test Case 2: At least 3 feature cards are rendered
  describe('Test Case 2: Feature cards count', () => {
    it('renders at least 3 feature cards', () => {
      renderWithProviders(<FeaturesSection />)

      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards.length).toBeGreaterThanOrEqual(3)
    })

    it('renders exactly 4 default feature cards', () => {
      renderWithProviders(<FeaturesSection />)

      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards).toHaveLength(4)
    })
  })

  // Test Case 3: URL Shortening feature card
  describe('Test Case 3: URL Shortening feature', () => {
    it('displays URL Shortening feature card with title', () => {
      renderWithProviders(<FeaturesSection />)

      const title = screen.getByText('URL Shortening')
      expect(title).toBeInTheDocument()
    })

    it('URL Shortening card has a description about creating short links', () => {
      renderWithProviders(<FeaturesSection />)

      const description = screen.getByText(/create short, memorable links/i)
      expect(description).toBeInTheDocument()
    })
  })

  // Test Case 4: Analytics feature card
  describe('Test Case 4: Analytics feature', () => {
    it('displays Click Analytics feature card with title', () => {
      renderWithProviders(<FeaturesSection />)

      const title = screen.getByText('Click Analytics')
      expect(title).toBeInTheDocument()
    })

    it('Analytics card has a description about tracking performance', () => {
      renderWithProviders(<FeaturesSection />)

      const description = screen.getByText(/track performance with detailed insights/i)
      expect(description).toBeInTheDocument()
    })
  })

  // Test Case 5: Dashboard feature card
  describe('Test Case 5: Dashboard feature', () => {
    it('displays Dashboard Management feature card with title', () => {
      renderWithProviders(<FeaturesSection />)

      const title = screen.getByText('Dashboard Management')
      expect(title).toBeInTheDocument()
    })

    it('Dashboard card has a description about managing links', () => {
      renderWithProviders(<FeaturesSection />)

      const description = screen.getByText(/organize and manage all your links/i)
      expect(description).toBeInTheDocument()
    })
  })

  // Test Case 6: Each feature card has an icon
  describe('Test Case 6: Feature icons', () => {
    it('each feature card contains an icon element', () => {
      renderWithProviders(<FeaturesSection />)

      const featureCards = screen.getAllByTestId('feature-card')

      featureCards.forEach((card) => {
        const iconContainer = within(card).getByTestId('feature-icon')
        expect(iconContainer).toBeInTheDocument()

        // Check that icon container has an SVG child (the Heroicon)
        const svg = iconContainer.querySelector('svg')
        expect(svg).toBeInTheDocument()
      })
    })

    it('all icons have aria-hidden attribute for accessibility', () => {
      renderWithProviders(<FeaturesSection />)

      const featureCards = screen.getAllByTestId('feature-card')

      featureCards.forEach((card) => {
        const iconContainer = within(card).getByTestId('feature-icon')
        const svg = iconContainer.querySelector('svg')
        expect(svg).toHaveAttribute('aria-hidden', 'true')
      })
    })
  })

  // Additional tests for custom features
  describe('Custom features prop', () => {
    it('renders custom features when provided', () => {
      const customFeatures = [
        {
          icon: <span data-testid="custom-icon">Icon</span>,
          title: 'Custom Feature',
          description: 'Custom description',
        },
      ]

      renderWithProviders(<FeaturesSection features={customFeatures} />)

      expect(screen.getByText('Custom Feature')).toBeInTheDocument()
      expect(screen.getByText('Custom description')).toBeInTheDocument()
    })
  })

  // Section heading test
  describe('Section heading', () => {
    it('displays Features heading', () => {
      renderWithProviders(<FeaturesSection />)

      const heading = screen.getByRole('heading', { name: /features/i, level: 2 })
      expect(heading).toBeInTheDocument()
    })
  })
})
