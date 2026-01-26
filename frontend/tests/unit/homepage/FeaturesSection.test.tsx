/**
 * FeaturesSection Unit Tests
 * Owner: Scenario 4 - Features Section Display
 *
 * Tests for the FeaturesSection component to verify:
 * - Features section renders correctly with 3 feature cards
 * - Each feature card has icon, title, and description
 * - URL shortening feature is present
 * - Analytics feature is present
 * - Dashboard management feature is present
 */

import { describe, it, expect } from 'vitest'
import { render, screen, within } from './setup'
import { FeaturesSection } from '@/components/homepage/FeaturesSection'

describe('FeaturesSection', () => {
  describe('Test Case 1: Features section contains exactly 3 feature cards', () => {
    it('renders the features section', () => {
      render(<FeaturesSection />)

      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()
    })

    it('renders exactly 3 feature cards', () => {
      render(<FeaturesSection />)

      const card0 = screen.getByTestId('feature-card-0')
      const card1 = screen.getByTestId('feature-card-1')
      const card2 = screen.getByTestId('feature-card-2')

      expect(card0).toBeInTheDocument()
      expect(card1).toBeInTheDocument()
      expect(card2).toBeInTheDocument()

      // Verify no 4th card exists
      const card3 = screen.queryByTestId('feature-card-3')
      expect(card3).not.toBeInTheDocument()
    })

    it('renders the features grid container', () => {
      render(<FeaturesSection />)

      const grid = screen.getByTestId('features-grid')
      expect(grid).toBeInTheDocument()
    })

    it('has section heading', () => {
      render(<FeaturesSection />)

      const heading = screen.getByTestId('features-heading')
      expect(heading).toBeInTheDocument()
      expect(heading).toHaveTextContent('Everything You Need')
    })
  })

  describe('Test Case 2: URL shortening feature card', () => {
    it('displays URL shortening feature with correct title', () => {
      render(<FeaturesSection />)

      const title0 = screen.getByTestId('feature-title-0')
      expect(title0).toHaveTextContent(/url/i)
      expect(title0).toHaveTextContent(/shortening/i)
    })

    it('URL shortening feature has a description', () => {
      render(<FeaturesSection />)

      const description0 = screen.getByTestId('feature-description-0')
      expect(description0).toBeInTheDocument()
      expect(description0.textContent).not.toBe('')
      expect(description0.textContent?.length).toBeGreaterThan(20)
    })

    it('URL shortening feature has an icon', () => {
      render(<FeaturesSection />)

      const icon0 = screen.getByTestId('feature-icon-0')
      expect(icon0).toBeInTheDocument()
      // Icon should have SVG child
      expect(icon0.querySelector('svg')).toBeInTheDocument()
    })
  })

  describe('Test Case 3: Analytics feature card', () => {
    it('displays analytics feature with correct title', () => {
      render(<FeaturesSection />)

      const title1 = screen.getByTestId('feature-title-1')
      expect(title1).toHaveTextContent(/analytics/i)
    })

    it('analytics feature has a description mentioning tracking', () => {
      render(<FeaturesSection />)

      const description1 = screen.getByTestId('feature-description-1')
      expect(description1).toBeInTheDocument()
      expect(description1.textContent).not.toBe('')
      // Description should mention click tracking or analytics-related content
      expect(description1.textContent?.toLowerCase()).toMatch(/track|click|monitor|analytic/i)
    })

    it('analytics feature has an icon', () => {
      render(<FeaturesSection />)

      const icon1 = screen.getByTestId('feature-icon-1')
      expect(icon1).toBeInTheDocument()
      expect(icon1.querySelector('svg')).toBeInTheDocument()
    })
  })

  describe('Test Case 4: Dashboard management feature card', () => {
    it('displays dashboard feature with correct title', () => {
      render(<FeaturesSection />)

      const title2 = screen.getByTestId('feature-title-2')
      expect(title2).toHaveTextContent(/dashboard/i)
      expect(title2).toHaveTextContent(/management/i)
    })

    it('dashboard feature has a description', () => {
      render(<FeaturesSection />)

      const description2 = screen.getByTestId('feature-description-2')
      expect(description2).toBeInTheDocument()
      expect(description2.textContent).not.toBe('')
      expect(description2.textContent?.length).toBeGreaterThan(20)
    })

    it('dashboard feature has an icon', () => {
      render(<FeaturesSection />)

      const icon2 = screen.getByTestId('feature-icon-2')
      expect(icon2).toBeInTheDocument()
      expect(icon2.querySelector('svg')).toBeInTheDocument()
    })
  })

  describe('Accessibility', () => {
    it('has proper section with aria-labelledby', () => {
      render(<FeaturesSection />)

      const section = screen.getByTestId('features-section')
      expect(section).toHaveAttribute('aria-labelledby', 'features-heading')
    })

    it('has proper heading hierarchy with h2', () => {
      render(<FeaturesSection />)

      const heading = screen.getByRole('heading', { level: 2 })
      expect(heading).toBeInTheDocument()
      expect(heading).toHaveAttribute('id', 'features-heading')
    })

    it('feature cards have proper heading hierarchy with h3', () => {
      render(<FeaturesSection />)

      const cardHeadings = screen.getAllByRole('heading', { level: 3 })
      expect(cardHeadings).toHaveLength(3)
    })

    it('section has id for anchor navigation', () => {
      render(<FeaturesSection />)

      const section = screen.getByTestId('features-section')
      expect(section).toHaveAttribute('id', 'features')
    })
  })

  describe('Feature card structure', () => {
    it('each feature card has all required elements', () => {
      render(<FeaturesSection />)

      for (let i = 0; i < 3; i++) {
        const card = screen.getByTestId(`feature-card-${i}`)
        const icon = screen.getByTestId(`feature-icon-${i}`)
        const title = screen.getByTestId(`feature-title-${i}`)
        const description = screen.getByTestId(`feature-description-${i}`)

        expect(card).toBeInTheDocument()
        expect(icon).toBeInTheDocument()
        expect(title).toBeInTheDocument()
        expect(description).toBeInTheDocument()
      }
    })

    it('feature titles are rendered as h3 elements', () => {
      render(<FeaturesSection />)

      const title0 = screen.getByTestId('feature-title-0')
      const title1 = screen.getByTestId('feature-title-1')
      const title2 = screen.getByTestId('feature-title-2')

      expect(title0.tagName).toBe('H3')
      expect(title1.tagName).toBe('H3')
      expect(title2.tagName).toBe('H3')
    })
  })
})
