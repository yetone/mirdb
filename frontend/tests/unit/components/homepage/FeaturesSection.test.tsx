/**
 * FeaturesSection Component Tests
 * Owner: Scenario 3 - Features Section Display
 *
 * Tests for:
 * - URL Shortening feature card display
 * - Analytics Dashboard feature card display
 * - Click Tracking feature card display
 * - Responsive layout (three-column desktop, single column mobile)
 * - GlassMorphismCard integration with backdrop blur
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, within } from '../../../utils/test-utils'
import { FeaturesSection } from '../../../../src/components/homepage/FeaturesSection'

describe('FeaturesSection', () => {
  describe('Feature Cards Display', () => {
    it('renders URL Shortening feature card with icon and description', () => {
      render(<FeaturesSection />)

      expect(screen.getByText('URL Shortening')).toBeInTheDocument()
      expect(
        screen.getByText(/Transform long, unwieldy URLs into clean, shareable short links/)
      ).toBeInTheDocument()
      expect(screen.getByLabelText('URL Shortening icon')).toBeInTheDocument()
    })

    it('renders Analytics Dashboard feature card with icon and description', () => {
      render(<FeaturesSection />)

      expect(screen.getByText('Analytics Dashboard')).toBeInTheDocument()
      expect(
        screen.getByText(/Gain deep insights into your link performance/)
      ).toBeInTheDocument()
      expect(screen.getByLabelText('Analytics Dashboard icon')).toBeInTheDocument()
    })

    it('renders Click Tracking feature card with icon and description', () => {
      render(<FeaturesSection />)

      expect(screen.getByText('Click Tracking')).toBeInTheDocument()
      expect(
        screen.getByText(/Monitor every click in real-time/)
      ).toBeInTheDocument()
      expect(screen.getByLabelText('Click Tracking icon')).toBeInTheDocument()
    })

    it('renders all three feature cards', () => {
      render(<FeaturesSection />)

      const featuresGrid = screen.getByTestId('features-grid')
      const cards = within(featuresGrid).getAllByRole('img')

      expect(cards).toHaveLength(3)
    })

    it('accepts custom features via props', () => {
      const customFeatures = [
        {
          icon: '🎯',
          title: 'Custom Feature',
          description: 'Custom description for testing',
        },
      ]

      render(<FeaturesSection features={customFeatures} />)

      expect(screen.getByText('Custom Feature')).toBeInTheDocument()
      expect(screen.getByText('Custom description for testing')).toBeInTheDocument()
    })
  })

  describe('Responsive Layout', () => {
    beforeEach(() => {
      vi.clearAllMocks()
    })

    it('has three-column layout class for desktop (lg:grid-cols-3)', () => {
      render(<FeaturesSection />)

      const featuresGrid = screen.getByTestId('features-grid')
      expect(featuresGrid).toHaveClass('lg:grid-cols-3')
    })

    it('has single-column layout class for mobile (grid-cols-1)', () => {
      render(<FeaturesSection />)

      const featuresGrid = screen.getByTestId('features-grid')
      expect(featuresGrid).toHaveClass('grid-cols-1')
    })

    it('has intermediate two-column layout for medium screens (md:grid-cols-2)', () => {
      render(<FeaturesSection />)

      const featuresGrid = screen.getByTestId('features-grid')
      expect(featuresGrid).toHaveClass('md:grid-cols-2')
    })
  })

  describe('GlassMorphismCard Integration', () => {
    it('renders feature cards with GlassMorphismCard backdrop blur effect', () => {
      render(<FeaturesSection />)

      const featuresGrid = screen.getByTestId('features-grid')
      const cards = featuresGrid.querySelectorAll('[class*="backdrop-blur"]')

      expect(cards.length).toBe(3)
    })

    it('each card has GlassMorphismCard styling with rounded corners', () => {
      render(<FeaturesSection />)

      const featuresGrid = screen.getByTestId('features-grid')
      const cards = featuresGrid.querySelectorAll('[class*="rounded-2xl"]')

      expect(cards.length).toBe(3)
    })

    it('each card has GlassMorphismCard semi-transparent background', () => {
      render(<FeaturesSection />)

      const featuresGrid = screen.getByTestId('features-grid')
      const cards = featuresGrid.querySelectorAll('[class*="bg-base-100/50"]')

      expect(cards.length).toBe(3)
    })
  })

  describe('Section Structure', () => {
    it('renders features section with proper id for navigation', () => {
      render(<FeaturesSection />)

      const section = screen.getByRole('region', { name: 'Features' })
      expect(section).toHaveAttribute('id', 'features')
    })

    it('renders section heading', () => {
      render(<FeaturesSection />)

      expect(screen.getByRole('heading', { level: 2, name: 'Powerful Features' })).toBeInTheDocument()
    })

    it('renders feature titles as h3 headings', () => {
      render(<FeaturesSection />)

      const headings = screen.getAllByRole('heading', { level: 3 })
      expect(headings).toHaveLength(3)
      expect(headings[0]).toHaveTextContent('URL Shortening')
      expect(headings[1]).toHaveTextContent('Analytics Dashboard')
      expect(headings[2]).toHaveTextContent('Click Tracking')
    })
  })

  describe('Accessibility', () => {
    it('has accessible icon labels', () => {
      render(<FeaturesSection />)

      expect(screen.getByLabelText('URL Shortening icon')).toBeInTheDocument()
      expect(screen.getByLabelText('Analytics Dashboard icon')).toBeInTheDocument()
      expect(screen.getByLabelText('Click Tracking icon')).toBeInTheDocument()
    })

    it('section has aria-label for screen readers', () => {
      render(<FeaturesSection />)

      expect(screen.getByRole('region', { name: 'Features' })).toBeInTheDocument()
    })
  })
})
