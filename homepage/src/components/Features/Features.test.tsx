/**
 * Unit tests for Features section component.
 * Owner: Scenario 3 - Features Section
 *
 * Test cases covered:
 * - TC1: Features section contains at least 3 feature cards
 * - TC2: Each feature card has icon, heading, and description text
 * - TC3: Icons have proper alt text or aria-hidden with text alternatives
 */

import { describe, it, expect } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { Features } from './Features'
import { FeatureCard } from './FeatureCard'
import type { Feature } from '../../types'

const mockFeatures: Feature[] = [
  {
    icon: 'rocket',
    title: 'Lightning Fast',
    description: 'Experience blazing fast performance.',
  },
  {
    icon: 'shield',
    title: 'Secure by Design',
    description: 'Your data is protected.',
  },
  {
    icon: 'chart',
    title: 'Scalable Solution',
    description: 'Grow without limits.',
  },
]

describe('Features', () => {
  // TC1: Features section contains at least 3 feature cards
  describe('TC1: Features section contains at least 3 feature cards', () => {
    it('renders at least 3 feature cards by default', () => {
      render(<Features />)

      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards.length).toBeGreaterThanOrEqual(3)
    })

    it('renders the correct number of feature cards when custom features are provided', () => {
      render(<Features features={mockFeatures} />)

      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards).toHaveLength(3)
    })

    it('renders the features section with proper id for navigation', () => {
      render(<Features />)

      const section = document.getElementById('features')
      expect(section).toBeInTheDocument()
    })

    it('renders a heading for the features section', () => {
      render(<Features title="Our Features" />)

      const heading = screen.getByRole('heading', { name: /our features/i })
      expect(heading).toBeInTheDocument()
    })
  })

  // TC2: Each feature card has icon, heading, and description text
  describe('TC2: Each feature card has icon, heading, and description', () => {
    it('renders feature card with icon', () => {
      render(<Features features={mockFeatures} />)

      const icons = screen.getAllByTestId('feature-icon')
      expect(icons).toHaveLength(3)
    })

    it('renders feature card with heading', () => {
      render(<Features features={mockFeatures} />)

      const titles = screen.getAllByTestId('feature-title')
      expect(titles).toHaveLength(3)
      expect(titles[0]).toHaveTextContent('Lightning Fast')
      expect(titles[1]).toHaveTextContent('Secure by Design')
      expect(titles[2]).toHaveTextContent('Scalable Solution')
    })

    it('renders feature card with description', () => {
      render(<Features features={mockFeatures} />)

      const descriptions = screen.getAllByTestId('feature-description')
      expect(descriptions).toHaveLength(3)
      expect(descriptions[0]).toHaveTextContent(
        'Experience blazing fast performance.'
      )
    })

    it('renders each card with all required elements', () => {
      render(<Features features={mockFeatures} />)

      const cards = screen.getAllByTestId('feature-card')

      cards.forEach((card) => {
        const icon = within(card).getByTestId('feature-icon')
        const title = within(card).getByTestId('feature-title')
        const description = within(card).getByTestId('feature-description')

        expect(icon).toBeInTheDocument()
        expect(title).toBeInTheDocument()
        expect(description).toBeInTheDocument()
      })
    })
  })

  // TC3: Icons have proper alt text or aria-hidden with text alternatives
  describe('TC3: Icons have proper accessibility attributes', () => {
    it('icons are marked aria-hidden for decorative purposes', () => {
      render(<Features features={mockFeatures} />)

      const iconWrappers = screen.getAllByTestId('feature-icon')

      iconWrappers.forEach((wrapper) => {
        expect(wrapper).toHaveAttribute('aria-hidden', 'true')
      })
    })

    it('icons have role="img" with aria-label as text alternative', () => {
      render(<Features features={mockFeatures} />)

      const icons = screen.getAllByRole('img', { hidden: true })
      expect(icons.length).toBeGreaterThanOrEqual(3)

      icons.forEach((icon) => {
        expect(icon).toHaveAttribute('aria-label')
        const label = icon.getAttribute('aria-label')
        expect(label).toBeTruthy()
        expect(label).toContain('icon')
      })
    })

    it('feature titles serve as text alternatives for icons', () => {
      render(<Features features={mockFeatures} />)

      const cards = screen.getAllByTestId('feature-card')

      cards.forEach((card) => {
        const title = within(card).getByTestId('feature-title')
        expect(title.tagName).toBe('H3')
        expect(title.textContent).toBeTruthy()
      })
    })

    it('features section has proper aria-labelledby', () => {
      render(<Features />)

      const section = screen.getByRole('region')
      expect(section).toHaveAttribute('aria-labelledby', 'features-heading')

      const heading = document.getElementById('features-heading')
      expect(heading).toBeInTheDocument()
    })
  })
})

describe('FeatureCard', () => {
  it('renders with provided props', () => {
    render(
      <FeatureCard
        icon="rocket"
        title="Test Feature"
        description="Test Description"
      />
    )

    expect(screen.getByTestId('feature-card')).toBeInTheDocument()
    expect(screen.getByTestId('feature-title')).toHaveTextContent('Test Feature')
    expect(screen.getByTestId('feature-description')).toHaveTextContent(
      'Test Description'
    )
  })

  it('uses fallback icon for unknown icon names', () => {
    render(
      <FeatureCard
        icon="unknown-icon"
        title="Test Feature"
        description="Test Description"
      />
    )

    const icon = screen.getByRole('img', { hidden: true })
    expect(icon).toHaveTextContent('⭐')
  })

  it('renders correct emoji for known icons', () => {
    const { rerender } = render(
      <FeatureCard
        icon="rocket"
        title="Rocket Feature"
        description="Description"
      />
    )

    let icon = screen.getByRole('img', { hidden: true })
    expect(icon).toHaveTextContent('🚀')

    rerender(
      <FeatureCard
        icon="shield"
        title="Shield Feature"
        description="Description"
      />
    )

    icon = screen.getByRole('img', { hidden: true })
    expect(icon).toHaveTextContent('🛡️')
  })
})
