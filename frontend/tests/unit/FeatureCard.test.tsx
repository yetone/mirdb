/**
 * Unit tests for FeatureCard component.
 * Owner: Scenario 4 - Features Section Display
 *
 * Test coverage:
 * - Renders icon, title, and description
 * - Has proper ARIA attributes
 * - Hover styles are applied
 * - Uses consistent styling with design system
 */

import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import FeatureCard from '../../src/components/home/FeatureCard'

const MockIcon = () => (
  <svg data-testid="mock-icon" aria-hidden="true">
    <circle cx="12" cy="12" r="10" />
  </svg>
)

const defaultProps = {
  icon: <MockIcon />,
  title: 'Test Feature',
  description: 'This is a test description for the feature card.',
}

describe('FeatureCard', () => {
  describe('Rendering', () => {
    it('should render the title', () => {
      render(<FeatureCard {...defaultProps} />)

      expect(screen.getByText('Test Feature')).toBeInTheDocument()
    })

    it('should render the description', () => {
      render(<FeatureCard {...defaultProps} />)

      expect(screen.getByText('This is a test description for the feature card.')).toBeInTheDocument()
    })

    it('should render the icon', () => {
      render(<FeatureCard {...defaultProps} />)

      expect(screen.getByTestId('mock-icon')).toBeInTheDocument()
    })

    it('should render as an article element', () => {
      render(<FeatureCard {...defaultProps} />)

      expect(screen.getByRole('article')).toBeInTheDocument()
    })
  })

  describe('Accessibility', () => {
    it('should have an aria-label with the feature title', () => {
      render(<FeatureCard {...defaultProps} />)

      const article = screen.getByRole('article')
      expect(article).toHaveAttribute('aria-label', 'Feature: Test Feature')
    })

    it('should render title as h3 heading', () => {
      render(<FeatureCard {...defaultProps} />)

      const heading = screen.getByRole('heading', { level: 3 })
      expect(heading).toHaveTextContent('Test Feature')
    })
  })

  describe('Styling', () => {
    it('should have card styling classes', () => {
      render(<FeatureCard {...defaultProps} />)

      const article = screen.getByRole('article')
      expect(article).toHaveClass('card')
      expect(article).toHaveClass('bg-base-100')
      expect(article).toHaveClass('shadow-xl')
    })

    it('should have hover shadow transition class', () => {
      render(<FeatureCard {...defaultProps} />)

      const article = screen.getByRole('article')
      expect(article).toHaveClass('hover:shadow-2xl')
    })
  })
})
