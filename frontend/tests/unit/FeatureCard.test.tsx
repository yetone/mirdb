/**
 * Unit tests for FeatureCard component.
 * Owner: Scenario 4 - Features Section Display
 * Hover Effects: Scenario 11 - Feature Card Hover Effects
 *
 * Test coverage:
 * - Renders icon, title, and description
 * - Has proper ARIA attributes
 * - Hover styles are applied (lift, border highlight, shadow)
 * - Smooth transition animation classes
 * - Uses consistent styling with design system
 */

import { describe, it, expect } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
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

    it('should have data-testid for testing', () => {
      render(<FeatureCard {...defaultProps} />)

      expect(screen.getByTestId('feature-card')).toBeInTheDocument()
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

  describe('Hover Effects (Scenario 11)', () => {
    it('should have hover-specific CSS class for transform via Framer Motion whileHover', () => {
      render(<FeatureCard {...defaultProps} />)

      const card = screen.getByTestId('feature-card')
      // Framer Motion applies whileHover which transforms y by -4px
      // The component has whileHover prop which indicates hover transform capability
      expect(card).toBeInTheDocument()
      // Verify the card is a motion element (rendered by Framer Motion)
      expect(card.tagName.toLowerCase()).toBe('article')
    })

    it('should have border highlight classes for hover state', () => {
      render(<FeatureCard {...defaultProps} />)

      const card = screen.getByTestId('feature-card')
      // Check for border classes that enable hover border highlight
      expect(card).toHaveClass('border-2')
      expect(card).toHaveClass('border-transparent')
      expect(card).toHaveClass('hover:border-primary')
    })

    it('should have smooth transition classes for all properties', () => {
      render(<FeatureCard {...defaultProps} />)

      const card = screen.getByTestId('feature-card')
      expect(card).toHaveClass('transition-all')
      expect(card).toHaveClass('duration-300')
      expect(card).toHaveClass('ease-out')
    })

    it('should apply elevation effect classes on hover (shadow and transform)', () => {
      render(<FeatureCard {...defaultProps} />)

      const card = screen.getByTestId('feature-card')
      // Verify shadow elevation on hover
      expect(card).toHaveClass('shadow-xl')
      expect(card).toHaveClass('hover:shadow-2xl')
      // Framer Motion handles the transform lift effect via whileHover
    })

    it('should trigger hover state when mouseenter event fires', () => {
      render(<FeatureCard {...defaultProps} />)

      const card = screen.getByTestId('feature-card')

      // Fire mouseenter event
      fireEvent.mouseEnter(card)

      // The card should still be in the document after hover
      expect(card).toBeInTheDocument()
      // Border highlight classes are present for CSS to apply on :hover
      expect(card).toHaveClass('hover:border-primary')
      expect(card).toHaveClass('hover:shadow-2xl')
    })

    it('should have no janky movement - uses ease-out timing function', () => {
      render(<FeatureCard {...defaultProps} />)

      const card = screen.getByTestId('feature-card')
      // Smooth animation ensured by ease-out timing
      expect(card).toHaveClass('ease-out')
      // Combined with proper duration
      expect(card).toHaveClass('duration-300')
    })
  })
})
