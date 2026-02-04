import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { createElement } from 'react'
import { Link2 } from 'lucide-react'
import { FeatureCard } from '../../src/components/FeatureCard'

/**
 * FeatureCard Component Tests
 * Scenario 1: Unit tests for FeatureCard component
 */

describe('FeatureCard', () => {
  const defaultProps = {
    icon: createElement(Link2, {
      className: 'w-8 h-8 text-primary',
      'aria-hidden': 'true',
    }),
    title: 'Test Feature',
    description: 'This is a test feature description.',
  }

  it('should render the feature title', () => {
    render(<FeatureCard {...defaultProps} />)

    expect(screen.getByText('Test Feature')).toBeInTheDocument()
  })

  it('should render the feature description', () => {
    render(<FeatureCard {...defaultProps} />)

    expect(
      screen.getByText('This is a test feature description.')
    ).toBeInTheDocument()
  })

  it('should render the icon', () => {
    render(<FeatureCard {...defaultProps} />)

    const iconContainer = screen.getByTestId('feature-icon')
    expect(iconContainer).toBeInTheDocument()

    const svg = iconContainer.querySelector('svg')
    expect(svg).toBeInTheDocument()
  })

  it('should have accessible article element with proper labeling', () => {
    render(<FeatureCard {...defaultProps} />)

    const article = screen.getByRole('article')
    expect(article).toBeInTheDocument()
    expect(article).toHaveAttribute('aria-labelledby')
  })

  it('should render title as h3 heading', () => {
    render(<FeatureCard {...defaultProps} />)

    const heading = screen.getByRole('heading', { level: 3 })
    expect(heading).toBeInTheDocument()
    expect(heading).toHaveTextContent('Test Feature')
  })

  it('should generate unique id for title based on title text', () => {
    render(<FeatureCard {...defaultProps} />)

    const heading = screen.getByRole('heading', { level: 3 })
    expect(heading).toHaveAttribute('id', 'feature-title-test-feature')
  })
})
