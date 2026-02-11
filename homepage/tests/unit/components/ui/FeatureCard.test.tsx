/**
 * Unit tests for FeatureCard component.
 * Owner: Scenario 4 - Features Section
 */

import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { FeatureCard } from '../../../../src/components/ui/FeatureCard'

describe('FeatureCard Component', () => {
  it('should render feature name as heading', () => {
    render(
      <FeatureCard
        name="Test Feature"
        description="Test description for the feature"
      />
    )

    const heading = screen.getByRole('heading', { level: 3 })
    expect(heading).toBeInTheDocument()
    expect(heading).toHaveTextContent('Test Feature')
  })

  it('should render feature description', () => {
    render(
      <FeatureCard
        name="Test Feature"
        description="Test description for the feature"
      />
    )

    const description = screen.getByText('Test description for the feature')
    expect(description).toBeInTheDocument()
  })

  it('should render with correct semantic structure', () => {
    render(
      <FeatureCard
        name="Test Feature"
        description="Test description for the feature"
      />
    )

    const card = document.querySelector('.feature-card')
    expect(card).toBeInTheDocument()
  })

  it('should render optional icon when provided', () => {
    render(
      <FeatureCard
        name="Test Feature"
        description="Test description"
        icon={<span data-testid="test-icon">🔧</span>}
      />
    )

    const icon = screen.getByTestId('test-icon')
    expect(icon).toBeInTheDocument()
  })

  it('should render without icon when not provided', () => {
    render(
      <FeatureCard
        name="Test Feature"
        description="Test description"
      />
    )

    const iconContainer = document.querySelector('.feature-card__icon')
    expect(iconContainer).toBeNull()
  })
})
