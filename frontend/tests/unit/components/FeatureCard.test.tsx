/**
 * FeatureCard Unit Tests
 * Owner: Scenario 2 - Features Section with Three Core Features
 *
 * Tests for the FeatureCard component
 */
import { render, screen } from '@testing-library/react'
import { describe, it, expect } from 'vitest'
import { FeatureCard } from '@/components/homepage/FeatureCard'

describe('FeatureCard', () => {
  const mockIcon = <svg data-testid="mock-icon" />

  it('renders with icon, title, and description', () => {
    render(
      <FeatureCard
        icon={mockIcon}
        title="Test Feature"
        description="This is a test description"
      />
    )

    expect(screen.getByTestId('feature-card')).toBeInTheDocument()
    expect(screen.getByTestId('feature-icon')).toBeInTheDocument()
    expect(screen.getByTestId('mock-icon')).toBeInTheDocument()
    expect(screen.getByTestId('feature-title')).toHaveTextContent('Test Feature')
    expect(screen.getByTestId('feature-description')).toHaveTextContent(
      'This is a test description'
    )
  })

  it('renders learn more link when provided', () => {
    render(
      <FeatureCard
        icon={mockIcon}
        title="Test Feature"
        description="This is a test description"
        learnMoreLink="/learn-more"
      />
    )

    const learnMoreLink = screen.getByTestId('feature-learn-more')
    expect(learnMoreLink).toBeInTheDocument()
    expect(learnMoreLink).toHaveAttribute('href', '/learn-more')
  })

  it('does not render learn more link when not provided', () => {
    render(
      <FeatureCard
        icon={mockIcon}
        title="Test Feature"
        description="This is a test description"
      />
    )

    expect(screen.queryByTestId('feature-learn-more')).not.toBeInTheDocument()
  })

  it('has proper card structure with card-body', () => {
    render(
      <FeatureCard
        icon={mockIcon}
        title="Test Feature"
        description="This is a test description"
      />
    )

    const card = screen.getByTestId('feature-card')
    expect(card).toHaveClass('card')
    expect(card.querySelector('.card-body')).toBeInTheDocument()
  })

  it('displays icon element correctly', () => {
    render(
      <FeatureCard
        icon={mockIcon}
        title="Test Feature"
        description="Test description"
      />
    )

    const iconContainer = screen.getByTestId('feature-icon')
    expect(iconContainer).toBeInTheDocument()
    expect(iconContainer.querySelector('[data-testid="mock-icon"]')).toBeInTheDocument()
  })
})
