/**
 * Unit tests for FeatureCard component
 * Owner: Scenario 3 - Features Section Display
 */

import { describe, it, expect } from 'vitest'
import { screen } from '@testing-library/react'
import { renderWithProviders } from '../../../test-utils'
import { FeatureCard } from '@/components/homepage'
import { Link2 } from 'lucide-react'

describe('FeatureCard', () => {
  const defaultProps = {
    icon: <Link2 data-testid="test-icon" className="w-8 h-8" />,
    title: 'Test Feature',
    description: 'This is a test feature description with multiple sentences.',
  }

  it('renders feature card with icon', () => {
    renderWithProviders(<FeatureCard {...defaultProps} />)

    const icon = screen.getByTestId('test-icon')
    expect(icon).toBeInTheDocument()
  })

  it('renders feature card with title', () => {
    renderWithProviders(<FeatureCard {...defaultProps} />)

    expect(screen.getByText('Test Feature')).toBeInTheDocument()
  })

  it('renders feature card with description', () => {
    renderWithProviders(<FeatureCard {...defaultProps} />)

    expect(
      screen.getByText('This is a test feature description with multiple sentences.')
    ).toBeInTheDocument()
  })

  it('displays title as heading element', () => {
    renderWithProviders(<FeatureCard {...defaultProps} />)

    const heading = screen.getByRole('heading', { level: 3 })
    expect(heading).toHaveTextContent('Test Feature')
  })

  it('icon container has proper styling classes', () => {
    renderWithProviders(<FeatureCard {...defaultProps} />)

    const iconContainer = screen.getByTestId('test-icon').parentElement
    expect(iconContainer).toHaveClass('rounded-full')
    expect(iconContainer).toHaveClass('bg-primary/20')
  })

  it('applies glassmorphism card styling', () => {
    const { container } = renderWithProviders(<FeatureCard {...defaultProps} />)

    const card = container.firstChild
    expect(card).toHaveClass('backdrop-blur-lg')
    expect(card).toHaveClass('rounded-xl')
  })
})
