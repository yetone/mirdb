/**
 * Unit Tests for FeatureCard Component
 * Owner: Scenario 5 - Features Section Display
 *
 * Tests:
 * - Card renders with icon, title, and description
 * - Icon has proper accessibility attributes
 * - Hover lift effect is present via CSS classes
 *
 * Requirements: REQ-4
 */

import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { FeatureCard } from '../../../src/components/homepage/FeatureCard'

describe('FeatureCard', () => {
  const mockFeature = {
    icon: '📊',
    title: 'Analytics Dashboard',
    description: 'Track your link performance with detailed analytics and insights.',
  }

  it('renders feature card with icon, title, and description', () => {
    render(<FeatureCard {...mockFeature} />)

    const card = screen.getByTestId('feature-card')
    expect(card).toBeInTheDocument()

    // Check icon is rendered
    const icon = screen.getByTestId('feature-icon')
    expect(icon).toBeInTheDocument()
    expect(icon.textContent).toBe(mockFeature.icon)

    // Check title is rendered
    const title = screen.getByTestId('feature-title')
    expect(title).toBeInTheDocument()
    expect(title.textContent).toBe(mockFeature.title)

    // Check description is rendered
    const description = screen.getByTestId('feature-description')
    expect(description).toBeInTheDocument()
    expect(description.textContent).toBe(mockFeature.description)
  })

  it('renders icon with proper accessibility attributes', () => {
    render(<FeatureCard {...mockFeature} />)

    const icon = screen.getByTestId('feature-icon')

    // Icon should have role="img" for accessibility
    expect(icon.getAttribute('role')).toBe('img')

    // Icon should have aria-label with feature title
    expect(icon.getAttribute('aria-label')).toContain(mockFeature.title)
  })

  it('has hover lift effect classes for interactive feedback', () => {
    render(<FeatureCard {...mockFeature} />)

    const card = screen.getByTestId('feature-card')

    // Check for hover transition classes
    expect(card.className).toContain('hover:shadow')
    expect(card.className).toContain('hover:-translate-y')
    expect(card.className).toContain('transition')
  })

  it('renders description with 1-2 sentences', () => {
    render(<FeatureCard {...mockFeature} />)

    const description = screen.getByTestId('feature-description')
    const text = description.textContent ?? ''

    // Check that description has substantive content
    expect(text.length).toBeGreaterThan(10)

    // Check for 1-2 sentences (1-2 sentence-ending punctuation marks)
    const sentenceEndings = (text.match(/[.!?]/g) ?? []).length
    expect(sentenceEndings).toBeGreaterThanOrEqual(1)
    expect(sentenceEndings).toBeLessThanOrEqual(2)
  })

  it('accepts and applies custom className', () => {
    const customClass = 'custom-test-class'
    render(<FeatureCard {...mockFeature} className={customClass} />)

    const card = screen.getByTestId('feature-card')
    expect(card.className).toContain(customClass)
  })

  it('renders title as heading element for semantic structure', () => {
    render(<FeatureCard {...mockFeature} />)

    // Title should be an h3 for proper heading hierarchy
    const title = screen.getByRole('heading', { level: 3 })
    expect(title).toBeInTheDocument()
    expect(title.textContent).toBe(mockFeature.title)
  })
})
