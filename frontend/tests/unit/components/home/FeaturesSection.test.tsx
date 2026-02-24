/**
 * FeaturesSection Component Tests
 * Owner: Scenario 6 - Features Section Display
 *
 * Unit tests for the FeaturesSection component.
 * Tests cover:
 * - Section contains at least 3 feature cards
 * - Analytics feature card displays icon, title, and description
 * - Dashboard feature card displays icon, title, and description
 * - Feature cards use GlassMorphismCard component (glass morphism visual effect)
 */

import { describe, it, expect } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { FeaturesSection, FeatureCard } from '../../../../src/components/home/FeaturesSection'
import type { Feature } from '../../../../src/types/home'

describe('FeaturesSection', () => {
  // Test Case 1: Section contains at least 3 feature cards
  it('renders at least 3 feature cards', () => {
    render(<FeaturesSection />)

    const featureCards = screen.getAllByTestId('feature-card')
    expect(featureCards.length).toBeGreaterThanOrEqual(3)
  })

  // Test Case 2: Analytics feature card displays icon, title, and description
  it('displays Analytics feature card with icon, title, and description of tracking capabilities', () => {
    render(<FeaturesSection />)

    // Find the Analytics heading
    const analyticsHeading = screen.getByRole('heading', { name: 'Analytics' })
    expect(analyticsHeading).toBeInTheDocument()

    // Find the card containing Analytics
    const analyticsCard = analyticsHeading.closest('[data-testid="feature-card"]')
    expect(analyticsCard).toBeInTheDocument()

    // Verify the card contains an icon
    const icon = within(analyticsCard as HTMLElement).getByRole('img', {
      name: /analytics icon/i,
    })
    expect(icon).toBeInTheDocument()

    // Verify the description mentions tracking capabilities
    const description = within(analyticsCard as HTMLElement).getByText(
      /track clicks and analyze performance/i
    )
    expect(description).toBeInTheDocument()
  })

  // Test Case 3: Dashboard feature card displays icon, title, and description
  it('displays Dashboard feature card with icon, title, and description of URL management', () => {
    render(<FeaturesSection />)

    // Find the Dashboard heading
    const dashboardHeading = screen.getByRole('heading', { name: 'Dashboard' })
    expect(dashboardHeading).toBeInTheDocument()

    // Find the card containing Dashboard
    const dashboardCard = dashboardHeading.closest('[data-testid="feature-card"]')
    expect(dashboardCard).toBeInTheDocument()

    // Verify the card contains an icon
    const icon = within(dashboardCard as HTMLElement).getByRole('img', {
      name: /dashboard icon/i,
    })
    expect(icon).toBeInTheDocument()

    // Verify the description mentions URL management
    const description = within(dashboardCard as HTMLElement).getByText(
      /manage all your shortened urls/i
    )
    expect(description).toBeInTheDocument()
  })

  // Test Case 4: Feature cards use GlassMorphismCard component (glass morphism visual effect)
  it('applies glass morphism visual effect to feature cards', () => {
    render(<FeaturesSection />)

    const featureCards = screen.getAllByTestId('feature-card')

    // Each card should have the glass morphism styling classes
    featureCards.forEach((card) => {
      // Check for backdrop-blur class (key indicator of glass morphism)
      expect(card.className).toContain('backdrop-blur')

      // Check for semi-transparent background
      expect(card.className).toContain('bg-base-100/30')

      // Check for border
      expect(card.className).toContain('border')

      // Check for rounded corners
      expect(card.className).toContain('rounded-xl')
    })
  })

  // Additional test: FeaturesSection has proper accessibility
  it('has proper accessibility attributes', () => {
    render(<FeaturesSection />)

    // Check for section with proper aria-label
    const section = screen.getByRole('region', { name: /features section/i })
    expect(section).toBeInTheDocument()

    // Check for section heading
    const heading = screen.getByRole('heading', { name: /powerful features/i })
    expect(heading).toBeInTheDocument()
  })

  // Additional test: All four features are displayed
  it('displays all four features: Analytics, Dashboard, Share Tokens, and Dark Mode', () => {
    render(<FeaturesSection />)

    expect(screen.getByRole('heading', { name: 'Analytics' })).toBeInTheDocument()
    expect(screen.getByRole('heading', { name: 'Dashboard' })).toBeInTheDocument()
    expect(screen.getByRole('heading', { name: 'Share Tokens' })).toBeInTheDocument()
    expect(screen.getByRole('heading', { name: 'Dark Mode' })).toBeInTheDocument()
  })

  // Additional test: Each feature card has correct structure
  it('renders feature cards with correct structure (icon, title, description)', () => {
    render(<FeaturesSection />)

    const featureCards = screen.getAllByTestId('feature-card')

    featureCards.forEach((card) => {
      // Each card should have an img role element (the emoji with role="img")
      const icon = within(card).getByRole('img')
      expect(icon).toBeInTheDocument()

      // Each card should have a heading (h3)
      const heading = within(card).getByRole('heading', { level: 3 })
      expect(heading).toBeInTheDocument()

      // Each card should have a paragraph (description)
      const description = card.querySelector('p')
      expect(description).toBeInTheDocument()
      expect(description?.textContent?.length).toBeGreaterThan(0)
    })
  })
})

describe('FeatureCard', () => {
  const mockFeature: Feature = {
    icon: '🚀',
    title: 'Test Feature',
    description: 'This is a test feature description.',
  }

  it('renders feature icon, title, and description', () => {
    render(<FeatureCard feature={mockFeature} />)

    // Check icon
    const icon = screen.getByRole('img', { name: /test feature icon/i })
    expect(icon).toBeInTheDocument()
    expect(icon).toHaveTextContent('🚀')

    // Check title
    const title = screen.getByRole('heading', { name: 'Test Feature' })
    expect(title).toBeInTheDocument()

    // Check description
    const description = screen.getByText('This is a test feature description.')
    expect(description).toBeInTheDocument()
  })

  it('applies GlassMorphismCard styling', () => {
    render(<FeatureCard feature={mockFeature} />)

    const card = screen.getByTestId('feature-card')
    expect(card.className).toContain('backdrop-blur')
    expect(card.className).toContain('rounded-xl')
  })
})
