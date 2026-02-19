/**
 * Features Section Tests
 * Owner: Scenario 3 - Features Section
 *
 * Test cases:
 * 1. Component renders 4 feature cards with correct content
 * 2. Card displays title 'LSM-Tree Storage', icon, and description mentioning high performance
 * 3. Card displays 'Memcached Compatible' with description of protocol support
 * 4. Card displays 'TTL Support' with description of key expiration
 * 5. Card displays 'Write-Ahead Log' with description of durability
 * 9. FeatureCard component accepts and renders title, description, and icon props correctly
 */

import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { Features } from './Features'
import { FeatureCard } from './FeatureCard'

describe('Features', () => {
  // Test case 1: Component renders 4 feature cards with correct content
  it('renders 4 feature cards with correct content', () => {
    render(<Features />)

    const featureCards = screen.getAllByTestId('feature-card')
    expect(featureCards).toHaveLength(4)

    // Check section heading
    expect(screen.getByRole('heading', { name: 'Features' })).toBeInTheDocument()
  })

  // Test case 2: Card displays title 'LSM-Tree Storage', icon, and description mentioning high performance
  it('displays LSM-Tree Storage feature with correct content', () => {
    render(<Features />)

    expect(screen.getByText('LSM-Tree Storage')).toBeInTheDocument()
    expect(screen.getByText(/high-performance/i)).toBeInTheDocument()
    expect(screen.getByText(/storage engine/i)).toBeInTheDocument()
  })

  // Test case 3: Card displays 'Memcached Compatible' with description of protocol support
  it('displays Memcached Compatible feature with protocol support description', () => {
    render(<Features />)

    expect(screen.getByText('Memcached Compatible')).toBeInTheDocument()
    expect(screen.getByText(/drop-in replacement/i)).toBeInTheDocument()
    expect(screen.getByText(/protocol support/i)).toBeInTheDocument()
  })

  // Test case 4: Card displays 'TTL Support' with description of key expiration
  it('displays TTL Support feature with key expiration description', () => {
    render(<Features />)

    expect(screen.getByText('TTL Support')).toBeInTheDocument()
    expect(screen.getByText(/key expiration/i)).toBeInTheDocument()
    expect(screen.getByText(/time-to-live/i)).toBeInTheDocument()
  })

  // Test case 5: Card displays 'Write-Ahead Log' with description of durability
  it('displays Write-Ahead Log feature with durability description', () => {
    render(<Features />)

    expect(screen.getByText('Write-Ahead Log')).toBeInTheDocument()
    expect(screen.getByText(/durability/i)).toBeInTheDocument()
    expect(screen.getByText(/crash recovery/i)).toBeInTheDocument()
  })

  it('renders features grid', () => {
    render(<Features />)

    const grid = screen.getByTestId('features-grid')
    expect(grid).toBeInTheDocument()
  })

  it('has correct section id for navigation', () => {
    render(<Features />)

    const section = document.getElementById('features')
    expect(section).toBeInTheDocument()
  })
})

describe('FeatureCard', () => {
  // Test case 9: FeatureCard component accepts and renders title, description, and icon props correctly
  it('accepts and renders title, description, and icon props correctly', () => {
    const testProps = {
      title: 'Test Feature',
      description: 'This is a test description for the feature card.',
      icon: 'database',
    }

    render(<FeatureCard {...testProps} />)

    expect(screen.getByText('Test Feature')).toBeInTheDocument()
    expect(screen.getByText('This is a test description for the feature card.')).toBeInTheDocument()

    // Verify the card is rendered
    const card = screen.getByTestId('feature-card')
    expect(card).toBeInTheDocument()

    // Verify the icon SVG is rendered
    const svg = card.querySelector('svg')
    expect(svg).toBeInTheDocument()
  })

  it('renders the correct heading level for accessibility', () => {
    render(
      <FeatureCard
        title="Accessible Feature"
        description="Description"
        icon="shield"
      />
    )

    const heading = screen.getByRole('heading', { level: 3, name: 'Accessible Feature' })
    expect(heading).toBeInTheDocument()
  })

  it('renders as an article element for semantic markup', () => {
    render(
      <FeatureCard
        title="Article Feature"
        description="Description"
        icon="clock"
      />
    )

    const article = screen.getByRole('article')
    expect(article).toBeInTheDocument()
  })
})
