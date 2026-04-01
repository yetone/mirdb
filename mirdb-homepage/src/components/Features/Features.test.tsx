/**
 * Features Section Tests.
 * Owner: Scenario 3 - Features Section
 */

import { describe, it, expect } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { Features } from './Features'
import { features } from '../../data/features'

describe('Features Component', () => {
  // Test Case 1: Component renders with at least 5 feature cards
  it('renders at least 5 feature cards', () => {
    render(<Features />)
    const featureCards = screen.getAllByTestId('feature-card')
    expect(featureCards.length).toBeGreaterThanOrEqual(5)
  })

  // Test Case 2: Feature card with title containing 'Tokio' or 'async' exists
  it('displays Tokio async runtime feature', () => {
    render(<Features />)
    const featureTitles = screen.getAllByTestId('feature-title')
    const hasTokioFeature = featureTitles.some(
      (title) =>
        title.textContent?.toLowerCase().includes('tokio') ||
        title.textContent?.toLowerCase().includes('async')
    )
    expect(hasTokioFeature).toBe(true)
  })

  // Test Case 3: Feature card with title containing 'Memcached' or 'protocol' exists
  it('displays Memcached protocol feature', () => {
    render(<Features />)
    const featureTitles = screen.getAllByTestId('feature-title')
    const hasMemcachedFeature = featureTitles.some(
      (title) =>
        title.textContent?.toLowerCase().includes('memcached') ||
        title.textContent?.toLowerCase().includes('protocol')
    )
    expect(hasMemcachedFeature).toBe(true)
  })

  // Test Case 4: Feature card with title containing 'Skiplist' or 'memtable' exists
  it('displays Skiplist memtable feature', () => {
    render(<Features />)
    const featureTitles = screen.getAllByTestId('feature-title')
    const hasSkiplistFeature = featureTitles.some(
      (title) =>
        title.textContent?.toLowerCase().includes('skiplist') ||
        title.textContent?.toLowerCase().includes('memtable')
    )
    expect(hasSkiplistFeature).toBe(true)
  })

  // Test Case 5: Feature cards for both 'minor compaction' and 'major compaction' exist
  it('displays both minor and major compaction features', () => {
    render(<Features />)
    const featureTitles = screen.getAllByTestId('feature-title')
    const titleTexts = featureTitles.map((title) => title.textContent?.toLowerCase() ?? '')

    const hasMinorCompaction = titleTexts.some((text) => text.includes('minor'))
    const hasMajorCompaction = titleTexts.some((text) => text.includes('major'))

    expect(hasMinorCompaction).toBe(true)
    expect(hasMajorCompaction).toBe(true)
  })

  // Test Case 6: Each feature card has an icon/emoji, title element, and description element
  it('renders feature cards with icon, title, and description', () => {
    render(<Features />)
    const featureCards = screen.getAllByTestId('feature-card')

    featureCards.forEach((card) => {
      const icon = within(card).getByTestId('feature-icon')
      const title = within(card).getByTestId('feature-title')
      const description = within(card).getByTestId('feature-description')

      expect(icon).toBeInTheDocument()
      expect(icon.textContent).toBeTruthy()
      expect(title).toBeInTheDocument()
      expect(title.textContent).toBeTruthy()
      expect(description).toBeInTheDocument()
      expect(description.textContent).toBeTruthy()
    })
  })

  // Test Case 7: Completed features have checkmark indicator
  it('displays checkmark indicator for completed features', () => {
    render(<Features />)

    // All features in our data are complete
    const completeIndicators = screen.getAllByTestId('status-complete')
    const completeFeatures = features.filter((f) => f.status === 'complete')

    expect(completeIndicators.length).toBe(completeFeatures.length)

    completeIndicators.forEach((indicator) => {
      expect(indicator.textContent).toContain('✓')
    })
  })

  // Additional test: Features section has proper heading
  it('renders with Features heading', () => {
    render(<Features />)
    const heading = screen.getByRole('heading', { name: /features/i, level: 2 })
    expect(heading).toBeInTheDocument()
  })

  // Additional test: Features grid is present
  it('renders features in a grid layout', () => {
    render(<Features />)
    const grid = screen.getByTestId('features-grid')
    expect(grid).toBeInTheDocument()
    expect(grid.className).toContain('grid')
  })
})
