/**
 * Unit tests for Features Section Component
 * Owner: Scenario 4 - Features Section Display
 *
 * Tests verify REQ-3: Display key MirDB features including
 * persistent storage, Memcached protocol, LSM-tree, WAL, and atomic compaction.
 */

import { describe, it, expect } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { Features } from '../../../../src/components/sections/Features'
import { FEATURES } from '../../../../src/utils/constants'

describe('Features Section Component', () => {
  /**
   * Test Case 1: At least 4 feature cards are rendered
   */
  it('should render at least 4 feature cards', () => {
    render(<Features />)

    const featureCards = screen.getAllByTestId('feature-card')
    expect(featureCards.length).toBeGreaterThanOrEqual(4)
  })

  /**
   * Test Case 2: Feature card for 'Persistent Key-Value Storage' exists
   */
  it("should render feature card for 'Persistent Key-Value Storage'", () => {
    render(<Features />)

    const heading = screen.getByRole('heading', {
      name: /persistent key-value storage/i,
    })
    expect(heading).toBeInTheDocument()

    // Verify it's within a feature card by checking heading exists in a card
    const featureCards = screen.getAllByTestId('feature-card')
    const persistentStorageCard = featureCards.find(
      (card) => within(card).queryByRole('heading', { name: /persistent key-value storage/i }) !== null
    )
    expect(persistentStorageCard).toBeDefined()
  })

  /**
   * Test Case 3: Feature card for 'Memcached Protocol Compatibility' exists
   */
  it("should render feature card for 'Memcached Protocol Compatibility'", () => {
    render(<Features />)

    const heading = screen.getByRole('heading', {
      name: /memcached protocol compatibility/i,
    })
    expect(heading).toBeInTheDocument()

    // Verify it's within a feature card
    const featureCards = screen.getAllByTestId('feature-card')
    const memcachedCard = featureCards.find(
      (card) => within(card).queryByText(/memcached protocol compatibility/i) !== null
    )
    expect(memcachedCard).toBeDefined()
  })

  /**
   * Test Case 4: Feature card for 'LSM-tree Implementation' exists
   */
  it("should render feature card for 'LSM-tree Implementation'", () => {
    render(<Features />)

    const heading = screen.getByRole('heading', {
      name: /lsm-tree implementation/i,
    })
    expect(heading).toBeInTheDocument()

    // Verify it's within a feature card
    const featureCards = screen.getAllByTestId('feature-card')
    const lsmTreeCard = featureCards.find(
      (card) => within(card).queryByText(/lsm-tree implementation/i) !== null
    )
    expect(lsmTreeCard).toBeDefined()
  })

  /**
   * Test Case 5: Feature card for 'Write-Ahead Logging' exists
   */
  it("should render feature card for 'Write-Ahead Logging'", () => {
    render(<Features />)

    const heading = screen.getByRole('heading', {
      name: /write-ahead logging/i,
    })
    expect(heading).toBeInTheDocument()

    // Verify it's within a feature card by checking heading exists in a card
    const featureCards = screen.getAllByTestId('feature-card')
    const walCard = featureCards.find(
      (card) => within(card).queryByRole('heading', { name: /write-ahead logging/i }) !== null
    )
    expect(walCard).toBeDefined()
  })

  /**
   * Test Case 6: Each feature card has a title and description
   */
  it('should render each feature card with a title and description', () => {
    render(<Features />)

    const featureCards = screen.getAllByTestId('feature-card')

    // Verify each card has both a heading (title) and paragraph (description)
    featureCards.forEach((card) => {
      // Check for h3 heading (title)
      const title = within(card).getByRole('heading', { level: 3 })
      expect(title).toBeInTheDocument()
      expect(title.textContent).toBeTruthy()

      // Check for paragraph (description)
      const description = card.querySelector('p')
      expect(description).not.toBeNull()
      expect(description?.textContent).toBeTruthy()
    })

    // Verify all features from constants are rendered
    FEATURES.forEach((feature) => {
      expect(screen.getByText(feature.title)).toBeInTheDocument()
      expect(screen.getByText(feature.description)).toBeInTheDocument()
    })
  })

  /**
   * Additional test: Features section has proper accessibility attributes
   */
  it('should have proper accessibility attributes on the features section', () => {
    render(<Features />)

    const featuresSection = screen.getByTestId('features')
    expect(featuresSection).toHaveAttribute('aria-labelledby', 'features-heading')

    const heading = screen.getByRole('heading', { name: /features/i, level: 2 })
    expect(heading).toHaveAttribute('id', 'features-heading')
  })

  /**
   * Additional test: Feature cards are rendered as article elements
   */
  it('should render feature cards as article elements for semantics', () => {
    render(<Features />)

    const featureCards = screen.getAllByTestId('feature-card')
    featureCards.forEach((card) => {
      expect(card.tagName.toLowerCase()).toBe('article')
    })
  })
})
