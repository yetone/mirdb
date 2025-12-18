import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import FeaturesSection from './FeaturesSection'

describe('FeaturesSection', () => {
  // Test Case 1: Memcached Compatible feature card
  it('displays Memcached Compatible feature card with icon, title, and description mentioning existing memcached clients', () => {
    render(<FeaturesSection />)

    // Check for title
    expect(screen.getByText(/Memcached Compatible/i)).toBeInTheDocument()

    // Check for description mentioning existing memcached clients
    const description = screen.getByText(/existing memcached clients/i)
    expect(description).toBeInTheDocument()

    // Check for icon in the Memcached Compatible card
    const memcachedCard = screen.getByTestId('feature-card-memcached')
    const icon = memcachedCard.querySelector('[data-testid="feature-icon"]')
    expect(icon).toBeInTheDocument()
  })

  // Test Case 2: Persistent Storage feature card
  it('displays Persistent Storage feature card with icon, title, and description mentioning SSTable storage or data survives restarts', () => {
    render(<FeaturesSection />)

    // Check for title
    expect(screen.getByText(/Persistent Storage/i)).toBeInTheDocument()

    // Check for description mentioning SSTable storage or data survives restarts
    const persistentCard = screen.getByTestId('feature-card-persistent')
    const cardText = persistentCard.textContent
    const mentionsSSTable = /sstable/i.test(cardText)
    const mentionsSurvivesRestarts = /data survives restarts/i.test(cardText)
    expect(mentionsSSTable || mentionsSurvivesRestarts).toBe(true)

    // Check for icon
    const icon = persistentCard.querySelector('[data-testid="feature-icon"]')
    expect(icon).toBeInTheDocument()
  })

  // Test Case 3: High Performance feature card
  it('displays High Performance feature card with icon, title, and description mentioning LSM tree or async I/O', () => {
    render(<FeaturesSection />)

    // Check for title
    expect(screen.getByText(/High Performance/i)).toBeInTheDocument()

    // Check for description mentioning LSM tree or async I/O
    const performanceCard = screen.getByTestId('feature-card-performance')
    const cardText = performanceCard.textContent
    const mentionsLSM = /lsm tree/i.test(cardText)
    const mentionsAsyncIO = /async i\/o/i.test(cardText)
    expect(mentionsLSM || mentionsAsyncIO).toBe(true)

    // Check for icon
    const icon = performanceCard.querySelector('[data-testid="feature-icon"]')
    expect(icon).toBeInTheDocument()
  })

  // Test Case 4: Each feature card contains a visual icon element
  it('ensures each feature card contains a visual icon element', () => {
    render(<FeaturesSection />)

    const featureCards = screen.getAllByTestId(/^feature-card-/)
    expect(featureCards).toHaveLength(3)

    featureCards.forEach((card) => {
      const icon = card.querySelector('[data-testid="feature-icon"]')
      expect(icon).toBeInTheDocument()
      // Icon should be a visual element (svg or img)
      const isSvg = icon.tagName.toLowerCase() === 'svg'
      const isImg = icon.tagName.toLowerCase() === 'img'
      const hasAriaLabel = icon.hasAttribute('aria-label')
      expect(isSvg || isImg || hasAriaLabel).toBe(true)
    })
  })

  // Test Case 5: Feature cards are arranged horizontally in a row on desktop viewport
  it('arranges feature cards horizontally in a row on desktop viewport', () => {
    render(<FeaturesSection />)

    const featuresContainer = screen.getByTestId('features-container')
    const computedStyle = window.getComputedStyle(featuresContainer)

    // Check that display is flex (for horizontal arrangement)
    expect(computedStyle.display).toBe('flex')

    // Check that flex-direction is row (horizontal)
    expect(computedStyle.flexDirection).toBe('row')
  })
})
