import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import Features from '../../../src/components/sections/Features'

describe('Features', () => {
  // Test Case 1: Component renders with at least 5 feature cards
  it('renders with at least 5 feature cards', () => {
    render(<Features />)

    const featureCards = screen.getAllByTestId('feature-card')
    expect(featureCards.length).toBeGreaterThanOrEqual(5)
  })

  // Test Case 2: Text content includes 'Memcached' or 'memcached protocol'
  it('includes Memcached protocol content', () => {
    render(<Features />)

    const section = screen.getByRole('region', { name: /features/i })
    const textContent = section.textContent?.toLowerCase() || ''

    const hasMemcached = textContent.includes('memcached')
    expect(hasMemcached).toBe(true)
  })

  // Test Case 3: Text content includes 'persistent' or 'LSM tree'
  it('includes persistent storage content', () => {
    render(<Features />)

    const section = screen.getByRole('region', { name: /features/i })
    const textContent = section.textContent?.toLowerCase() || ''

    const hasPersistent = textContent.includes('persistent') || textContent.includes('lsm tree')
    expect(hasPersistent).toBe(true)
  })

  // Test Case 4: Text content includes 'skip-list' or 'memtable'
  it('includes skip-list or memtable content', () => {
    render(<Features />)

    const section = screen.getByRole('region', { name: /features/i })
    const textContent = section.textContent?.toLowerCase() || ''

    const hasSkipList = textContent.includes('skip-list') || textContent.includes('skip list') || textContent.includes('memtable')
    expect(hasSkipList).toBe(true)
  })

  // Test Case 5: Text content includes 'compaction'
  it('includes compaction content', () => {
    render(<Features />)

    const section = screen.getByRole('region', { name: /features/i })
    const textContent = section.textContent?.toLowerCase() || ''

    const hasCompaction = textContent.includes('compaction')
    expect(hasCompaction).toBe(true)
  })

  // Test Case 6: Text content includes 'Rust'
  it('includes Rust implementation content', () => {
    render(<Features />)

    const section = screen.getByRole('region', { name: /features/i })
    const textContent = section.textContent?.toLowerCase() || ''

    const hasRust = textContent.includes('rust')
    expect(hasRust).toBe(true)
  })

  // Test Case 7: Each feature card has an icon or visual indicator
  it('each feature card has an icon', () => {
    render(<Features />)

    const featureIcons = screen.getAllByTestId('feature-icon')
    const featureCards = screen.getAllByTestId('feature-card')

    expect(featureIcons.length).toBe(featureCards.length)
    expect(featureIcons.length).toBeGreaterThanOrEqual(5)
  })

  it('renders the section heading', () => {
    render(<Features />)

    const heading = screen.getByRole('heading', { level: 2 })
    expect(heading).toBeInTheDocument()
    expect(heading).toHaveTextContent(/features/i)
  })

  it('renders the features grid', () => {
    render(<Features />)

    const grid = screen.getByTestId('features-grid')
    expect(grid).toBeInTheDocument()
  })

  it('is accessible with proper ARIA attributes', () => {
    render(<Features />)

    const section = screen.getByRole('region', { name: /features/i })
    expect(section).toBeInTheDocument()
    expect(section).toHaveAttribute('aria-labelledby', 'features-heading')
  })
})
