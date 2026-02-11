/**
 * Unit tests for Features component.
 * Owner: Scenario 4 - Features Section
 */

import { describe, it, expect } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { Features } from '../../../../src/components/sections/Features'

describe('Features Component', () => {
  // Test Case 1: Features section contains exactly 5 feature cards
  it('should render exactly 5 feature cards', () => {
    render(<Features />)

    const featureCards = document.querySelectorAll('.feature-card')
    expect(featureCards.length).toBe(5)
  })

  // Test Case 2: Memcached Protocol feature card
  it('should render Memcached Protocol feature card with title and description about protocol compatibility', () => {
    render(<Features />)

    // Find heading with "Memcached" in its text
    const headings = screen.getAllByRole('heading', { level: 3 })
    const memcachedHeading = headings.find(h =>
      h.textContent?.toLowerCase().includes('memcached')
    )
    expect(memcachedHeading).toBeDefined()
    expect(memcachedHeading).toBeInTheDocument()

    // Find the parent card and check for description about protocol/compatibility
    const card = memcachedHeading?.closest('.feature-card')
    expect(card).toBeInTheDocument()

    // Check that the card contains description with relevant keywords
    const cardContent = within(card as HTMLElement)
    const descriptions = cardContent.queryAllByText(/compatib|protocol|client/i)
    expect(descriptions.length).toBeGreaterThan(0)
  })

  // Test Case 3: Persistence feature card
  it('should render Persistence feature card with title containing "Persist" and description about durability or disk storage', () => {
    render(<Features />)

    // Find heading with "Persist" in its text
    const headings = screen.getAllByRole('heading', { level: 3 })
    const persistenceHeading = headings.find(h =>
      h.textContent?.toLowerCase().includes('persist')
    )
    expect(persistenceHeading).toBeDefined()
    expect(persistenceHeading).toBeInTheDocument()

    // Find the parent card and check for description about durability/disk
    const card = persistenceHeading?.closest('.feature-card')
    expect(card).toBeInTheDocument()

    // Check that the card contains description with relevant keywords
    const cardContent = within(card as HTMLElement)
    const descriptions = cardContent.queryAllByText(/dura|disk|surviv|restart|log/i)
    expect(descriptions.length).toBeGreaterThan(0)
  })

  // Test Case 4: LSM Tree feature card
  it('should render LSM Tree feature card with title containing "LSM" and description about tree architecture or compaction', () => {
    render(<Features />)

    // Find heading with "LSM" in its text
    const headings = screen.getAllByRole('heading', { level: 3 })
    const lsmHeading = headings.find(h =>
      h.textContent?.toLowerCase().includes('lsm')
    )
    expect(lsmHeading).toBeDefined()
    expect(lsmHeading).toBeInTheDocument()

    // Find the parent card and check for description about architecture/compaction/performance
    const card = lsmHeading?.closest('.feature-card')
    expect(card).toBeInTheDocument()

    // Check that the card contains description with relevant keywords
    const cardContent = within(card as HTMLElement)
    const descriptions = cardContent.queryAllByText(/tree|architecture|compaction|performance|efficient/i)
    expect(descriptions.length).toBeGreaterThan(0)
  })

  // Test Case 5: Skip-list feature card
  it('should render Skip-list feature card with title containing "Skip-list" or "Memtable" and description about in-memory operations', () => {
    render(<Features />)

    // Find heading with "Skip-list" or "Memtable" in its text
    const headings = screen.getAllByRole('heading', { level: 3 })
    const skipListHeading = headings.find(h =>
      h.textContent?.toLowerCase().includes('skip-list') ||
      h.textContent?.toLowerCase().includes('memtable')
    )
    expect(skipListHeading).toBeDefined()
    expect(skipListHeading).toBeInTheDocument()

    // Find the parent card and check for description about in-memory operations
    const card = skipListHeading?.closest('.feature-card')
    expect(card).toBeInTheDocument()

    // Check that the card contains description with relevant keywords
    const cardContent = within(card as HTMLElement)
    const descriptions = cardContent.queryAllByText(/memory|fast|in-memory|operation/i)
    expect(descriptions.length).toBeGreaterThan(0)
  })

  // Test Case 6: Compaction feature card
  it('should render Compaction feature card with title containing "Compaction" and description about multi-level strategy', () => {
    render(<Features />)

    // Find heading with "Compaction" in its text - looking specifically for "Multi-level Compaction"
    const headings = screen.getAllByRole('heading', { level: 3 })
    const compactionHeading = headings.find(h =>
      h.textContent?.toLowerCase().includes('compaction') &&
      h.textContent?.toLowerCase().includes('multi')
    )
    expect(compactionHeading).toBeDefined()
    expect(compactionHeading).toBeInTheDocument()

    // Find the parent card and check for description about multi-level strategy
    const card = compactionHeading?.closest('.feature-card')
    expect(card).toBeInTheDocument()

    // Check that the card contains description with relevant keywords
    const cardContent = within(card as HTMLElement)
    const descriptions = cardContent.queryAllByText(/multi-level|level|strateg|optimiz|storage/i)
    expect(descriptions.length).toBeGreaterThan(0)
  })

  it('should render Features section with correct semantic structure', () => {
    render(<Features />)

    // Check for section element
    const section = document.querySelector('section.features')
    expect(section).toBeInTheDocument()
    expect(section).toHaveAttribute('id', 'features')
  })

  it('should render section heading', () => {
    render(<Features />)

    const sectionHeading = screen.getByRole('heading', { level: 2 })
    expect(sectionHeading).toBeInTheDocument()
    expect(sectionHeading).toHaveTextContent(/features|capabilities/i)
  })

  it('should render features in a grid container', () => {
    render(<Features />)

    const grid = document.querySelector('.features__grid')
    expect(grid).toBeInTheDocument()

    const featureCards = document.querySelectorAll('.feature-card')
    featureCards.forEach(card => {
      expect(grid).toContainElement(card)
    })
  })
})
