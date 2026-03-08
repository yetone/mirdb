import { describe, it, expect } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { Roadmap } from './Roadmap'
import { RoadmapItem } from './RoadmapItem'
import { roadmapItems } from '../../data/roadmap'
import type { RoadmapItem as RoadmapItemType } from '../../types'

describe('Roadmap Component', () => {
  // Test Case 1: Roadmap component renders without errors
  it('renders without errors', () => {
    expect(() => render(<Roadmap />)).not.toThrow()
  })

  // Test Case 2: List of implemented features exists with completion indicators
  it('displays implemented features section with items', () => {
    render(<Roadmap />)
    const completedSection = screen.getByTestId('completed-section')
    expect(completedSection).toBeInTheDocument()

    const completedItems = screen.getByTestId('completed-items')
    const items = within(completedItems).getAllByTestId('roadmap-item')
    expect(items.length).toBeGreaterThan(0)

    // Verify all items in completed section have 'complete' status
    items.forEach((item) => {
      expect(item).toHaveAttribute('data-status', 'complete')
    })
  })

  // Test Case 3: Async networking feature shown as implemented (Tokio)
  it('shows Async Networking with Tokio as implemented', () => {
    render(<Roadmap />)
    const tokioItem = screen.getByText('Async Networking with Tokio')
    expect(tokioItem).toBeInTheDocument()

    // Find the parent roadmap item and verify it's marked complete
    const itemContainer = tokioItem.closest('[data-testid="roadmap-item"]')
    expect(itemContainer).toHaveAttribute('data-status', 'complete')
  })

  // Test Case 4: Memtable with skip list shown as implemented
  it('shows Skip List Memtable as implemented', () => {
    render(<Roadmap />)
    const memtableItem = screen.getByText('Skip List Memtable')
    expect(memtableItem).toBeInTheDocument()

    const itemContainer = memtableItem.closest('[data-testid="roadmap-item"]')
    expect(itemContainer).toHaveAttribute('data-status', 'complete')
  })

  // Test Case 5: Minor and major compaction shown as implemented
  it('shows Minor Compaction as implemented', () => {
    render(<Roadmap />)
    const minorCompaction = screen.getByText('Minor Compaction')
    expect(minorCompaction).toBeInTheDocument()

    const itemContainer = minorCompaction.closest('[data-testid="roadmap-item"]')
    expect(itemContainer).toHaveAttribute('data-status', 'complete')
  })

  it('shows Major Compaction as implemented', () => {
    render(<Roadmap />)
    const majorCompaction = screen.getByText('Major Compaction')
    expect(majorCompaction).toBeInTheDocument()

    const itemContainer = majorCompaction.closest('[data-testid="roadmap-item"]')
    expect(itemContainer).toHaveAttribute('data-status', 'complete')
  })

  // Test Case 6: Raft consensus appears in planned/future features
  it('shows Raft Consensus as planned', () => {
    render(<Roadmap />)
    const raftItem = screen.getByText('Raft Consensus')
    expect(raftItem).toBeInTheDocument()

    const itemContainer = raftItem.closest('[data-testid="roadmap-item"]')
    expect(itemContainer).toHaveAttribute('data-status', 'planned')
  })

  it('has a planned features section', () => {
    render(<Roadmap />)
    const plannedSection = screen.getByTestId('planned-section')
    expect(plannedSection).toBeInTheDocument()
  })

  it('renders the section with proper heading', () => {
    render(<Roadmap />)
    expect(screen.getByRole('heading', { name: /roadmap & status/i })).toBeInTheDocument()
  })

  it('has accessible section landmark', () => {
    render(<Roadmap />)
    const section = screen.getByRole('region', { name: /roadmap & status/i })
    expect(section).toBeInTheDocument()
  })

  it('displays section headings for completed and planned', () => {
    render(<Roadmap />)
    expect(screen.getByText('Implemented Features')).toBeInTheDocument()
    expect(screen.getByText('Planned Features')).toBeInTheDocument()
  })
})

describe('RoadmapItem Component', () => {
  const completeItem: RoadmapItemType = {
    title: 'Completed Feature',
    status: 'complete',
    description: 'This feature is done',
  }

  const plannedItem: RoadmapItemType = {
    title: 'Planned Feature',
    status: 'planned',
    description: 'This feature is coming soon',
  }

  const inProgressItem: RoadmapItemType = {
    title: 'In Progress Feature',
    status: 'in-progress',
    description: 'This feature is being worked on',
  }

  it('renders item title', () => {
    render(<RoadmapItem item={completeItem} />)
    expect(screen.getByText('Completed Feature')).toBeInTheDocument()
  })

  it('renders item description', () => {
    render(<RoadmapItem item={completeItem} />)
    expect(screen.getByText('This feature is done')).toBeInTheDocument()
  })

  // Test Case 7: Complete and planned items are visually distinguishable
  it('displays Done badge for complete items', () => {
    render(<RoadmapItem item={completeItem} />)
    const badge = screen.getByTestId('status-badge')
    expect(badge).toHaveTextContent('Done')
  })

  it('displays Planned badge for planned items', () => {
    render(<RoadmapItem item={plannedItem} />)
    const badge = screen.getByTestId('status-badge')
    expect(badge).toHaveTextContent('Planned')
  })

  it('displays In Progress badge for in-progress items', () => {
    render(<RoadmapItem item={inProgressItem} />)
    const badge = screen.getByTestId('status-badge')
    expect(badge).toHaveTextContent('In Progress')
  })

  it('shows complete icon for completed items', () => {
    render(<RoadmapItem item={completeItem} />)
    expect(screen.getByTestId('complete-icon')).toBeInTheDocument()
  })

  it('shows planned icon for planned items', () => {
    render(<RoadmapItem item={plannedItem} />)
    expect(screen.getByTestId('planned-icon')).toBeInTheDocument()
  })

  it('renders as an article element', () => {
    render(<RoadmapItem item={completeItem} />)
    expect(screen.getByRole('article')).toBeInTheDocument()
  })

  it('has different background colors for complete vs planned', () => {
    const { rerender } = render(<RoadmapItem item={completeItem} />)
    const completeArticle = screen.getByRole('article')
    expect(completeArticle.className).toContain('green')

    rerender(<RoadmapItem item={plannedItem} />)
    const plannedArticle = screen.getByRole('article')
    expect(plannedArticle.className).toContain('blue')
  })
})

describe('Roadmap Data', () => {
  it('contains implemented features', () => {
    const completedItems = roadmapItems.filter((item) => item.status === 'complete')
    expect(completedItems.length).toBeGreaterThan(0)
  })

  it('contains planned features', () => {
    const plannedItems = roadmapItems.filter((item) => item.status === 'planned')
    expect(plannedItems.length).toBeGreaterThan(0)
  })

  it('each roadmap item has required properties', () => {
    roadmapItems.forEach((item) => {
      expect(item).toHaveProperty('title')
      expect(item).toHaveProperty('status')
      expect(item).toHaveProperty('description')
      expect(typeof item.title).toBe('string')
      expect(['complete', 'in-progress', 'planned']).toContain(item.status)
      expect(typeof item.description).toBe('string')
      expect(item.title.length).toBeGreaterThan(0)
      expect(item.description.length).toBeGreaterThan(0)
    })
  })

  it('includes Tokio async networking as complete', () => {
    const tokioItem = roadmapItems.find(
      (item) => item.title.toLowerCase().includes('tokio') || item.title.toLowerCase().includes('async networking')
    )
    expect(tokioItem).toBeDefined()
    expect(tokioItem?.status).toBe('complete')
  })

  it('includes Skip List Memtable as complete', () => {
    const memtableItem = roadmapItems.find(
      (item) => item.title.toLowerCase().includes('memtable') || item.title.toLowerCase().includes('skip list')
    )
    expect(memtableItem).toBeDefined()
    expect(memtableItem?.status).toBe('complete')
  })

  it('includes compaction features as complete', () => {
    const minorCompaction = roadmapItems.find(
      (item) => item.title.toLowerCase().includes('minor compaction')
    )
    const majorCompaction = roadmapItems.find(
      (item) => item.title.toLowerCase().includes('major compaction')
    )
    expect(minorCompaction).toBeDefined()
    expect(minorCompaction?.status).toBe('complete')
    expect(majorCompaction).toBeDefined()
    expect(majorCompaction?.status).toBe('complete')
  })

  it('includes Raft consensus as planned', () => {
    const raftItem = roadmapItems.find(
      (item) => item.title.toLowerCase().includes('raft')
    )
    expect(raftItem).toBeDefined()
    expect(raftItem?.status).toBe('planned')
  })
})
