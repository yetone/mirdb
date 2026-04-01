/**
 * Roadmap Section Tests.
 * Owner: Scenario 4 - Roadmap Section
 */

import { describe, it, expect } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { Roadmap } from './Roadmap'
import { roadmapItems } from '../../data/roadmap'

describe('Roadmap Component', () => {
  // Test Case 1: Component renders with list or timeline of features
  it('renders roadmap component with list of features', () => {
    render(<Roadmap />)

    const roadmapSection = screen.getByTestId('roadmap-section')
    expect(roadmapSection).toBeInTheDocument()

    const roadmapList = screen.getByTestId('roadmap-list')
    expect(roadmapList).toBeInTheDocument()

    const roadmapItemElements = screen.getAllByTestId('roadmap-item')
    expect(roadmapItemElements.length).toBeGreaterThan(0)
  })

  // Test Case 2: At least one item has completed status indicator (checkmark)
  it('displays at least one completed item with checkmark indicator', () => {
    render(<Roadmap />)

    const completeIndicators = screen.getAllByTestId('status-complete')
    expect(completeIndicators.length).toBeGreaterThan(0)

    // Verify the checkmark is present
    completeIndicators.forEach((indicator) => {
      expect(indicator.textContent).toContain('✓')
    })
  })

  // Test Case 3: Item containing 'Raft' or 'consensus' exists with in-progress or planned status
  it('displays Raft consensus feature with planned or in-progress status', () => {
    render(<Roadmap />)

    const roadmapItemTitles = screen.getAllByTestId('roadmap-item-title')
    const hasRaftFeature = roadmapItemTitles.some(
      (title) =>
        title.textContent?.toLowerCase().includes('raft') ||
        title.textContent?.toLowerCase().includes('consensus')
    )
    expect(hasRaftFeature).toBe(true)

    // Verify it has planned or in-progress status (not complete)
    const raftItem = roadmapItems.find(
      (item) =>
        item.title.toLowerCase().includes('raft') ||
        item.title.toLowerCase().includes('consensus')
    )
    expect(raftItem).toBeDefined()
    expect(['planned', 'in-progress']).toContain(raftItem?.status)
  })

  // Test Case 4: Roadmap contains items with at least 2 different status types
  it('displays items with at least 2 different status types', () => {
    render(<Roadmap />)

    const statusTypes = new Set<string>()

    const completeIndicators = screen.queryAllByTestId('status-complete')
    if (completeIndicators.length > 0) statusTypes.add('complete')

    const inProgressIndicators = screen.queryAllByTestId('status-in-progress')
    if (inProgressIndicators.length > 0) statusTypes.add('in-progress')

    const plannedIndicators = screen.queryAllByTestId('status-planned')
    if (plannedIndicators.length > 0) statusTypes.add('planned')

    expect(statusTypes.size).toBeGreaterThanOrEqual(2)
  })

  // Additional test: Roadmap section has proper heading
  it('renders with Roadmap heading', () => {
    render(<Roadmap />)
    const heading = screen.getByRole('heading', { name: /roadmap/i, level: 2 })
    expect(heading).toBeInTheDocument()
  })

  // Additional test: Each roadmap item has required elements
  it('renders roadmap items with title and status indicator', () => {
    render(<Roadmap />)
    const roadmapItemElements = screen.getAllByTestId('roadmap-item')

    roadmapItemElements.forEach((item) => {
      const title = within(item).getByTestId('roadmap-item-title')
      expect(title).toBeInTheDocument()
      expect(title.textContent).toBeTruthy()

      // Each item should have exactly one status indicator
      const statusIndicators = [
        within(item).queryByTestId('status-complete'),
        within(item).queryByTestId('status-in-progress'),
        within(item).queryByTestId('status-planned'),
      ].filter(Boolean)

      expect(statusIndicators.length).toBe(1)
    })
  })

  // Additional test: Completed items count matches data
  it('renders correct number of completed items', () => {
    render(<Roadmap />)

    const completedItemsInData = roadmapItems.filter((item) => item.status === 'complete')
    const completeIndicators = screen.getAllByTestId('status-complete')

    expect(completeIndicators.length).toBe(completedItemsInData.length)
  })

  // Additional test: Section has accessible structure
  it('has accessible section structure', () => {
    render(<Roadmap />)

    const section = screen.getByTestId('roadmap-section')
    expect(section).toHaveAttribute('aria-labelledby', 'roadmap-heading')

    const heading = screen.getByRole('heading', { name: /roadmap/i })
    expect(heading).toHaveAttribute('id', 'roadmap-heading')
  })
})
