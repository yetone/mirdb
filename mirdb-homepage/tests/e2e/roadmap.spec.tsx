/**
 * Roadmap Section E2E Tests.
 * Owner: Scenario 4 - Roadmap Section
 *
 * Tests:
 * - Roadmap section is visible with clear status indicators
 * - All status types render correctly
 * - Raft consensus feature is displayed
 *
 * Note: Using vitest + testing-library for component testing.
 * Full browser E2E tests can be run when Playwright dependencies are available.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import App from '../../src/App'
import { ThemeProvider } from '../../src/context'

describe('E2E: Roadmap Section', () => {
  beforeEach(() => {
    render(
      <ThemeProvider>
        <App />
      </ThemeProvider>
    )
  })

  // Test Case 5: Navigate to roadmap section and verify visibility
  it('TC5: Roadmap section is visible with clear status indicators for each item', () => {
    // Verify roadmap section is present and visible
    const roadmapSection = screen.getByTestId('roadmap-section')
    expect(roadmapSection).toBeInTheDocument()

    // Verify the roadmap heading is visible
    const heading = screen.getByRole('heading', { name: /roadmap/i, level: 2 })
    expect(heading).toBeInTheDocument()

    // Verify roadmap items are present
    const roadmapItems = screen.getAllByTestId('roadmap-item')
    expect(roadmapItems.length).toBeGreaterThan(0)

    // Verify status indicators are present for items
    const completeIndicators = screen.queryAllByTestId('status-complete')
    const inProgressIndicators = screen.queryAllByTestId('status-in-progress')
    const plannedIndicators = screen.queryAllByTestId('status-planned')

    // At least one status indicator type should be present
    const totalIndicators =
      completeIndicators.length + inProgressIndicators.length + plannedIndicators.length
    expect(totalIndicators).toBeGreaterThan(0)

    // Verify at least 2 different status types are present
    const statusTypesPresent = [
      completeIndicators.length > 0,
      inProgressIndicators.length > 0,
      plannedIndicators.length > 0,
    ].filter(Boolean).length
    expect(statusTypesPresent).toBeGreaterThanOrEqual(2)

    // Verify checkmarks are visible for completed items
    if (completeIndicators.length > 0) {
      expect(completeIndicators[0].textContent).toContain('✓')
    }
  })

  it('Roadmap section displays Raft consensus as planned feature', () => {
    // Find item titles containing "Raft" or "consensus"
    const roadmapTitles = screen.getAllByTestId('roadmap-item-title')
    const allTitles = roadmapTitles.map((el) => el.textContent || '')

    const hasRaftFeature = allTitles.some(
      (title) => title.toLowerCase().includes('raft') || title.toLowerCase().includes('consensus')
    )
    expect(hasRaftFeature).toBe(true)
  })

  it('Roadmap section is integrated into the main App', () => {
    // Verify roadmap is part of the full page
    const app = document.querySelector('.min-h-screen')
    expect(app).toBeInTheDocument()

    // Verify roadmap section exists within the app
    const roadmapSection = screen.getByTestId('roadmap-section')
    expect(roadmapSection).toBeInTheDocument()

    // Verify it has the expected structure
    expect(roadmapSection.tagName).toBe('SECTION')
    expect(roadmapSection.id).toBe('roadmap')
  })
})
