import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import ComparisonSection from './ComparisonSection'

describe('ComparisonSection', () => {
  // Test Case 1: Comparison table or grid is displayed with MirDB and Memcached columns
  it('displays a comparison table or grid with MirDB and Memcached columns', () => {
    render(<ComparisonSection />)

    // Check for comparison section
    const comparisonSection = screen.getByTestId('comparison-section')
    expect(comparisonSection).toBeInTheDocument()

    // Check that there is a table or grid structure
    const table = comparisonSection.querySelector('table, [role="table"], .comparison-table, .comparison-grid')
    expect(table).toBeInTheDocument()

    // Check for MirDB column header in the table
    const mirdbHeader = table.querySelector('.mirdb-header')
    expect(mirdbHeader).toBeInTheDocument()
    expect(mirdbHeader.textContent).toMatch(/MirDB/i)

    // Check for Memcached column header in the table
    const memcachedHeader = table.querySelector('.memcached-header')
    expect(memcachedHeader).toBeInTheDocument()
    expect(memcachedHeader.textContent).toMatch(/Memcached/i)
  })

  // Test Case 2: Data Persistence row shows MirDB as 'Yes' or 'SSTable' and Memcached as 'No' or 'Memory only'
  it('shows Data Persistence row with MirDB as Yes/SSTable and Memcached as No/Memory only', () => {
    render(<ComparisonSection />)

    // Find the persistence row
    const persistenceRow = screen.getByTestId('comparison-row-persistence')
    expect(persistenceRow).toBeInTheDocument()

    // Check MirDB shows persistence (Yes or SSTable)
    const rowText = persistenceRow.textContent.toLowerCase()
    const mirDBHasPersistence = /yes|sstable/i.test(rowText)
    expect(mirDBHasPersistence).toBe(true)

    // Check Memcached shows no persistence (No or Memory only)
    const memcachedNoPersistence = /no|memory only/i.test(rowText)
    expect(memcachedNoPersistence).toBe(true)
  })

  // Test Case 3: Protocol row shows both support Memcached protocol
  it('shows Protocol row indicating both support Memcached protocol', () => {
    render(<ComparisonSection />)

    // Find the protocol row
    const protocolRow = screen.getByTestId('comparison-row-protocol')
    expect(protocolRow).toBeInTheDocument()

    // Check that both show Memcached protocol support
    const rowText = protocolRow.textContent.toLowerCase()
    // Both should mention memcached protocol
    const mentionsMemcachedProtocol = /memcached/i.test(rowText)
    expect(mentionsMemcachedProtocol).toBe(true)
  })

  // Test Case 4: Crash Recovery row shows MirDB has WAL + SSTable and Memcached has None
  it('shows Crash Recovery row with MirDB as WAL + SSTable and Memcached as None', () => {
    render(<ComparisonSection />)

    // Find the crash recovery row
    const crashRecoveryRow = screen.getByTestId('comparison-row-crash-recovery')
    expect(crashRecoveryRow).toBeInTheDocument()

    // Check MirDB shows WAL + SSTable
    const rowText = crashRecoveryRow.textContent
    const mirDBHasWAL = /wal/i.test(rowText)
    const mirDBHasSSTable = /sstable/i.test(rowText)
    expect(mirDBHasWAL && mirDBHasSSTable).toBe(true)

    // Check Memcached shows None
    const memcachedHasNone = /none/i.test(rowText)
    expect(memcachedHasNone).toBe(true)
  })

  // Test Case 5: Async I/O row shows MirDB is Tokio-based
  it('shows Async I/O row with MirDB as Tokio-based', () => {
    render(<ComparisonSection />)

    // Find the async I/O row
    const asyncIORow = screen.getByTestId('comparison-row-async-io')
    expect(asyncIORow).toBeInTheDocument()

    // Check MirDB mentions Tokio-based
    const rowText = asyncIORow.textContent
    const mentionsTokio = /tokio/i.test(rowText)
    expect(mentionsTokio).toBe(true)
  })

  // Test Case 6 (Integration): MirDB advantages are visually emphasized
  it('visually emphasizes MirDB advantages with highlighting, checkmarks, or color coding', () => {
    render(<ComparisonSection />)

    // Check for visual emphasis elements - checkmarks, highlighting, or advantage indicators
    const comparisonSection = screen.getByTestId('comparison-section')

    // Look for checkmark icons, advantage classes, or colored indicators
    const checkIcons = comparisonSection.querySelectorAll('[data-testid="advantage-indicator"], .advantage, .checkmark, svg')
    const advantageCells = comparisonSection.querySelectorAll('.advantage, .mirdb-advantage, [data-advantage="true"]')
    const coloredCells = comparisonSection.querySelectorAll('.highlight, .positive, .success, .text-green, .text-success')

    // At least one type of visual emphasis should be present
    const hasCheckIcons = checkIcons.length > 0
    const hasAdvantageCells = advantageCells.length > 0
    const hasColoredCells = coloredCells.length > 0

    // MirDB column cells with advantages should have some visual indicator
    const mirDBCells = comparisonSection.querySelectorAll('[data-testid*="mirdb-value"]')
    let hasVisualEmphasis = false

    mirDBCells.forEach(cell => {
      const hasAdvantageClass = cell.classList.contains('advantage') ||
                                cell.classList.contains('positive') ||
                                cell.classList.contains('highlight') ||
                                cell.closest('[data-advantage="true"]') !== null
      const hasCheckIcon = cell.querySelector('svg, .checkmark, [data-testid="checkmark-icon"]') !== null
      const hasGreenColor = window.getComputedStyle(cell).color.includes('22, 163, 74') ||
                           window.getComputedStyle(cell).color.includes('rgb(22, 163, 74)')

      if (hasAdvantageClass || hasCheckIcon || hasGreenColor) {
        hasVisualEmphasis = true
      }
    })

    expect(hasCheckIcons || hasAdvantageCells || hasColoredCells || hasVisualEmphasis).toBe(true)
  })
})
