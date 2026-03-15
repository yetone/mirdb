import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import Architecture from '../../../src/components/sections/Architecture'

describe('Architecture', () => {
  // Test Case 1: Component renders with architecture content
  it('renders with architecture content', () => {
    render(<Architecture />)

    const section = screen.getByRole('region', { name: /architecture/i })
    expect(section).toBeInTheDocument()

    const heading = screen.getByRole('heading', { level: 2, name: /architecture overview/i })
    expect(heading).toBeInTheDocument()
  })

  // Test Case 2: Content includes 'LSM' or 'Log-Structured Merge'
  it('includes LSM tree content', () => {
    render(<Architecture />)

    const section = screen.getByRole('region', { name: /architecture/i })
    const textContent = section.textContent?.toLowerCase() || ''

    const hasLSM = textContent.includes('lsm') || textContent.includes('log-structured merge')
    expect(hasLSM).toBe(true)
  })

  // Test Case 3: Content includes 'skip-list' or 'memtable'
  it('includes skip-list or memtable content', () => {
    render(<Architecture />)

    const section = screen.getByRole('region', { name: /architecture/i })
    const textContent = section.textContent?.toLowerCase() || ''

    const hasSkipList = textContent.includes('skip-list') || textContent.includes('skip list') || textContent.includes('memtable')
    expect(hasSkipList).toBe(true)
  })

  // Test Case 4: Content includes 'compaction'
  it('includes compaction content', () => {
    render(<Architecture />)

    const section = screen.getByRole('region', { name: /architecture/i })
    const textContent = section.textContent?.toLowerCase() || ''

    const hasCompaction = textContent.includes('compaction')
    expect(hasCompaction).toBe(true)
  })

  // Test Case 5: Architecture diagram or visual representation is present
  it('includes architecture diagram or visual representation', () => {
    render(<Architecture />)

    const diagram = screen.getByTestId('architecture-diagram')
    expect(diagram).toBeInTheDocument()

    // Also verify there's an SVG diagram
    const svg = screen.getByTestId('architecture-svg')
    expect(svg).toBeInTheDocument()
  })

  // Additional tests for comprehensive coverage

  it('renders architecture layers with descriptions', () => {
    render(<Architecture />)

    const layers = screen.getAllByTestId('architecture-layer')
    expect(layers.length).toBeGreaterThanOrEqual(3) // At least 3 architecture layers
  })

  it('has proper accessibility attributes', () => {
    render(<Architecture />)

    const section = screen.getByRole('region', { name: /architecture/i })
    expect(section).toHaveAttribute('aria-labelledby', 'architecture-heading')

    // Diagram has aria-label for screen readers
    const diagram = screen.getByTestId('architecture-diagram')
    expect(diagram).toHaveAttribute('role', 'img')
    expect(diagram).toHaveAttribute('aria-label')
  })

  it('displays both minor and major compaction information', () => {
    render(<Architecture />)

    const section = screen.getByRole('region', { name: /architecture/i })
    const textContent = section.textContent?.toLowerCase() || ''

    expect(textContent).toContain('minor compaction')
    expect(textContent).toContain('major compaction')
  })

  it('renders architecture benefits section', () => {
    render(<Architecture />)

    const benefits = screen.getAllByTestId('architecture-benefit')
    expect(benefits.length).toBeGreaterThanOrEqual(2)
  })

  it('mentions SSTable in architecture description', () => {
    render(<Architecture />)

    const section = screen.getByRole('region', { name: /architecture/i })
    const textContent = section.textContent?.toLowerCase() || ''

    expect(textContent).toContain('sstable')
  })
})
