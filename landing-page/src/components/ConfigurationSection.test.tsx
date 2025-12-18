import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import ConfigurationSection from './ConfigurationSection'

describe('ConfigurationSection', () => {
  // Test Case 1: Default port 12333 is displayed
  it('displays default port 12333', () => {
    render(<ConfigurationSection />)

    // Check for the port value - may appear multiple times (TOML and table)
    const portElements = screen.getAllByText(/12333/)
    expect(portElements.length).toBeGreaterThan(0)
    expect(portElements[0]).toBeInTheDocument()
  })

  // Test Case 2: Max LSM levels value (7) is displayed
  it('displays max LSM levels value (7)', () => {
    render(<ConfigurationSection />)

    // Check for max_level = 7
    const content = document.body.textContent || ''
    expect(content).toMatch(/max_level\s*=\s*7|Max.*LSM.*7|max_level.*7/i)
  })

  // Test Case 3: Memtable size (4MB) is displayed
  it('displays memtable size (4MB)', () => {
    render(<ConfigurationSection />)

    // Check for memtable size 4M or 4MB
    const content = document.body.textContent || ''
    expect(content).toMatch(/mem_table_max_size\s*=\s*"4M"|4MB|4M/i)
  })

  // Test Case 4: SSTable max size (100MB) is displayed
  it('displays SSTable max size (100MB)', () => {
    render(<ConfigurationSection />)

    // Check for SSTable max size 100M or 100MB
    const content = document.body.textContent || ''
    expect(content).toMatch(/sst_max_size\s*=\s*"100M"|100MB|100M/i)
  })

  // Test Case 5: Block size (4KB) is displayed
  it('displays block size (4KB)', () => {
    render(<ConfigurationSection />)

    // Check for block size 4K or 4KB
    const content = document.body.textContent || ''
    expect(content).toMatch(/block_size\s*=\s*"4K"|4KB|4K/i)
  })

  // Test Case 6: Configuration is shown in TOML format or structured table
  it('displays configuration in TOML format or structured table', () => {
    render(<ConfigurationSection />)

    // Check for TOML format indicators (key = value syntax) or table structure
    const codeBlock = screen.queryByTestId('config-code-block')
    const configTable = screen.queryByRole('table')

    // Should have either a code block with TOML format or a structured table
    const hasTomlFormat = codeBlock !== null
    const hasTable = configTable !== null

    // Also check for typical TOML assignment syntax in content
    const content = document.body.textContent || ''
    const hasTomlSyntax = content.includes('=') && (
      content.includes('addr') ||
      content.includes('max_level') ||
      content.includes('sst_max_size') ||
      content.includes('block_size')
    )

    expect(hasTomlFormat || hasTable || hasTomlSyntax).toBe(true)
  })

  // Additional test: section is properly labeled
  it('has a descriptive title for the configuration section', () => {
    render(<ConfigurationSection />)

    // Look for a heading that mentions configuration
    const heading = screen.getByRole('heading', { name: /config/i })
    expect(heading).toBeInTheDocument()
  })

  // Additional test: all default values are present together
  it('displays all required default configuration values', () => {
    render(<ConfigurationSection />)

    const content = document.body.textContent || ''

    // All required values from PRD should be present
    expect(content).toContain('12333')  // Port
    expect(content).toMatch(/7/)         // Max LSM levels
    expect(content).toMatch(/4M/i)       // Memtable size (will match 4M in 4MB)
    expect(content).toMatch(/100M/i)     // SSTable max size (will match 100M in 100MB)
    expect(content).toMatch(/4K/i)       // Block size (will match 4K in 4KB)
  })
})
