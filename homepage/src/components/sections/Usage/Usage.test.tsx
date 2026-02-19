/**
 * Usage Section Tests
 * Owner: Scenario 5 - Usage Demonstration
 *
 * Test cases:
 * 1. Render Usage component - Component renders with section heading and usage demonstration
 * 2. Check for usage.gif image - Image element with src pointing to usage.gif is rendered
 * 4. Render CommandTable component - Table renders with all 10 supported Memcached commands
 * 5. Check 'get' command in table - Row shows 'get <key1> <key2> ...' with description
 * 6. Check 'set' command in table - Row shows 'set <key> <flags> <ttl> <bytes>' with description
 * 7. Check 'delete' command in table - Row shows 'delete <key>' with description
 * 9. Check configuration code block - TOML configuration example is displayed with addr, work_dir, etc.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { Usage } from './Usage'
import { CommandTable } from './CommandTable'
import { commands } from '@/data/commands'

// Mock navigator.clipboard
const mockClipboard = {
  writeText: vi.fn(),
}

beforeEach(() => {
  vi.clearAllMocks()
  Object.assign(navigator, {
    clipboard: mockClipboard,
  })
  mockClipboard.writeText.mockResolvedValue(undefined)
})

describe('Usage', () => {
  // Test case 1: Render Usage component with section heading and usage demonstration
  it('renders with section heading and usage demonstration', () => {
    render(<Usage />)

    // Check section heading
    const heading = screen.getByRole('heading', { name: 'Usage' })
    expect(heading).toBeInTheDocument()

    // Check section has correct id for navigation
    const section = document.getElementById('usage')
    expect(section).toBeInTheDocument()

    // Check aria-labelledby for accessibility
    expect(section).toHaveAttribute('aria-labelledby', 'usage-heading')
  })

  // Test case 2: Check for usage.gif image
  it('renders image element with src pointing to usage.gif', () => {
    render(<Usage />)

    const usageGif = screen.getByTestId('usage-gif')
    expect(usageGif).toBeInTheDocument()
    expect(usageGif).toHaveAttribute('src', '/assets/usage.gif')
    expect(usageGif).toHaveAttribute('alt')
    expect(usageGif.getAttribute('alt')).toContain('MirDB')
    expect(usageGif.getAttribute('alt')).toContain('demonstration')
  })

  it('renders the description text', () => {
    render(<Usage />)

    expect(screen.getByText(/MirDB is fully compatible/i)).toBeInTheDocument()
    expect(screen.getByText(/Memcached text protocol/i)).toBeInTheDocument()
  })

  it('renders "Supported Commands" subheading', () => {
    render(<Usage />)

    const subheading = screen.getByRole('heading', { name: /Supported Commands/i })
    expect(subheading).toBeInTheDocument()
  })

  it('renders "Command Examples" subheading', () => {
    render(<Usage />)

    const subheading = screen.getByRole('heading', { name: /Command Examples/i })
    expect(subheading).toBeInTheDocument()
  })

  it('renders "Default Configuration" subheading', () => {
    render(<Usage />)

    const subheading = screen.getByRole('heading', { name: /Default Configuration/i })
    expect(subheading).toBeInTheDocument()
  })

  // Test case 9: Check configuration code block
  it('displays TOML configuration example with addr, work_dir, etc.', () => {
    render(<Usage />)

    const configBlock = screen.getByTestId('config-block')
    expect(configBlock).toBeInTheDocument()

    // Check for TOML language indicator
    expect(screen.getByText('toml')).toBeInTheDocument()

    // Check for key configuration values
    expect(screen.getByText(/addr = "0\.0\.0\.0:12333"/)).toBeInTheDocument()
    expect(screen.getByText(/work_dir = "\/tmp\/mirdb"/)).toBeInTheDocument()
    expect(screen.getByText(/max_level = 7/)).toBeInTheDocument()
    expect(screen.getByText(/sst_max_size = "100M"/)).toBeInTheDocument()
    expect(screen.getByText(/mem_table_max_size = "4M"/)).toBeInTheDocument()
  })

  it('renders example command blocks', () => {
    render(<Usage />)

    // Check for set example
    const setExample = screen.getByTestId('example-set-example')
    expect(setExample).toBeInTheDocument()

    // Check for get example
    const getExample = screen.getByTestId('example-get-example')
    expect(getExample).toBeInTheDocument()

    // Check for delete example
    const deleteExample = screen.getByTestId('example-delete-example')
    expect(deleteExample).toBeInTheDocument()

    // Check for info example
    const infoExample = screen.getByTestId('example-info-example')
    expect(infoExample).toBeInTheDocument()
  })

  it('renders copy buttons for code blocks', () => {
    render(<Usage />)

    const copyButtons = screen.getAllByRole('button', { name: /copy/i })
    expect(copyButtons.length).toBeGreaterThan(0)
  })

  it('copy button copies text to clipboard and shows success state', async () => {
    render(<Usage />)

    const copyButton = screen.getByTestId('config-block-copy-button')
    expect(copyButton).toBeInTheDocument()

    fireEvent.click(copyButton)

    await waitFor(() => {
      expect(mockClipboard.writeText).toHaveBeenCalled()
    })

    await waitFor(() => {
      expect(screen.getByText('Copied!')).toBeInTheDocument()
    })
  })
})

describe('CommandTable', () => {
  // Test case 4: Table renders with all 10 supported Memcached commands
  it('renders table with all 10 supported Memcached commands', () => {
    render(<CommandTable commands={commands} />)

    const table = screen.getByTestId('command-table')
    expect(table).toBeInTheDocument()

    // Check for all 10 commands
    const commandRows = commands.map((cmd) =>
      screen.getByTestId(`command-row-${cmd.name}`)
    )
    expect(commandRows).toHaveLength(10)
  })

  it('renders table headers correctly', () => {
    render(<CommandTable commands={commands} />)

    expect(screen.getByRole('columnheader', { name: 'Command' })).toBeInTheDocument()
    expect(screen.getByRole('columnheader', { name: 'Syntax' })).toBeInTheDocument()
    expect(screen.getByRole('columnheader', { name: 'Description' })).toBeInTheDocument()
  })

  // Test case 5: Check 'get' command in table
  it('shows get command with correct syntax and description', () => {
    render(<CommandTable commands={commands} />)

    const getRow = screen.getByTestId('command-row-get')
    expect(getRow).toBeInTheDocument()

    // Check command name
    expect(screen.getByText('get')).toBeInTheDocument()

    // Check syntax
    expect(screen.getByText('get <key1> <key2> ...')).toBeInTheDocument()

    // Check description contains key information
    expect(screen.getByText(/Retrieve values by key/i)).toBeInTheDocument()
  })

  // Test case 6: Check 'set' command in table
  it('shows set command with correct syntax and description', () => {
    render(<CommandTable commands={commands} />)

    const setRow = screen.getByTestId('command-row-set')
    expect(setRow).toBeInTheDocument()

    // Check command name
    expect(screen.getByText('set')).toBeInTheDocument()

    // Check syntax
    expect(screen.getByText('set <key> <flags> <ttl> <bytes>')).toBeInTheDocument()

    // Check description
    expect(screen.getByText(/Set a key-value pair/i)).toBeInTheDocument()
  })

  // Test case 7: Check 'delete' command in table
  it('shows delete command with correct syntax and description', () => {
    render(<CommandTable commands={commands} />)

    const deleteRow = screen.getByTestId('command-row-delete')
    expect(deleteRow).toBeInTheDocument()

    // Check command name
    expect(screen.getByText('delete')).toBeInTheDocument()

    // Check syntax
    expect(screen.getByText('delete <key>')).toBeInTheDocument()

    // Check description
    expect(screen.getByText(/Delete a key/i)).toBeInTheDocument()
  })

  it('shows gets command with CAS support description', () => {
    render(<CommandTable commands={commands} />)

    const getsRow = screen.getByTestId('command-row-gets')
    expect(getsRow).toBeInTheDocument()
    expect(screen.getByText('gets <key1> <key2> ...')).toBeInTheDocument()
    expect(screen.getByText(/CAS/i)).toBeInTheDocument()
  })

  it('shows add command', () => {
    render(<CommandTable commands={commands} />)

    const addRow = screen.getByTestId('command-row-add')
    expect(addRow).toBeInTheDocument()
    expect(screen.getByText('add <key> <flags> <ttl> <bytes>')).toBeInTheDocument()
  })

  it('shows replace command', () => {
    render(<CommandTable commands={commands} />)

    const replaceRow = screen.getByTestId('command-row-replace')
    expect(replaceRow).toBeInTheDocument()
    expect(screen.getByText('replace <key> <flags> <ttl> <bytes>')).toBeInTheDocument()
  })

  it('shows append command', () => {
    render(<CommandTable commands={commands} />)

    const appendRow = screen.getByTestId('command-row-append')
    expect(appendRow).toBeInTheDocument()
    expect(screen.getByText('append <key> <flags> <ttl> <bytes>')).toBeInTheDocument()
  })

  it('shows prepend command', () => {
    render(<CommandTable commands={commands} />)

    const prependRow = screen.getByTestId('command-row-prepend')
    expect(prependRow).toBeInTheDocument()
    expect(screen.getByText('prepend <key> <flags> <ttl> <bytes>')).toBeInTheDocument()
  })

  it('shows info command', () => {
    render(<CommandTable commands={commands} />)

    const infoRow = screen.getByTestId('command-row-info')
    expect(infoRow).toBeInTheDocument()
    expect(screen.getByText(/Query database information/i)).toBeInTheDocument()
  })

  it('shows major_compaction command', () => {
    render(<CommandTable commands={commands} />)

    const compactionRow = screen.getByTestId('command-row-major_compaction')
    expect(compactionRow).toBeInTheDocument()
    expect(screen.getByText(/Trigger a full compaction/i)).toBeInTheDocument()
  })

  it('renders table wrapper for responsive scrolling', () => {
    render(<CommandTable commands={commands} />)

    const wrapper = screen.getByTestId('command-table-wrapper')
    expect(wrapper).toBeInTheDocument()
  })
})
