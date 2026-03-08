import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { UsageExample } from './UsageExample'

describe('UsageExample', () => {
  it('renders without errors', () => {
    render(<UsageExample />)

    // Check that the section title is rendered
    expect(screen.getByRole('heading', { name: /see it in action/i })).toBeInTheDocument()
  })

  it('contains code block or pre element', () => {
    render(<UsageExample />)

    // Check for pre element with code
    const codeBlock = screen.getByTestId('usage-example-code')
    expect(codeBlock).toBeInTheDocument()
    expect(codeBlock.tagName).toBe('PRE')

    // Check that code element is inside pre
    const codeElement = codeBlock.querySelector('code')
    expect(codeElement).toBeInTheDocument()
  })

  it('contains set memcached command', () => {
    render(<UsageExample />)

    const codeBlock = screen.getByTestId('usage-example-code')

    // Check for 'set' command in the code example
    expect(codeBlock.textContent).toContain('set')
    expect(codeBlock.textContent).toContain('mykey')
    expect(codeBlock.textContent).toContain('STORED')
  })

  it('contains get memcached command', () => {
    render(<UsageExample />)

    const codeBlock = screen.getByTestId('usage-example-code')

    // Check for 'get' command in the code example
    expect(codeBlock.textContent).toContain('get')
    expect(codeBlock.textContent).toContain('VALUE mykey')
    expect(codeBlock.textContent).toContain('END')
  })

  it('has proper section structure and accessibility', () => {
    render(<UsageExample />)

    // Check for section with proper ID
    const section = document.getElementById('usage-example')
    expect(section).toBeInTheDocument()
    expect(section?.tagName).toBe('SECTION')

    // Check for aria-labelledby
    expect(section).toHaveAttribute('aria-labelledby', 'usage-example-title')
  })

  it('displays memcached protocol format comment', () => {
    render(<UsageExample />)

    const codeBlock = screen.getByTestId('usage-example-code')

    // Check for format explanation
    expect(codeBlock.textContent).toContain('set <key> <flags> <ttl> <bytes>')
  })

  it('shows delete command', () => {
    render(<UsageExample />)

    const codeBlock = screen.getByTestId('usage-example-code')

    // Check for delete command
    expect(codeBlock.textContent).toContain('delete')
    expect(codeBlock.textContent).toContain('DELETED')
  })

  it('has syntax highlighting CSS classes applied', () => {
    render(<UsageExample />)

    const codeBlock = screen.getByTestId('usage-example-code')

    // Check that the pre element has the syntax-highlighted class
    expect(codeBlock).toHaveClass('syntax-highlighted')

    // Check for syntax highlighting elements
    const commands = codeBlock.querySelectorAll('.syntax-command')
    expect(commands.length).toBeGreaterThan(0)

    const responses = codeBlock.querySelectorAll('.syntax-response')
    expect(responses.length).toBeGreaterThan(0)
  })
})
