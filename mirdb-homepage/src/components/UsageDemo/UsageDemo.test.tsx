/**
 * UsageDemo Component Tests.
 * Owner: Scenario 2 - Usage Demo Section
 *
 * Tests:
 * - Component renders without errors
 * - SET command example is displayed
 * - GET command example is displayed
 * - DELETE command example is displayed
 * - Code blocks have terminal-style styling
 */

import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { UsageDemo } from './UsageDemo'
import { CodeBlock } from './CodeBlock'

describe('UsageDemo Component', () => {
  it('renders without errors and contains code block elements', () => {
    render(<UsageDemo />)

    // Check for usage section
    const section = screen.getByLabelText('Usage Demo')
    expect(section).toBeInTheDocument()

    // Check for code blocks
    const codeBlocks = screen.getAllByTestId('code-block')
    expect(codeBlocks.length).toBeGreaterThanOrEqual(3)
  })

  it('displays SET command with format showing key, flags, exptime, bytes', () => {
    render(<UsageDemo />)

    // Check for SET command
    const content = screen.getByTestId('usage-demo-content')
    expect(content.textContent).toContain('set')
    expect(content.textContent).toContain('mykey')
    expect(content.textContent).toContain('0 0 5') // flags, exptime, bytes
    expect(content.textContent).toContain('STORED')
  })

  it('displays GET command with VALUE response format', () => {
    render(<UsageDemo />)

    const content = screen.getByTestId('usage-demo-content')
    expect(content.textContent).toContain('get')
    expect(content.textContent).toContain('VALUE')
    expect(content.textContent).toContain('mykey')
    expect(content.textContent).toContain('END')
  })

  it('displays DELETE command example', () => {
    render(<UsageDemo />)

    const content = screen.getByTestId('usage-demo-content')
    expect(content.textContent).toContain('delete')
    expect(content.textContent).toContain('DELETED')
  })

  it('has proper section structure with heading', () => {
    render(<UsageDemo />)

    const heading = screen.getByRole('heading', { name: /usage demo/i })
    expect(heading).toBeInTheDocument()
    expect(heading.tagName).toBe('H2')
  })
})

describe('CodeBlock Component', () => {
  const sampleCode = 'set mykey 0 0 5\nhello\nSTORED'

  it('renders with terminal-style dark background', () => {
    render(<CodeBlock code={sampleCode} />)

    const codeBlock = screen.getByTestId('code-block')
    expect(codeBlock).toHaveClass('bg-gray-900')
  })

  it('displays code content with monospace font', () => {
    render(<CodeBlock code={sampleCode} />)

    const preElement = document.querySelector('pre')
    expect(preElement).toHaveClass('font-mono')
  })

  it('has terminal window decorations (traffic lights)', () => {
    render(<CodeBlock code={sampleCode} />)

    // Check for traffic light buttons
    const codeBlock = screen.getByTestId('code-block')
    const trafficLights = codeBlock.querySelectorAll('.rounded-full')
    expect(trafficLights.length).toBeGreaterThanOrEqual(3)
  })

  it('shows copy button by default', () => {
    render(<CodeBlock code={sampleCode} />)

    const copyButton = screen.getByTestId('copy-button')
    expect(copyButton).toBeInTheDocument()
    expect(copyButton).toHaveTextContent(/copy/i)
  })

  it('hides copy button when showCopyButton is false', () => {
    render(<CodeBlock code={sampleCode} showCopyButton={false} />)

    const copyButton = screen.queryByTestId('copy-button')
    expect(copyButton).not.toBeInTheDocument()
  })

  it('displays the code content correctly', () => {
    render(<CodeBlock code={sampleCode} />)

    const codeContent = screen.getByTestId('code-content')
    expect(codeContent.textContent).toContain('set')
    expect(codeContent.textContent).toContain('mykey')
    expect(codeContent.textContent).toContain('STORED')
  })

  it('displays language indicator', () => {
    render(<CodeBlock code={sampleCode} language="shell" />)

    const codeBlock = screen.getByTestId('code-block')
    expect(codeBlock.textContent).toContain('shell')
  })

  it('has border for visual definition', () => {
    render(<CodeBlock code={sampleCode} />)

    const codeBlock = screen.getByTestId('code-block')
    expect(codeBlock).toHaveClass('border')
    expect(codeBlock).toHaveClass('border-gray-700')
  })
})
