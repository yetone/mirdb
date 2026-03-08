import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { GettingStarted } from './GettingStarted'
import { CodeBlock } from './CodeBlock'
import { steps } from '../../data/gettingStarted'

describe('GettingStarted Component', () => {
  // Test Case 1: GettingStarted component renders without errors
  it('renders without errors', () => {
    expect(() => render(<GettingStarted />)).not.toThrow()
  })

  // Test Case 2: Check for installation step
  it('contains step for installing/running MirDB', () => {
    render(<GettingStarted />)
    expect(screen.getByText('Install and Run MirDB')).toBeInTheDocument()
    // Verify installation commands are present in the code blocks (text may be split by highlighting spans)
    const section = screen.getByRole('region', { name: /getting started/i })
    expect(section.textContent).toContain('cargo build')
    expect(section.textContent).toContain('cargo run')
  })

  // Test Case 3: Check for connection step
  it('contains step showing client connection command', () => {
    render(<GettingStarted />)
    expect(screen.getByText('Connect with a Memcached Client')).toBeInTheDocument()
    // Verify connection commands are present
    const section = screen.getByRole('region', { name: /getting started/i })
    expect(section.textContent).toContain('localhost:12333')
  })

  // Test Case 4: Check for set operation
  it('shows set command example', () => {
    render(<GettingStarted />)
    expect(screen.getByText('Set a Value')).toBeInTheDocument()
    // Verify set command is shown (text may be split by highlighting spans)
    const section = screen.getByRole('region', { name: /getting started/i })
    expect(section.textContent).toContain('set mykey')
  })

  // Test Case 5: Check for get operation
  it('shows get command example', () => {
    render(<GettingStarted />)
    expect(screen.getByText('Get a Value')).toBeInTheDocument()
    // Verify get command is shown (text may be split by highlighting spans)
    const section = screen.getByRole('region', { name: /getting started/i })
    expect(section.textContent).toContain('get mykey')
  })

  // Test Case 6: Check for delete operation
  it('shows delete command example', () => {
    render(<GettingStarted />)
    expect(screen.getByText('Delete a Value')).toBeInTheDocument()
    // Verify delete command is shown (text may be split by highlighting spans)
    const section = screen.getByRole('region', { name: /getting started/i })
    expect(section.textContent).toContain('delete mykey')
  })

  // Test Case 8: Verify step numbering
  it('has steps that are clearly numbered', () => {
    render(<GettingStarted />)
    const stepNumbers = screen.getAllByTestId('step-number')
    expect(stepNumbers).toHaveLength(5)
    // Verify numbers 1-5 are present
    expect(stepNumbers[0]).toHaveTextContent('1')
    expect(stepNumbers[1]).toHaveTextContent('2')
    expect(stepNumbers[2]).toHaveTextContent('3')
    expect(stepNumbers[3]).toHaveTextContent('4')
    expect(stepNumbers[4]).toHaveTextContent('5')
  })

  it('renders all steps from data', () => {
    render(<GettingStarted />)
    const stepItems = screen.getAllByTestId('step-item')
    expect(stepItems).toHaveLength(steps.length)
  })

  it('renders the section with proper heading', () => {
    render(<GettingStarted />)
    expect(screen.getByRole('heading', { name: /getting started/i })).toBeInTheDocument()
  })

  it('has accessible section landmark', () => {
    render(<GettingStarted />)
    const section = screen.getByRole('region', { name: /getting started/i })
    expect(section).toBeInTheDocument()
  })

  it('contains an ordered list for steps', () => {
    render(<GettingStarted />)
    const stepsList = screen.getByTestId('steps-list')
    expect(stepsList.tagName.toLowerCase()).toBe('ol')
  })

  it('each step has screen reader accessible step number', () => {
    render(<GettingStarted />)
    // Check for sr-only text with step numbers
    const srOnlyTexts = document.querySelectorAll('.sr-only')
    expect(srOnlyTexts.length).toBeGreaterThan(0)
  })
})

describe('CodeBlock Component', () => {
  const mockCode = `set mykey 0 0 5
hello
STORED`

  it('renders code content', () => {
    render(<CodeBlock code={mockCode} />)
    expect(screen.getByText(/set/)).toBeInTheDocument()
    expect(screen.getByText(/STORED/)).toBeInTheDocument()
  })

  it('renders as a pre element with code', () => {
    render(<CodeBlock code={mockCode} />)
    const preElement = document.querySelector('pre')
    expect(preElement).toBeInTheDocument()
    const codeElement = document.querySelector('code')
    expect(codeElement).toBeInTheDocument()
  })

  it('has data-highlighted attribute indicating syntax highlighting', () => {
    render(<CodeBlock code={mockCode} />)
    const preElement = document.querySelector('pre')
    expect(preElement).toHaveAttribute('data-highlighted', 'true')
  })

  it('has data-language attribute', () => {
    render(<CodeBlock code="echo hello" language="bash" />)
    const preElement = document.querySelector('pre')
    expect(preElement).toHaveAttribute('data-language', 'bash')
  })

  it('applies syntax highlighting to memcached commands', () => {
    render(<CodeBlock code={mockCode} />)
    const codeBlock = screen.getByTestId('code-block')
    // Check that the code has been highlighted (contains span elements)
    const codeElement = codeBlock.querySelector('code')
    expect(codeElement?.innerHTML).toContain('<span')
  })

  it('highlights comments starting with #', () => {
    const codeWithComment = '# This is a comment\necho hello'
    render(<CodeBlock code={codeWithComment} />)
    const codeBlock = screen.getByTestId('code-block')
    const codeElement = codeBlock.querySelector('code')
    // Comments should be wrapped in a span with text-slate-500 class
    expect(codeElement?.innerHTML).toContain('text-slate-500')
  })

  it('has a copy button', () => {
    render(<CodeBlock code={mockCode} />)
    const copyButton = screen.getByRole('button', { name: /copy/i })
    expect(copyButton).toBeInTheDocument()
  })

  it('copy button copies code to clipboard when clicked', async () => {
    const mockWriteText = vi.fn()
    Object.assign(navigator, {
      clipboard: {
        writeText: mockWriteText,
      },
    })

    render(<CodeBlock code={mockCode} />)
    const copyButton = screen.getByRole('button', { name: /copy/i })
    copyButton.click()

    expect(mockWriteText).toHaveBeenCalledWith(mockCode)
  })
})

describe('GettingStarted Data', () => {
  it('contains at least 3 steps', () => {
    expect(steps.length).toBeGreaterThanOrEqual(3)
  })

  it('each step has required properties', () => {
    steps.forEach((step) => {
      expect(step).toHaveProperty('number')
      expect(step).toHaveProperty('title')
      expect(step).toHaveProperty('content')
      expect(typeof step.number).toBe('number')
      expect(typeof step.title).toBe('string')
      expect(step.title.length).toBeGreaterThan(0)
    })
  })

  it('steps are numbered sequentially', () => {
    steps.forEach((step, index) => {
      expect(step.number).toBe(index + 1)
    })
  })

  it('includes install step with cargo command', () => {
    const installStep = steps.find((s) => s.title.toLowerCase().includes('install'))
    expect(installStep).toBeDefined()
    expect(installStep?.code).toContain('cargo')
  })

  it('includes connection step with localhost:12333', () => {
    const connectStep = steps.find((s) => s.title.toLowerCase().includes('connect'))
    expect(connectStep).toBeDefined()
    expect(connectStep?.code).toContain('localhost:12333')
  })

  it('includes set operation step', () => {
    const setStep = steps.find((s) => s.title.toLowerCase().includes('set'))
    expect(setStep).toBeDefined()
    expect(setStep?.code).toContain('set')
  })

  it('includes get operation step', () => {
    const getStep = steps.find((s) => s.title.toLowerCase().includes('get'))
    expect(getStep).toBeDefined()
    expect(getStep?.code).toContain('get')
  })

  it('includes delete operation step', () => {
    const deleteStep = steps.find((s) => s.title.toLowerCase().includes('delete'))
    expect(deleteStep).toBeDefined()
    expect(deleteStep?.code).toContain('delete')
  })
})
