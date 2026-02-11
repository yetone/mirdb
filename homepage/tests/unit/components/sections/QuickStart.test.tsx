/**
 * Unit tests for QuickStart component.
 * Owner: Scenario 5 - Quick Start Section
 */

import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { QuickStart } from '../../../../src/components/sections/QuickStart'

describe('QuickStart Component', () => {
  // Test Case 1: Section contains heading 'Quick Start' or 'Get Started' or 'Installation'
  it('should render section with Quick Start heading', () => {
    render(<QuickStart />)

    const heading = screen.getByRole('heading', { level: 2 })
    expect(heading).toBeInTheDocument()
    expect(heading).toHaveTextContent(/Quick Start/i)
  })

  // Test Case 2: Code block contains 'cargo' command for installing mirdb
  it('should render installation code block with cargo command', () => {
    render(<QuickStart />)

    // Check for cargo install command
    const cargoText = screen.getByText(/cargo install mirdb-server/i)
    expect(cargoText).toBeInTheDocument()
  })

  // Test Case 3: Code block shows memcached commands (set, get) or telnet/nc connection example
  it('should render usage code block with memcached commands', () => {
    render(<QuickStart />)

    // Check for memcached commands
    const setCommand = screen.getByText(/set mykey/i)
    expect(setCommand).toBeInTheDocument()

    const getCommand = screen.getByText(/get mykey/i)
    expect(getCommand).toBeInTheDocument()

    // Check for telnet connection example
    const telnetCommand = screen.getByText(/telnet localhost 11211/i)
    expect(telnetCommand).toBeInTheDocument()
  })

  // Test Case 5: Code blocks have syntax highlighting and monospace font
  it('should render code blocks with proper styling classes', () => {
    render(<QuickStart />)

    // Check for code elements with language class
    const codeElements = document.querySelectorAll('.code-block__code')
    expect(codeElements.length).toBeGreaterThan(0)

    // Check that code elements have language class
    codeElements.forEach(codeElement => {
      expect(codeElement.className).toMatch(/language-/)
    })
  })

  // Test Case 6: Code blocks are accessible via keyboard, copy button has aria-label
  it('should render accessible copy buttons with aria-label', () => {
    render(<QuickStart />)

    // Check for copy buttons with proper aria-label
    const copyButtons = screen.getAllByRole('button', { name: /copy code to clipboard/i })
    expect(copyButtons.length).toBeGreaterThan(0)

    // Each copy button should have proper accessibility attributes
    copyButtons.forEach(button => {
      expect(button).toHaveAttribute('aria-label')
    })
  })

  it('should render quick-start section with correct semantic structure', () => {
    render(<QuickStart />)

    // Check that the section exists with correct id
    const section = document.querySelector('section.quick-start')
    expect(section).toBeInTheDocument()
    expect(section).toHaveAttribute('id', 'quick-start')
  })

  it('should render description text', () => {
    render(<QuickStart />)

    const description = screen.getByText(/Get MirDB up and running/i)
    expect(description).toBeInTheDocument()
  })

  it('should render code blocks with titles', () => {
    render(<QuickStart />)

    // Check for Installation title
    const installationTitle = screen.getByText('Installation')
    expect(installationTitle).toBeInTheDocument()

    // Check for Usage title
    const usageTitle = screen.getByText('Usage')
    expect(usageTitle).toBeInTheDocument()
  })
})
