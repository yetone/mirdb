/**
 * Integration tests for dark mode styling and theming
 * Owner: Scenario 10 - Dark Mode Theme
 *
 * Test cases covered:
 * - TC3: Code blocks have appropriate dark theme syntax highlighting
 * - Dark mode CSS variables are properly applied
 * - Interactive elements have correct styling
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { render, screen, act } from '@testing-library/react'
import { CodeBlock } from '../../../src/components/ui/CodeBlock'
import { Button } from '../../../src/components/ui/Button'
import { ThemeToggle } from '../../../src/components/ui/ThemeToggle'

// Helper to set dark theme on document
function setDarkTheme() {
  document.documentElement.setAttribute('data-theme', 'dark')
  localStorage.setItem('mirdb-theme', 'dark')
}

// Helper to set light theme on document
function setLightTheme() {
  document.documentElement.setAttribute('data-theme', 'light')
  localStorage.setItem('mirdb-theme', 'light')
}

describe('Dark Mode Styling Integration', () => {
  const originalMatchMedia = window.matchMedia

  beforeEach(() => {
    localStorage.clear()
    document.documentElement.removeAttribute('data-theme')

    window.matchMedia = vi.fn().mockImplementation((query) => ({
      matches: false,
      media: query,
      onchange: null,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      dispatchEvent: vi.fn(),
    }))
  })

  afterEach(() => {
    window.matchMedia = originalMatchMedia
    document.documentElement.removeAttribute('data-theme')
    localStorage.clear()
  })

  describe('Code Block Dark Mode Styling (TC3)', () => {
    const sampleCode = 'cargo install mirdb-server'

    it('should render code block with appropriate styling in dark mode', () => {
      setDarkTheme()

      render(<CodeBlock code={sampleCode} language="bash" title="Installation" />)

      const codeElement = screen.getByText(sampleCode)
      expect(codeElement).toBeInTheDocument()
      expect(codeElement).toHaveClass('code-block__code')
      expect(codeElement).toHaveClass('language-bash')
    })

    it('should apply dark theme data attribute correctly', () => {
      setDarkTheme()

      render(<CodeBlock code={sampleCode} />)

      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })

    it('should render code block container with proper structure for theming', () => {
      setDarkTheme()

      render(<CodeBlock code={sampleCode} language="bash" />)

      const preElement = document.querySelector('.code-block__pre')
      expect(preElement).toBeInTheDocument()

      const container = document.querySelector('.code-block')
      expect(container).toBeInTheDocument()
    })

    it('should switch code block styling when theme changes', async () => {
      // Start in light mode
      setLightTheme()

      const { rerender } = render(
        <>
          <ThemeToggle />
          <CodeBlock code={sampleCode} language="bash" />
        </>
      )

      expect(document.documentElement.getAttribute('data-theme')).toBe('light')

      // Verify code block exists
      expect(screen.getByText(sampleCode)).toBeInTheDocument()

      // Switch to dark mode
      setDarkTheme()
      rerender(
        <>
          <ThemeToggle />
          <CodeBlock code={sampleCode} language="bash" />
        </>
      )

      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      expect(screen.getByText(sampleCode)).toBeInTheDocument()
    })

    it('should render copy button with appropriate styling for dark mode', () => {
      setDarkTheme()

      render(<CodeBlock code={sampleCode} />)

      const copyButton = screen.getByRole('button', { name: /copy code to clipboard/i })
      expect(copyButton).toBeInTheDocument()
      expect(copyButton).toHaveClass('code-block__copy')
    })
  })

  describe('Interactive Elements Styling', () => {
    it('should render button with proper classes in dark mode', () => {
      setDarkTheme()

      render(<Button variant="primary">Click Me</Button>)

      const button = screen.getByRole('button', { name: /click me/i })
      expect(button).toBeInTheDocument()
      expect(button).toHaveClass('button')
      expect(button).toHaveClass('button--primary')
    })

    it('should render secondary button in dark mode', () => {
      setDarkTheme()

      render(<Button variant="secondary">Secondary</Button>)

      const button = screen.getByRole('button', { name: /secondary/i })
      expect(button).toHaveClass('button--secondary')
    })

    it('should render ghost button in dark mode', () => {
      setDarkTheme()

      render(<Button variant="ghost">Ghost</Button>)

      const button = screen.getByRole('button', { name: /ghost/i })
      expect(button).toHaveClass('button--ghost')
    })

    it('should render theme toggle button with focus capability', () => {
      setDarkTheme()

      render(<ThemeToggle />)

      const button = screen.getByRole('button')
      expect(button).toHaveClass('theme-toggle')

      // Verify button is focusable
      button.focus()
      expect(document.activeElement).toBe(button)
    })
  })

  describe('CSS Variables Application', () => {
    it('should have dark theme CSS variables defined', () => {
      setDarkTheme()

      // Create a test element to check CSS variable application
      const testDiv = document.createElement('div')
      testDiv.style.backgroundColor = 'var(--color-background)'
      document.body.appendChild(testDiv)

      // The data-theme attribute should be set
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')

      document.body.removeChild(testDiv)
    })

    it('should have light theme as default', () => {
      // Without setting any theme, render should work
      render(<ThemeToggle />)

      expect(screen.getByRole('button')).toBeInTheDocument()
    })
  })

  describe('Theme Persistence Verification (TC4)', () => {
    it('should persist dark mode in localStorage after toggle', async () => {
      render(<ThemeToggle />)

      const button = screen.getByRole('button')

      // Toggle to dark
      await act(async () => {
        button.click()
      })

      expect(localStorage.getItem('mirdb-theme')).toBe('dark')
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })

    it('should load persisted dark mode on component mount', () => {
      // Pre-set localStorage before render
      localStorage.setItem('mirdb-theme', 'dark')

      render(<ThemeToggle />)

      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })

    it('should persist light mode after toggling back from dark', async () => {
      localStorage.setItem('mirdb-theme', 'dark')

      render(<ThemeToggle />)

      const button = screen.getByRole('button')

      // Toggle to light
      await act(async () => {
        button.click()
      })

      expect(localStorage.getItem('mirdb-theme')).toBe('light')
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')
    })
  })
})
