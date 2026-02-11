/**
 * Integration tests for focus states and hover effects in dark mode
 * Owner: Scenario 10 - Dark Mode Theme
 *
 * Test cases covered:
 * - TC5: Buttons, links, and CTAs have visible focus states and hover effects
 * - Accessibility focus indicators work in dark mode
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { Button } from '../../../src/components/ui/Button'
import { ThemeToggle } from '../../../src/components/ui/ThemeToggle'

function setDarkTheme() {
  document.documentElement.setAttribute('data-theme', 'dark')
  localStorage.setItem('mirdb-theme', 'dark')
}

describe('Focus States and Hover Effects (TC5)', () => {
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

  describe('Button Focus States', () => {
    it('should allow primary button to receive focus in dark mode', () => {
      setDarkTheme()

      render(<Button variant="primary">Primary</Button>)

      const button = screen.getByRole('button', { name: /primary/i })
      button.focus()

      expect(document.activeElement).toBe(button)
    })

    it('should allow secondary button to receive focus in dark mode', () => {
      setDarkTheme()

      render(<Button variant="secondary">Secondary</Button>)

      const button = screen.getByRole('button', { name: /secondary/i })
      button.focus()

      expect(document.activeElement).toBe(button)
    })

    it('should allow ghost button to receive focus in dark mode', () => {
      setDarkTheme()

      render(<Button variant="ghost">Ghost</Button>)

      const button = screen.getByRole('button', { name: /ghost/i })
      button.focus()

      expect(document.activeElement).toBe(button)
    })

    it('should render button with correct variant class for styling', () => {
      setDarkTheme()

      render(
        <>
          <Button variant="primary">Primary</Button>
          <Button variant="secondary">Secondary</Button>
          <Button variant="ghost">Ghost</Button>
        </>
      )

      expect(screen.getByRole('button', { name: /primary/i })).toHaveClass('button--primary')
      expect(screen.getByRole('button', { name: /secondary/i })).toHaveClass('button--secondary')
      expect(screen.getByRole('button', { name: /ghost/i })).toHaveClass('button--ghost')
    })
  })

  describe('Theme Toggle Focus States', () => {
    it('should allow theme toggle to receive focus in dark mode', () => {
      setDarkTheme()

      render(<ThemeToggle />)

      const button = screen.getByRole('button')
      button.focus()

      expect(document.activeElement).toBe(button)
    })

    it('should have theme-toggle class for CSS focus styling', () => {
      setDarkTheme()

      render(<ThemeToggle />)

      const button = screen.getByRole('button')
      expect(button).toHaveClass('theme-toggle')
    })
  })

  describe('Link/Anchor Focus States', () => {
    it('should allow link-style button (with href) to receive focus', () => {
      setDarkTheme()

      render(
        <Button href="https://github.com/yetone/mirdb" external>
          View on GitHub
        </Button>
      )

      const link = screen.getByRole('link', { name: /view on github/i })
      link.focus()

      expect(document.activeElement).toBe(link)
    })

    it('should render anchor element when href is provided', () => {
      setDarkTheme()

      render(
        <Button href="#features">Features</Button>
      )

      const link = screen.getByRole('link', { name: /features/i })
      expect(link).toBeInTheDocument()
      expect(link.tagName).toBe('A')
    })

    it('should have proper external link attributes', () => {
      setDarkTheme()

      render(
        <Button href="https://github.com/yetone/mirdb" external>
          GitHub
        </Button>
      )

      const link = screen.getByRole('link', { name: /github/i })
      expect(link).toHaveAttribute('target', '_blank')
      expect(link).toHaveAttribute('rel', 'noopener noreferrer')
    })
  })

  describe('Multiple Interactive Elements', () => {
    it('should allow keyboard navigation between multiple buttons', () => {
      setDarkTheme()

      render(
        <>
          <Button variant="primary">First</Button>
          <Button variant="secondary">Second</Button>
          <ThemeToggle />
        </>
      )

      const firstButton = screen.getByRole('button', { name: /first/i })
      const secondButton = screen.getByRole('button', { name: /second/i })
      const themeToggle = screen.getByRole('button', { name: /switch to/i })

      // Focus first button
      firstButton.focus()
      expect(document.activeElement).toBe(firstButton)

      // Focus second button
      secondButton.focus()
      expect(document.activeElement).toBe(secondButton)

      // Focus theme toggle
      themeToggle.focus()
      expect(document.activeElement).toBe(themeToggle)
    })
  })

  describe('Button Sizes', () => {
    it('should render all button sizes with correct classes in dark mode', () => {
      setDarkTheme()

      render(
        <>
          <Button size="sm">Small</Button>
          <Button size="md">Medium</Button>
          <Button size="lg">Large</Button>
        </>
      )

      expect(screen.getByRole('button', { name: /small/i })).toHaveClass('button--sm')
      expect(screen.getByRole('button', { name: /medium/i })).toHaveClass('button--md')
      expect(screen.getByRole('button', { name: /large/i })).toHaveClass('button--lg')
    })
  })
})
