/**
 * Unit tests for ThemeToggle component.
 * Owner: Scenario 10 - Dark Mode Theme
 *
 * Requirements tested:
 * - REQ-12: Dark mode theme toggle
 * - Accessibility (aria-label, focus states)
 * - Visual feedback (icon changes)
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { render, screen, fireEvent, act } from '@testing-library/react'
import { ThemeToggle } from '../../../../src/components/ui/ThemeToggle'

describe('ThemeToggle', () => {
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
  })

  describe('rendering', () => {
    it('should render a button element', () => {
      render(<ThemeToggle />)

      const button = screen.getByRole('button')
      expect(button).toBeInTheDocument()
    })

    it('should have theme-toggle class', () => {
      render(<ThemeToggle />)

      const button = screen.getByRole('button')
      expect(button).toHaveClass('theme-toggle')
    })

    it('should render an SVG icon', () => {
      render(<ThemeToggle />)

      const svg = document.querySelector('.theme-toggle__icon')
      expect(svg).toBeInTheDocument()
    })
  })

  describe('accessibility', () => {
    it('should have appropriate aria-label for light mode', () => {
      render(<ThemeToggle />)

      const button = screen.getByRole('button', { name: /switch to dark mode/i })
      expect(button).toBeInTheDocument()
    })

    it('should have appropriate aria-label for dark mode', () => {
      localStorage.setItem('mirdb-theme', 'dark')

      render(<ThemeToggle />)

      const button = screen.getByRole('button', { name: /switch to light mode/i })
      expect(button).toBeInTheDocument()
    })

    it('should update aria-label when theme changes', async () => {
      render(<ThemeToggle />)

      const button = screen.getByRole('button', { name: /switch to dark mode/i })
      expect(button).toBeInTheDocument()

      await act(async () => {
        fireEvent.click(button)
      })

      const updatedButton = screen.getByRole('button', { name: /switch to light mode/i })
      expect(updatedButton).toBeInTheDocument()
    })

    it('should have title attribute for tooltip', () => {
      render(<ThemeToggle />)

      const button = screen.getByRole('button')
      expect(button).toHaveAttribute('title', 'Switch to dark mode')
    })

    it('should be focusable', () => {
      render(<ThemeToggle />)

      const button = screen.getByRole('button')
      button.focus()

      expect(document.activeElement).toBe(button)
    })
  })

  describe('theme toggle functionality', () => {
    it('should toggle theme when clicked', async () => {
      render(<ThemeToggle />)

      expect(document.documentElement.getAttribute('data-theme')).toBe('light')

      const button = screen.getByRole('button')

      await act(async () => {
        fireEvent.click(button)
      })

      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })

    it('should update localStorage when toggled', async () => {
      render(<ThemeToggle />)

      const button = screen.getByRole('button')

      await act(async () => {
        fireEvent.click(button)
      })

      expect(localStorage.getItem('mirdb-theme')).toBe('dark')
    })
  })

  describe('icon display', () => {
    it('should show moon icon in light mode (indicating switch to dark)', () => {
      render(<ThemeToggle />)

      const button = screen.getByRole('button')
      const svg = button.querySelector('svg')

      expect(svg).toBeInTheDocument()
      // Moon icon has a specific path for dark mode toggle
      const path = svg?.querySelector('path')
      expect(path).toHaveAttribute('d', expect.stringContaining('21 12.79'))
    })

    it('should show sun icon in dark mode (indicating switch to light)', () => {
      localStorage.setItem('mirdb-theme', 'dark')

      render(<ThemeToggle />)

      const button = screen.getByRole('button')
      const svg = button.querySelector('svg')

      expect(svg).toBeInTheDocument()
      // Sun icon has a circle element
      const circle = svg?.querySelector('circle')
      expect(circle).toBeInTheDocument()
    })

    it('should change icon when toggled', async () => {
      render(<ThemeToggle />)

      const button = screen.getByRole('button')

      // Initially should have moon icon (path)
      let svg = button.querySelector('svg')
      expect(svg?.querySelector('path')).toHaveAttribute('d', expect.stringContaining('21 12.79'))

      await act(async () => {
        fireEvent.click(button)
      })

      // After toggle should have sun icon (circle)
      svg = button.querySelector('svg')
      expect(svg?.querySelector('circle')).toBeInTheDocument()
    })
  })

  describe('SVG attributes', () => {
    it('should have aria-hidden on SVG icon', () => {
      render(<ThemeToggle />)

      const svg = document.querySelector('.theme-toggle__icon')
      expect(svg).toHaveAttribute('aria-hidden', 'true')
    })

    it('should have proper SVG dimensions', () => {
      render(<ThemeToggle />)

      const svg = document.querySelector('.theme-toggle__icon')
      expect(svg).toHaveAttribute('width', '20')
      expect(svg).toHaveAttribute('height', '20')
    })
  })
})
