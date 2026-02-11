/**
 * Integration tests for theme toggle functionality
 * Owner: Scenario 10 - Dark Mode Theme
 *
 * Test cases covered:
 * - TC1: Toggle to dark mode - background color changes
 * - TC4: Theme persists after reload (localStorage)
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { render, screen, fireEvent, act } from '@testing-library/react'
import { ThemeToggle } from '../../../src/components/ui/ThemeToggle'

describe('Theme Toggle Integration', () => {
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

  it('toggles theme between light and dark mode when clicked', async () => {
    render(<ThemeToggle />)

    const button = screen.getByRole('button', { name: /switch to .* mode/i })

    // Initial state should be light (default)
    expect(document.documentElement.getAttribute('data-theme')).toBe('light')

    // Click to toggle to dark
    await act(async () => {
      fireEvent.click(button)
    })

    expect(document.documentElement.getAttribute('data-theme')).toBe('dark')

    // Click again to toggle back to light
    await act(async () => {
      fireEvent.click(button)
    })

    expect(document.documentElement.getAttribute('data-theme')).toBe('light')
  })

  it('persists theme preference in localStorage', async () => {
    render(<ThemeToggle />)

    const button = screen.getByRole('button', { name: /switch to .* mode/i })

    // Toggle to dark
    await act(async () => {
      fireEvent.click(button)
    })

    expect(localStorage.getItem('mirdb-theme')).toBe('dark')

    // Toggle back to light
    await act(async () => {
      fireEvent.click(button)
    })

    expect(localStorage.getItem('mirdb-theme')).toBe('light')
  })

  it('loads saved theme preference from localStorage', () => {
    localStorage.setItem('mirdb-theme', 'dark')

    render(<ThemeToggle />)

    expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
  })

  it('defaults to light theme when no preference is saved', () => {
    render(<ThemeToggle />)

    expect(document.documentElement.getAttribute('data-theme')).toBe('light')
  })

  it('respects system preference when no saved preference exists', () => {
    window.matchMedia = vi.fn().mockImplementation((query) => ({
      matches: query === '(prefers-color-scheme: dark)',
      media: query,
      onchange: null,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      dispatchEvent: vi.fn(),
    }))

    render(<ThemeToggle />)

    expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
  })

  it('has correct aria-label for accessibility', async () => {
    render(<ThemeToggle />)

    // Initial state - light mode, button should say switch to dark
    let button = screen.getByRole('button', { name: /switch to dark mode/i })
    expect(button).toBeInTheDocument()

    // Toggle to dark mode
    await act(async () => {
      fireEvent.click(button)
    })

    // Now button should say switch to light
    button = screen.getByRole('button', { name: /switch to light mode/i })
    expect(button).toBeInTheDocument()
  })
})
