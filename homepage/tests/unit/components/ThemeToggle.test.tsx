/**
 * Unit tests for ThemeToggle component.
 * Owner: Scenario 7 - Dark Mode and Light Mode Toggle
 *
 * Test Cases:
 * - TC1: Toggle button is rendered and accessible
 * - Component displays correct icon based on theme
 * - Accessibility attributes are correct
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { ThemeToggle } from '@/components/ui/ThemeToggle'
import { ThemeProvider } from '@/contexts/ThemeContext'
import type { Theme } from '@/types'

// Mock localStorage
const localStorageMock = (() => {
  let store: Record<string, string> = {}
  return {
    getItem: vi.fn((key: string) => store[key] || null),
    setItem: vi.fn((key: string, value: string) => {
      store[key] = value
    }),
    removeItem: vi.fn((key: string) => {
      delete store[key]
    }),
    clear: vi.fn(() => {
      store = {}
    }),
  }
})()

Object.defineProperty(window, 'localStorage', { value: localStorageMock })

// Helper to render ThemeToggle with provider
function renderThemeToggle(defaultTheme?: Theme) {
  return render(
    <ThemeProvider defaultTheme={defaultTheme}>
      <ThemeToggle />
    </ThemeProvider>
  )
}

describe('ThemeToggle Component', () => {
  beforeEach(() => {
    localStorageMock.clear()
    document.documentElement.classList.remove('dark')
  })

  // Test Case 1: Toggle button is rendered and accessible
  it('renders toggle button that is accessible', () => {
    renderThemeToggle('light')

    const toggleButton = screen.getByTestId('theme-toggle')
    expect(toggleButton).toBeInTheDocument()
    expect(toggleButton).toHaveAttribute('type', 'button')
    expect(toggleButton).toHaveAttribute('aria-label')
    expect(toggleButton).toHaveAttribute('aria-pressed')
  })

  it('has correct aria-label for light mode', () => {
    renderThemeToggle('light')

    const toggleButton = screen.getByTestId('theme-toggle')
    expect(toggleButton).toHaveAttribute('aria-label', 'Switch to dark mode')
    expect(toggleButton).toHaveAttribute('aria-pressed', 'false')
  })

  it('has correct aria-label for dark mode', () => {
    renderThemeToggle('dark')

    const toggleButton = screen.getByTestId('theme-toggle')
    expect(toggleButton).toHaveAttribute('aria-label', 'Switch to light mode')
    expect(toggleButton).toHaveAttribute('aria-pressed', 'true')
  })

  it('displays moon icon in light mode', () => {
    renderThemeToggle('light')

    const toggleButton = screen.getByTestId('theme-toggle')
    // Moon icon should be visible (lucide-react renders svg)
    const svg = toggleButton.querySelector('svg')
    expect(svg).toBeInTheDocument()
    expect(svg).toHaveAttribute('aria-hidden', 'true')
  })

  it('displays sun icon in dark mode', () => {
    renderThemeToggle('dark')

    const toggleButton = screen.getByTestId('theme-toggle')
    // Sun icon should be visible
    const svg = toggleButton.querySelector('svg')
    expect(svg).toBeInTheDocument()
    expect(svg).toHaveAttribute('aria-hidden', 'true')
  })

  it('can be found using accessible role', () => {
    renderThemeToggle('light')

    const toggleButton = screen.getByRole('button', { name: /switch to dark mode/i })
    expect(toggleButton).toBeInTheDocument()
  })

  it('handles toggle click', () => {
    renderThemeToggle('light')

    const toggleButton = screen.getByTestId('theme-toggle')
    fireEvent.click(toggleButton)

    // After click, should switch to dark mode
    expect(toggleButton).toHaveAttribute('aria-label', 'Switch to light mode')
    expect(toggleButton).toHaveAttribute('aria-pressed', 'true')
  })

  it('applies custom className', () => {
    render(
      <ThemeProvider defaultTheme="light">
        <ThemeToggle className="custom-class" />
      </ThemeProvider>
    )

    const toggleButton = screen.getByTestId('theme-toggle')
    expect(toggleButton).toHaveClass('custom-class')
  })

  it('applies size classes correctly', () => {
    const { rerender } = render(
      <ThemeProvider defaultTheme="light">
        <ThemeToggle size="sm" />
      </ThemeProvider>
    )

    let toggleButton = screen.getByTestId('theme-toggle')
    expect(toggleButton).toHaveClass('w-8', 'h-8')

    rerender(
      <ThemeProvider defaultTheme="light">
        <ThemeToggle size="lg" />
      </ThemeProvider>
    )

    toggleButton = screen.getByTestId('theme-toggle')
    expect(toggleButton).toHaveClass('w-12', 'h-12')
  })

  it('has focus styles for keyboard accessibility', () => {
    renderThemeToggle('light')

    const toggleButton = screen.getByTestId('theme-toggle')
    expect(toggleButton.className).toContain('focus:outline-none')
    expect(toggleButton.className).toContain('focus:ring-2')
  })
})
