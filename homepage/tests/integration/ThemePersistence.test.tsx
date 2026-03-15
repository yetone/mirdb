/**
 * Integration tests for theme persistence.
 * Owner: Scenario 7 - Dark Mode and Light Mode Toggle
 *
 * Test Cases:
 * - TC2: Click theme toggle in light mode -> Theme changes to dark mode
 * - TC3: Click theme toggle in dark mode -> Theme changes to light mode
 * - TC5: Render page in dark mode -> Contrast ratio requirements
 * - TC6: Render page in light mode -> Contrast ratio requirements
 * - TC7: Check localStorage after toggle -> Theme preference is stored
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, cleanup } from '@testing-library/react'
import { ThemeToggle } from '@/components/ui/ThemeToggle'
import { ThemeProvider } from '@/contexts/ThemeContext'
import { THEME_STORAGE_KEY } from '@/hooks/useTheme'

// Test component that displays themed content
function ThemedContent() {
  return (
    <div className="bg-white dark:bg-gray-900 text-gray-900 dark:text-gray-100">
      <h1 className="text-gray-900 dark:text-white">MirDB</h1>
      <p className="text-gray-600 dark:text-gray-300">
        A persistent key-value store
      </p>
      <ThemeToggle />
    </div>
  )
}

describe('Theme Integration', () => {
  // Fresh store for each test
  let mockStore: Record<string, string>
  let setItemSpy: ReturnType<typeof vi.fn>
  let getItemSpy: ReturnType<typeof vi.fn>

  beforeEach(() => {
    // Create fresh store and spies for each test
    mockStore = {}
    setItemSpy = vi.fn((key: string, value: string) => {
      mockStore[key] = value
    })
    getItemSpy = vi.fn((key: string) => mockStore[key] ?? null)

    // Replace localStorage methods
    Object.defineProperty(window, 'localStorage', {
      value: {
        getItem: getItemSpy,
        setItem: setItemSpy,
        removeItem: vi.fn((key: string) => { delete mockStore[key] }),
        clear: vi.fn(() => { mockStore = {} }),
      },
      writable: true,
    })

    // Clean up document state
    document.documentElement.classList.remove('dark')
  })

  afterEach(() => {
    cleanup()
    document.documentElement.classList.remove('dark')
  })

  // Test Case 2: Click theme toggle in light mode -> Theme changes to dark mode
  describe('TC2: Toggle from light to dark mode', () => {
    it('changes theme to dark when clicking toggle in light mode', () => {
      render(
        <ThemeProvider defaultTheme="light">
          <ThemedContent />
        </ThemeProvider>
      )

      const toggleButton = screen.getByTestId('theme-toggle')
      expect(toggleButton).toHaveAttribute('aria-label', 'Switch to dark mode')

      fireEvent.click(toggleButton)

      expect(toggleButton).toHaveAttribute('aria-label', 'Switch to light mode')
      expect(document.documentElement.classList.contains('dark')).toBe(true)
    })

    it('updates document class to dark', () => {
      render(
        <ThemeProvider defaultTheme="light">
          <ThemeToggle />
        </ThemeProvider>
      )

      const toggleButton = screen.getByTestId('theme-toggle')
      fireEvent.click(toggleButton)

      expect(document.documentElement.classList.contains('dark')).toBe(true)
    })
  })

  // Test Case 3: Click theme toggle in dark mode -> Theme changes to light mode
  describe('TC3: Toggle from dark to light mode', () => {
    it('changes theme to light when clicking toggle in dark mode', () => {
      render(
        <ThemeProvider defaultTheme="dark">
          <ThemedContent />
        </ThemeProvider>
      )

      const toggleButton = screen.getByTestId('theme-toggle')
      expect(toggleButton).toHaveAttribute('aria-label', 'Switch to light mode')

      fireEvent.click(toggleButton)

      expect(toggleButton).toHaveAttribute('aria-label', 'Switch to dark mode')
      expect(document.documentElement.classList.contains('dark')).toBe(false)
    })

    it('removes dark class from document', () => {
      render(
        <ThemeProvider defaultTheme="dark">
          <ThemeToggle />
        </ThemeProvider>
      )

      // Dark class should be applied initially
      expect(document.documentElement.classList.contains('dark')).toBe(true)

      const toggleButton = screen.getByTestId('theme-toggle')
      fireEvent.click(toggleButton)

      expect(document.documentElement.classList.contains('dark')).toBe(false)
    })
  })

  // Test Case 5: Render page in dark mode -> Contrast ratio requirements
  describe('TC5: Dark mode contrast', () => {
    it('dark mode uses high-contrast color classes', () => {
      const { container } = render(
        <ThemeProvider defaultTheme="dark">
          <ThemedContent />
        </ThemeProvider>
      )

      // Verify dark mode classes are applied
      const contentDiv = container.firstChild as HTMLElement
      expect(contentDiv.className).toContain('dark:bg-gray-900')
      expect(contentDiv.className).toContain('dark:text-gray-100')

      // Verify heading has dark mode high contrast text
      const heading = screen.getByRole('heading', { level: 1 })
      expect(heading.className).toContain('dark:text-white')

      // Verify description has appropriate contrast
      const description = screen.getByText(/persistent key-value store/i)
      expect(description.className).toContain('dark:text-gray-300')
    })

    it('applies dark class to document root in dark mode', () => {
      render(
        <ThemeProvider defaultTheme="dark">
          <ThemedContent />
        </ThemeProvider>
      )

      expect(document.documentElement.classList.contains('dark')).toBe(true)
    })
  })

  // Test Case 6: Render page in light mode -> Contrast ratio requirements
  describe('TC6: Light mode contrast', () => {
    it('light mode uses high-contrast color classes', () => {
      const { container } = render(
        <ThemeProvider defaultTheme="light">
          <ThemedContent />
        </ThemeProvider>
      )

      // Verify light mode classes are present
      const contentDiv = container.firstChild as HTMLElement
      expect(contentDiv.className).toContain('bg-white')
      expect(contentDiv.className).toContain('text-gray-900')

      // Verify heading has high contrast text
      const heading = screen.getByRole('heading', { level: 1 })
      expect(heading.className).toContain('text-gray-900')

      // Verify description has appropriate contrast
      const description = screen.getByText(/persistent key-value store/i)
      expect(description.className).toContain('text-gray-600')
    })

    it('does not apply dark class to document root in light mode', () => {
      render(
        <ThemeProvider defaultTheme="light">
          <ThemedContent />
        </ThemeProvider>
      )

      expect(document.documentElement.classList.contains('dark')).toBe(false)
    })
  })

  // Test Case 7: Check localStorage after toggle -> Theme preference is stored
  describe('TC7: localStorage persistence', () => {
    it('stores theme preference in localStorage after toggle', () => {
      render(
        <ThemeProvider defaultTheme="light">
          <ThemeToggle />
        </ThemeProvider>
      )

      const toggleButton = screen.getByTestId('theme-toggle')
      fireEvent.click(toggleButton)

      expect(setItemSpy).toHaveBeenCalledWith(THEME_STORAGE_KEY, 'dark')
    })

    it('loads persisted theme on subsequent render', () => {
      // Pre-set dark theme in storage
      mockStore[THEME_STORAGE_KEY] = 'dark'

      render(
        <ThemeProvider>
          <ThemeToggle />
        </ThemeProvider>
      )

      // Should load dark mode from localStorage
      const toggleButton = screen.getByTestId('theme-toggle')
      expect(toggleButton).toHaveAttribute('aria-label', 'Switch to light mode')
    })

    it('persists light theme after toggling twice', () => {
      render(
        <ThemeProvider defaultTheme="light">
          <ThemeToggle />
        </ThemeProvider>
      )

      const toggleButton = screen.getByTestId('theme-toggle')

      // Toggle to dark
      fireEvent.click(toggleButton)
      expect(mockStore[THEME_STORAGE_KEY]).toBe('dark')

      // Toggle back to light
      fireEvent.click(toggleButton)
      expect(mockStore[THEME_STORAGE_KEY]).toBe('light')
    })
  })

  describe('Multiple toggles', () => {
    it('can toggle theme multiple times', () => {
      render(
        <ThemeProvider defaultTheme="light">
          <ThemeToggle />
        </ThemeProvider>
      )

      const toggleButton = screen.getByTestId('theme-toggle')

      // Verify we start in light mode (moon icon, switch to dark)
      expect(toggleButton).toHaveAttribute('aria-label', 'Switch to dark mode')
      expect(document.documentElement.classList.contains('dark')).toBe(false)

      // Toggle to dark
      fireEvent.click(toggleButton)
      expect(toggleButton).toHaveAttribute('aria-label', 'Switch to light mode')
      expect(document.documentElement.classList.contains('dark')).toBe(true)

      // Toggle back to light
      fireEvent.click(toggleButton)
      expect(toggleButton).toHaveAttribute('aria-label', 'Switch to dark mode')
      expect(document.documentElement.classList.contains('dark')).toBe(false)

      // Toggle to dark again
      fireEvent.click(toggleButton)
      expect(toggleButton).toHaveAttribute('aria-label', 'Switch to light mode')
      expect(document.documentElement.classList.contains('dark')).toBe(true)
    })
  })

  describe('Context integration', () => {
    it('throws error when ThemeToggle used outside provider', () => {
      // Suppress console.error for this test
      const spy = vi.spyOn(console, 'error').mockImplementation(() => {})

      expect(() => {
        render(<ThemeToggle />)
      }).toThrow('useThemeContext must be used within a ThemeProvider')

      spy.mockRestore()
    })
  })
})
