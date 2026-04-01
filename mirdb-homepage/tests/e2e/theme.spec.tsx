/**
 * Theme Toggle E2E Tests.
 * Owner: Scenario 10 - Dark Mode Theme Toggle
 *
 * Tests:
 * - Toggle switches theme
 * - Theme persists on reload
 * - All elements remain readable
 * - Smooth transition animation
 *
 * Note: Using vitest + testing-library for component testing.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, cleanup } from '@testing-library/react'
import App from '../../src/App'
import { ThemeProvider } from '../../src/context'
import { THEME_STORAGE_KEY } from '../../src/utils/constants'

function renderApp() {
  return render(
    <ThemeProvider>
      <App />
    </ThemeProvider>
  )
}

describe('E2E: Dark Mode Theme Toggle', () => {
  beforeEach(() => {
    localStorage.clear()
    document.documentElement.classList.remove('dark')
  })

  afterEach(() => {
    cleanup()
  })

  // Test Case 1: Theme toggle button is present and accessible
  it('TC1: Theme toggle button is present and accessible', () => {
    renderApp()

    const toggleButton = screen.getByTestId('theme-toggle')
    expect(toggleButton).toBeInTheDocument()
    expect(toggleButton).toHaveAttribute('aria-label')
  })

  // Test Case 2: Click theme toggle from light mode applies dark class
  it('TC2: Click theme toggle from light mode applies dark class', () => {
    renderApp()

    // Verify starting in light mode (no dark class)
    expect(document.documentElement.classList.contains('dark')).toBe(false)

    // Click toggle
    const toggleButton = screen.getByTestId('theme-toggle')
    fireEvent.click(toggleButton)

    // Verify dark class is applied
    expect(document.documentElement.classList.contains('dark')).toBe(true)
  })

  // Test Case 3: Theme preference is stored in localStorage
  it('TC3: Theme preference is stored in localStorage', () => {
    renderApp()

    // Toggle to dark mode
    const toggleButton = screen.getByTestId('theme-toggle')
    fireEvent.click(toggleButton)

    // Check localStorage
    expect(localStorage.getItem(THEME_STORAGE_KEY)).toBe('dark')
  })

  // Test Case 4: Page loads with dark theme when preference is stored
  it('TC4: Page loads with dark theme when preference is stored', () => {
    // Set dark preference in localStorage before rendering
    localStorage.setItem(THEME_STORAGE_KEY, 'dark')

    renderApp()

    // Verify dark mode is applied
    expect(document.documentElement.classList.contains('dark')).toBe(true)
  })

  // Test Case 5: Background color changes when toggling theme
  it('TC5: Background color changes when toggling theme', () => {
    renderApp()

    // Get the main container
    const mainContainer = screen.getByRole('main').parentElement
    expect(mainContainer).toBeInTheDocument()

    // Initially should not have dark class
    expect(document.documentElement.classList.contains('dark')).toBe(false)

    // Toggle to dark mode
    const toggleButton = screen.getByTestId('theme-toggle')
    fireEvent.click(toggleButton)

    // Now should have dark class
    expect(document.documentElement.classList.contains('dark')).toBe(true)

    // The container should have dark mode class
    expect(mainContainer?.className).toContain('dark:bg-gray-900')
  })

  // Test Case 6: UI remains readable in dark mode - text contrast
  it('TC6: UI remains readable in dark mode', () => {
    localStorage.setItem(THEME_STORAGE_KEY, 'dark')

    renderApp()

    // Check that main heading is visible
    const heading = screen.getByRole('heading', { level: 1 })
    expect(heading).toBeInTheDocument()
    expect(heading).toBeVisible()

    // Check that heading has dark mode text color class
    const mainContainer = screen.getByRole('main').parentElement
    expect(mainContainer?.className).toContain('dark:text-gray-100')
  })

  // Test Case 7: Theme transition has smooth animation
  it('TC7: Theme transition has smooth animation (body has transition class)', () => {
    renderApp()

    // Verify the main container has transition classes
    const mainContainer = screen.getByRole('main').parentElement
    expect(mainContainer?.className).toContain('transition-colors')
    expect(mainContainer?.className).toContain('duration-200')
  })

  // Additional E2E tests
  it('Toggle button updates aria-label when theme changes', () => {
    renderApp()

    const toggleButton = screen.getByTestId('theme-toggle')

    // In light mode, should show "Switch to dark mode"
    expect(toggleButton).toHaveAttribute('aria-label', 'Switch to dark mode')

    // Toggle to dark mode
    fireEvent.click(toggleButton)

    // Should now show "Switch to light mode"
    expect(toggleButton).toHaveAttribute('aria-label', 'Switch to light mode')
  })

  it('Toggle works both ways - dark to light and back', () => {
    renderApp()

    const toggleButton = screen.getByTestId('theme-toggle')

    // Start in light mode
    expect(document.documentElement.classList.contains('dark')).toBe(false)

    // Toggle to dark
    fireEvent.click(toggleButton)
    expect(document.documentElement.classList.contains('dark')).toBe(true)

    // Toggle back to light
    fireEvent.click(toggleButton)
    expect(document.documentElement.classList.contains('dark')).toBe(false)
  })

  it('ThemeToggle component is visible in the fixed position container', () => {
    renderApp()

    const toggleButton = screen.getByTestId('theme-toggle')
    expect(toggleButton).toBeInTheDocument()

    // The toggle should be within a fixed position container
    const fixedContainer = toggleButton.closest('.fixed')
    expect(fixedContainer).toBeInTheDocument()
    expect(fixedContainer?.className).toContain('bottom-4')
    expect(fixedContainer?.className).toContain('right-4')
  })

  it('Multiple sections maintain proper dark mode styling', () => {
    localStorage.setItem(THEME_STORAGE_KEY, 'dark')

    renderApp()

    // Check that the app container has dark mode background class
    const appContainer = document.querySelector('.min-h-screen')
    expect(appContainer).toBeInTheDocument()
    expect(appContainer?.className).toContain('dark:bg-gray-900')
  })
})
