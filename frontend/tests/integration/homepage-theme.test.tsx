/**
 * Integration tests for homepage theme toggle functionality.
 * Owner: Scenario 7 - Theme Toggle Functionality
 *
 * Tests theme toggle on homepage:
 * - Theme toggle button is present and visible
 * - Clicking theme toggle changes the theme context
 * - Theme changes update document data-theme attribute
 * - Theme preference persists via localStorage
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, within, cleanup } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter } from 'react-router-dom'
import React, { useEffect, useState } from 'react'
import Navbar from '@/components/Navbar'
import ThemeToggle from '@/components/ThemeToggle'
import { ThemeProvider, useTheme } from '@/contexts/ThemeContext'
import { AuthProvider } from '@/contexts/AuthContext'
import Home from '@/pages/Home'

/**
 * Custom render helper for theme tests using MemoryRouter
 * Includes all necessary providers
 */
const renderWithProviders = (
  ui: React.ReactElement,
  { initialEntries = ['/'] } = {}
) => {
  return render(
    <MemoryRouter initialEntries={initialEntries}>
      <ThemeProvider>
        <AuthProvider>
          {ui}
        </AuthProvider>
      </ThemeProvider>
    </MemoryRouter>
  )
}

/**
 * Helper to get the first theme toggle button
 * (Navbar has two - one for desktop, one for mobile)
 */
const getThemeToggle = () => {
  const toggles = screen.getAllByRole('button', { name: /toggle theme/i })
  return toggles[0]
}

/**
 * Component to display current theme for testing
 */
const ThemeDisplay: React.FC = () => {
  const { theme } = useTheme()
  return <div data-testid="theme-display">{theme}</div>
}

/**
 * Component to render with theme display for verification
 */
const NavbarWithThemeDisplay: React.FC = () => {
  return (
    <>
      <Navbar />
      <ThemeDisplay />
    </>
  )
}

describe('Homepage Theme Toggle Functionality', () => {
  beforeEach(() => {
    // Clear localStorage before each test
    localStorage.clear()
    vi.clearAllMocks()
    // Reset document attribute
    document.documentElement.removeAttribute('data-theme')
  })

  afterEach(() => {
    cleanup()
  })

  describe('Test Case 1: Theme toggle button is present and visible in navigation', () => {
    it('should render theme toggle button in the navbar', () => {
      renderWithProviders(<Navbar />)

      const themeToggle = getThemeToggle()
      expect(themeToggle).toBeInTheDocument()
    })

    it('should have theme toggle visible within the navigation', () => {
      renderWithProviders(<Navbar />)

      const navbar = screen.getByRole('navigation')
      const themeToggles = within(navbar).getAllByRole('button', { name: /toggle theme/i })

      // Should have at least one visible theme toggle
      expect(themeToggles.length).toBeGreaterThan(0)
      expect(themeToggles[0]).toBeInTheDocument()
    })

    it('should render theme toggle with correct aria-label for accessibility', () => {
      renderWithProviders(<Navbar />)

      const themeToggle = getThemeToggle()
      expect(themeToggle).toHaveAttribute('aria-label', 'Toggle theme')
    })

    it('should display theme toggle when rendering full homepage', () => {
      renderWithProviders(
        <>
          <Navbar />
          <Home />
        </>
      )

      const themeToggle = getThemeToggle()
      expect(themeToggle).toBeInTheDocument()
    })
  })

  describe('Test Case 2: Click theme toggle changes theme context value', () => {
    it('should change theme from dark to cyberpunk on first click', async () => {
      const user = userEvent.setup()
      renderWithProviders(<NavbarWithThemeDisplay />)

      // Default theme is 'dark'
      const themeDisplay = screen.getByTestId('theme-display')
      expect(themeDisplay).toHaveTextContent('dark')

      const themeToggle = getThemeToggle()
      await user.click(themeToggle)

      // Theme should cycle to 'cyberpunk' (after dark in the cycle: light -> dark -> cyberpunk -> synthwave)
      expect(themeDisplay).toHaveTextContent('cyberpunk')
    })

    it('should cycle through all themes when clicking toggle repeatedly', async () => {
      const user = userEvent.setup()

      // Start with light theme via mock
      vi.mocked(localStorage.getItem).mockReturnValue('light')

      renderWithProviders(<NavbarWithThemeDisplay />)

      const themeDisplay = screen.getByTestId('theme-display')
      const themeToggle = getThemeToggle()

      // Should start at light (from localStorage mock)
      expect(themeDisplay).toHaveTextContent('light')

      // Click to go to dark
      await user.click(themeToggle)
      expect(themeDisplay).toHaveTextContent('dark')

      // Click to go to cyberpunk
      await user.click(themeToggle)
      expect(themeDisplay).toHaveTextContent('cyberpunk')

      // Click to go to synthwave
      await user.click(themeToggle)
      expect(themeDisplay).toHaveTextContent('synthwave')

      // Click to cycle back to light
      await user.click(themeToggle)
      expect(themeDisplay).toHaveTextContent('light')
    })

    it('should update theme context when toggle is clicked', async () => {
      const user = userEvent.setup()
      renderWithProviders(<NavbarWithThemeDisplay />)

      const themeToggle = getThemeToggle()
      const themeDisplay = screen.getByTestId('theme-display')

      const initialTheme = themeDisplay.textContent
      await user.click(themeToggle)
      const newTheme = themeDisplay.textContent

      expect(newTheme).not.toBe(initialTheme)
    })
  })

  describe('Test Case 3: Click theme toggle updates document data-theme attribute', () => {
    it('should set data-theme attribute on document.documentElement', async () => {
      const user = userEvent.setup()
      // Ensure no saved theme (uses default 'dark')
      vi.mocked(localStorage.getItem).mockReturnValue(null)

      renderWithProviders(<NavbarWithThemeDisplay />)

      // After initial render, data-theme should be set to default (dark)
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')

      const themeToggle = getThemeToggle()
      await user.click(themeToggle)

      // After clicking, should update to next theme (cyberpunk)
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')
    })

    it('should update data-theme attribute through full theme cycle', async () => {
      const user = userEvent.setup()
      // Mock localStorage to return 'light' initially
      vi.mocked(localStorage.getItem).mockReturnValue('light')

      renderWithProviders(<NavbarWithThemeDisplay />)

      const themeToggle = getThemeToggle()

      // Should start at light (from localStorage)
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')

      // Cycle through all themes
      await user.click(themeToggle)
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')

      await user.click(themeToggle)
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')

      await user.click(themeToggle)
      expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave')

      await user.click(themeToggle)
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')
    })

    it('should reflect theme change visually via data-theme', async () => {
      const user = userEvent.setup()
      renderWithProviders(<Navbar />)

      const themeToggle = getThemeToggle()

      // Get initial data-theme
      const initialTheme = document.documentElement.getAttribute('data-theme')

      await user.click(themeToggle)

      // data-theme should have changed
      const newTheme = document.documentElement.getAttribute('data-theme')
      expect(newTheme).not.toBe(initialTheme)
      expect(newTheme).toBeTruthy()
    })
  })

  describe('Test Case 4: Theme preference persists via localStorage', () => {
    it('should save theme preference to localStorage when toggled', async () => {
      const user = userEvent.setup()
      // Start with no saved theme (default dark)
      vi.mocked(localStorage.getItem).mockReturnValue(null)

      renderWithProviders(<NavbarWithThemeDisplay />)

      const themeToggle = getThemeToggle()
      const themeDisplay = screen.getByTestId('theme-display')

      // Initial theme should be dark (default)
      expect(themeDisplay).toHaveTextContent('dark')

      await user.click(themeToggle)

      // After clicking, should save to localStorage
      expect(localStorage.setItem).toHaveBeenCalledWith('theme', 'cyberpunk')
    })

    it('should restore theme from localStorage on component mount', () => {
      // Pre-set localStorage to 'dark'
      localStorage.setItem('theme', 'dark')
      // Mock getItem to return 'dark'
      vi.mocked(localStorage.getItem).mockReturnValue('dark')

      renderWithProviders(<NavbarWithThemeDisplay />)

      const themeDisplay = screen.getByTestId('theme-display')
      expect(themeDisplay).toHaveTextContent('dark')
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })

    it('should restore synthwave theme from localStorage', () => {
      // Mock getItem to return 'synthwave'
      vi.mocked(localStorage.getItem).mockReturnValue('synthwave')

      renderWithProviders(<NavbarWithThemeDisplay />)

      const themeDisplay = screen.getByTestId('theme-display')
      expect(themeDisplay).toHaveTextContent('synthwave')
      expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave')
    })

    it('should restore cyberpunk theme from localStorage', () => {
      // Mock getItem to return 'cyberpunk'
      vi.mocked(localStorage.getItem).mockReturnValue('cyberpunk')

      renderWithProviders(<NavbarWithThemeDisplay />)

      const themeDisplay = screen.getByTestId('theme-display')
      expect(themeDisplay).toHaveTextContent('cyberpunk')
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')
    })

    it('should persist theme preference across multiple toggles', async () => {
      const user = userEvent.setup()
      localStorage.setItem('theme', 'light')
      vi.mocked(localStorage.getItem).mockReturnValue('light')

      renderWithProviders(<NavbarWithThemeDisplay />)

      const themeToggle = getThemeToggle()

      // Toggle multiple times
      await user.click(themeToggle) // light -> dark
      expect(localStorage.setItem).toHaveBeenLastCalledWith('theme', 'dark')

      await user.click(themeToggle) // dark -> cyberpunk
      expect(localStorage.setItem).toHaveBeenLastCalledWith('theme', 'cyberpunk')

      await user.click(themeToggle) // cyberpunk -> synthwave
      expect(localStorage.setItem).toHaveBeenLastCalledWith('theme', 'synthwave')
    })

    it('should fall back to default dark theme if localStorage has invalid value', () => {
      // Mock getItem to return invalid theme
      vi.mocked(localStorage.getItem).mockReturnValue('invalid-theme')

      renderWithProviders(<NavbarWithThemeDisplay />)

      const themeDisplay = screen.getByTestId('theme-display')
      // Should fall back to default 'dark' theme
      expect(themeDisplay).toHaveTextContent('dark')
    })

    it('should use default dark theme if localStorage is empty', () => {
      // Mock getItem to return null
      vi.mocked(localStorage.getItem).mockReturnValue(null)

      renderWithProviders(<NavbarWithThemeDisplay />)

      const themeDisplay = screen.getByTestId('theme-display')
      expect(themeDisplay).toHaveTextContent('dark')
    })
  })

  describe('Theme toggle icon display', () => {
    it('should display appropriate icon based on current theme', () => {
      renderWithProviders(<Navbar />)

      const themeToggle = getThemeToggle()
      // Button should contain an icon (svg element)
      const icon = themeToggle.querySelector('svg')
      expect(icon).toBeInTheDocument()
    })
  })

  describe('Theme integration with homepage components', () => {
    it('should allow theme toggle from homepage with navbar', async () => {
      const user = userEvent.setup()
      renderWithProviders(
        <>
          <Navbar />
          <Home />
          <ThemeDisplay />
        </>
      )

      const themeToggle = getThemeToggle()
      const themeDisplay = screen.getByTestId('theme-display')

      expect(themeDisplay).toHaveTextContent('dark')

      await user.click(themeToggle)

      expect(themeDisplay).toHaveTextContent('cyberpunk')
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')
    })
  })
})
