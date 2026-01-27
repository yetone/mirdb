/**
 * Theme Integration Tests
 * Owner: Scenario 5 - Theme Switching
 *
 * Tests theme toggle functionality on the homepage:
 * - Theme toggle presence in navbar
 * - Initial theme application
 * - Theme switching via toggle
 * - DOM data-theme attribute updates
 * - localStorage persistence
 * - Theme persistence across page reloads
 * - Multiple theme options support
 * - Content visibility across all themes
 *
 * Requirements: REQ-8 - Support theme switching (light/dark mode)
 * User Story: US-6 - Toggle Theme
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, waitFor, within, cleanup } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter } from 'react-router-dom'
import { ThemeProvider, AVAILABLE_THEMES, type ThemeName } from '../../src/contexts/ThemeContext'
import { ThemeToggle } from '../../src/components/ThemeToggle'
import { Navbar } from '../../src/components/Navbar'
import { Home } from '../../src/pages/Home'

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <div {...props}>{children}</div>
    ),
    section: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <section {...props}>{children}</section>
    ),
    nav: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <nav {...props}>{children}</nav>
    ),
    h1: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <h1 {...props}>{children}</h1>
    ),
    p: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <p {...props}>{children}</p>
    ),
    button: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <button {...props}>{children}</button>
    ),
  },
}))

// Helper to render with providers
function renderWithProviders(
  ui: React.ReactElement,
  { defaultTheme }: { defaultTheme?: ThemeName } = {}
) {
  return render(
    <MemoryRouter initialEntries={['/']}>
      <ThemeProvider defaultTheme={defaultTheme}>
        {ui}
      </ThemeProvider>
    </MemoryRouter>
  )
}

// Mock localStorage
const localStorageMock = (() => {
  let store: Record<string, string> = {}
  return {
    getItem: vi.fn((key: string) => store[key] ?? null),
    setItem: vi.fn((key: string, value: string) => {
      store[key] = value
    }),
    removeItem: vi.fn((key: string) => {
      delete store[key]
    }),
    clear: vi.fn(() => {
      store = {}
    }),
    get length() {
      return Object.keys(store).length
    },
    key: vi.fn((i: number) => Object.keys(store)[i] ?? null),
  }
})()

describe('Theme Switching - Integration Tests', () => {
  beforeEach(() => {
    // Reset localStorage mock
    localStorageMock.clear()
    Object.defineProperty(window, 'localStorage', {
      value: localStorageMock,
      writable: true,
    })
    // Reset document attribute
    document.documentElement.removeAttribute('data-theme')
  })

  afterEach(() => {
    vi.clearAllMocks()
    document.documentElement.removeAttribute('data-theme')
  })

  /**
   * Test Case 1: ThemeToggle component is rendered in navbar
   * Input: Render Home page with ThemeContext
   * Expected: ThemeToggle component is rendered in navbar
   */
  describe('TC1: ThemeToggle in Navbar', () => {
    it('should render ThemeToggle component in the navbar', () => {
      renderWithProviders(<Navbar />)

      const themeToggle = screen.getByTestId('theme-toggle')
      expect(themeToggle).toBeInTheDocument()
    })

    it('should render ThemeToggle when Home page is rendered with ThemeContext', () => {
      renderWithProviders(<Home />)

      const navbar = screen.getByTestId('navbar')
      expect(navbar).toBeInTheDocument()

      const themeToggle = within(navbar).getByTestId('theme-toggle')
      expect(themeToggle).toBeInTheDocument()
    })

    it('should have a button to toggle theme', () => {
      renderWithProviders(<Navbar />)

      const toggleButton = screen.getByTestId('theme-toggle-button')
      expect(toggleButton).toBeInTheDocument()
      expect(toggleButton.tagName).toBe('BUTTON')
    })
  })

  /**
   * Test Case 2: Initial theme application
   * Input: Check initial theme application
   * Expected: Document has data-theme attribute or theme class applied
   */
  describe('TC2: Initial Theme Application', () => {
    it('should apply default theme to document on mount', () => {
      renderWithProviders(<Home />, { defaultTheme: 'light' })

      expect(document.documentElement.getAttribute('data-theme')).toBe('light')
    })

    it('should apply dark theme when specified as default', () => {
      renderWithProviders(<Home />, { defaultTheme: 'dark' })

      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })

    it('should have data-theme attribute set on the document', () => {
      renderWithProviders(<Home />, { defaultTheme: 'cyberpunk' })

      const dataTheme = document.documentElement.getAttribute('data-theme')
      expect(dataTheme).toBeTruthy()
      expect(AVAILABLE_THEMES).toContain(dataTheme)
    })
  })

  /**
   * Test Case 3: Click theme toggle button
   * Input: Click theme toggle button
   * Expected: Theme context value changes
   */
  describe('TC3: Theme Toggle Click', () => {
    it('should show dropdown with theme options when clicked', async () => {
      const user = userEvent.setup()
      renderWithProviders(<ThemeToggle />)

      const toggleButton = screen.getByTestId('theme-toggle-button')
      await user.click(toggleButton)

      const dropdown = screen.getByTestId('theme-dropdown')
      expect(dropdown).toBeInTheDocument()
    })

    it('should change theme when a different theme is selected', async () => {
      const user = userEvent.setup()
      renderWithProviders(<ThemeToggle />, { defaultTheme: 'light' })

      // Initial theme is light
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')

      // Open dropdown and select dark theme
      const toggleButton = screen.getByTestId('theme-toggle-button')
      await user.click(toggleButton)

      const darkOption = screen.getByTestId('theme-option-dark')
      await user.click(darkOption)

      // Theme should change to dark
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })

    it('should update theme context when theme is toggled', async () => {
      const user = userEvent.setup()
      renderWithProviders(<Home />, { defaultTheme: 'light' })

      // Open theme dropdown and select cyberpunk
      const toggleButton = screen.getByTestId('theme-toggle-button')
      await user.click(toggleButton)

      const cyberpunkOption = screen.getByTestId('theme-option-cyberpunk')
      await user.click(cyberpunkOption)

      // The theme should be updated
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')
    })
  })

  /**
   * Test Case 4: Verify DOM theme attribute after toggle
   * Input: Verify DOM theme attribute after toggle
   * Expected: data-theme attribute changes to reflect new theme
   */
  describe('TC4: DOM Theme Attribute Changes', () => {
    it('should update data-theme attribute when switching from light to dark', async () => {
      const user = userEvent.setup()
      renderWithProviders(<ThemeToggle />, { defaultTheme: 'light' })

      expect(document.documentElement.getAttribute('data-theme')).toBe('light')

      await user.click(screen.getByTestId('theme-toggle-button'))
      await user.click(screen.getByTestId('theme-option-dark'))

      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })

    it('should update data-theme attribute when switching to cyberpunk', async () => {
      const user = userEvent.setup()
      renderWithProviders(<ThemeToggle />, { defaultTheme: 'light' })

      await user.click(screen.getByTestId('theme-toggle-button'))
      await user.click(screen.getByTestId('theme-option-cyberpunk'))

      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')
    })

    it('should update data-theme attribute when switching to synthwave', async () => {
      const user = userEvent.setup()
      renderWithProviders(<ThemeToggle />, { defaultTheme: 'light' })

      await user.click(screen.getByTestId('theme-toggle-button'))
      await user.click(screen.getByTestId('theme-option-synthwave'))

      expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave')
    })
  })

  /**
   * Test Case 5: Check localStorage after theme change
   * Input: Check localStorage after theme change
   * Expected: Theme preference is saved to localStorage
   */
  describe('TC5: localStorage Persistence', () => {
    it('should save theme preference to localStorage when changed', async () => {
      const user = userEvent.setup()
      renderWithProviders(<ThemeToggle />, { defaultTheme: 'light' })

      await user.click(screen.getByTestId('theme-toggle-button'))
      await user.click(screen.getByTestId('theme-option-dark'))

      expect(localStorageMock.setItem).toHaveBeenCalledWith('theme-preference', 'dark')
    })

    it('should save each theme selection to localStorage', async () => {
      const user = userEvent.setup()
      renderWithProviders(<ThemeToggle />, { defaultTheme: 'light' })

      // Select cyberpunk
      await user.click(screen.getByTestId('theme-toggle-button'))
      await user.click(screen.getByTestId('theme-option-cyberpunk'))
      expect(localStorageMock.setItem).toHaveBeenCalledWith('theme-preference', 'cyberpunk')

      // Select synthwave
      await user.click(screen.getByTestId('theme-toggle-button'))
      await user.click(screen.getByTestId('theme-option-synthwave'))
      expect(localStorageMock.setItem).toHaveBeenCalledWith('theme-preference', 'synthwave')
    })
  })

  /**
   * Test Case 6: Reload page and check theme
   * Input: Reload page and check theme
   * Expected: Previously selected theme is applied on load
   */
  describe('TC6: Theme Persistence After Reload', () => {
    it('should read theme from localStorage on initial render', () => {
      // Set stored theme before render
      localStorageMock.setItem('theme-preference', 'cyberpunk')

      // Mock getItem to return the stored value
      localStorageMock.getItem.mockReturnValueOnce('cyberpunk')

      renderWithProviders(<Home />)

      // Theme should be read from localStorage (we verify the getItem was called)
      expect(localStorageMock.getItem).toHaveBeenCalled()
    })

    it('should apply stored dark theme on reload', async () => {
      const user = userEvent.setup()

      // First render - set theme to dark
      const { unmount } = renderWithProviders(<ThemeToggle />, { defaultTheme: 'light' })
      await user.click(screen.getByTestId('theme-toggle-button'))
      await user.click(screen.getByTestId('theme-option-dark'))

      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')

      // Unmount to simulate leaving page
      unmount()

      // Mock localStorage returning the saved theme
      localStorageMock.getItem.mockReturnValue('dark')

      // Render again - should load from localStorage
      renderWithProviders(<ThemeToggle />)

      // The theme should still be dark (restored from localStorage in ThemeProvider)
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      })
    })
  })

  /**
   * Test Case 7: Test multiple theme options
   * Input: Test multiple theme options
   * Expected: All available themes (light, dark, cyberpunk, etc.) can be selected
   */
  describe('TC7: Multiple Theme Options', () => {
    it('should display all available theme options in dropdown', async () => {
      const user = userEvent.setup()
      renderWithProviders(<ThemeToggle />)

      await user.click(screen.getByTestId('theme-toggle-button'))

      // Check each available theme has an option
      for (const theme of AVAILABLE_THEMES) {
        const option = screen.getByTestId(`theme-option-${theme}`)
        expect(option).toBeInTheDocument()
      }
    })

    it('should allow selecting each available theme', async () => {
      const user = userEvent.setup()
      renderWithProviders(<ThemeToggle />, { defaultTheme: 'light' })

      // Test each theme can be selected
      for (const theme of AVAILABLE_THEMES) {
        await user.click(screen.getByTestId('theme-toggle-button'))
        await user.click(screen.getByTestId(`theme-option-${theme}`))

        expect(document.documentElement.getAttribute('data-theme')).toBe(theme)
      }
    })

    it('should have light, dark, cyberpunk, and synthwave themes available', async () => {
      const user = userEvent.setup()
      renderWithProviders(<ThemeToggle />)

      await user.click(screen.getByTestId('theme-toggle-button'))

      expect(screen.getByTestId('theme-option-light')).toBeInTheDocument()
      expect(screen.getByTestId('theme-option-dark')).toBeInTheDocument()
      expect(screen.getByTestId('theme-option-cyberpunk')).toBeInTheDocument()
      expect(screen.getByTestId('theme-option-synthwave')).toBeInTheDocument()
    })
  })

  /**
   * Test Case 8: Verify homepage content visible in all themes
   * Input: Verify homepage content visible in all themes
   * Expected: All sections maintain readability and contrast in each theme
   */
  describe('TC8: Content Visibility Across Themes', () => {
    it('should render homepage content in light theme', () => {
      renderWithProviders(<Home />, { defaultTheme: 'light' })

      expect(screen.getByTestId('home-page')).toBeInTheDocument()
      expect(screen.getByTestId('navbar')).toBeInTheDocument()
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
    })

    it('should render homepage content in dark theme', () => {
      renderWithProviders(<Home />, { defaultTheme: 'dark' })

      expect(screen.getByTestId('home-page')).toBeInTheDocument()
      expect(screen.getByTestId('navbar')).toBeInTheDocument()
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
    })

    it('should render homepage content in cyberpunk theme', () => {
      renderWithProviders(<Home />, { defaultTheme: 'cyberpunk' })

      expect(screen.getByTestId('home-page')).toBeInTheDocument()
      expect(screen.getByTestId('navbar')).toBeInTheDocument()
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
    })

    it('should render homepage content in synthwave theme', () => {
      renderWithProviders(<Home />, { defaultTheme: 'synthwave' })

      expect(screen.getByTestId('home-page')).toBeInTheDocument()
      expect(screen.getByTestId('navbar')).toBeInTheDocument()
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
    })

    it('should maintain homepage structure when switching themes', async () => {
      const user = userEvent.setup()
      renderWithProviders(<Home />, { defaultTheme: 'light' })

      // Verify initial structure
      expect(screen.getByTestId('home-page')).toBeInTheDocument()

      // Switch to each theme and verify content is still visible
      for (const theme of ['dark', 'cyberpunk', 'synthwave'] as ThemeName[]) {
        await user.click(screen.getByTestId('theme-toggle-button'))
        await user.click(screen.getByTestId(`theme-option-${theme}`))

        // Content should still be visible
        expect(screen.getByTestId('home-page')).toBeInTheDocument()
        expect(screen.getByTestId('navbar')).toBeInTheDocument()
        expect(screen.getByTestId('hero-section')).toBeInTheDocument()
        expect(screen.getByTestId('features-section')).toBeInTheDocument()
        expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
      }
    })
  })

  // Additional edge case tests
  describe('Edge Cases', () => {
    it('should have accessible theme toggle button', () => {
      renderWithProviders(<ThemeToggle />)

      const toggleButton = screen.getByTestId('theme-toggle-button')
      expect(toggleButton).toHaveAttribute('aria-label')
      expect(toggleButton).toHaveAttribute('aria-haspopup', 'listbox')
    })

    it('should indicate current theme in dropdown', async () => {
      const user = userEvent.setup()
      renderWithProviders(<ThemeToggle />, { defaultTheme: 'light' })

      await user.click(screen.getByTestId('theme-toggle-button'))

      const lightOption = screen.getByTestId('theme-option-light')
      expect(lightOption.closest('li')).toHaveAttribute('aria-selected', 'true')
    })
  })
})
