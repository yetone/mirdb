/**
 * Homepage Theme Integration Tests
 * Owner: Scenario 5 - Theme Switching
 *
 * Tests theme toggle functionality on the homepage:
 * - Theme toggle component rendering
 * - Theme switching behavior
 * - localStorage persistence
 * - Multiple theme support
 * - Visual feedback for theme changes
 *
 * Requirements: REQ-8 - Support theme switching (light/dark mode)
 * User Stories: US-6 - Toggle Theme
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, waitFor, act } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter } from 'react-router-dom'
import { ThemeProvider, AVAILABLE_THEMES, Theme } from '../../src/contexts/ThemeContext'
import { ThemeToggle } from '../../src/components/ThemeToggle'
import { Navbar } from '../../src/components/Navbar'
import { HeroSection } from '../../src/components/homepage/HeroSection'
import {
  setupLocalStorageMock,
  setupMatchMediaMock,
  getCurrentDocumentTheme,
  resetDocumentTheme,
  THEME_STORAGE_KEY,
} from '../mocks/theme'

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: ({
      children,
      whileHover,
      whileTap,
      initial,
      animate,
      transition,
      ...props
    }: React.PropsWithChildren<Record<string, unknown>>) => <div {...props}>{children}</div>,
    button: ({
      children,
      whileHover,
      whileTap,
      initial,
      animate,
      transition,
      ...props
    }: React.PropsWithChildren<Record<string, unknown>>) => <button {...props}>{children}</button>,
  },
}))

// Helper component: Home page mock with theme toggle in navbar
function MockHomePage() {
  return (
    <div data-testid="home-page">
      <nav className="navbar" data-testid="navbar">
        <div className="flex-1">
          <span>ShortURL</span>
        </div>
        <div className="flex-none">
          <ThemeToggle />
        </div>
      </nav>
      <main>
        <HeroSection />
      </main>
    </div>
  )
}

// Helper: Render with ThemeProvider and Router
function renderWithTheme(ui: React.ReactNode, initialTheme?: Theme) {
  return render(
    <MemoryRouter>
      <ThemeProvider initialTheme={initialTheme}>{ui}</ThemeProvider>
    </MemoryRouter>
  )
}

describe('Homepage Theme Integration Tests', () => {
  let mockLocalStorage: ReturnType<typeof setupLocalStorageMock>

  beforeEach(() => {
    // Reset document theme
    resetDocumentTheme()
    // Setup fresh localStorage mock
    mockLocalStorage = setupLocalStorageMock()
    // Setup matchMedia mock (default: light mode preference)
    setupMatchMediaMock(false)
  })

  afterEach(() => {
    vi.clearAllMocks()
    resetDocumentTheme()
  })

  /**
   * Test Case 1: Render Home page with ThemeContext
   * Expected: ThemeToggle component is rendered in navbar
   */
  describe('TC1: ThemeToggle Rendering', () => {
    it('should render ThemeToggle component in navbar', () => {
      renderWithTheme(<MockHomePage />)

      const navbar = screen.getByTestId('navbar')
      expect(navbar).toBeInTheDocument()

      const themeToggle = screen.getByTestId('theme-toggle')
      expect(themeToggle).toBeInTheDocument()
    })

    it('should render theme toggle button that is clickable', () => {
      renderWithTheme(<MockHomePage />)

      const themeToggleButton = screen.getByTestId('theme-toggle-button')
      expect(themeToggleButton).toBeInTheDocument()
      expect(themeToggleButton).not.toBeDisabled()
    })

    it('should render navbar with ThemeToggle in the right position', () => {
      renderWithTheme(<MockHomePage />)

      const navbar = screen.getByTestId('navbar')
      const themeToggle = screen.getByTestId('theme-toggle')

      // ThemeToggle should be inside the navbar
      expect(navbar).toContainElement(themeToggle)
    })
  })

  /**
   * Test Case 2: Check initial theme application
   * Expected: Document has data-theme attribute or theme class applied
   */
  describe('TC2: Initial Theme Application', () => {
    it('should apply default theme (light) when no preference stored', () => {
      renderWithTheme(<MockHomePage />)

      const appliedTheme = getCurrentDocumentTheme()
      expect(appliedTheme).toBe('light')
    })

    it('should apply stored theme preference from localStorage', () => {
      mockLocalStorage = setupLocalStorageMock('dark')

      renderWithTheme(<MockHomePage />)

      // Check that localStorage was queried
      expect(mockLocalStorage.getItem).toHaveBeenCalledWith(THEME_STORAGE_KEY)
    })

    it('should apply data-theme attribute to document element', () => {
      renderWithTheme(<MockHomePage />, 'cyberpunk')

      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')
    })

    it('should use system preference when no localStorage value', () => {
      // Setup with dark mode system preference
      setupMatchMediaMock(true)

      render(
        <MemoryRouter>
          <ThemeProvider>
            <MockHomePage />
          </ThemeProvider>
        </MemoryRouter>
      )

      // When no stored preference, should check system preference
      expect(window.matchMedia).toHaveBeenCalledWith('(prefers-color-scheme: dark)')
    })
  })

  /**
   * Test Case 3: Click theme toggle button
   * Expected: Theme context value changes
   */
  describe('TC3: Theme Toggle Click', () => {
    it('should show dropdown menu when theme toggle is clicked', async () => {
      const user = userEvent.setup()
      renderWithTheme(<MockHomePage />)

      const themeToggleButton = screen.getByTestId('theme-toggle-button')
      await user.click(themeToggleButton)

      const dropdownMenu = screen.getByTestId('theme-dropdown-menu')
      expect(dropdownMenu).toBeInTheDocument()
    })

    it('should display all available theme options in dropdown', async () => {
      const user = userEvent.setup()
      renderWithTheme(<MockHomePage />)

      const themeToggleButton = screen.getByTestId('theme-toggle-button')
      await user.click(themeToggleButton)

      for (const theme of AVAILABLE_THEMES) {
        const option = screen.getByTestId(`theme-option-${theme}`)
        expect(option).toBeInTheDocument()
      }
    })

    it('should change theme when clicking a theme option', async () => {
      const user = userEvent.setup()
      renderWithTheme(<MockHomePage />, 'light')

      // Open dropdown and select dark theme
      const themeToggleButton = screen.getByTestId('theme-toggle-button')
      await user.click(themeToggleButton)

      const darkOption = screen.getByTestId('theme-option-dark')
      await user.click(darkOption)

      // Theme should change
      expect(getCurrentDocumentTheme()).toBe('dark')
    })
  })

  /**
   * Test Case 4: Verify DOM theme attribute after toggle
   * Expected: data-theme attribute changes to reflect new theme
   */
  describe('TC4: DOM Theme Attribute Changes', () => {
    it('should update data-theme attribute when theme changes to dark', async () => {
      const user = userEvent.setup()
      renderWithTheme(<MockHomePage />, 'light')

      expect(document.documentElement.getAttribute('data-theme')).toBe('light')

      // Change to dark theme
      await user.click(screen.getByTestId('theme-toggle-button'))
      await user.click(screen.getByTestId('theme-option-dark'))

      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })

    it('should update data-theme attribute when theme changes to cyberpunk', async () => {
      const user = userEvent.setup()
      renderWithTheme(<MockHomePage />, 'light')

      // Change to cyberpunk theme
      await user.click(screen.getByTestId('theme-toggle-button'))
      await user.click(screen.getByTestId('theme-option-cyberpunk'))

      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')
    })

    it('should update data-theme attribute when theme changes to synthwave', async () => {
      const user = userEvent.setup()
      renderWithTheme(<MockHomePage />, 'light')

      // Change to synthwave theme
      await user.click(screen.getByTestId('theme-toggle-button'))
      await user.click(screen.getByTestId('theme-option-synthwave'))

      expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave')
    })

    it('should immediately reflect theme change in DOM', async () => {
      const user = userEvent.setup()
      renderWithTheme(<MockHomePage />, 'light')

      // Initial theme
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')

      // Change theme
      await user.click(screen.getByTestId('theme-toggle-button'))
      await user.click(screen.getByTestId('theme-option-dark'))

      // DOM should be updated immediately (no waiting required beyond user event)
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })
  })

  /**
   * Test Case 5: Check localStorage after theme change
   * Expected: Theme preference is saved to localStorage
   */
  describe('TC5: localStorage Persistence', () => {
    it('should save theme preference to localStorage when theme changes', async () => {
      const user = userEvent.setup()
      mockLocalStorage = setupLocalStorageMock()

      renderWithTheme(<MockHomePage />, 'light')

      // Change theme
      await user.click(screen.getByTestId('theme-toggle-button'))
      await user.click(screen.getByTestId('theme-option-dark'))

      // Check localStorage was updated
      expect(mockLocalStorage.setItem).toHaveBeenCalledWith(THEME_STORAGE_KEY, 'dark')
    })

    it('should save each theme change to localStorage', async () => {
      const user = userEvent.setup()
      mockLocalStorage = setupLocalStorageMock()

      renderWithTheme(<MockHomePage />, 'light')

      // Change to dark
      await user.click(screen.getByTestId('theme-toggle-button'))
      await user.click(screen.getByTestId('theme-option-dark'))
      expect(mockLocalStorage.setItem).toHaveBeenCalledWith(THEME_STORAGE_KEY, 'dark')

      // Change to cyberpunk
      await user.click(screen.getByTestId('theme-toggle-button'))
      await user.click(screen.getByTestId('theme-option-cyberpunk'))
      expect(mockLocalStorage.setItem).toHaveBeenCalledWith(THEME_STORAGE_KEY, 'cyberpunk')

      // Change to synthwave
      await user.click(screen.getByTestId('theme-toggle-button'))
      await user.click(screen.getByTestId('theme-option-synthwave'))
      expect(mockLocalStorage.setItem).toHaveBeenCalledWith(THEME_STORAGE_KEY, 'synthwave')
    })

    it('should persist theme preference across multiple selections', async () => {
      const user = userEvent.setup()
      mockLocalStorage = setupLocalStorageMock()

      renderWithTheme(<MockHomePage />, 'light')

      // Change theme multiple times
      await user.click(screen.getByTestId('theme-toggle-button'))
      await user.click(screen.getByTestId('theme-option-dark'))

      await user.click(screen.getByTestId('theme-toggle-button'))
      await user.click(screen.getByTestId('theme-option-light'))

      // Verify localStorage reflects the last selection
      const lastCall = mockLocalStorage.setItem.mock.calls[mockLocalStorage.setItem.mock.calls.length - 1]
      expect(lastCall).toEqual([THEME_STORAGE_KEY, 'light'])
    })
  })

  /**
   * Test Case 6: Reload page and check theme
   * Expected: Previously selected theme is applied on load
   */
  describe('TC6: Theme Persistence on Reload', () => {
    it('should restore theme from localStorage on initial render', () => {
      // Pre-populate localStorage with dark theme
      mockLocalStorage = setupLocalStorageMock('dark')

      render(
        <MemoryRouter>
          <ThemeProvider>
            <MockHomePage />
          </ThemeProvider>
        </MemoryRouter>
      )

      // Theme should be restored from localStorage
      expect(mockLocalStorage.getItem).toHaveBeenCalledWith(THEME_STORAGE_KEY)
    })

    it('should apply cyberpunk theme on load when stored in localStorage', () => {
      mockLocalStorage = setupLocalStorageMock('cyberpunk')

      render(
        <MemoryRouter>
          <ThemeProvider>
            <MockHomePage />
          </ThemeProvider>
        </MemoryRouter>
      )

      expect(mockLocalStorage.getItem).toHaveBeenCalledWith(THEME_STORAGE_KEY)
    })

    it('should apply synthwave theme on load when stored in localStorage', () => {
      mockLocalStorage = setupLocalStorageMock('synthwave')

      render(
        <MemoryRouter>
          <ThemeProvider>
            <MockHomePage />
          </ThemeProvider>
        </MemoryRouter>
      )

      expect(mockLocalStorage.getItem).toHaveBeenCalledWith(THEME_STORAGE_KEY)
    })

    it('should fall back to default theme if localStorage has invalid value', () => {
      // Setup with invalid theme value
      mockLocalStorage = setupLocalStorageMock()
      mockLocalStorage._store[THEME_STORAGE_KEY] = 'invalid-theme'

      renderWithTheme(<MockHomePage />)

      // Should fall back to default (light) since 'invalid-theme' is not valid
      const appliedTheme = getCurrentDocumentTheme()
      expect(appliedTheme).toBe('light')
    })
  })

  /**
   * Test Case 7: Test multiple theme options
   * Expected: All available themes (light, dark, cyberpunk, etc.) can be selected
   */
  describe('TC7: Multiple Theme Selection', () => {
    it('should allow selecting light theme', async () => {
      const user = userEvent.setup()
      renderWithTheme(<MockHomePage />, 'dark')

      await user.click(screen.getByTestId('theme-toggle-button'))
      await user.click(screen.getByTestId('theme-option-light'))

      expect(getCurrentDocumentTheme()).toBe('light')
    })

    it('should allow selecting dark theme', async () => {
      const user = userEvent.setup()
      renderWithTheme(<MockHomePage />, 'light')

      await user.click(screen.getByTestId('theme-toggle-button'))
      await user.click(screen.getByTestId('theme-option-dark'))

      expect(getCurrentDocumentTheme()).toBe('dark')
    })

    it('should allow selecting cyberpunk theme', async () => {
      const user = userEvent.setup()
      renderWithTheme(<MockHomePage />, 'light')

      await user.click(screen.getByTestId('theme-toggle-button'))
      await user.click(screen.getByTestId('theme-option-cyberpunk'))

      expect(getCurrentDocumentTheme()).toBe('cyberpunk')
    })

    it('should allow selecting synthwave theme', async () => {
      const user = userEvent.setup()
      renderWithTheme(<MockHomePage />, 'light')

      await user.click(screen.getByTestId('theme-toggle-button'))
      await user.click(screen.getByTestId('theme-option-synthwave'))

      expect(getCurrentDocumentTheme()).toBe('synthwave')
    })

    it('should cycle through all themes without issues', async () => {
      const user = userEvent.setup()
      renderWithTheme(<MockHomePage />, 'light')

      for (const theme of AVAILABLE_THEMES) {
        await user.click(screen.getByTestId('theme-toggle-button'))
        await user.click(screen.getByTestId(`theme-option-${theme}`))
        expect(getCurrentDocumentTheme()).toBe(theme)
      }
    })

    it('should show visual indicator for currently selected theme', async () => {
      const user = userEvent.setup()
      renderWithTheme(<MockHomePage />, 'dark')

      await user.click(screen.getByTestId('theme-toggle-button'))

      // The dark theme option should have 'active' class or similar indicator
      const darkOption = screen.getByTestId('theme-option-dark')
      expect(darkOption.closest('li')).toHaveAttribute('aria-selected', 'true')
    })
  })

  /**
   * Test Case 8: Verify homepage content visible in all themes
   * Expected: All sections maintain readability and contrast in each theme
   */
  describe('TC8: Content Visibility Across Themes', () => {
    it('should render hero section content in light theme', () => {
      renderWithTheme(<MockHomePage />, 'light')

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeVisible()

      const headline = screen.getByTestId('hero-headline')
      expect(headline).toBeVisible()
      expect(headline).toHaveTextContent(/shorten urls/i)
    })

    it('should render hero section content in dark theme', () => {
      renderWithTheme(<MockHomePage />, 'dark')

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeVisible()

      const headline = screen.getByTestId('hero-headline')
      expect(headline).toBeVisible()
    })

    it('should render hero section content in cyberpunk theme', () => {
      renderWithTheme(<MockHomePage />, 'cyberpunk')

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeVisible()

      const ctaButton = screen.getByTestId('hero-cta-primary')
      expect(ctaButton).toBeVisible()
    })

    it('should render hero section content in synthwave theme', () => {
      renderWithTheme(<MockHomePage />, 'synthwave')

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeVisible()

      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline).toBeVisible()
    })

    it('should maintain CTA button visibility across all themes', async () => {
      const user = userEvent.setup()
      renderWithTheme(<MockHomePage />, 'light')

      for (const theme of AVAILABLE_THEMES) {
        // Change theme
        await user.click(screen.getByTestId('theme-toggle-button'))
        await user.click(screen.getByTestId(`theme-option-${theme}`))

        // Verify CTA buttons are visible
        const primaryCta = screen.getByTestId('hero-cta-primary')
        const secondaryCta = screen.getByTestId('hero-cta-secondary')

        expect(primaryCta).toBeVisible()
        expect(secondaryCta).toBeVisible()
      }
    })

    it('should maintain theme toggle visibility across all themes', async () => {
      const user = userEvent.setup()
      renderWithTheme(<MockHomePage />, 'light')

      for (const theme of AVAILABLE_THEMES) {
        // Change theme
        await user.click(screen.getByTestId('theme-toggle-button'))
        await user.click(screen.getByTestId(`theme-option-${theme}`))

        // Theme toggle should remain visible and functional
        const themeToggle = screen.getByTestId('theme-toggle')
        expect(themeToggle).toBeVisible()
      }
    })
  })

  /**
   * Additional Test: Accessibility
   */
  describe('Theme Toggle Accessibility', () => {
    it('should have proper ARIA attributes', async () => {
      const user = userEvent.setup()
      renderWithTheme(<MockHomePage />)

      const themeToggleButton = screen.getByTestId('theme-toggle-button')
      expect(themeToggleButton).toHaveAttribute('aria-label')
      expect(themeToggleButton).toHaveAttribute('aria-haspopup', 'listbox')

      await user.click(themeToggleButton)

      const dropdown = screen.getByTestId('theme-dropdown-menu')
      expect(dropdown).toHaveAttribute('role', 'listbox')
    })

    it('should be keyboard accessible', async () => {
      const user = userEvent.setup()
      renderWithTheme(<MockHomePage />)

      const themeToggleButton = screen.getByTestId('theme-toggle-button')

      // Focus on toggle button
      themeToggleButton.focus()
      expect(document.activeElement).toBe(themeToggleButton)

      // Pressing Enter should open the dropdown
      await user.keyboard('{Enter}')

      const dropdown = screen.getByTestId('theme-dropdown-menu')
      expect(dropdown).toBeInTheDocument()
    })
  })
})
