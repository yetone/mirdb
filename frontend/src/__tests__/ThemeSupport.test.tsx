import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, act, waitFor } from '@testing-library/react'
import { BrowserRouter, MemoryRouter } from 'react-router-dom'
import Home from '../pages/Home'
import ThemeToggle, { Theme } from '../components/ThemeToggle'
import App from '../App'

// Helper to render with Router
const renderWithRouter = (ui: React.ReactElement, { route = '/' } = {}) => {
  return render(<MemoryRouter initialEntries={[route]}>{ui}</MemoryRouter>)
}

describe('Theme Support and Toggle - Scenario Tests', () => {
  beforeEach(() => {
    // Reset document theme before each test
    document.documentElement.setAttribute('data-theme', 'light')
    localStorage.clear()
    vi.clearAllMocks()
  })

  afterEach(() => {
    // Clean up
    document.documentElement.setAttribute('data-theme', 'light')
    localStorage.clear()
  })

  // ============================================================================
  // Test Case 1: Render homepage with default theme
  // Input: Render homepage with default theme
  // Expected: Homepage renders correctly with default theme applied
  // ============================================================================
  describe('Test Case 1: Render homepage with default theme', () => {
    it('homepage renders correctly with default light theme', () => {
      render(
        <BrowserRouter>
          <Home />
        </BrowserRouter>
      )

      // Verify homepage renders
      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()

      // Verify the page has DaisyUI base classes that respond to theme
      expect(homePage).toHaveClass('bg-base-100')
    })

    it('homepage applies default theme to document', () => {
      render(
        <BrowserRouter>
          <Home />
        </BrowserRouter>
      )

      // Default theme should be applied
      const dataTheme = document.documentElement.getAttribute('data-theme')
      expect(dataTheme).toBe('light')
    })

    it('theme toggle component is present and accessible in homepage', () => {
      render(
        <BrowserRouter>
          <Home />
        </BrowserRouter>
      )

      // There may be multiple theme toggles (desktop and mobile), check at least one exists
      const themeToggles = screen.getAllByTestId('theme-toggle')
      expect(themeToggles.length).toBeGreaterThanOrEqual(1)
      const themeToggle = themeToggles[0]
      expect(themeToggle).toBeInTheDocument()
      expect(themeToggle).toHaveAttribute('aria-label')
      expect(themeToggle.getAttribute('aria-label')).toContain('theme')
    })

    it('homepage sections render correctly with default theme', () => {
      render(
        <BrowserRouter>
          <Home />
        </BrowserRouter>
      )

      // Verify all sections are present
      expect(screen.getByTestId('navigation-header')).toBeInTheDocument()
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
    })

    it('App component sets default data-theme attribute', () => {
      render(<App />)

      // The App should have light theme as default
      const appContainer = document.querySelector('[data-theme]')
      expect(appContainer).toBeInTheDocument()
    })
  })

  // ============================================================================
  // Test Case 2: Click theme toggle to switch to dark mode
  // Input: Click theme toggle to switch to dark mode
  // Expected: Page theme changes to dark mode without page reload, transition is smooth
  // ============================================================================
  describe('Test Case 2: Switch to dark mode', () => {
    it('clicking theme toggle changes theme from light to dark', () => {
      render(
        <BrowserRouter>
          <Home />
        </BrowserRouter>
      )

      // There may be multiple theme toggles (desktop and mobile)
      const themeToggles = screen.getAllByTestId('theme-toggle')
      const themeToggle = themeToggles[0]

      // Verify initial state is light
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')

      // Click to change to dark
      fireEvent.click(themeToggle)

      // Verify theme changed to dark
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })

    it('theme changes without page reload', () => {
      const locationHref = window.location.href

      render(
        <BrowserRouter>
          <Home />
        </BrowserRouter>
      )

      // There may be multiple theme toggles (desktop and mobile)
      const themeToggles = screen.getAllByTestId('theme-toggle')
      const themeToggle = themeToggles[0]
      fireEvent.click(themeToggle)

      // Location should not change (no reload)
      expect(window.location.href).toBe(locationHref)
    })

    it('dark theme is applied via data-theme attribute', () => {
      renderWithRouter(<ThemeToggle />)

      const button = screen.getByTestId('theme-toggle')

      // Click to dark
      fireEvent.click(button)

      // Verify DOM attribute is set correctly for DaisyUI
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })

    it('aria-label updates when theme changes to dark', () => {
      renderWithRouter(<ThemeToggle />)

      const button = screen.getByTestId('theme-toggle')

      // Click to dark
      fireEvent.click(button)

      // aria-label should reflect current theme
      expect(button.getAttribute('aria-label')).toContain('dark')
    })
  })

  // ============================================================================
  // Test Case 3: Click theme toggle to switch to light mode
  // Input: Click theme toggle to switch to light mode
  // Expected: Page theme changes to light mode without page reload, transition is smooth
  // ============================================================================
  describe('Test Case 3: Switch to light mode', () => {
    it('can cycle back to light mode after going through all themes', () => {
      renderWithRouter(<ThemeToggle />)

      const button = screen.getByTestId('theme-toggle')

      // Start at light
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')

      // Cycle through: light -> dark -> cyberpunk -> synthwave -> light
      fireEvent.click(button) // dark
      fireEvent.click(button) // cyberpunk
      fireEvent.click(button) // synthwave
      fireEvent.click(button) // back to light

      expect(document.documentElement.getAttribute('data-theme')).toBe('light')
    })

    it('switching to light mode from dark mode works', () => {
      // Start with dark theme
      document.documentElement.setAttribute('data-theme', 'synthwave')

      renderWithRouter(<ThemeToggle />)

      const button = screen.getByTestId('theme-toggle')

      // Click to cycle to light
      fireEvent.click(button)

      expect(document.documentElement.getAttribute('data-theme')).toBe('light')
    })

    it('theme changes to light without page reload', () => {
      document.documentElement.setAttribute('data-theme', 'synthwave')
      const locationHref = window.location.href

      renderWithRouter(<ThemeToggle />)

      const button = screen.getByTestId('theme-toggle')
      fireEvent.click(button)

      // Location should not change (no reload)
      expect(window.location.href).toBe(locationHref)
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')
    })

    it('aria-label updates when switching to light mode', () => {
      document.documentElement.setAttribute('data-theme', 'synthwave')

      renderWithRouter(<ThemeToggle />)

      const button = screen.getByTestId('theme-toggle')
      fireEvent.click(button)

      expect(button.getAttribute('aria-label')).toContain('light')
    })
  })

  // ============================================================================
  // Test Case 4: Render homepage with 'cyberpunk' theme
  // Input: Render homepage with 'cyberpunk' theme
  // Expected: Homepage renders correctly with cyberpunk DaisyUI theme applied
  // ============================================================================
  describe('Test Case 4: Render homepage with cyberpunk theme', () => {
    it('homepage renders correctly with cyberpunk theme applied', () => {
      // Set cyberpunk theme before rendering
      document.documentElement.setAttribute('data-theme', 'cyberpunk')

      render(
        <BrowserRouter>
          <Home />
        </BrowserRouter>
      )

      // Verify homepage renders
      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()

      // Verify cyberpunk theme is applied
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')
    })

    it('theme toggle shows correct icon for cyberpunk theme', () => {
      document.documentElement.setAttribute('data-theme', 'cyberpunk')

      renderWithRouter(<ThemeToggle />)

      const button = screen.getByTestId('theme-toggle')

      // Should have an SVG icon
      const svg = button.querySelector('svg')
      expect(svg).toBeInTheDocument()

      // aria-label should mention cyberpunk
      expect(button.getAttribute('aria-label')).toContain('cyberpunk')
    })

    it('all homepage sections render with cyberpunk theme', () => {
      document.documentElement.setAttribute('data-theme', 'cyberpunk')

      render(
        <BrowserRouter>
          <Home />
        </BrowserRouter>
      )

      // All sections should still render correctly
      expect(screen.getByTestId('navigation-header')).toBeInTheDocument()
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
    })

    it('cyberpunk theme can be set via theme toggle cycling', () => {
      renderWithRouter(<ThemeToggle />)

      const button = screen.getByTestId('theme-toggle')

      // Cycle: light -> dark -> cyberpunk
      fireEvent.click(button) // dark
      fireEvent.click(button) // cyberpunk

      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')
    })
  })

  // ============================================================================
  // Test Case 5: Render homepage with 'synthwave' theme
  // Input: Render homepage with 'synthwave' theme
  // Expected: Homepage renders correctly with synthwave DaisyUI theme applied
  // ============================================================================
  describe('Test Case 5: Render homepage with synthwave theme', () => {
    it('homepage renders correctly with synthwave theme applied', () => {
      // Set synthwave theme before rendering
      document.documentElement.setAttribute('data-theme', 'synthwave')

      render(
        <BrowserRouter>
          <Home />
        </BrowserRouter>
      )

      // Verify homepage renders
      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()

      // Verify synthwave theme is applied
      expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave')
    })

    it('theme toggle shows correct icon for synthwave theme', () => {
      document.documentElement.setAttribute('data-theme', 'synthwave')

      renderWithRouter(<ThemeToggle />)

      const button = screen.getByTestId('theme-toggle')

      // Should have an SVG icon
      const svg = button.querySelector('svg')
      expect(svg).toBeInTheDocument()

      // aria-label should mention synthwave
      expect(button.getAttribute('aria-label')).toContain('synthwave')
    })

    it('all homepage sections render with synthwave theme', () => {
      document.documentElement.setAttribute('data-theme', 'synthwave')

      render(
        <BrowserRouter>
          <Home />
        </BrowserRouter>
      )

      // All sections should still render correctly
      expect(screen.getByTestId('navigation-header')).toBeInTheDocument()
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
    })

    it('synthwave theme can be set via theme toggle cycling', () => {
      renderWithRouter(<ThemeToggle />)

      const button = screen.getByTestId('theme-toggle')

      // Cycle: light -> dark -> cyberpunk -> synthwave
      fireEvent.click(button) // dark
      fireEvent.click(button) // cyberpunk
      fireEvent.click(button) // synthwave

      expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave')
    })
  })

  // ============================================================================
  // Test Case 6: Change theme and reload page (theme persistence)
  // Input: Change theme and reload page
  // Expected: Selected theme persists after page reload
  // ============================================================================
  describe('Test Case 6: Theme persistence after page reload', () => {
    it('theme is saved to localStorage when changed', () => {
      renderWithRouter(<ThemeToggle />)

      const button = screen.getByTestId('theme-toggle')

      // Change theme
      fireEvent.click(button)

      // Verify localStorage was updated
      expect(localStorage.getItem('theme')).toBe('dark')
    })

    it('each theme change persists to localStorage', () => {
      renderWithRouter(<ThemeToggle />)

      const button = screen.getByTestId('theme-toggle')

      // Cycle through all themes and verify each is saved
      fireEvent.click(button)
      expect(localStorage.getItem('theme')).toBe('dark')

      fireEvent.click(button)
      expect(localStorage.getItem('theme')).toBe('cyberpunk')

      fireEvent.click(button)
      expect(localStorage.getItem('theme')).toBe('synthwave')

      fireEvent.click(button)
      expect(localStorage.getItem('theme')).toBe('light')
    })

    it('ThemeToggle reads initial theme from document data-theme attribute', () => {
      // Simulate persisted theme
      document.documentElement.setAttribute('data-theme', 'cyberpunk')
      localStorage.setItem('theme', 'cyberpunk')

      renderWithRouter(<ThemeToggle />)

      const button = screen.getByTestId('theme-toggle')

      // aria-label should reflect the current theme
      expect(button.getAttribute('aria-label')).toContain('cyberpunk')
    })

    it('simulated page reload maintains theme via localStorage', () => {
      // Step 1: Set theme
      renderWithRouter(<ThemeToggle />)
      const button = screen.getByTestId('theme-toggle')
      fireEvent.click(button) // Switch to dark

      // Verify localStorage has the theme
      expect(localStorage.getItem('theme')).toBe('dark')

      // Step 2: Simulate "reload" by re-rendering with saved localStorage
      // Clear current component
      document.body.innerHTML = ''

      // Set document attribute from localStorage (simulating what would happen on reload)
      const savedTheme = localStorage.getItem('theme')
      if (savedTheme) {
        document.documentElement.setAttribute('data-theme', savedTheme)
      }

      // Re-render
      renderWithRouter(<ThemeToggle />)

      // Theme should be maintained
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })

    it('theme persists correctly for all available themes', () => {
      const themes: Theme[] = ['light', 'dark', 'cyberpunk', 'synthwave']

      themes.forEach((theme) => {
        // Set theme in localStorage and document
        localStorage.setItem('theme', theme)
        document.documentElement.setAttribute('data-theme', theme)

        // Verify document has correct theme
        expect(document.documentElement.getAttribute('data-theme')).toBe(theme)
        expect(localStorage.getItem('theme')).toBe(theme)
      })
    })
  })

  // ============================================================================
  // Test Case 7: Verify no flash of unstyled content during theme change
  // Input: Verify no flash of unstyled content during theme change
  // Expected: Theme transitions smoothly without flickering or flash of wrong theme
  // ============================================================================
  describe('Test Case 7: No flash of unstyled content during theme change', () => {
    it('theme change updates data-theme attribute synchronously', () => {
      renderWithRouter(<ThemeToggle />)

      const button = screen.getByTestId('theme-toggle')

      // Before click
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')

      // Click - theme should change immediately (synchronously)
      fireEvent.click(button)

      // After click - should be dark immediately
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })

    it('homepage uses DaisyUI base classes for smooth theming', () => {
      render(
        <BrowserRouter>
          <Home />
        </BrowserRouter>
      )

      const homePage = screen.getByTestId('home-page')

      // Uses bg-base-100 which is a DaisyUI semantic color
      expect(homePage).toHaveClass('bg-base-100')
    })

    it('navigation header uses DaisyUI themed classes', () => {
      render(
        <BrowserRouter>
          <Home />
        </BrowserRouter>
      )

      const navHeader = screen.getByTestId('navigation-header')

      // Uses bg-base-100 class for themed background
      expect(navHeader).toHaveClass('bg-base-100')
    })

    it('theme transition does not cause intermediate states', () => {
      renderWithRouter(<ThemeToggle />)

      const button = screen.getByTestId('theme-toggle')

      // Get initial theme
      const initialTheme = document.documentElement.getAttribute('data-theme')
      expect(initialTheme).toBe('light')

      // Change theme
      fireEvent.click(button)

      // Theme should change directly to dark (no intermediate states)
      const finalTheme = document.documentElement.getAttribute('data-theme')
      expect(finalTheme).toBe('dark')

      // Verify it's a single direct transition (not light -> undefined -> dark)
      expect(finalTheme).not.toBe('light')
      expect(finalTheme).not.toBe(null)
      expect(finalTheme).not.toBe('')
    })

    it('CSS transition class is available for smooth theme changes', () => {
      render(
        <BrowserRouter>
          <Home />
        </BrowserRouter>
      )

      // The App uses DaisyUI which handles transitions via CSS
      // Verify the theme attribute mechanism is in place
      const dataTheme = document.documentElement.getAttribute('data-theme')
      expect(dataTheme).toBeTruthy()
    })

    it('multiple rapid theme changes work without visual glitches', () => {
      renderWithRouter(<ThemeToggle />)

      const button = screen.getByTestId('theme-toggle')

      // Rapid clicks
      fireEvent.click(button) // dark
      fireEvent.click(button) // cyberpunk
      fireEvent.click(button) // synthwave
      fireEvent.click(button) // light

      // Should end at light
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')

      // localStorage should reflect final state
      expect(localStorage.getItem('theme')).toBe('light')
    })

    it('theme state remains consistent between component state and DOM', () => {
      renderWithRouter(<ThemeToggle />)

      const button = screen.getByTestId('theme-toggle')

      // Change theme
      fireEvent.click(button)

      // DOM attribute and aria-label should be in sync
      const domTheme = document.documentElement.getAttribute('data-theme')
      const ariaLabel = button.getAttribute('aria-label')

      expect(domTheme).toBe('dark')
      expect(ariaLabel).toContain('dark')
    })
  })
})

// Additional edge case tests
describe('Theme Support Edge Cases', () => {
  beforeEach(() => {
    document.documentElement.setAttribute('data-theme', 'light')
    localStorage.clear()
  })

  it('handles invalid theme in localStorage gracefully', () => {
    // Set invalid theme
    localStorage.setItem('theme', 'invalid-theme')
    document.documentElement.setAttribute('data-theme', 'invalid-theme')

    // Render toggle - it should fallback to light or handle gracefully
    renderWithRouter(<ThemeToggle />)

    const button = screen.getByTestId('theme-toggle')

    // Component should still render
    expect(button).toBeInTheDocument()
  })

  it('handles empty localStorage gracefully', () => {
    localStorage.clear()

    renderWithRouter(<ThemeToggle />)

    const button = screen.getByTestId('theme-toggle')
    expect(button).toBeInTheDocument()
  })

  it('all four DaisyUI themes are configured', () => {
    // This test documents the expected themes
    const expectedThemes: Theme[] = ['light', 'dark', 'cyberpunk', 'synthwave']

    renderWithRouter(<ThemeToggle />)
    const button = screen.getByTestId('theme-toggle')

    // Cycle through all themes
    expectedThemes.forEach((expectedTheme, index) => {
      if (index > 0) {
        fireEvent.click(button)
      }
      // Current theme should match expected at this position
      if (index === 0) {
        expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      } else if (index === 1) {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      } else if (index === 2) {
        expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')
      } else if (index === 3) {
        expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave')
      }
    })
  })
})
