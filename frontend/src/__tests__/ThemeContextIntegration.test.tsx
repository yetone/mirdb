import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { render, screen, act, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter } from 'react-router-dom'
import { ThemeProvider, useTheme, type Theme } from '../contexts/ThemeContext'
import App from '../App'
import Home from '../pages/Home'

/**
 * ThemeContext Integration Tests
 *
 * Scenario: Verify homepage components correctly consume ThemeContext for theme awareness
 *
 * Test Cases:
 * 1. Homepage renders with ThemeContext without context-related errors
 * 2. Switch theme from light to dark - all homepage sections update to dark theme
 * 3. Theme change is instantaneous without page reload
 */

// Helper to clean up theme after tests
const cleanupTheme = () => {
  document.documentElement.removeAttribute('data-theme')
  localStorage.clear()
}

// Test wrapper component that provides ThemeContext
const TestWrapper = ({ children, defaultTheme }: { children: React.ReactNode; defaultTheme?: Theme }) => (
  <MemoryRouter initialEntries={['/']}>
    <ThemeProvider defaultTheme={defaultTheme}>{children}</ThemeProvider>
  </MemoryRouter>
)

describe('ThemeContext Integration', () => {
  beforeEach(() => {
    cleanupTheme()
  })

  afterEach(() => {
    cleanupTheme()
  })

  /**
   * Test Case 1: Homepage renders with ThemeContext without context-related errors
   * Type: unit
   * Input: Render homepage with ThemeContext
   * Expected: Homepage renders without context-related errors
   */
  describe('Test Case 1: Homepage renders with ThemeContext', () => {
    it('should render homepage wrapped in ThemeContext provider without errors', () => {
      // Mount homepage wrapped in ThemeContext provider
      const { container } = render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      // Verify no errors occurred and homepage rendered
      expect(container).toBeDefined()
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer-section')).toBeInTheDocument()
    })

    it('should not throw context-related errors when rendering homepage', () => {
      // This test verifies that the useTheme hook works correctly within ThemeProvider
      expect(() => {
        render(
          <TestWrapper>
            <App />
          </TestWrapper>
        )
      }).not.toThrow()
    })

    it('should throw error when useTheme is used outside ThemeProvider', () => {
      // Component that uses useTheme without provider
      const ComponentWithoutProvider = () => {
        const { theme } = useTheme()
        return <div>{theme}</div>
      }

      // Suppress console.error for this test since we expect an error
      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {})

      expect(() => {
        render(
          <MemoryRouter>
            <ComponentWithoutProvider />
          </MemoryRouter>
        )
      }).toThrow('useTheme must be used within a ThemeProvider')

      consoleSpy.mockRestore()
    })

    it('should initialize with default light theme when no saved preference exists', () => {
      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      // Verify data-theme attribute is set to light
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')
    })

    it('should render all homepage sections with correct theme classes', () => {
      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      // Hero section should have theme-aware gradient
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toHaveClass('from-primary')
      expect(heroSection).toHaveClass('via-secondary')
      expect(heroSection).toHaveClass('to-accent')

      // Features section should use base-200 background
      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toHaveClass('bg-base-200')

      // How It Works section should use base-200 background
      const howItWorksSection = screen.getByTestId('how-it-works-section')
      expect(howItWorksSection).toHaveClass('bg-base-200')

      // Footer should use base-200 background
      const footerSection = screen.getByTestId('footer-section')
      expect(footerSection).toHaveClass('bg-base-200')
    })
  })

  /**
   * Test Case 2: Switch theme from light to dark - all homepage sections update
   * Type: integration
   * Input: Switch theme from light to dark
   * Expected: All homepage sections update to dark theme
   */
  describe('Test Case 2: Switch theme from light to dark', () => {
    it('should update theme attribute when theme is changed via context', async () => {
      const user = userEvent.setup()

      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      // Initial theme should be light
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')

      // Click theme toggle button to switch to dark
      const themeToggle = screen.getByTestId('theme-toggle')
      await user.click(themeToggle)

      // Theme should now be dark
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })

    it('should show correct icon based on current theme', async () => {
      const user = userEvent.setup()

      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      // In light mode, should show moon icon (to switch to dark)
      expect(screen.getByTestId('moon-icon')).toBeInTheDocument()

      // Toggle to dark mode
      const themeToggle = screen.getByTestId('theme-toggle')
      await user.click(themeToggle)

      // In dark mode, should show sun icon (to switch to light)
      expect(screen.getByTestId('sun-icon')).toBeInTheDocument()
    })

    it('should update all homepage sections when theme changes to dark', async () => {
      const user = userEvent.setup()

      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      // Get all sections before theme change
      const heroSection = screen.getByTestId('hero-section')
      const featuresSection = screen.getByTestId('features-section')
      const howItWorksSection = screen.getByTestId('how-it-works-section')
      const footerSection = screen.getByTestId('footer-section')

      // Verify sections exist with initial light theme
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')

      // Toggle to dark theme
      const themeToggle = screen.getByTestId('theme-toggle')
      await user.click(themeToggle)

      // Verify dark theme is applied
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')

      // All sections should still be present and using semantic DaisyUI classes
      // These classes automatically adapt to the theme
      expect(heroSection).toBeInTheDocument()
      expect(heroSection).toHaveClass('from-primary', 'via-secondary', 'to-accent')

      expect(featuresSection).toBeInTheDocument()
      expect(featuresSection).toHaveClass('bg-base-200')

      expect(howItWorksSection).toBeInTheDocument()
      expect(howItWorksSection).toHaveClass('bg-base-200')

      expect(footerSection).toBeInTheDocument()
      expect(footerSection).toHaveClass('bg-base-200')
    })

    it('should render feature cards with GlassMorphism styling in dark mode', async () => {
      const user = userEvent.setup()

      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      // Toggle to dark theme
      const themeToggle = screen.getByTestId('theme-toggle')
      await user.click(themeToggle)

      // Verify dark theme is active
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')

      // Get glassmorphism cards
      const cards = screen.getAllByTestId('glassmorphism-card')
      expect(cards).toHaveLength(3)

      // Each card should have proper glassmorphism classes that work in dark mode
      cards.forEach((card) => {
        expect(card).toHaveClass('bg-base-100/30')
        expect(card).toHaveClass('border-base-content/10')
        expect(card).toHaveClass('backdrop-blur-md')
      })
    })

    it('should update step cards in How It Works section for dark theme', async () => {
      const user = userEvent.setup()

      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      // Toggle to dark theme
      const themeToggle = screen.getByTestId('theme-toggle')
      await user.click(themeToggle)

      // Verify dark theme is active
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')

      // Step cards should use base-100 which adapts to dark theme
      const stepCards = screen.getAllByTestId(/step-card-\d/)
      expect(stepCards).toHaveLength(3)

      stepCards.forEach((card) => {
        expect(card).toHaveClass('bg-base-100')
        expect(card).toHaveClass('shadow-xl')
      })
    })

    it('should persist theme preference in localStorage', async () => {
      const user = userEvent.setup()

      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      // Toggle to dark theme
      const themeToggle = screen.getByTestId('theme-toggle')
      await user.click(themeToggle)

      // Theme should be stored in localStorage
      expect(localStorage.getItem('theme')).toBe('dark')
    })
  })

  /**
   * Test Case 3: Theme change is instantaneous without page reload
   * Type: integration
   * Input: Switch theme while on homepage
   * Expected: Theme change is instantaneous without page reload
   */
  describe('Test Case 3: Theme change is instantaneous without page reload', () => {
    it('should change theme immediately without page reload', async () => {
      const user = userEvent.setup()

      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      // Get a reference to a section before theme change
      const featuresSection = screen.getByTestId('features-section')
      const initialFeaturesSectionRef = featuresSection

      // Toggle theme
      const themeToggle = screen.getByTestId('theme-toggle')
      await user.click(themeToggle)

      // Verify theme changed
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')

      // Verify the same section element is still in the DOM (no reload occurred)
      // If the page reloaded, the element reference would be different
      const featuresSectionAfter = screen.getByTestId('features-section')
      expect(featuresSectionAfter).toBe(initialFeaturesSectionRef)

      // Verify all sections are still present (page didn't reload and lose content)
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer-section')).toBeInTheDocument()
    })

    it('should update theme synchronously when toggle is clicked', async () => {
      const user = userEvent.setup()

      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      // Initial theme is light
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')

      // Toggle theme
      const themeToggle = screen.getByTestId('theme-toggle')

      // Use act to ensure all React updates are flushed
      await act(async () => {
        await user.click(themeToggle)
      })

      // Theme should be immediately updated (synchronously)
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })

    it('should maintain all homepage content during theme transition', async () => {
      const user = userEvent.setup()

      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      // Get content before theme change
      const mainHeadline = screen.getByRole('heading', { level: 1 })
      const headlineTextBefore = mainHeadline.textContent

      const featuresHeading = screen.getByTestId('features-heading')
      const featuresTextBefore = featuresHeading.textContent

      const howItWorksHeading = screen.getByTestId('how-it-works-heading')
      const howItWorksTextBefore = howItWorksHeading.textContent

      // Toggle theme
      const themeToggle = screen.getByTestId('theme-toggle')
      await user.click(themeToggle)

      // Verify all content is preserved
      expect(mainHeadline.textContent).toBe(headlineTextBefore)
      expect(featuresHeading.textContent).toBe(featuresTextBefore)
      expect(howItWorksHeading.textContent).toBe(howItWorksTextBefore)
    })

    it('should allow multiple rapid theme toggles without issues', async () => {
      const user = userEvent.setup()

      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      const themeToggle = screen.getByTestId('theme-toggle')

      // Initial state
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')

      // Rapidly toggle theme multiple times
      await user.click(themeToggle) // light -> dark
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')

      await user.click(themeToggle) // dark -> light
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')

      await user.click(themeToggle) // light -> dark
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')

      await user.click(themeToggle) // dark -> light
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')

      // All sections should still be intact
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer-section')).toBeInTheDocument()
    })

    it('should update theme toggle aria-label immediately', async () => {
      const user = userEvent.setup()

      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      const themeToggle = screen.getByTestId('theme-toggle')

      // In light mode, aria-label should indicate switching to dark
      expect(themeToggle).toHaveAttribute('aria-label', 'Switch to dark mode')

      // Toggle to dark
      await user.click(themeToggle)

      // In dark mode, aria-label should indicate switching to light
      expect(themeToggle).toHaveAttribute('aria-label', 'Switch to light mode')
    })

    it('should not cause visual inconsistencies between sections during theme change', async () => {
      const user = userEvent.setup()

      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      // Toggle to dark theme
      const themeToggle = screen.getByTestId('theme-toggle')
      await user.click(themeToggle)

      // All sections should have consistent theme-aware styling
      const featuresSection = screen.getByTestId('features-section')
      const howItWorksSection = screen.getByTestId('how-it-works-section')
      const footerSection = screen.getByTestId('footer-section')

      // All should use the same base background class
      expect(featuresSection).toHaveClass('bg-base-200')
      expect(howItWorksSection).toHaveClass('bg-base-200')
      expect(footerSection).toHaveClass('bg-base-200')

      // Hero uses gradient which also adapts
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toHaveClass('from-primary', 'via-secondary', 'to-accent')
    })
  })

  // Additional integration tests for ThemeContext
  describe('ThemeContext with saved preferences', () => {
    it('should load saved theme from localStorage', () => {
      // Set a saved theme preference
      localStorage.setItem('theme', 'dark')

      render(
        <TestWrapper>
          <App />
        </TestWrapper>
      )

      // Theme should be loaded from localStorage
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      expect(screen.getByTestId('sun-icon')).toBeInTheDocument()
    })

    it('should respect defaultTheme prop when provided', () => {
      render(
        <TestWrapper defaultTheme="dark">
          <App />
        </TestWrapper>
      )

      // Theme should use defaultTheme prop
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })
  })
})
