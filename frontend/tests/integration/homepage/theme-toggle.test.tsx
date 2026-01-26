/**
 * Theme Toggle Integration Tests
 * Owner: Scenario 10 - Theme Toggle - Dark Mode
 *
 * Tests for theme switching functionality and localStorage persistence.
 */

// Mock IntersectionObserver for framer-motion's whileInView
const mockIntersectionObserver = vi.fn()
mockIntersectionObserver.mockReturnValue({
  observe: () => null,
  unobserve: () => null,
  disconnect: () => null,
})
window.IntersectionObserver = mockIntersectionObserver

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { BrowserRouter } from 'react-router-dom'
import { ThemeProvider } from '../../../src/contexts/ThemeContext'
import Home from '../../../src/pages/Home'
import { ThemeToggle } from '../../../src/components/ThemeToggle'
import '@testing-library/jest-dom'

// Helper to render with all providers
function renderWithProviders(ui: React.ReactElement) {
  return render(
    <BrowserRouter>
      <ThemeProvider>{ui}</ThemeProvider>
    </BrowserRouter>
  )
}

describe('Theme Toggle Integration Tests', () => {
  beforeEach(() => {
    // Clear localStorage before each test
    localStorage.clear()
    // Reset document attribute
    document.documentElement.removeAttribute('data-theme')
  })

  afterEach(() => {
    localStorage.clear()
    document.documentElement.removeAttribute('data-theme')
  })

  describe('Test Case 1: Theme toggle button is present and functional', () => {
    it('should render Home component with ThemeContext and display theme toggle', () => {
      renderWithProviders(<Home />)

      // Theme toggle should be present
      const themeToggle = screen.getByTestId('theme-toggle')
      expect(themeToggle).toBeInTheDocument()
      expect(themeToggle).toBeEnabled()
    })

    it('should have accessible theme toggle button with aria-label', () => {
      renderWithProviders(<Home />)

      const themeToggle = screen.getByTestId('theme-toggle')
      expect(themeToggle).toHaveAttribute('aria-label')
    })
  })

  describe('Test Case 2: Theme context updates on toggle click', () => {
    it('should toggle from dark to light mode when clicked', async () => {
      // Set initial theme to dark
      localStorage.setItem('theme', 'dark')
      renderWithProviders(<Home />)

      const themeToggle = screen.getByTestId('theme-toggle')

      // Initial state should be dark
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')

      // Click to toggle
      await userEvent.click(themeToggle)

      // Should now be light
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      })
    })

    it('should toggle from light to dark mode when clicked', async () => {
      // Set initial theme to light
      localStorage.setItem('theme', 'light')
      renderWithProviders(<Home />)

      const themeToggle = screen.getByTestId('theme-toggle')

      // Initial state should be light
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')

      // Click to toggle
      await userEvent.click(themeToggle)

      // Should now be dark
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      })
    })

    it('should update aria-label based on current theme', async () => {
      localStorage.setItem('theme', 'dark')
      renderWithProviders(<Home />)

      const themeToggle = screen.getByTestId('theme-toggle')

      // In dark mode, label should say "Switch to light mode"
      expect(themeToggle).toHaveAttribute('aria-label', 'Switch to light mode')

      // Click to toggle to light
      await userEvent.click(themeToggle)

      await waitFor(() => {
        expect(themeToggle).toHaveAttribute('aria-label', 'Switch to dark mode')
      })
    })
  })

  describe('Test Case 3: Theme preference persists to localStorage', () => {
    it('should save theme preference to localStorage when toggled', async () => {
      localStorage.setItem('theme', 'dark')
      renderWithProviders(<Home />)

      const themeToggle = screen.getByTestId('theme-toggle')

      // Toggle to light mode
      await userEvent.click(themeToggle)

      await waitFor(() => {
        expect(localStorage.getItem('theme')).toBe('light')
      })
    })

    it('should toggle theme multiple times and persist each change', async () => {
      localStorage.setItem('theme', 'dark')
      renderWithProviders(<Home />)

      const themeToggle = screen.getByTestId('theme-toggle')

      // Toggle to light
      await userEvent.click(themeToggle)
      await waitFor(() => {
        expect(localStorage.getItem('theme')).toBe('light')
      })

      // Toggle back to dark
      await userEvent.click(themeToggle)
      await waitFor(() => {
        expect(localStorage.getItem('theme')).toBe('dark')
      })

      // Toggle to light again
      await userEvent.click(themeToggle)
      await waitFor(() => {
        expect(localStorage.getItem('theme')).toBe('light')
      })
    })
  })

  describe('Theme Context Provider Tests', () => {
    it('should load initial theme from localStorage', () => {
      localStorage.setItem('theme', 'light')
      renderWithProviders(<ThemeToggle />)

      // Document should have light theme
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')
    })

    it('should default to dark theme when no localStorage value exists', () => {
      // Don't set localStorage
      renderWithProviders(<ThemeToggle />)

      // Should default to dark
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })
  })

  describe('Hero Section Theme Integration', () => {
    it('should render hero section correctly when theme is dark', async () => {
      localStorage.setItem('theme', 'dark')
      renderWithProviders(<Home />)

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Verify document has dark theme
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })

    it('should render hero section correctly when theme is light', async () => {
      localStorage.setItem('theme', 'light')
      renderWithProviders(<Home />)

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Verify document has light theme
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')
    })

    it('should update hero section styling when theme toggles', async () => {
      localStorage.setItem('theme', 'dark')
      renderWithProviders(<Home />)

      const themeToggle = screen.getByTestId('theme-toggle')
      const heroSection = screen.getByTestId('hero-section')

      // Hero section exists in dark mode
      expect(heroSection).toBeInTheDocument()
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')

      // Toggle to light
      await userEvent.click(themeToggle)

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      })

      // Hero section should still be present
      expect(heroSection).toBeInTheDocument()
    })
  })

  describe('Feature Cards Theme Integration', () => {
    it('should render feature cards correctly when theme is dark', async () => {
      localStorage.setItem('theme', 'dark')
      renderWithProviders(<Home />)

      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()

      // Check for feature cards
      const featureCard0 = screen.getByTestId('feature-card-0')
      expect(featureCard0).toBeInTheDocument()

      // Verify document has dark theme
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })

    it('should render feature cards correctly when theme is light', async () => {
      localStorage.setItem('theme', 'light')
      renderWithProviders(<Home />)

      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()

      // Check for feature cards
      const featureCard0 = screen.getByTestId('feature-card-0')
      expect(featureCard0).toBeInTheDocument()

      // Verify document has light theme
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')
    })

    it('should maintain feature cards when theme toggles', async () => {
      localStorage.setItem('theme', 'dark')
      renderWithProviders(<Home />)

      const themeToggle = screen.getByTestId('theme-toggle')

      // Verify all three feature cards exist
      expect(screen.getByTestId('feature-card-0')).toBeInTheDocument()
      expect(screen.getByTestId('feature-card-1')).toBeInTheDocument()
      expect(screen.getByTestId('feature-card-2')).toBeInTheDocument()

      // Toggle theme
      await userEvent.click(themeToggle)

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      })

      // Feature cards should still be present
      expect(screen.getByTestId('feature-card-0')).toBeInTheDocument()
      expect(screen.getByTestId('feature-card-1')).toBeInTheDocument()
      expect(screen.getByTestId('feature-card-2')).toBeInTheDocument()
    })
  })

  describe('Theme Toggle Accessibility', () => {
    it('should be keyboard accessible', async () => {
      localStorage.setItem('theme', 'dark')
      renderWithProviders(<Home />)

      const themeToggle = screen.getByTestId('theme-toggle')

      // Focus the toggle
      themeToggle.focus()
      expect(document.activeElement).toBe(themeToggle)

      // Press Enter to toggle
      fireEvent.keyDown(themeToggle, { key: 'Enter', code: 'Enter' })
      await userEvent.keyboard('{Enter}')

      await waitFor(() => {
        expect(localStorage.getItem('theme')).toBe('light')
      })
    })

    it('should have appropriate button role', () => {
      renderWithProviders(<ThemeToggle />)

      const themeToggle = screen.getByTestId('theme-toggle')
      expect(themeToggle.tagName.toLowerCase()).toBe('button')
    })
  })
})
