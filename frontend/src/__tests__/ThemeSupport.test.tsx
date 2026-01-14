import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import Home from '../pages/Home'
import { ThemeProvider, ThemePreference } from '../contexts/ThemeContext'

// Helper to render homepage with a specific theme
const renderHomeWithTheme = (theme: ThemePreference) => {
  // Set initial data-theme on document before render
  const resolvedTheme = theme === 'system' ? 'light' : theme
  document.documentElement.setAttribute('data-theme', resolvedTheme)

  return render(
    <ThemeProvider defaultTheme={theme}>
      <MemoryRouter>
        <Home />
      </MemoryRouter>
    </ThemeProvider>
  )
}

describe('Theme Toggle - Multiple Theme Support', () => {
  beforeEach(() => {
    // Clear localStorage before each test
    localStorage.clear()
    // Reset data-theme attribute
    document.documentElement.removeAttribute('data-theme')
  })

  afterEach(() => {
    // Clean up after each test
    document.documentElement.removeAttribute('data-theme')
    localStorage.clear()
  })

  describe('Test Case 1: Cyberpunk Theme', () => {
    it("should apply cyberpunk theme when ThemeContext is set to 'cyberpunk'", async () => {
      renderHomeWithTheme('cyberpunk')

      // Verify the data-theme attribute is set correctly on document
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')
      })

      // Verify homepage sections are rendered
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByText('Powerful Features')).toBeInTheDocument()
      expect(screen.getByTestId('footer-section')).toBeInTheDocument()

      // Verify homepage uses DaisyUI theme classes that will pick up cyberpunk palette
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toHaveClass('bg-gradient-to-br')
      expect(heroSection.className).toMatch(/from-base-200/)
      expect(heroSection.className).toMatch(/to-base-300/)

      // Verify feature cards use base-100 which will be themed
      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards.length).toBeGreaterThan(0)
      featureCards.forEach(card => {
        expect(card.className).toMatch(/bg-base-100/)
      })

      // Verify footer uses base-300 background
      const footer = screen.getByTestId('footer-section')
      expect(footer.className).toMatch(/bg-base-300/)
    })

    it("should render all homepage components correctly with cyberpunk theme", () => {
      renderHomeWithTheme('cyberpunk')

      // Hero section
      expect(screen.getByRole('heading', { level: 1 })).toBeInTheDocument()
      expect(screen.getByTestId('cta-get-started')).toBeInTheDocument()
      expect(screen.getByTestId('cta-login')).toBeInTheDocument()

      // Features section
      expect(screen.getByText('URL Shortening')).toBeInTheDocument()
      expect(screen.getByText('Analytics Dashboard')).toBeInTheDocument()
      expect(screen.getByText('Share Statistics')).toBeInTheDocument()

      // Footer section
      expect(screen.getByRole('contentinfo')).toBeInTheDocument()
    })

    it("should read cyberpunk theme from localStorage if previously set", async () => {
      // Set theme preference in localStorage before render
      localStorage.setItem('theme-preference', 'cyberpunk')

      render(
        <ThemeProvider>
          <MemoryRouter>
            <Home />
          </MemoryRouter>
        </ThemeProvider>
      )

      // Verify theme is applied from localStorage
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')
      })
    })
  })

  describe('Test Case 2: Synthwave Theme', () => {
    it("should apply synthwave theme when ThemeContext is set to 'synthwave'", async () => {
      renderHomeWithTheme('synthwave')

      // Verify the data-theme attribute is set correctly on document
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave')
      })

      // Verify homepage sections are rendered
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByText('Powerful Features')).toBeInTheDocument()
      expect(screen.getByTestId('footer-section')).toBeInTheDocument()

      // Verify homepage uses DaisyUI theme classes that will pick up synthwave palette
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toHaveClass('bg-gradient-to-br')
      expect(heroSection.className).toMatch(/from-base-200/)
      expect(heroSection.className).toMatch(/to-base-300/)

      // Verify feature cards use base-100 which will be themed
      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards.length).toBeGreaterThan(0)
      featureCards.forEach(card => {
        expect(card.className).toMatch(/bg-base-100/)
      })

      // Verify footer uses base-300 background
      const footer = screen.getByTestId('footer-section')
      expect(footer.className).toMatch(/bg-base-300/)
    })

    it("should render all homepage components correctly with synthwave theme", () => {
      renderHomeWithTheme('synthwave')

      // Hero section
      expect(screen.getByRole('heading', { level: 1 })).toBeInTheDocument()
      expect(screen.getByTestId('cta-get-started')).toBeInTheDocument()
      expect(screen.getByTestId('cta-login')).toBeInTheDocument()

      // Features section
      expect(screen.getByText('URL Shortening')).toBeInTheDocument()
      expect(screen.getByText('Analytics Dashboard')).toBeInTheDocument()
      expect(screen.getByText('Share Statistics')).toBeInTheDocument()

      // Footer section
      expect(screen.getByRole('contentinfo')).toBeInTheDocument()
    })

    it("should read synthwave theme from localStorage if previously set", async () => {
      // Set theme preference in localStorage before render
      localStorage.setItem('theme-preference', 'synthwave')

      render(
        <ThemeProvider>
          <MemoryRouter>
            <Home />
          </MemoryRouter>
        </ThemeProvider>
      )

      // Verify theme is applied from localStorage
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave')
      })
    })
  })

  describe('Test Case 3: Light Theme', () => {
    it("should apply light theme when ThemeContext is set to 'light'", async () => {
      renderHomeWithTheme('light')

      // Verify the data-theme attribute is set correctly on document
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      })

      // Verify homepage sections are rendered
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByText('Powerful Features')).toBeInTheDocument()
      expect(screen.getByTestId('footer-section')).toBeInTheDocument()

      // Verify homepage uses DaisyUI theme classes that will pick up light palette
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toHaveClass('bg-gradient-to-br')
      expect(heroSection.className).toMatch(/from-base-200/)
      expect(heroSection.className).toMatch(/to-base-300/)

      // Verify feature cards use base-100 which will be themed
      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards.length).toBeGreaterThan(0)
      featureCards.forEach(card => {
        expect(card.className).toMatch(/bg-base-100/)
      })

      // Verify footer uses base-300 background
      const footer = screen.getByTestId('footer-section')
      expect(footer.className).toMatch(/bg-base-300/)
    })

    it("should render all homepage components correctly with light theme", () => {
      renderHomeWithTheme('light')

      // Hero section
      expect(screen.getByRole('heading', { level: 1 })).toBeInTheDocument()
      expect(screen.getByTestId('cta-get-started')).toBeInTheDocument()
      expect(screen.getByTestId('cta-login')).toBeInTheDocument()

      // Features section
      expect(screen.getByText('URL Shortening')).toBeInTheDocument()
      expect(screen.getByText('Analytics Dashboard')).toBeInTheDocument()
      expect(screen.getByText('Share Statistics')).toBeInTheDocument()

      // Footer section
      expect(screen.getByRole('contentinfo')).toBeInTheDocument()
    })

    it("should read light theme from localStorage if previously set", async () => {
      // Set theme preference in localStorage before render
      localStorage.setItem('theme-preference', 'light')

      render(
        <ThemeProvider>
          <MemoryRouter>
            <Home />
          </MemoryRouter>
        </ThemeProvider>
      )

      // Verify theme is applied from localStorage
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      })
    })
  })

  describe('Theme Integration with Homepage Components', () => {
    it("should have consistent theming across all homepage sections", () => {
      renderHomeWithTheme('cyberpunk')

      // Verify all sections use DaisyUI semantic color classes
      const heroSection = screen.getByTestId('hero-section')
      const footer = screen.getByTestId('footer-section')

      // Hero uses base-200 to base-300 gradient
      expect(heroSection.className).toContain('from-base-200')
      expect(heroSection.className).toContain('to-base-300')

      // Footer uses base-300
      expect(footer.className).toContain('bg-base-300')

      // Buttons use btn-primary and btn-outline classes (DaisyUI themed)
      const primaryButton = screen.getByTestId('cta-get-started')
      const outlineButton = screen.getByTestId('cta-login')

      expect(primaryButton).toHaveClass('btn-primary')
      expect(outlineButton).toHaveClass('btn-outline')
    })

    it("should have text using semantic color classes", () => {
      renderHomeWithTheme('synthwave')

      // Check that headings use text-base-content class
      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline.className).toContain('text-base-content')

      // Check features heading
      const featuresHeading = screen.getByText('Powerful Features')
      expect(featuresHeading.className).toContain('text-base-content')
    })

    it("should maintain accessibility with any theme", () => {
      renderHomeWithTheme('dark')

      // All interactive elements should still be accessible
      const getStartedButton = screen.getByTestId('cta-get-started')
      const loginButton = screen.getByTestId('cta-login')

      expect(getStartedButton).toHaveAttribute('href', '/register')
      expect(loginButton).toHaveAttribute('href', '/login')

      // Footer navigation should still work
      const footerNav = screen.getByRole('navigation', { name: /footer navigation/i })
      expect(footerNav).toBeInTheDocument()

      // Features section should have proper landmark
      const featuresSection = screen.getByRole('region', { name: /features/i })
      expect(featuresSection).toBeInTheDocument()
    })
  })
})
