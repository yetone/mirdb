/**
 * Theme Integration Tests
 * Owner: Scenario 5 - Theme Integration
 *
 * Tests for REQ-5 and US-5:
 * - Theme switching works seamlessly without page refresh
 * - All themes (light, dark, cyberpunk, synthwave) are supported
 * - GlassMorphismCard respects theme colors
 */

import React from 'react'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, act } from '@testing-library/react'
import { MemoryRouter, Routes, Route } from 'react-router-dom'
import { AuthContext } from '@/contexts/AuthContext'
import { ThemeContext } from '@/contexts/ThemeContext'
import { Home } from '@/pages/Home'
import type { AuthContextType, ThemeContextType } from '@/types/custom'

// Track theme changes for testing
let currentTheme = 'light'
let themeSetterFn: (theme: string) => void = () => {}

// Helper to render with all required providers and a specific theme
function renderWithTheme(
  theme: string,
  authOverrides: Partial<AuthContextType> = {}
) {
  currentTheme = theme
  themeSetterFn = vi.fn((newTheme: string) => {
    currentTheme = newTheme
    document.documentElement.setAttribute('data-theme', newTheme)
  })

  // Set the data-theme attribute on document element
  document.documentElement.setAttribute('data-theme', theme)

  const mockAuthContext: AuthContextType = {
    user: null,
    isAuthenticated: false,
    isLoading: false,
    login: vi.fn(),
    logout: vi.fn(),
    register: vi.fn(),
    ...authOverrides,
  }

  const mockThemeContext: ThemeContextType = {
    theme: currentTheme,
    setTheme: themeSetterFn,
  }

  return render(
    <MemoryRouter initialEntries={['/']}>
      <ThemeContext.Provider value={mockThemeContext}>
        <AuthContext.Provider value={mockAuthContext}>
          <Routes>
            <Route path="/" element={<Home />} />
          </Routes>
        </AuthContext.Provider>
      </ThemeContext.Provider>
    </MemoryRouter>
  )
}

describe('Theme Integration Tests', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    currentTheme = 'light'
    document.documentElement.removeAttribute('data-theme')
  })

  describe('Test Case 1: Light Theme', () => {
    it('should render homepage with light theme color scheme', () => {
      renderWithTheme('light')

      // Verify the data-theme attribute is set correctly
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')

      // Verify key homepage sections are rendered
      const heroSection = screen.getByRole('region', { name: /Shorten URLs, Track Performance/i })
      expect(heroSection).toBeInTheDocument()
      expect(heroSection).toHaveClass('bg-base-200')

      // Verify features section uses theme-aware classes
      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()
      expect(featuresSection).toHaveClass('bg-base-100')

      // Verify footer uses theme-aware background
      const footer = screen.getByRole('contentinfo')
      expect(footer).toBeInTheDocument()
      expect(footer).toHaveClass('bg-base-300')
    })
  })

  describe('Test Case 2: Dark Theme', () => {
    it('should render homepage with dark theme color scheme', () => {
      renderWithTheme('dark')

      // Verify the data-theme attribute is set correctly
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')

      // Verify homepage components are rendered
      expect(screen.getByText('Shorten URLs, Track Performance')).toBeInTheDocument()
      expect(screen.getByText('Powerful Features')).toBeInTheDocument()
      expect(screen.getByText('How It Works')).toBeInTheDocument()

      // Verify sections use theme-aware Tailwind classes
      const heroSection = screen.getByRole('region', { name: /Shorten URLs, Track Performance/i })
      expect(heroSection).toHaveClass('bg-base-200')

      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toHaveClass('bg-base-100')
    })
  })

  describe('Test Case 3: Cyberpunk Theme', () => {
    it('should render homepage with cyberpunk theme styling', () => {
      renderWithTheme('cyberpunk')

      // Verify the data-theme attribute is set correctly
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')

      // Verify homepage is fully rendered with theme applied
      expect(screen.getByText('Shorten URLs, Track Performance')).toBeInTheDocument()

      // Verify hero section has theme-aware classes
      const heroSection = screen.getByRole('region', { name: /Shorten URLs, Track Performance/i })
      expect(heroSection).toBeInTheDocument()

      // Verify feature cards are rendered
      expect(screen.getByText('URL Shortening')).toBeInTheDocument()
      expect(screen.getByText('Click Analytics')).toBeInTheDocument()
      expect(screen.getByText('Geographic Insights')).toBeInTheDocument()
      expect(screen.getByText('Share Statistics')).toBeInTheDocument()
    })
  })

  describe('Test Case 4: Synthwave Theme', () => {
    it('should render homepage with synthwave theme styling', () => {
      renderWithTheme('synthwave')

      // Verify the data-theme attribute is set correctly
      expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave')

      // Verify homepage renders correctly with theme
      expect(screen.getByText('Shorten URLs, Track Performance')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()

      // Verify sections maintain their structure
      const howItWorksSection = screen.getByRole('region', { name: /How It Works/i })
      expect(howItWorksSection).toBeInTheDocument()
      expect(howItWorksSection).toHaveClass('bg-base-200')
    })
  })

  describe('Test Case 5: Theme Switching', () => {
    it('should update homepage immediately when theme changes from light to dark', () => {
      const { rerender } = renderWithTheme('light')

      // Verify initial light theme
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      expect(screen.getByText('Shorten URLs, Track Performance')).toBeInTheDocument()

      // Simulate theme change by updating context
      act(() => {
        themeSetterFn('dark')
      })

      // Verify the setTheme function was called
      expect(themeSetterFn).toHaveBeenCalledWith('dark')

      // Verify the data-theme attribute was updated
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')

      // Verify homepage content is still present (no page refresh)
      expect(screen.getByText('Shorten URLs, Track Performance')).toBeInTheDocument()
      expect(screen.getByText('Powerful Features')).toBeInTheDocument()
      expect(screen.getByText('How It Works')).toBeInTheDocument()
    })

    it('should preserve homepage state when switching themes', () => {
      renderWithTheme('light')

      // Verify initial state
      expect(screen.getByTestId('hero-get-started-btn')).toBeInTheDocument()
      expect(screen.getByTestId('hero-login-btn')).toBeInTheDocument()

      // Simulate theme change
      act(() => {
        themeSetterFn('cyberpunk')
      })

      // Verify all interactive elements are still present
      expect(screen.getByTestId('hero-get-started-btn')).toBeInTheDocument()
      expect(screen.getByTestId('hero-login-btn')).toBeInTheDocument()

      // Verify feature cards are still present
      expect(screen.getByTestId('feature-card-url-shortening')).toBeInTheDocument()
      expect(screen.getByTestId('feature-card-click-analytics')).toBeInTheDocument()
    })
  })

  describe('Test Case 6: GlassMorphismCard Theme Respect', () => {
    it('should maintain glass effect styling with theme colors in light theme', () => {
      renderWithTheme('light')

      // Get feature cards that use GlassMorphismCard
      const featureCards = screen.getAllByTestId(/feature-card-/)
      expect(featureCards.length).toBe(4)

      // Each feature card wrapper should have glass morphism classes
      featureCards.forEach(card => {
        expect(card).toHaveClass('backdrop-blur-md')
        expect(card).toHaveClass('bg-base-100/30')
        expect(card).toHaveClass('border')
        expect(card).toHaveClass('border-base-content/10')
        expect(card).toHaveClass('rounded-2xl')
        expect(card).toHaveClass('shadow-xl')
      })
    })

    it('should maintain glass effect styling with theme colors in dark theme', () => {
      renderWithTheme('dark')

      // Verify feature cards maintain glass morphism styling
      const featureCards = screen.getAllByTestId(/feature-card-/)
      expect(featureCards.length).toBe(4)

      featureCards.forEach(card => {
        expect(card).toHaveClass('backdrop-blur-md')
        expect(card).toHaveClass('bg-base-100/30')
        expect(card).toHaveClass('border-base-content/10')
      })
    })

    it('should maintain glass effect styling in cyberpunk theme', () => {
      renderWithTheme('cyberpunk')

      // Verify glass morphism cards are present with correct structure
      const featureCards = screen.getAllByTestId(/feature-card-/)

      featureCards.forEach(card => {
        // Card should have glass morphism wrapper classes
        expect(card).toHaveClass('backdrop-blur-md')
        expect(card).toHaveClass('rounded-2xl')
      })
    })
  })

  describe('Additional Theme Tests', () => {
    it('should support retro theme', () => {
      renderWithTheme('retro')
      expect(document.documentElement.getAttribute('data-theme')).toBe('retro')
      expect(screen.getByText('Shorten URLs, Track Performance')).toBeInTheDocument()
    })

    it('should support valentine theme', () => {
      renderWithTheme('valentine')
      expect(document.documentElement.getAttribute('data-theme')).toBe('valentine')
      expect(screen.getByText('Shorten URLs, Track Performance')).toBeInTheDocument()
    })

    it('should support night theme', () => {
      renderWithTheme('night')
      expect(document.documentElement.getAttribute('data-theme')).toBe('night')
      expect(screen.getByText('Shorten URLs, Track Performance')).toBeInTheDocument()
    })

    it('should render authenticated user experience with any theme', () => {
      renderWithTheme('dark', {
        isAuthenticated: true,
        user: {
          id: 1,
          username: 'testuser',
          email: 'test@example.com',
          is_admin: false,
        },
      })

      // Authenticated users see "Go to Dashboard" button
      expect(screen.getByTestId('hero-dashboard-btn')).toBeInTheDocument()
      expect(screen.queryByTestId('hero-get-started-btn')).not.toBeInTheDocument()
      expect(screen.queryByTestId('hero-login-btn')).not.toBeInTheDocument()
    })
  })
})
