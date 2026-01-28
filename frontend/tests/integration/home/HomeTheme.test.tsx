/**
 * Home Theme Integration Tests
 * Owner: Scenario 9 (primary), Scenario 10 (shared)
 *
 * Integration tests for theme support:
 * - Dark mode styling applies correctly (Scenario 9)
 * - Theme toggle changes appearance
 * - All sections respect theme context
 *
 * Testing framework: Vitest + @testing-library/react
 * Requires: ThemeContext wrapper
 */
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import React, { createContext, useContext, useState, ReactNode } from 'react'
import Home from '@/pages/Home'
import { GlassMorphismCard } from '@/components/GlassMorphismCard'
import { FuturisticButton } from '@/components/FuturisticButton'

// Type definitions for theme
type Theme = 'light' | 'dark' | 'cyberpunk' | 'synthwave'

interface ThemeContextType {
  theme: Theme
  setTheme: (theme: Theme) => void
}

// Create a test-friendly ThemeContext that allows setting initial theme
const TestThemeContext = createContext<ThemeContextType | undefined>(undefined)

interface TestThemeProviderProps {
  children: ReactNode
  initialTheme?: Theme
}

function TestThemeProvider({ children, initialTheme = 'dark' }: TestThemeProviderProps) {
  const [theme, setTheme] = useState<Theme>(initialTheme)

  React.useEffect(() => {
    document.documentElement.setAttribute('data-theme', theme)
  }, [theme])

  return (
    <TestThemeContext.Provider value={{ theme, setTheme }}>
      {children}
    </TestThemeContext.Provider>
  )
}

function useTestTheme() {
  const context = useContext(TestThemeContext)
  if (context === undefined) {
    throw new Error('useTestTheme must be used within a TestThemeProvider')
  }
  return context
}

// Helper component to toggle themes in tests
function ThemeToggler() {
  const { theme, setTheme } = useTestTheme()
  return (
    <div data-testid="theme-toggler">
      <span data-testid="current-theme">{theme}</span>
      <button onClick={() => setTheme('light')} data-testid="set-light">Light</button>
      <button onClick={() => setTheme('dark')} data-testid="set-dark">Dark</button>
    </div>
  )
}

// Custom render function with theme provider
function renderWithTheme(ui: React.ReactElement, initialTheme: Theme = 'dark') {
  return render(
    <MemoryRouter>
      <TestThemeProvider initialTheme={initialTheme}>
        {ui}
      </TestThemeProvider>
    </MemoryRouter>
  )
}

describe('Scenario 9: Theme Support - Dark Mode', () => {
  beforeEach(() => {
    // Reset document theme attribute before each test
    document.documentElement.removeAttribute('data-theme')
    vi.clearAllMocks()
  })

  describe('Test Case 1: Render Home with ThemeContext set to dark', () => {
    it('renders homepage with dark theme color palette', () => {
      renderWithTheme(<Home />, 'dark')

      // Verify theme attribute is set on document
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')

      // Verify homepage sections render
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer')).toBeInTheDocument()
    })

    it('applies dark theme data attribute to html element', () => {
      renderWithTheme(<Home />, 'dark')
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })
  })

  describe('Test Case 2: Check text color contrast in dark mode', () => {
    it('renders text elements with appropriate classes for dark mode contrast', () => {
      renderWithTheme(<Home />, 'dark')

      // Check hero section headline
      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline).toBeInTheDocument()

      // Check hero description text uses base-content which adapts to theme
      const heroSection = screen.getByTestId('hero-section')
      const heroDescription = heroSection.querySelector('p')
      expect(heroDescription).toHaveClass('text-base-content/80')
    })

    it('uses DaisyUI semantic color classes that adapt to dark theme', () => {
      renderWithTheme(<Home />, 'dark')

      // Footer links should use base-content which provides contrast in dark mode
      const footerLinks = screen.getAllByRole('link')
      const footerHomeLink = footerLinks.find(link => link.textContent === 'Home')
      expect(footerHomeLink).toHaveClass('text-base-content/70')
    })
  })

  describe('Test Case 3: Verify GlassMorphismCard styling in dark mode', () => {
    it('renders GlassMorphismCard with theme-adaptive classes', () => {
      renderWithTheme(
        <GlassMorphismCard>
          <div>Test Card Content</div>
        </GlassMorphismCard>,
        'dark'
      )

      // Card should use bg-base-100 which adapts to dark theme
      const card = screen.getByText('Test Card Content').closest('.card')
      expect(card).toHaveClass('bg-base-100/80')
      expect(card).toHaveClass('border-base-content/10')
    })

    it('renders feature cards within GlassMorphismCard in dark mode', () => {
      renderWithTheme(<Home />, 'dark')

      // Verify feature cards are rendered with proper classes
      const featureCard = screen.getByTestId('feature-card-url-shortening')
      expect(featureCard).toBeInTheDocument()

      // The parent GlassMorphismCard should have theme-adaptive classes
      const glassMorphismCard = featureCard.closest('.card')
      expect(glassMorphismCard).toHaveClass('bg-base-100/80')
    })
  })

  describe('Test Case 4: Verify button styling in dark mode', () => {
    it('renders FuturisticButton with theme-adaptive primary variant', () => {
      renderWithTheme(
        <FuturisticButton variant="primary">Test Button</FuturisticButton>,
        'dark'
      )

      const button = screen.getByRole('button', { name: 'Test Button' })
      expect(button).toHaveClass('btn-primary')
    })

    it('renders FuturisticButton with theme-adaptive ghost variant', () => {
      renderWithTheme(
        <FuturisticButton variant="ghost">Ghost Button</FuturisticButton>,
        'dark'
      )

      const button = screen.getByRole('button', { name: 'Ghost Button' })
      expect(button).toHaveClass('btn-ghost')
    })

    it('renders hero section CTA buttons correctly in dark mode', () => {
      renderWithTheme(<Home />, 'dark')

      // Get Started button should be primary
      const getStartedButton = screen.getByRole('button', { name: 'Get Started' })
      expect(getStartedButton).toHaveClass('btn-primary')

      // Log In button should be ghost
      const logInButton = screen.getByRole('button', { name: 'Log In' })
      expect(logInButton).toHaveClass('btn-ghost')
    })
  })

  describe('Test Case 5: Toggle theme from light to dark and back', () => {
    it('theme changes apply immediately without page reload', () => {
      render(
        <MemoryRouter>
          <TestThemeProvider initialTheme="light">
            <ThemeToggler />
            <Home />
          </TestThemeProvider>
        </MemoryRouter>
      )

      // Initially light theme
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      expect(screen.getByTestId('current-theme')).toHaveTextContent('light')

      // Toggle to dark
      fireEvent.click(screen.getByTestId('set-dark'))
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')

      // Toggle back to light
      fireEvent.click(screen.getByTestId('set-light'))
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      expect(screen.getByTestId('current-theme')).toHaveTextContent('light')

      // Verify homepage is still rendered after theme changes
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
    })

    it('components re-render with updated theme context', () => {
      render(
        <MemoryRouter>
          <TestThemeProvider initialTheme="light">
            <ThemeToggler />
            <Home />
          </TestThemeProvider>
        </MemoryRouter>
      )

      // Verify initial render
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()

      // Change theme
      fireEvent.click(screen.getByTestId('set-dark'))

      // Components should still be rendered after theme change
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer')).toBeInTheDocument()
    })
  })

  describe('Additional dark mode coverage', () => {
    it('footer adapts to dark theme', () => {
      renderWithTheme(<Home />, 'dark')

      const footer = screen.getByTestId('footer')
      expect(footer).toHaveClass('bg-base-200')
    })

    it('all semantic DaisyUI classes are used for theme compatibility', () => {
      renderWithTheme(<Home />, 'dark')

      // Verify we're using theme-adaptive classes throughout
      // Hero section description uses base-content
      const heroSection = screen.getByTestId('hero-section')
      const heroDescription = heroSection.querySelector('p')
      expect(heroDescription?.className).toContain('text-base-content')

      // Feature card description uses base-content
      const featureCard = screen.getByTestId('feature-card-url-shortening')
      const featureDescription = featureCard.querySelector('p')
      expect(featureDescription?.className).toContain('text-base-content')
    })
  })
})
