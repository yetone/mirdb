/**
 * Home Theme Integration Tests
 * Owner: Scenario 9 (primary), Scenario 10 (shared)
 *
 * Integration tests for theme support:
 * - Dark mode styling applies correctly (Scenario 9)
 * - Cyberpunk/synthwave themes work (Scenario 10)
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
    <MemoryRouter initialEntries={['/']}>
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

describe('Scenario 10: Theme Support - Alternative Themes', () => {
  beforeEach(() => {
    // Clear any previous theme attributes
    document.documentElement.removeAttribute('data-theme')
  })

  describe('Test Case 1: Render Home with ThemeContext set to cyberpunk', () => {
    it('should render homepage with cyberpunk DaisyUI theme colors', () => {
      renderWithTheme(<Home />, 'cyberpunk')

      // Verify the theme attribute is set on document
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')

      // Verify all main sections are rendered
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer')).toBeInTheDocument()

      // Verify primary elements use DaisyUI semantic classes (which will apply cyberpunk colors)
      const heroHeading = screen.getByRole('heading', { level: 1 })
      expect(heroHeading).toBeInTheDocument()

      // Check for primary/secondary color classes in hero
      expect(heroHeading.className).toContain('from-primary')
      expect(heroHeading.className).toContain('to-secondary')
    })

    it('should apply cyberpunk theme to feature cards', () => {
      renderWithTheme(<Home />, 'cyberpunk')

      // Feature section should exist with proper theming
      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()

      // Feature icons should use primary color class
      const urlShorteningCard = screen.getByTestId('feature-card-url-shortening')
      expect(urlShorteningCard).toBeInTheDocument()

      // Icons should have text-primary class (cyberpunk will apply its colors)
      const urlShorteningIcon = screen.getByTestId('feature-icon-url-shortening')
      expect(urlShorteningIcon.querySelector('svg')).toHaveClass('text-primary')
    })
  })

  describe('Test Case 2: Render Home with ThemeContext set to synthwave', () => {
    it('should render homepage with synthwave DaisyUI theme colors', () => {
      renderWithTheme(<Home />, 'synthwave')

      // Verify the theme attribute is set on document
      expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave')

      // Verify all main sections are rendered
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer')).toBeInTheDocument()

      // Verify primary elements use DaisyUI semantic classes
      const heroHeading = screen.getByRole('heading', { level: 1 })
      expect(heroHeading).toBeInTheDocument()
    })

    it('should apply synthwave theme to How It Works section', () => {
      renderWithTheme(<Home />, 'synthwave')

      // How it works section should exist with proper theming
      const howItWorksSection = screen.getByTestId('how-it-works-section')
      expect(howItWorksSection).toBeInTheDocument()

      // Should use base-200 for background (synthwave will apply its colors)
      expect(howItWorksSection.className).toContain('bg-base-200')

      // Step numbers should use primary colors
      const stepNumber1 = screen.getByTestId('step-number-1')
      expect(stepNumber1).toHaveClass('bg-primary')
      expect(stepNumber1).toHaveClass('text-primary-content')
    })
  })

  describe('Test Case 3: Verify theme consistency across all sections with alternate themes', () => {
    const alternateThemes: Theme[] = ['cyberpunk', 'synthwave']

    alternateThemes.forEach((theme) => {
      describe(`Theme: ${theme}`, () => {
        it(`should apply ${theme} theme consistently to Hero section`, () => {
          renderWithTheme(<Home />, theme)

          expect(document.documentElement.getAttribute('data-theme')).toBe(theme)

          // Hero section uses semantic color classes
          const hero = screen.getByTestId('hero-section')
          expect(hero).toBeInTheDocument()

          // Hero description uses base-content color (more specific text to hero)
          const description = hero.querySelector('p')
          expect(description).toBeInTheDocument()
          expect(description?.className).toContain('text-base-content')
        })

        it(`should apply ${theme} theme consistently to Features section`, () => {
          renderWithTheme(<Home />, theme)

          const features = screen.getByTestId('features-section')
          expect(features).toBeInTheDocument()

          // Feature cards should render with proper styling
          const analyticsCard = screen.getByTestId('feature-card-analytics')
          expect(analyticsCard).toBeInTheDocument()

          // Feature description uses base-content color
          const cardDescription = analyticsCard.querySelector('.text-base-content\\/70')
          expect(cardDescription).toBeInTheDocument()
        })

        it(`should apply ${theme} theme consistently to How It Works section`, () => {
          renderWithTheme(<Home />, theme)

          const howItWorks = screen.getByTestId('how-it-works-section')
          expect(howItWorks).toBeInTheDocument()

          // Background uses base-200 (will apply theme colors)
          expect(howItWorks.className).toContain('bg-base-200')

          // Step icons use primary color
          const stepIcon1 = screen.getByTestId('step-icon-1')
          expect(stepIcon1.className).toContain('bg-primary')
        })

        it(`should apply ${theme} theme consistently to Footer section`, () => {
          renderWithTheme(<Home />, theme)

          const footer = screen.getByTestId('footer')
          expect(footer).toBeInTheDocument()

          // Footer uses base-200 background
          expect(footer.className).toContain('bg-base-200')

          // Links use base-content colors with hover to primary
          const homeLink = screen.getByRole('link', { name: 'Home' })
          expect(homeLink.className).toContain('text-base-content')
          expect(homeLink.className).toContain('hover:text-primary')
        })
      })
    })
  })

  describe('Test Case 4: Cycling through available themes - Integration portion', () => {
    const allThemes: Theme[] = ['light', 'dark', 'cyberpunk', 'synthwave']

    allThemes.forEach((theme) => {
      it(`should render correctly with ${theme} theme without visual artifacts`, () => {
        renderWithTheme(<Home />, theme)

        // Verify theme is applied
        expect(document.documentElement.getAttribute('data-theme')).toBe(theme)

        // Verify all sections render without errors
        expect(screen.getByTestId('hero-section')).toBeInTheDocument()
        expect(screen.getByTestId('features-section')).toBeInTheDocument()
        expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
        expect(screen.getByTestId('footer')).toBeInTheDocument()

        // Verify key content is present
        expect(screen.getByText('Shorten URLs, Amplify Your Reach')).toBeInTheDocument()
        expect(screen.getByText('Powerful Features')).toBeInTheDocument()
        expect(screen.getByText('How It Works')).toBeInTheDocument()

        // Verify navigation links are present and functional
        expect(screen.getByRole('link', { name: 'Get Started' })).toBeInTheDocument()
        expect(screen.getByRole('link', { name: 'Log In' })).toBeInTheDocument()
      })

      it(`should maintain proper semantic structure with ${theme} theme`, () => {
        renderWithTheme(<Home />, theme)

        // Verify proper heading hierarchy exists
        const h1 = screen.getByRole('heading', { level: 1 })
        expect(h1).toBeInTheDocument()

        const h2Elements = screen.getAllByRole('heading', { level: 2 })
        expect(h2Elements.length).toBeGreaterThanOrEqual(2) // At least Features and How It Works

        // Verify semantic structure
        const main = document.querySelector('main')
        expect(main).toBeInTheDocument()

        const footer = screen.getByRole('contentinfo')
        expect(footer).toBeInTheDocument()
      })
    })
  })

  describe('DaisyUI Semantic Color Usage Verification', () => {
    it('should use DaisyUI semantic color classes throughout the homepage', () => {
      renderWithTheme(<Home />, 'cyberpunk')

      // Verify Hero section uses semantic colors
      const heroHeading = screen.getByRole('heading', { level: 1 })
      expect(heroHeading.className).toMatch(/from-primary/)
      expect(heroHeading.className).toMatch(/to-secondary/)

      // Verify Hero description uses base-content (get from hero section)
      const heroSection = screen.getByTestId('hero-section')
      const heroDescription = heroSection.querySelector('p')
      expect(heroDescription?.className).toMatch(/text-base-content/)

      // Verify How It Works uses bg-base-200
      const howItWorks = screen.getByTestId('how-it-works-section')
      expect(howItWorks.className).toMatch(/bg-base-200/)

      // Verify Footer uses bg-base-200
      const footer = screen.getByTestId('footer')
      expect(footer.className).toMatch(/bg-base-200/)
    })

    it('should have consistent primary color usage across all sections', () => {
      renderWithTheme(<Home />, 'synthwave')

      // Primary color used in feature icons
      const urlShorteningIcon = screen.getByTestId('feature-icon-url-shortening')
      expect(urlShorteningIcon.querySelector('svg')).toHaveClass('text-primary')

      // Primary color used in step numbers
      const stepNumber1 = screen.getByTestId('step-number-1')
      expect(stepNumber1).toHaveClass('bg-primary')

      // Primary color used in link hover states (in class definition)
      const loginFooterLink = screen.getByRole('navigation', { name: 'Footer navigation' })
        .querySelector('a[href="/login"]')
      expect(loginFooterLink?.className).toContain('hover:text-primary')
    })
  })
})
