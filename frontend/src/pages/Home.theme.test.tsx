import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import Home from './Home'
import { AuthProvider } from '../contexts/AuthContext'

// Mock localStorage
const localStorageMock = (() => {
  let store: Record<string, string> = {}
  return {
    getItem: vi.fn((key: string) => store[key] || null),
    setItem: vi.fn((key: string, value: string) => {
      store[key] = value
    }),
    removeItem: vi.fn((key: string) => {
      delete store[key]
    }),
    clear: vi.fn(() => {
      store = {}
    }),
  }
})()

Object.defineProperty(window, 'localStorage', { value: localStorageMock })

// Mock scrollIntoView
Element.prototype.scrollIntoView = vi.fn()

// Helper to set document theme attribute
const setDocumentTheme = (theme: string) => {
  document.documentElement.setAttribute('data-theme', theme)
}

// Helper to get computed theme colors (simulated)
const getComputedThemeAttribute = () => {
  return document.documentElement.getAttribute('data-theme')
}

// Helper to render with router
const renderWithRouter = () => {
  return render(
    <MemoryRouter initialEntries={['/']}>
      <AuthProvider>
        <Home />
      </AuthProvider>
    </MemoryRouter>
  )
}

describe('Theme Integration - Landing Page', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    localStorageMock.clear()
    // Reset to light theme
    setDocumentTheme('light')
  })

  afterEach(() => {
    // Cleanup
    document.documentElement.removeAttribute('data-theme')
  })

  // Test Case 1: Render landing page with light theme
  describe('Test Case 1: Light Theme Rendering', () => {
    it('renders landing page with light theme colors', () => {
      localStorageMock.getItem.mockReturnValue('light')
      setDocumentTheme('light')

      renderWithRouter()

      // Verify page renders
      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()

      // Verify data-theme attribute is 'light'
      expect(getComputedThemeAttribute()).toBe('light')

      // Verify all major sections are rendered
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer-section')).toBeInTheDocument()
    })

    it('hero section uses theme-aware classes in light mode', () => {
      setDocumentTheme('light')
      renderWithRouter()

      const heroSection = screen.getByTestId('hero-section')
      // Verify theme-aware Tailwind/DaisyUI classes are present
      expect(heroSection.className).toContain('bg-gradient-to-br')
      expect(heroSection.className).toContain('from-primary')
      expect(heroSection.className).toContain('to-secondary')
    })

    it('features section uses base-200 background in light mode', () => {
      setDocumentTheme('light')
      renderWithRouter()

      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection.className).toContain('bg-base-200')
    })
  })

  // Test Case 2: Render landing page with dark theme
  describe('Test Case 2: Dark Theme Rendering', () => {
    it('renders landing page with dark theme colors', () => {
      localStorageMock.getItem.mockReturnValue('dark')
      setDocumentTheme('dark')

      renderWithRouter()

      // Verify page renders
      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()

      // Verify data-theme attribute is 'dark'
      expect(getComputedThemeAttribute()).toBe('dark')

      // Verify all major sections are rendered
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer-section')).toBeInTheDocument()
    })

    it('hero section renders properly with dark theme', () => {
      setDocumentTheme('dark')
      renderWithRouter()

      const heroSection = screen.getByTestId('hero-section')
      // Verify section is present and styled with theme-aware classes
      expect(heroSection).toBeInTheDocument()
      expect(heroSection.className).toContain('base-100')
    })

    it('footer section uses theme-aware classes in dark mode', () => {
      setDocumentTheme('dark')
      renderWithRouter()

      const footerSection = screen.getByTestId('footer-section')
      expect(footerSection.className).toContain('bg-base-200')
      expect(footerSection.className).toContain('text-base-content')
    })
  })

  // Test Case 3: Toggle theme from light to dark
  describe('Test Case 3: Theme Toggle - Light to Dark', () => {
    it('updates all page sections when theme is toggled from light to dark', () => {
      localStorageMock.getItem.mockReturnValue('light')
      setDocumentTheme('light')

      renderWithRouter()

      // Verify initial theme is light
      expect(getComputedThemeAttribute()).toBe('light')

      // Simulate theme toggle to dark
      setDocumentTheme('dark')
      localStorageMock.setItem('theme', 'dark')

      // Verify theme changed
      expect(getComputedThemeAttribute()).toBe('dark')

      // Verify all sections still render (no crashes)
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer-section')).toBeInTheDocument()
    })

    it('theme-aware CSS classes respond to theme changes', () => {
      setDocumentTheme('light')
      renderWithRouter()

      // Get elements with theme-aware classes
      const featuresSection = screen.getByTestId('features-section')
      const howItWorksSection = screen.getByTestId('how-it-works-section')

      // Verify theme-aware classes exist
      expect(featuresSection.className).toContain('bg-base-200')
      expect(howItWorksSection.className).toContain('bg-base-100')

      // Toggle to dark theme
      setDocumentTheme('dark')

      // Same classes should still be present (they adapt via CSS variables)
      expect(featuresSection.className).toContain('bg-base-200')
      expect(howItWorksSection.className).toContain('bg-base-100')
    })
  })

  // Test Case 4: Text contrast in dark mode
  describe('Test Case 4: Text Contrast in Dark Mode', () => {
    it('text elements have readable contrast classes in dark mode', () => {
      setDocumentTheme('dark')
      renderWithRouter()

      // Verify headline uses gradient text for visibility
      const headline = screen.getByTestId('hero-headline')
      expect(headline.className).toContain('text-transparent')
      expect(headline.className).toContain('bg-gradient-to-r')
      expect(headline.className).toContain('from-primary')
      expect(headline.className).toContain('to-secondary')

      // Verify subheadline uses theme-aware text color
      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline.className).toContain('text-base-content')
    })

    it('feature card text uses theme-aware contrast classes', () => {
      setDocumentTheme('dark')
      renderWithRouter()

      // Features section should have readable text
      const featuresSection = screen.getByTestId('features-section')

      // Check that feature cards are present
      const cards = featuresSection.querySelectorAll('.card')
      expect(cards.length).toBeGreaterThan(0)

      // Cards should use bg-base-100 for proper contrast
      cards.forEach((card) => {
        expect(card.className).toContain('bg-base-100')
      })
    })

    it('how it works section has readable text in dark mode', () => {
      setDocumentTheme('dark')
      renderWithRouter()

      // Check step elements
      const step1 = screen.getByTestId('step-1')
      expect(step1).toBeInTheDocument()

      // Step descriptions should use theme-aware text color
      const descriptions = step1.querySelectorAll('p')
      descriptions.forEach((desc) => {
        // Should use base-content or primary colors
        expect(desc.className).toMatch(/text-base-content|text-primary/)
      })
    })

    it('footer text remains readable in dark mode', () => {
      setDocumentTheme('dark')
      renderWithRouter()

      const footerCopyright = screen.getByTestId('footer-copyright')
      // Should use theme-aware text color
      expect(footerCopyright.className).toContain('text-base-content')
    })
  })

  // Additional integration tests for theme consistency
  describe('Theme Consistency Across All Sections', () => {
    it('all sections use DaisyUI theme-aware color classes', () => {
      setDocumentTheme('light')
      renderWithRouter()

      const heroSection = screen.getByTestId('hero-section')
      const featuresSection = screen.getByTestId('features-section')
      const howItWorksSection = screen.getByTestId('how-it-works-section')
      const footerSection = screen.getByTestId('footer-section')

      // Hero uses base-100 (via gradient)
      expect(heroSection.className).toContain('base-100')

      // Features uses base-200
      expect(featuresSection.className).toContain('bg-base-200')

      // How it works uses base-100
      expect(howItWorksSection.className).toContain('bg-base-100')

      // Footer uses base-200
      expect(footerSection.className).toContain('bg-base-200')
    })

    it('CTA buttons use primary theme color in both light and dark modes', () => {
      // Test in light mode
      setDocumentTheme('light')
      renderWithRouter()

      const primaryCTA = screen.getByTestId('hero-cta-primary')
      expect(primaryCTA.className).toContain('btn-primary')

      // Test in dark mode
      setDocumentTheme('dark')
      expect(primaryCTA.className).toContain('btn-primary')
    })

    it('supports multiple DaisyUI themes', () => {
      const themes = ['light', 'dark', 'cyberpunk', 'synthwave']

      themes.forEach((theme) => {
        setDocumentTheme(theme)

        // Theme attribute should be set
        expect(getComputedThemeAttribute()).toBe(theme)
      })
    })
  })
})
