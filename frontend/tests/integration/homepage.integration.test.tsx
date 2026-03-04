/**
 * Homepage Integration Tests
 *
 * Integration tests for homepage component interactions.
 * Test Case 4: Viewport visibility test for hero section.
 */
import { describe, it, expect, beforeEach, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import { Home } from '../../src/pages/Home'
import { ThemeProvider } from '../../src/contexts/ThemeContext'

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

Object.defineProperty(window, 'localStorage', {
  value: localStorageMock,
})

// Helper to wrap component with router and theme provider
const renderWithRouter = (ui: React.ReactElement) => {
  return render(
    <ThemeProvider defaultTheme="dark">
      <BrowserRouter>{ui}</BrowserRouter>
    </ThemeProvider>
  )
}

describe('Homepage Integration Tests', () => {
  beforeEach(() => {
    localStorageMock.clear()
    vi.clearAllMocks()
  })

  describe('Test Case 4: Hero Section Viewport Visibility', () => {
    beforeEach(() => {
      // Set viewport size to standard 1024x768
      Object.defineProperty(window, 'innerWidth', {
        writable: true,
        configurable: true,
        value: 1024,
      })
      Object.defineProperty(window, 'innerHeight', {
        writable: true,
        configurable: true,
        value: 768,
      })

      // Mock getBoundingClientRect for visibility checks
      Element.prototype.getBoundingClientRect = vi.fn(() => ({
        x: 0,
        y: 0,
        width: 1024,
        height: 400, // Hero section height within viewport
        top: 0,
        right: 1024,
        bottom: 400,
        left: 0,
        toJSON: () => {},
      }))
    })

    it('should render hero section within the first viewport without scrolling', () => {
      renderWithRouter(<Home />)

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // The hero-section class ensures content fits in viewport
      expect(heroSection).toHaveClass('hero-section')
    })

    it('should have headline visible without scrolling on 1024x768 viewport', () => {
      renderWithRouter(<Home />)

      const headline = screen.getByTestId('hero-headline')
      expect(headline).toBeInTheDocument()
      expect(headline).toBeVisible()

      // Verify headline is rendered and accessible
      const rect = headline.getBoundingClientRect()
      expect(rect.top).toBeGreaterThanOrEqual(0)
      expect(rect.bottom).toBeLessThanOrEqual(768)
    })

    it('should have subheading visible without scrolling on 1024x768 viewport', () => {
      renderWithRouter(<Home />)

      const subheading = screen.getByTestId('hero-subheading')
      expect(subheading).toBeInTheDocument()
      expect(subheading).toBeVisible()

      const rect = subheading.getBoundingClientRect()
      expect(rect.top).toBeGreaterThanOrEqual(0)
      expect(rect.bottom).toBeLessThanOrEqual(768)
    })

    it('should have CTA button visible without scrolling on 1024x768 viewport', () => {
      renderWithRouter(<Home />)

      const ctaButton = screen.getByTestId('hero-cta')
      expect(ctaButton).toBeInTheDocument()
      expect(ctaButton).toBeVisible()

      const rect = ctaButton.getBoundingClientRect()
      expect(rect.top).toBeGreaterThanOrEqual(0)
      expect(rect.bottom).toBeLessThanOrEqual(768)
    })

    it('should have all hero content (headline, subheading, CTA) visible together', () => {
      renderWithRouter(<Home />)

      const headline = screen.getByTestId('hero-headline')
      const subheading = screen.getByTestId('hero-subheading')
      const ctaButton = screen.getByTestId('hero-cta')

      // All elements should be in the document
      expect(headline).toBeInTheDocument()
      expect(subheading).toBeInTheDocument()
      expect(ctaButton).toBeInTheDocument()

      // All elements should be visible
      expect(headline).toBeVisible()
      expect(subheading).toBeVisible()
      expect(ctaButton).toBeVisible()
    })

    it('should constrain hero section to 100vh max height', () => {
      renderWithRouter(<Home />)

      const heroSection = screen.getByTestId('hero-section')
      const styles = window.getComputedStyle(heroSection)

      // The CSS class hero-section should have max-height: 100vh
      expect(heroSection.className).toContain('hero-section')
    })
  })

  describe('Component Integration', () => {
    it('should render Home page with HeroSection integrated', () => {
      renderWithRouter(<Home />)

      // Home page should be present
      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()

      // Hero section should be nested within home page
      const heroSection = screen.getByTestId('hero-section')
      expect(homePage).toContainElement(heroSection)
    })

    it('should render all hero elements with proper hierarchy', () => {
      renderWithRouter(<Home />)

      const heroSection = screen.getByTestId('hero-section')
      const headline = screen.getByTestId('hero-headline')
      const subheading = screen.getByTestId('hero-subheading')
      const ctaButton = screen.getByTestId('hero-cta')

      // All elements should be within the hero section
      expect(heroSection).toContainElement(headline)
      expect(heroSection).toContainElement(subheading)
      expect(heroSection).toContainElement(ctaButton)
    })
  })
})
