import { describe, it, expect, beforeEach, vi } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import Home from '../pages/Home'
import { ThemeProvider } from '../contexts/ThemeContext'

/**
 * Integration tests for Home Page Assembly
 * Scenario: Component Integration - Home Page Assembly
 *
 * These tests verify that all section components (HeroSection, FeaturesSection,
 * DemoSection, FooterSection) integrate correctly into the Home page.
 */

// Helper function to render Home with required providers
const renderHome = (theme?: 'light' | 'dark' | 'cyberpunk' | 'synthwave') => {
  return render(
    <MemoryRouter>
      <ThemeProvider defaultTheme={theme || 'light'}>
        <Home />
      </ThemeProvider>
    </MemoryRouter>
  )
}

// Helper function to render Home without ThemeProvider (for basic tests)
const renderHomeWithRouter = () => {
  return render(
    <MemoryRouter>
      <Home />
    </MemoryRouter>
  )
}

describe('Home Page Assembly - Component Integration', () => {
  beforeEach(() => {
    // Mock localStorage for ThemeProvider
    const localStorageMock = {
      getItem: vi.fn(() => null),
      setItem: vi.fn(),
      removeItem: vi.fn(),
      clear: vi.fn(),
    }
    Object.defineProperty(window, 'localStorage', { value: localStorageMock })

    // Mock matchMedia for system theme detection
    Object.defineProperty(window, 'matchMedia', {
      writable: true,
      value: vi.fn().mockImplementation((query: string) => ({
        matches: query === '(prefers-color-scheme: dark)' ? false : false,
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      })),
    })
  })

  describe('Test Case 1: HeroSection is rendered as first section', () => {
    it('renders HeroSection component when Home page is mounted', () => {
      renderHomeWithRouter()

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()
    })

    it('HeroSection appears before other sections in the DOM', () => {
      renderHomeWithRouter()

      const heroSection = screen.getByTestId('hero-section')
      const featuresSection = screen.getByTestId('features-section')
      const demoSection = screen.getByTestId('demo-section')
      const footerSection = screen.getByTestId('footer-section')

      // Get the document position of each section
      // Using compareDocumentPosition to check order
      const heroBeforeFeatures = heroSection.compareDocumentPosition(featuresSection) & Node.DOCUMENT_POSITION_FOLLOWING
      const heroBeforeDemo = heroSection.compareDocumentPosition(demoSection) & Node.DOCUMENT_POSITION_FOLLOWING
      const heroBeforeFooter = heroSection.compareDocumentPosition(footerSection) & Node.DOCUMENT_POSITION_FOLLOWING

      expect(heroBeforeFeatures).toBeTruthy()
      expect(heroBeforeDemo).toBeTruthy()
      expect(heroBeforeFooter).toBeTruthy()
    })

    it('HeroSection is the first section element inside main', () => {
      renderHomeWithRouter()

      const mainElement = screen.getByRole('main')
      const heroSection = screen.getByTestId('hero-section')

      // Get the first section element within main
      const firstSection = mainElement.querySelector('section')
      expect(firstSection).toBe(heroSection)
    })

    it('HeroSection contains the primary h1 heading', () => {
      renderHomeWithRouter()

      const heroSection = screen.getByTestId('hero-section')
      const h1 = within(heroSection).getByRole('heading', { level: 1 })

      expect(h1).toBeInTheDocument()
      expect(h1.textContent).toMatch(/shorten|url|link/i)
    })
  })

  describe('Test Case 2: FeaturesSection is present after hero', () => {
    it('renders FeaturesSection component when Home page is mounted', () => {
      renderHomeWithRouter()

      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()
    })

    it('FeaturesSection appears after HeroSection in the DOM', () => {
      renderHomeWithRouter()

      const heroSection = screen.getByTestId('hero-section')
      const featuresSection = screen.getByTestId('features-section')

      // Check that features section follows hero section
      const heroBeforeFeatures = heroSection.compareDocumentPosition(featuresSection) & Node.DOCUMENT_POSITION_FOLLOWING
      expect(heroBeforeFeatures).toBeTruthy()
    })

    it('FeaturesSection is the second section element inside main', () => {
      renderHomeWithRouter()

      const mainElement = screen.getByRole('main')
      const sectionsInMain = mainElement.querySelectorAll('section')
      const featuresSection = screen.getByTestId('features-section')

      // Features should be the second section (index 1)
      expect(sectionsInMain[1]).toBe(featuresSection)
    })

    it('FeaturesSection contains feature cards', () => {
      renderHomeWithRouter()

      const featuresSection = screen.getByTestId('features-section')
      const featureCards = within(featuresSection).getAllByTestId('feature-card')

      expect(featureCards.length).toBeGreaterThanOrEqual(3)
    })

    it('FeaturesSection appears before DemoSection', () => {
      renderHomeWithRouter()

      const featuresSection = screen.getByTestId('features-section')
      const demoSection = screen.getByTestId('demo-section')

      const featuresBeforeDemo = featuresSection.compareDocumentPosition(demoSection) & Node.DOCUMENT_POSITION_FOLLOWING
      expect(featuresBeforeDemo).toBeTruthy()
    })
  })

  describe('Test Case 3: FooterSection is rendered as last section', () => {
    it('renders FooterSection component when Home page is mounted', () => {
      renderHomeWithRouter()

      const footerSection = screen.getByTestId('footer-section')
      expect(footerSection).toBeInTheDocument()
    })

    it('FooterSection appears after all other sections in the DOM', () => {
      renderHomeWithRouter()

      const heroSection = screen.getByTestId('hero-section')
      const featuresSection = screen.getByTestId('features-section')
      const demoSection = screen.getByTestId('demo-section')
      const footerSection = screen.getByTestId('footer-section')

      // Footer should come after all other sections
      const heroBeforeFooter = heroSection.compareDocumentPosition(footerSection) & Node.DOCUMENT_POSITION_FOLLOWING
      const featuresBeforeFooter = featuresSection.compareDocumentPosition(footerSection) & Node.DOCUMENT_POSITION_FOLLOWING
      const demoBeforeFooter = demoSection.compareDocumentPosition(footerSection) & Node.DOCUMENT_POSITION_FOLLOWING

      expect(heroBeforeFooter).toBeTruthy()
      expect(featuresBeforeFooter).toBeTruthy()
      expect(demoBeforeFooter).toBeTruthy()
    })

    it('FooterSection is outside the main element (proper semantic structure)', () => {
      renderHomeWithRouter()

      const mainElement = screen.getByRole('main')
      const footerSection = screen.getByTestId('footer-section')

      // Footer should NOT be inside main element
      expect(mainElement).not.toContainElement(footerSection)
    })

    it('FooterSection has contentinfo role for accessibility', () => {
      renderHomeWithRouter()

      const footerSection = screen.getByTestId('footer-section')
      expect(footerSection).toHaveAttribute('role', 'contentinfo')
    })

    it('FooterSection contains copyright and navigation links', () => {
      renderHomeWithRouter()

      const footerSection = screen.getByTestId('footer-section')

      // Check for copyright notice
      const copyright = within(footerSection).getByTestId('footer-copyright')
      expect(copyright).toBeInTheDocument()
      expect(copyright.textContent).toMatch(/©.*\d{4}/)

      // Check for navigation links
      const homeLink = within(footerSection).getByRole('link', { name: /home/i })
      const loginLink = within(footerSection).getByRole('link', { name: /login/i })
      const registerLink = within(footerSection).getByRole('link', { name: /register/i })

      expect(homeLink).toBeInTheDocument()
      expect(loginLink).toBeInTheDocument()
      expect(registerLink).toBeInTheDocument()
    })
  })

  describe('Test Case 4: All sections receive theme context and render accordingly', () => {
    it('renders all sections with ThemeProvider wrapping the Home page', () => {
      renderHome('light')

      // All sections should render when wrapped with ThemeProvider
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('demo-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer-section')).toBeInTheDocument()
    })

    it('all sections render correctly with light theme', () => {
      renderHome('light')

      // Verify all sections are present and contain expected elements
      const heroSection = screen.getByTestId('hero-section')
      const featuresSection = screen.getByTestId('features-section')
      const demoSection = screen.getByTestId('demo-section')
      const footerSection = screen.getByTestId('footer-section')

      // Hero should have its heading
      expect(within(heroSection).getByRole('heading', { level: 1 })).toBeInTheDocument()

      // Features should have its heading
      expect(within(featuresSection).getByRole('heading', { level: 2 })).toBeInTheDocument()

      // Demo should have its heading
      expect(within(demoSection).getByRole('heading', { level: 2 })).toBeInTheDocument()

      // Footer should have copyright
      expect(within(footerSection).getByTestId('footer-copyright')).toBeInTheDocument()
    })

    it('all sections render correctly with dark theme', () => {
      renderHome('dark')

      // Verify all sections are present
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('demo-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer-section')).toBeInTheDocument()

      // Verify sections contain their key elements
      const heroH1 = screen.getByRole('heading', { level: 1 })
      expect(heroH1).toBeInTheDocument()

      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards.length).toBeGreaterThanOrEqual(3)
    })

    it('all sections render correctly with cyberpunk theme', () => {
      renderHome('cyberpunk')

      // Verify all sections are present
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('demo-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer-section')).toBeInTheDocument()
    })

    it('all sections render correctly with synthwave theme', () => {
      renderHome('synthwave')

      // Verify all sections are present
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('demo-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer-section')).toBeInTheDocument()
    })

    it('sections use theme-aware CSS classes (base-content, base-200, etc.)', () => {
      renderHome('light')

      const heroSection = screen.getByTestId('hero-section')
      const featuresSection = screen.getByTestId('features-section')
      const footerSection = screen.getByTestId('footer-section')

      // Verify theme-aware class usage in sections
      expect(heroSection.className).toMatch(/bg-gradient-to-br|from-base-200|to-base-300/)
      expect(featuresSection.className).toMatch(/bg-base-200/)
      expect(footerSection.className).toMatch(/bg-base-300/)
    })
  })

  describe('Section Order Verification', () => {
    it('verifies correct section order: Hero -> Features -> Demo -> Footer', () => {
      renderHomeWithRouter()

      const heroSection = screen.getByTestId('hero-section')
      const featuresSection = screen.getByTestId('features-section')
      const demoSection = screen.getByTestId('demo-section')
      const footerSection = screen.getByTestId('footer-section')

      // Get all sections in document order
      const allSections = document.querySelectorAll('section, footer')
      const sectionsArray = Array.from(allSections)

      // Find indices of each section
      const heroIndex = sectionsArray.findIndex(el => el === heroSection || el.contains(heroSection))
      const featuresIndex = sectionsArray.findIndex(el => el === featuresSection || el.contains(featuresSection))
      const demoIndex = sectionsArray.findIndex(el => el === demoSection || el.contains(demoSection))
      const footerIndex = sectionsArray.findIndex(el => el === footerSection || el.contains(footerSection))

      // Verify order
      expect(heroIndex).toBeLessThan(featuresIndex)
      expect(featuresIndex).toBeLessThan(demoIndex)
      expect(demoIndex).toBeLessThan(footerIndex)
    })

    it('all four sections are rendered', () => {
      renderHomeWithRouter()

      // All required sections must be present
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('demo-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer-section')).toBeInTheDocument()
    })

    it('DemoSection is present between Features and Footer', () => {
      renderHomeWithRouter()

      const demoSection = screen.getByTestId('demo-section')
      expect(demoSection).toBeInTheDocument()

      // Demo should be inside main element
      const mainElement = screen.getByRole('main')
      expect(mainElement).toContainElement(demoSection)

      // Demo should be the third section inside main
      const sectionsInMain = mainElement.querySelectorAll('section')
      expect(sectionsInMain[2]).toBe(demoSection)
    })
  })
})
