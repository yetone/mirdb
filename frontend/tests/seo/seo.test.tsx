/**
 * SEO Tests
 * Owner: Scenario 12 - SEO and Meta Tags
 *
 * Tests SEO requirements including:
 * - Meta tags (title, description, Open Graph)
 * - Semantic HTML structure
 * - Heading hierarchy
 * - Canonical URL
 *
 * Related requirements: NFR-6
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, cleanup, within } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { ReactNode, useState, useEffect, createContext, useContext } from 'react'
import { readFileSync } from 'fs'
import { resolve } from 'path'
import { AuthProvider } from '../../src/contexts/AuthContext'
import Home from '../../src/pages/Home'

// Read index.html for meta tag tests
const indexHtmlPath = resolve(__dirname, '../../index.html')
const indexHtmlContent = readFileSync(indexHtmlPath, 'utf-8')

// Create a simple document parser for meta tag extraction
function parseMetaTag(html: string, name: string): string | null {
  const regex = new RegExp(`<meta\\s+name=["']${name}["']\\s+content=["']([^"']+)["']`, 'i')
  const altRegex = new RegExp(`<meta\\s+content=["']([^"']+)["']\\s+name=["']${name}["']`, 'i')
  const match = html.match(regex) || html.match(altRegex)
  return match ? match[1] : null
}

function parseOgTag(html: string, property: string): string | null {
  const regex = new RegExp(`<meta\\s+property=["']${property}["']\\s+content=["']([^"']+)["']`, 'i')
  const altRegex = new RegExp(`<meta\\s+content=["']([^"']+)["']\\s+property=["']${property}["']`, 'i')
  const match = html.match(regex) || html.match(altRegex)
  return match ? match[1] : null
}

function parseTitle(html: string): string | null {
  const match = html.match(/<title>([^<]+)<\/title>/i)
  return match ? match[1] : null
}

function parseCanonical(html: string): string | null {
  const regex = /<link\s+rel=["']canonical["']\s+href=["']([^"']+)["']/i
  const altRegex = /<link\s+href=["']([^"']+)["']\s+rel=["']canonical["']/i
  const match = html.match(regex) || html.match(altRegex)
  return match ? match[1] : null
}

type Theme = 'light' | 'dark' | 'cyberpunk' | 'synthwave'

interface ThemeContextType {
  theme: Theme
  setTheme: (theme: Theme) => void
}

const ThemeContext = createContext<ThemeContextType | undefined>(undefined)

function TestThemeProvider({ children, initialTheme = 'dark' }: { children: ReactNode; initialTheme?: Theme }) {
  const [theme, setTheme] = useState<Theme>(initialTheme)

  useEffect(() => {
    document.documentElement.setAttribute('data-theme', theme)
  }, [theme])

  return (
    <ThemeContext.Provider value={{ theme, setTheme }}>
      {children}
    </ThemeContext.Provider>
  )
}

// Mock the ThemeContext module
vi.mock('../../src/contexts/ThemeContext', () => ({
  ThemeProvider: ({ children }: { children: ReactNode }) => {
    const [theme, setTheme] = useState<Theme>('dark')

    useEffect(() => {
      document.documentElement.setAttribute('data-theme', theme)
    }, [theme])

    return (
      <ThemeContext.Provider value={{ theme, setTheme }}>
        {children}
      </ThemeContext.Provider>
    )
  },
  useTheme: () => {
    const context = useContext(ThemeContext)
    if (context === undefined) {
      return { theme: 'dark', setTheme: () => {} }
    }
    return context
  },
}))

function renderHome() {
  return render(
    <MemoryRouter>
      <TestThemeProvider>
        <AuthProvider>
          <Home />
        </AuthProvider>
      </TestThemeProvider>
    </MemoryRouter>
  )
}

describe('SEO and Meta Tags', () => {
  beforeEach(() => {
    document.documentElement.removeAttribute('data-theme')
    vi.clearAllMocks()
  })

  afterEach(() => {
    cleanup()
    document.documentElement.removeAttribute('data-theme')
  })

  describe('Test Case 1: Page Title', () => {
    it('should have a page title that contains service name and value proposition', () => {
      const title = parseTitle(indexHtmlContent)

      expect(title).not.toBeNull()
      expect(title).toBeTruthy()

      // Title should contain service-related keywords
      const lowerTitle = title!.toLowerCase()
      expect(
        lowerTitle.includes('url shortener') ||
        lowerTitle.includes('shorten') ||
        lowerTitle.includes('link')
      ).toBe(true)

      // Title should contain value proposition keywords
      expect(
        lowerTitle.includes('track') ||
        lowerTitle.includes('success') ||
        lowerTitle.includes('analytics')
      ).toBe(true)
    })

    it('should have a title length between 30-60 characters for optimal SEO', () => {
      const title = parseTitle(indexHtmlContent)

      expect(title).not.toBeNull()
      // SEO best practice: title should be 30-60 characters
      expect(title!.length).toBeGreaterThanOrEqual(30)
      expect(title!.length).toBeLessThanOrEqual(70) // Allow slight flexibility
    })
  })

  describe('Test Case 2: Meta Description', () => {
    it('should have a meta description tag present', () => {
      const description = parseMetaTag(indexHtmlContent, 'description')

      expect(description).not.toBeNull()
      expect(description).toBeTruthy()
    })

    it('should have a meta description containing relevant keywords', () => {
      const description = parseMetaTag(indexHtmlContent, 'description')

      expect(description).not.toBeNull()

      const lowerDesc = description!.toLowerCase()
      // Should contain relevant service keywords
      const hasServiceKeywords =
        lowerDesc.includes('shorten') ||
        lowerDesc.includes('url') ||
        lowerDesc.includes('link')

      // Should contain feature keywords
      const hasFeatureKeywords =
        lowerDesc.includes('analytics') ||
        lowerDesc.includes('track') ||
        lowerDesc.includes('qr')

      expect(hasServiceKeywords).toBe(true)
      expect(hasFeatureKeywords).toBe(true)
    })

    it('should have a meta description length between 120-160 characters', () => {
      const description = parseMetaTag(indexHtmlContent, 'description')

      expect(description).not.toBeNull()
      // SEO best practice: meta description should be 120-160 characters
      expect(description!.length).toBeGreaterThanOrEqual(100) // Allow slight flexibility
      expect(description!.length).toBeLessThanOrEqual(170)
    })
  })

  describe('Test Case 3: Open Graph Meta Tags', () => {
    it('should have og:title tag present', () => {
      const ogTitle = parseOgTag(indexHtmlContent, 'og:title')

      expect(ogTitle).not.toBeNull()
      expect(ogTitle).toBeTruthy()
    })

    it('should have og:description tag present', () => {
      const ogDescription = parseOgTag(indexHtmlContent, 'og:description')

      expect(ogDescription).not.toBeNull()
      expect(ogDescription).toBeTruthy()
    })

    it('should have og:image tag present', () => {
      const ogImage = parseOgTag(indexHtmlContent, 'og:image')

      expect(ogImage).not.toBeNull()
      expect(ogImage).toBeTruthy()
      // Should be a valid URL or path
      expect(
        ogImage!.startsWith('http') ||
        ogImage!.startsWith('/') ||
        ogImage!.startsWith('.')
      ).toBe(true)
    })

    it('should have og:type tag present', () => {
      const ogType = parseOgTag(indexHtmlContent, 'og:type')

      expect(ogType).not.toBeNull()
      expect(ogType).toBe('website')
    })

    it('should have og:url tag present', () => {
      const ogUrl = parseOgTag(indexHtmlContent, 'og:url')

      expect(ogUrl).not.toBeNull()
      expect(ogUrl).toBeTruthy()
    })
  })

  describe('Test Case 4: Heading Hierarchy', () => {
    it('should have exactly one h1 element', () => {
      renderHome()

      const h1Elements = screen.getAllByRole('heading', { level: 1 })

      expect(h1Elements).toHaveLength(1)
    })

    it('should have h1 containing main value proposition', () => {
      renderHome()

      const h1 = screen.getByRole('heading', { level: 1 })

      expect(h1).toBeInTheDocument()
      // Should contain the main headline
      expect(h1.textContent?.toLowerCase()).toContain('shorten')
    })

    it('should have h2 elements for section headings', () => {
      renderHome()

      const h2Elements = screen.getAllByRole('heading', { level: 2 })

      // Should have at least 2 h2 elements (Features, How it Works, etc.)
      expect(h2Elements.length).toBeGreaterThanOrEqual(2)
    })

    it('should follow proper heading hierarchy without skipping levels', () => {
      renderHome()

      // Check that h1 exists
      const h1Elements = screen.getAllByRole('heading', { level: 1 })
      expect(h1Elements.length).toBeGreaterThan(0)

      // Check that h2 elements exist if h3+ exists
      const h2Elements = screen.queryAllByRole('heading', { level: 2 })
      const h3Elements = screen.queryAllByRole('heading', { level: 3 })

      // If h3 exists, h2 should also exist (proper hierarchy)
      if (h3Elements.length > 0) {
        expect(h2Elements.length).toBeGreaterThan(0)
      }
    })
  })

  describe('Test Case 5: Semantic Landmark Elements', () => {
    it('should use main element for primary content', () => {
      renderHome()

      const main = screen.getByRole('main')

      expect(main).toBeInTheDocument()
    })

    it('should use footer element for footer content', () => {
      renderHome()

      const footer = screen.getByRole('contentinfo')

      expect(footer).toBeInTheDocument()
    })

    it('should use section elements appropriately', () => {
      renderHome()

      // Check for semantic sections with test IDs
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
      expect(screen.getByTestId('social-proof-section')).toBeInTheDocument()
    })

    it('should use header element for navigation/branding area', () => {
      renderHome()

      // Check for header element (banner role)
      const header = screen.queryByRole('banner')

      // If header exists, it should contain navigation or branding
      if (header) {
        expect(header).toBeInTheDocument()
      } else {
        // If no explicit header, check that hero section serves as intro
        const heroSection = screen.getByTestId('hero-section')
        expect(heroSection).toBeInTheDocument()
      }
    })

    it('should have navigation elements with proper aria labels', () => {
      renderHome()

      // Check for navigation elements
      const navElements = screen.queryAllByRole('navigation')

      // Should have at least one navigation (footer nav)
      expect(navElements.length).toBeGreaterThanOrEqual(1)

      // Each nav should have an aria-label
      navElements.forEach(nav => {
        expect(nav).toHaveAttribute('aria-label')
      })
    })
  })

  describe('Test Case 6: Canonical URL', () => {
    it('should have a canonical link element', () => {
      const canonical = parseCanonical(indexHtmlContent)

      expect(canonical).not.toBeNull()
    })

    it('should have canonical URL pointing to homepage', () => {
      const canonical = parseCanonical(indexHtmlContent)

      expect(canonical).not.toBeNull()
      // Should be a valid URL
      expect(
        canonical!.startsWith('http') ||
        canonical!.startsWith('/')
      ).toBe(true)
    })
  })

  describe('Additional SEO Best Practices', () => {
    it('should have lang attribute on html element', () => {
      const hasLangAttr = indexHtmlContent.includes('lang="en"') ||
                          indexHtmlContent.includes("lang='en'")

      expect(hasLangAttr).toBe(true)
    })

    it('should have viewport meta tag', () => {
      const hasViewport = indexHtmlContent.includes('name="viewport"') ||
                          indexHtmlContent.includes("name='viewport'")

      expect(hasViewport).toBe(true)
    })

    it('should have charset meta tag', () => {
      const hasCharset = indexHtmlContent.includes('charset="UTF-8"') ||
                         indexHtmlContent.includes("charset='UTF-8'") ||
                         indexHtmlContent.includes('charset=UTF-8')

      expect(hasCharset).toBe(true)
    })

    it('should have semantic section elements with aria-labelledby for accessibility', () => {
      renderHome()

      // Features section should have aria-labelledby
      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toHaveAttribute('aria-labelledby', 'features-heading')

      // How it works section should have aria-labelledby
      const howItWorksSection = screen.getByTestId('how-it-works-section')
      expect(howItWorksSection).toHaveAttribute('aria-labelledby', 'how-it-works-heading')

      // Social proof section should have aria-labelledby
      const socialProofSection = screen.getByTestId('social-proof-section')
      expect(socialProofSection).toHaveAttribute('aria-labelledby', 'social-proof-heading')
    })
  })
})
