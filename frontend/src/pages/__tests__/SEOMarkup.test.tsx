import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import { ThemeProvider } from '../../contexts/ThemeContext'
import Home from '../Home'

/**
 * SEO Markup Tests for Homepage
 * Validates NFR-4: SEO-friendly markup with appropriate meta tags
 * Scenario: SEO Markup
 *
 * Note: Test Cases 1 and 2 (title and meta description) are tested via E2E tests
 * since they are defined in index.html and not accessible in jsdom unit tests.
 * See e2e/seo-markup.spec.ts for those tests.
 */

// Helper to render Home with required providers
function renderHome() {
  return render(
    <ThemeProvider>
      <BrowserRouter>
        <Home />
      </BrowserRouter>
    </ThemeProvider>
  )
}

describe('SEO Markup - NFR-4 Compliance', () => {
  beforeEach(() => {
    localStorage.clear()
  })

  afterEach(() => {
    localStorage.clear()
  })

  /**
   * Test Case 1: Query for page title meta tag
   * Note: Title tag is set in index.html - validated in E2E tests
   * This unit test validates the h1 contains SEO-relevant content
   */
  describe('Test Case 1: Page Title (SEO Content Validation)', () => {
    it('should have primary heading content that matches SEO title intent', () => {
      renderHome()

      // The h1 should communicate the product value - matching title intent
      const h1 = screen.getByRole('heading', { level: 1 })
      expect(h1).toBeInTheDocument()
      expect(h1.textContent).toBeTruthy()
    })

    it('should have h1 content relevant to URL shortening service', () => {
      renderHome()

      const h1 = screen.getByRole('heading', { level: 1 })
      const text = h1.textContent || ''

      // H1 should contain keywords related to the service
      expect(text.toLowerCase()).toMatch(/shorten|track|share|url|link/i)
    })
  })

  /**
   * Test Case 2: Query for meta description tag
   * Note: Meta description is set in index.html - validated in E2E tests
   * This unit test validates the page has descriptive content for SEO
   */
  describe('Test Case 2: Meta Description (Content Validation)', () => {
    it('should have descriptive subheading content that supports SEO', () => {
      renderHome()

      // The hero subheading should contain relevant descriptive content
      const subheading = screen.getByText(/Transform long URLs/i)
      expect(subheading).toBeInTheDocument()
      expect(subheading.textContent).toBeTruthy()
    })

    it('should have page content mentioning URL shortening, analytics, and link management', () => {
      renderHome()

      // Page should have content relevant to meta description keywords
      expect(screen.getByText(/Transform long URLs/i)).toBeInTheDocument()
      // Multiple elements mention "Track clicks" - verify at least one exists
      const trackClicksElements = screen.getAllByText(/Track clicks/i)
      expect(trackClicksElements.length).toBeGreaterThan(0)
    })
  })

  /**
   * Test Case 3: Query for h1 heading on page
   * Input: Query for h1 heading on page
   * Expected: Page has exactly one h1 heading with relevant content
   */
  describe('Test Case 3: H1 Heading', () => {
    it('should have exactly one h1 heading on the page', () => {
      renderHome()

      const h1Elements = screen.getAllByRole('heading', { level: 1 })
      expect(h1Elements).toHaveLength(1)
    })

    it('should have an h1 heading with relevant content', () => {
      renderHome()

      const h1 = screen.getByRole('heading', { level: 1 })

      expect(h1).toBeInTheDocument()
      expect(h1.textContent).toBeTruthy()
      expect(h1.textContent!.length).toBeGreaterThan(0)
    })

    it('should have an h1 heading that communicates the product value', () => {
      renderHome()

      const h1 = screen.getByRole('heading', { level: 1 })
      const text = h1.textContent || ''

      // H1 should relate to the URL shortening service's value proposition
      // Current h1: "Shorten. Track. Share."
      expect(text.toLowerCase()).toMatch(/shorten|track|share|url|link/i)
    })
  })

  /**
   * Test Case 4: Check heading hierarchy
   * Input: Check heading hierarchy
   * Expected: Headings follow proper hierarchy (h1 > h2 > h3) without skipping levels
   */
  describe('Test Case 4: Heading Hierarchy', () => {
    it('should have headings in proper hierarchical order', () => {
      renderHome()

      // Get all headings
      const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6')
      const headingLevels: number[] = []

      allHeadings.forEach((heading) => {
        const level = parseInt(heading.tagName.charAt(1))
        headingLevels.push(level)
      })

      // Verify there are headings
      expect(headingLevels.length).toBeGreaterThan(0)

      // First heading should be h1
      expect(headingLevels[0]).toBe(1)
    })

    it('should not skip heading levels (h1 should be followed by h2, not h3)', () => {
      renderHome()

      const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6')
      const headingLevels: number[] = []

      allHeadings.forEach((heading) => {
        const level = parseInt(heading.tagName.charAt(1))
        headingLevels.push(level)
      })

      // Check that heading levels don't skip
      // Each heading level should not jump more than 1 level from the previous minimum
      let minLevelSeen = 1 // Start with h1

      for (let i = 1; i < headingLevels.length; i++) {
        const currentLevel = headingLevels[i]

        // Heading can go down (e.g., h1 -> h2) or stay same or go up
        // But going deeper should not skip levels
        // If current level is deeper than minLevelSeen + 1, it's a skip
        if (currentLevel > minLevelSeen + 1) {
          // This would be a skip (e.g., h1 -> h3)
          expect(currentLevel).toBeLessThanOrEqual(minLevelSeen + 1)
        }

        // Update minimum level seen if we go deeper
        if (currentLevel <= minLevelSeen + 1 && currentLevel > minLevelSeen) {
          minLevelSeen = currentLevel
        }
      }
    })

    it('should have proper section structure with h2 headings', () => {
      renderHome()

      const h2Elements = screen.getAllByRole('heading', { level: 2 })

      // Homepage should have multiple sections with h2 headings
      expect(h2Elements.length).toBeGreaterThanOrEqual(3)

      // Verify h2 headings have meaningful content
      h2Elements.forEach((h2) => {
        expect(h2.textContent).toBeTruthy()
        expect(h2.textContent!.length).toBeGreaterThan(0)
      })
    })

    it('should have h3 headings only after h2 headings have been established', () => {
      renderHome()

      const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6')
      let h2Found = false
      let h3BeforeH2 = false

      allHeadings.forEach((heading) => {
        const tagName = heading.tagName
        if (tagName === 'H2') {
          h2Found = true
        }
        if (tagName === 'H3' && !h2Found) {
          h3BeforeH2 = true
        }
      })

      // H3 should not appear before any H2
      expect(h3BeforeH2).toBe(false)
    })

    it('should have feature cards using h3 headings under the features h2 section', () => {
      renderHome()

      const h3Elements = screen.getAllByRole('heading', { level: 3 })

      // Features section should have h3 headings for individual features
      expect(h3Elements.length).toBeGreaterThanOrEqual(4)

      // Feature headings should have descriptive names
      const featureKeywords = ['shortening', 'analytics', 'management', 'theme']
      const h3Texts = h3Elements.map((h3) => h3.textContent?.toLowerCase() || '')

      // At least some feature-related h3s should exist
      const hasFeatureHeadings = featureKeywords.some((keyword) =>
        h3Texts.some((text) => text.includes(keyword))
      )
      expect(hasFeatureHeadings).toBe(true)
    })
  })
})

describe('Additional SEO Best Practices', () => {
  /**
   * Note: viewport, charset, and lang attributes are in index.html
   * and tested via E2E tests. Here we test the rendered content structure.
   */

  it('should have semantic HTML structure with main landmarks', () => {
    renderHome()

    // Navigation landmark
    const nav = document.querySelector('nav')
    expect(nav).toBeInTheDocument()

    // Footer landmark
    const footer = document.querySelector('footer')
    expect(footer).toBeInTheDocument()

    // Section elements
    const sections = document.querySelectorAll('section')
    expect(sections.length).toBeGreaterThanOrEqual(3)
  })

  it('should have accessible section structure with ids for navigation', () => {
    renderHome()

    // Features section should have an id for anchor linking
    const featuresSection = document.querySelector('#features')
    expect(featuresSection).toBeInTheDocument()

    // Social proof section should have an id
    const socialProofSection = document.querySelector('#social-proof')
    expect(socialProofSection).toBeInTheDocument()
  })

  it('should use descriptive text content for SEO crawlers', () => {
    renderHome()

    // Main CTA buttons should have clear, descriptive text
    const getStartedBtn = screen.getByTestId('get-started-btn')
    expect(getStartedBtn).toHaveTextContent(/get started/i)

    const loginBtn = screen.getByTestId('login-btn')
    expect(loginBtn).toHaveTextContent(/login/i)
  })
})
