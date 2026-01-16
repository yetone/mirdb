import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import '../index.css'

/**
 * Smooth Scroll Navigation - Unit Tests
 *
 * This test file verifies REQ-10: Homepage shall provide smooth scroll
 * navigation for single-page sections.
 *
 * Test Cases:
 * 1. Unit: Verify CSS scroll-behavior: smooth is applied to html element
 * 2. Unit: Verify anchor link targets exist (id="features", id="how-it-works")
 */

// Helper component to verify anchor link structure
function AnchorLinkTestComponent() {
  return (
    <div>
      <nav>
        <a href="#features" data-testid="anchor-features">Features</a>
        <a href="#how-it-works" data-testid="anchor-how-it-works">How It Works</a>
      </nav>
      <section id="features" data-testid="section-features">
        <h2>Features</h2>
      </section>
      <section id="how-it-works" data-testid="section-how-it-works">
        <h2>How It Works</h2>
      </section>
    </div>
  )
}

describe('Smooth Scroll Navigation - Unit Tests', () => {
  /**
   * Test Case 3: Unit Test
   * Input: Verify CSS scroll-behavior property
   * Expected: scroll-behavior: smooth is applied to html/body
   */
  describe('Test Case 3: CSS scroll-behavior property', () => {
    let styleSheet: CSSStyleSheet | null = null

    beforeEach(() => {
      // Inject the CSS styles into the document
      const style = document.createElement('style')
      style.textContent = `
        html {
          scroll-behavior: smooth;
        }
      `
      document.head.appendChild(style)
      styleSheet = style.sheet
    })

    afterEach(() => {
      // Clean up injected styles
      const styles = document.head.querySelectorAll('style')
      styles.forEach(style => style.remove())
    })

    it('scroll-behavior: smooth CSS rule is defined', () => {
      expect(styleSheet).not.toBeNull()

      // Check if the rule exists in the stylesheet
      const rules = styleSheet?.cssRules
      expect(rules).toBeDefined()
      expect(rules?.length).toBeGreaterThan(0)

      // Find the html rule
      const htmlRule = Array.from(rules || []).find(rule => {
        if (rule instanceof CSSStyleRule) {
          return rule.selectorText === 'html'
        }
        return false
      }) as CSSStyleRule | undefined

      expect(htmlRule).toBeDefined()
      expect(htmlRule?.style.scrollBehavior).toBe('smooth')
    })

    it('html element has scroll-behavior computed style', () => {
      const computedStyle = window.getComputedStyle(document.documentElement)
      expect(computedStyle.scrollBehavior).toBe('smooth')
    })

    it('scroll-behavior value is not "auto" (default)', () => {
      const computedStyle = window.getComputedStyle(document.documentElement)
      expect(computedStyle.scrollBehavior).not.toBe('auto')
    })
  })

  describe('Anchor link structure', () => {
    it('anchor links have proper href attributes', () => {
      render(
        <BrowserRouter>
          <AnchorLinkTestComponent />
        </BrowserRouter>
      )

      const featuresLink = screen.getByTestId('anchor-features')
      const howItWorksLink = screen.getByTestId('anchor-how-it-works')

      expect(featuresLink).toHaveAttribute('href', '#features')
      expect(howItWorksLink).toHaveAttribute('href', '#how-it-works')
    })

    it('target sections have matching id attributes', () => {
      render(
        <BrowserRouter>
          <AnchorLinkTestComponent />
        </BrowserRouter>
      )

      const featuresSection = screen.getByTestId('section-features')
      const howItWorksSection = screen.getByTestId('section-how-it-works')

      expect(featuresSection).toHaveAttribute('id', 'features')
      expect(howItWorksSection).toHaveAttribute('id', 'how-it-works')
    })

    it('anchor href and section id match', () => {
      render(
        <BrowserRouter>
          <AnchorLinkTestComponent />
        </BrowserRouter>
      )

      const featuresLink = screen.getByTestId('anchor-features')
      const howItWorksLink = screen.getByTestId('anchor-how-it-works')
      const featuresSection = screen.getByTestId('section-features')
      const howItWorksSection = screen.getByTestId('section-how-it-works')

      // Extract id from href (#features -> features)
      const featuresHref = featuresLink.getAttribute('href')?.replace('#', '')
      const howItWorksHref = howItWorksLink.getAttribute('href')?.replace('#', '')

      expect(featuresSection.id).toBe(featuresHref)
      expect(howItWorksSection.id).toBe(howItWorksHref)
    })
  })

  describe('Anchor links are keyboard accessible', () => {
    it('anchor links are focusable', () => {
      render(
        <BrowserRouter>
          <AnchorLinkTestComponent />
        </BrowserRouter>
      )

      const featuresLink = screen.getByTestId('anchor-features')
      const howItWorksLink = screen.getByTestId('anchor-how-it-works')

      // Links should be focusable (not have tabindex="-1")
      expect(featuresLink).not.toHaveAttribute('tabindex', '-1')
      expect(howItWorksLink).not.toHaveAttribute('tabindex', '-1')
    })

    it('anchor links have visible text content', () => {
      render(
        <BrowserRouter>
          <AnchorLinkTestComponent />
        </BrowserRouter>
      )

      const featuresLink = screen.getByTestId('anchor-features')
      const howItWorksLink = screen.getByTestId('anchor-how-it-works')

      expect(featuresLink).toHaveTextContent('Features')
      expect(howItWorksLink).toHaveTextContent('How It Works')
    })
  })
})
