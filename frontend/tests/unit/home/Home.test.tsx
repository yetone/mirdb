/**
 * Home Page Unit Tests - Accessibility Semantic HTML
 * Owner: Scenario 13 - Accessibility - Semantic HTML
 *
 * Tests for the main Home page component focusing on semantic HTML:
 * - Proper heading hierarchy (h1 > h2 > h3, no skipped levels)
 * - Main landmark element
 * - Footer landmark element
 * - Image alt attributes
 * - Axe accessibility audit
 *
 * Testing framework: Vitest + @testing-library/react + axe-core
 */
import { describe, it, expect } from 'vitest'
import { renderWithProviders, screen, within } from '../../utils/renderWithProviders'
import axe from 'axe-core'
import Home from '@/pages/Home'

describe('Home Page - Accessibility Semantic HTML', () => {
  describe('Test Case 1: Heading Hierarchy', () => {
    it('should have a single h1 for the main headline', () => {
      renderWithProviders(<Home />)

      const h1Elements = screen.getAllByRole('heading', { level: 1 })
      expect(h1Elements).toHaveLength(1)
      expect(h1Elements[0]).toHaveTextContent(/shorten urls/i)
    })

    it('should have h2 elements for section headings', () => {
      renderWithProviders(<Home />)

      const h2Elements = screen.getAllByRole('heading', { level: 2 })
      expect(h2Elements.length).toBeGreaterThanOrEqual(2)

      // Verify section headings exist
      expect(screen.getByRole('heading', { level: 2, name: /powerful features/i })).toBeInTheDocument()
      expect(screen.getByRole('heading', { level: 2, name: /how it works/i })).toBeInTheDocument()
    })

    it('should not skip heading levels', () => {
      renderWithProviders(<Home />)

      const allHeadings = screen.getAllByRole('heading')
      const headingLevels = allHeadings.map(h => {
        const tagName = h.tagName.toLowerCase()
        return parseInt(tagName.replace('h', ''), 10)
      })

      // Sort levels to check for gaps
      const sortedLevels = [...new Set(headingLevels)].sort((a, b) => a - b)

      // Check that there are no gaps (e.g., no h1 then h3 without h2)
      for (let i = 0; i < sortedLevels.length - 1; i++) {
        const currentLevel = sortedLevels[i]
        const nextLevel = sortedLevels[i + 1]
        expect(nextLevel - currentLevel).toBeLessThanOrEqual(1)
      }

      // Verify h1 is the first level
      expect(sortedLevels[0]).toBe(1)
    })
  })

  describe('Test Case 2: Main Landmark', () => {
    it('should have a main element as landmark for main content', () => {
      renderWithProviders(<Home />)

      const mainElement = screen.getByRole('main')
      expect(mainElement).toBeInTheDocument()
    })

    it('should contain main sections within the main landmark', () => {
      renderWithProviders(<Home />)

      const mainElement = screen.getByRole('main')

      // Hero section should be within main
      const heroSection = within(mainElement).getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Features section should be within main
      const featuresSection = within(mainElement).getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()

      // How it works section should be within main
      const howItWorksSection = within(mainElement).getByTestId('how-it-works-section')
      expect(howItWorksSection).toBeInTheDocument()
    })
  })

  describe('Test Case 3: Footer Landmark', () => {
    it('should have a footer element as landmark', () => {
      renderWithProviders(<Home />)

      const footerElement = screen.getByRole('contentinfo')
      expect(footerElement).toBeInTheDocument()
    })

    it('should use semantic footer element', () => {
      renderWithProviders(<Home />)

      const footer = screen.getByTestId('footer')
      expect(footer.tagName.toLowerCase()).toBe('footer')
    })

    it('should have footer navigation with proper aria-label', () => {
      renderWithProviders(<Home />)

      const footerNav = screen.getByRole('navigation', { name: /footer navigation/i })
      expect(footerNav).toBeInTheDocument()
    })
  })

  describe('Test Case 4: Image Alt Text', () => {
    it('should have alt text or aria-hidden on all images and SVG icons', () => {
      const { container } = renderWithProviders(<Home />)

      // Check all img elements
      const images = container.querySelectorAll('img')
      images.forEach(img => {
        const hasAlt = img.hasAttribute('alt')
        const isDecorativeAlt = img.getAttribute('alt') === ''
        const hasAriaHidden = img.getAttribute('aria-hidden') === 'true'
        const hasRole = img.getAttribute('role') === 'presentation' || img.getAttribute('role') === 'none'

        // Image should have either non-empty alt, empty alt (decorative), aria-hidden, or role="presentation"
        expect(hasAlt || hasAriaHidden || hasRole).toBe(true)
      })

      // Check all SVG elements for proper accessibility
      const svgs = container.querySelectorAll('svg')
      svgs.forEach(svg => {
        const hasAriaHidden = svg.getAttribute('aria-hidden') === 'true'
        const hasAriaLabel = svg.hasAttribute('aria-label')
        const hasTitle = svg.querySelector('title') !== null
        const hasRole = svg.getAttribute('role') === 'img' || svg.getAttribute('role') === 'presentation'

        // SVG should have either aria-hidden (decorative), aria-label, title, or role
        expect(hasAriaHidden || hasAriaLabel || hasTitle || hasRole).toBe(true)
      })
    })

    it('should mark decorative icons as aria-hidden', () => {
      const { container } = renderWithProviders(<Home />)

      // Feature and step icons should be decorative (have aria-hidden)
      const featureIcons = container.querySelectorAll('[data-testid^="feature-icon"] svg')
      featureIcons.forEach(icon => {
        expect(icon.getAttribute('aria-hidden')).toBe('true')
      })

      const stepIcons = container.querySelectorAll('[data-testid^="step-icon"] svg')
      stepIcons.forEach(icon => {
        expect(icon.getAttribute('aria-hidden')).toBe('true')
      })
    })
  })

  describe('Test Case 5: Axe Accessibility Audit', () => {
    it('should have no critical or serious accessibility violations', async () => {
      const { container } = renderWithProviders(<Home />)

      const results = await axe.run(container, {
        runOnly: {
          type: 'tag',
          values: ['wcag2a', 'wcag2aa', 'wcag21aa']
        }
      })

      // Filter for critical and serious violations only
      const criticalOrSerious = results.violations.filter(
        v => v.impact === 'critical' || v.impact === 'serious'
      )

      // Log violations for debugging if any
      if (criticalOrSerious.length > 0) {
        console.error('Accessibility violations:', JSON.stringify(criticalOrSerious, null, 2))
      }

      expect(criticalOrSerious).toHaveLength(0)
    })

    it('should have proper document structure', async () => {
      const { container } = renderWithProviders(<Home />)

      // Run specific rules related to document structure
      const results = await axe.run(container, {
        runOnly: {
          type: 'rule',
          values: ['landmark-one-main', 'region', 'heading-order']
        }
      })

      const criticalOrSerious = results.violations.filter(
        v => v.impact === 'critical' || v.impact === 'serious'
      )

      expect(criticalOrSerious).toHaveLength(0)
    })
  })
})
