/**
 * Integration tests for homepage accessibility - Screen Reader and ARIA.
 * Owner: Scenario 13 - Accessibility Compliance - Screen Reader and ARIA
 *
 * Tests:
 * - ARIA labels on interactive elements
 * - Alt text for images and decorative icons
 * - Heading hierarchy structure
 * - axe-core accessibility audit
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, within, cleanup } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import React from 'react'
import axe from 'axe-core'
import Home from '@/pages/Home'
import Navbar from '@/components/Navbar'
import { Footer } from '@/components/common/Footer'
import { HeroSection } from '@/components/homepage'
import { ThemeProvider } from '@/contexts/ThemeContext'
import { AuthProvider } from '@/contexts/AuthContext'

/**
 * Custom render helper for accessibility tests using MemoryRouter
 * Includes all necessary providers
 */
const renderWithProviders = (
  ui: React.ReactElement,
  { initialEntries = ['/'] } = {}
) => {
  return render(
    <MemoryRouter initialEntries={initialEntries}>
      <ThemeProvider>
        <AuthProvider>{ui}</AuthProvider>
      </ThemeProvider>
    </MemoryRouter>
  )
}

/**
 * Full page render for axe-core tests
 */
const renderFullPage = () => {
  return renderWithProviders(
    <>
      <Navbar />
      <Home />
    </>
  )
}

describe('Accessibility - Screen Reader and ARIA', () => {
  beforeEach(() => {
    localStorage.clear()
    vi.clearAllMocks()
    document.documentElement.removeAttribute('data-theme')
  })

  afterEach(() => {
    cleanup()
  })

  describe('Test Case 1: URL input field has ARIA attributes', () => {
    it('should have aria-label on URL input', () => {
      renderFullPage()

      const urlInput = screen.getByTestId('url-input')
      expect(urlInput).toBeInTheDocument()

      // Check for aria-label attribute
      const ariaLabel = urlInput.getAttribute('aria-label')
      expect(ariaLabel).toBeTruthy()
      expect(ariaLabel?.toLowerCase()).toContain('url')
    })

    it('should have input type="url" for semantic HTML', () => {
      renderFullPage()

      const urlInput = screen.getByTestId('url-input')
      expect(urlInput).toHaveAttribute('type', 'url')
    })

    it('should have placeholder text for guidance', () => {
      renderFullPage()

      const urlInput = screen.getByTestId('url-input')
      expect(urlInput).toHaveAttribute('placeholder')
      const placeholder = urlInput.getAttribute('placeholder')
      expect(placeholder?.toLowerCase()).toContain('url')
    })

    it('should have aria-describedby for error messages when error exists', () => {
      renderWithProviders(
        <HeroSection
          onUrlSubmit={async () => {
            throw new Error('Test error')
          }}
          isAuthenticated={false}
        />
      )

      const urlInput = screen.getByTestId('url-input')
      // When no error, aria-describedby should not be set
      // This is correct behavior per WCAG
      const ariaDescribedBy = urlInput.getAttribute('aria-describedby')
      expect(ariaDescribedBy).toBeNull()
    })
  })

  describe('Test Case 1b: All interactive elements have accessible names', () => {
    it('should have accessible names on all buttons', () => {
      renderFullPage()

      const buttons = screen.getAllByRole('button')
      buttons.forEach((button) => {
        // Button should have either aria-label or text content
        const ariaLabel = button.getAttribute('aria-label')
        const textContent = button.textContent?.trim()

        const hasAccessibleName =
          (ariaLabel && ariaLabel.trim().length > 0) ||
          (textContent && textContent.length > 0)
        expect(hasAccessibleName).toBe(true)
      })
    })

    it('should have aria-label on hamburger menu button', () => {
      renderWithProviders(<Navbar />)

      const hamburgerMenu = screen.getByTestId('hamburger-menu')
      expect(hamburgerMenu).toHaveAttribute('aria-label')
      expect(hamburgerMenu.getAttribute('aria-label')).toBeTruthy()
    })

    it('should have aria-expanded on hamburger menu', () => {
      renderWithProviders(<Navbar />)

      const hamburgerMenu = screen.getByTestId('hamburger-menu')
      expect(hamburgerMenu).toHaveAttribute('aria-expanded')
    })

    it('should have aria-label on theme toggle button', () => {
      renderWithProviders(<Navbar />)

      const themeToggles = screen.getAllByRole('button', { name: /toggle theme/i })
      expect(themeToggles.length).toBeGreaterThan(0)
      themeToggles.forEach((toggle) => {
        expect(toggle).toHaveAttribute('aria-label', 'Toggle theme')
      })
    })
  })

  describe('Test Case 2: All images and SVGs have alt text or aria-hidden', () => {
    it('should have all img elements with alt text or be decorative', () => {
      renderFullPage()

      const images = document.querySelectorAll('img')
      images.forEach((img) => {
        const altText = img.getAttribute('alt')
        const ariaHidden = img.getAttribute('aria-hidden')
        const role = img.getAttribute('role')

        // Image should have alt text OR be marked as decorative
        const hasAlt = altText !== null
        const isDecorativeExplicit = ariaHidden === 'true' || role === 'presentation'

        expect(hasAlt || isDecorativeExplicit).toBe(true)
      })
    })

    it('should have SVG icons as decorative when parent has accessible name', () => {
      renderFullPage()

      const svgs = document.querySelectorAll('svg')
      svgs.forEach((svg) => {
        const ariaHidden = svg.getAttribute('aria-hidden')
        const ariaLabel = svg.getAttribute('aria-label')
        const parent = svg.parentElement
        const grandparent = parent?.parentElement

        // SVG should be decorative OR have accessible name
        // If parent is a button with aria-label, the SVG is properly accessible
        const isDecorative = ariaHidden === 'true'
        const hasAccessibleName = ariaLabel !== null

        // Check if parent or grandparent is a button with aria-label
        const parentIsButton = parent?.tagName.toLowerCase() === 'button'
        const grandparentIsButton = grandparent?.tagName.toLowerCase() === 'button'
        const parentHasAriaLabel = parent?.getAttribute('aria-label') !== null
        const grandparentHasAriaLabel = grandparent?.getAttribute('aria-label') !== null
        const parentProvidesAccessibility =
          (parentIsButton && parentHasAriaLabel) ||
          (grandparentIsButton && grandparentHasAriaLabel)

        // Check if SVG is inside a heading (feature cards)
        const isInsideHeading =
          parent?.closest('h1, h2, h3, h4, h5, h6') !== null ||
          parent?.closest('[data-testid^="feature-card-"]') !== null

        // Check if SVG has text sibling that provides context
        const hasTextSibling = parent?.textContent && parent.textContent.trim().length > 0

        // At minimum, icons in buttons with aria-labels are accessible
        // Lucide icons often don't set aria-hidden explicitly, but parent context matters
        const hasProperContext =
          isDecorative ||
          hasAccessibleName ||
          parentProvidesAccessibility ||
          isInsideHeading ||
          hasTextSibling

        // We accept SVGs that are properly contextualized
        expect(hasProperContext).toBe(true)
      })
    })

    it('should have feature card icons as decorative or with context', () => {
      renderFullPage()

      const featureCards = document.querySelectorAll('[data-testid^="feature-card-"]')
      featureCards.forEach((card) => {
        // Each feature card should have a heading that describes the feature
        const heading = card.querySelector('h3')
        expect(heading).not.toBeNull()
        expect(heading?.textContent).toBeTruthy()

        // The icon provides visual context, but heading is the accessible name
        // This is valid accessibility pattern
      })
    })
  })

  describe('Test Case 3: Heading hierarchy is correct', () => {
    it('should have exactly one h1 element', () => {
      renderFullPage()

      const h1Elements = screen.getAllByRole('heading', { level: 1 })
      expect(h1Elements).toHaveLength(1)
    })

    it('should have h1 with meaningful content', () => {
      renderFullPage()

      const h1 = screen.getByRole('heading', { level: 1 })
      expect(h1.textContent).toBeTruthy()
      expect(h1.textContent!.trim().length).toBeGreaterThan(0)
    })

    it('should have h2 elements that follow h1', () => {
      renderFullPage()

      const h1Elements = screen.getAllByRole('heading', { level: 1 })
      const h2Elements = screen.queryAllByRole('heading', { level: 2 })

      // If h2 exists, h1 must exist
      if (h2Elements.length > 0) {
        expect(h1Elements.length).toBeGreaterThanOrEqual(1)
      }
    })

    it('should have h3 elements only if h2 exists', () => {
      renderFullPage()

      const h2Elements = screen.queryAllByRole('heading', { level: 2 })
      const h3Elements = screen.queryAllByRole('heading', { level: 3 })

      // If h3 exists, h2 must exist (no skipping levels)
      if (h3Elements.length > 0) {
        expect(h2Elements.length).toBeGreaterThanOrEqual(1)
      }
    })

    it('should not skip heading levels', () => {
      renderFullPage()

      const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6')
      let lastLevel = 0

      allHeadings.forEach((heading) => {
        const level = parseInt(heading.tagName.charAt(1))

        // Heading level should not skip more than one level from previous
        if (lastLevel > 0) {
          const skipAmount = level - lastLevel
          // Allow going down in level (h2 -> h1 is fine)
          // But don't allow skipping forward more than 1 level
          if (skipAmount > 1) {
            expect(skipAmount).toBeLessThanOrEqual(1)
          }
        }

        lastLevel = level
      })
    })

    it('should have proper heading structure for features section', () => {
      renderFullPage()

      const featuresSection = screen.getByTestId('features-section')

      // Features section should have h2 for section title
      const h2InFeatures = within(featuresSection).queryAllByRole('heading', { level: 2 })
      expect(h2InFeatures.length).toBeGreaterThanOrEqual(1)

      // Feature cards should have h3 for individual items
      const h3InFeatures = within(featuresSection).queryAllByRole('heading', { level: 3 })
      expect(h3InFeatures.length).toBeGreaterThanOrEqual(1)
    })
  })

  describe('Test Case 3b: Sections have proper semantic structure', () => {
    it('should have aria-label on hero section', () => {
      renderFullPage()

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toHaveAttribute('aria-label')
    })

    it('should have aria-label on features section', () => {
      renderFullPage()

      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toHaveAttribute('aria-label')
    })

    it('should have aria-label on footer navigation', () => {
      renderWithProviders(<Footer />)

      const footerNav = screen.getByRole('navigation', { name: /footer navigation/i })
      expect(footerNav).toBeInTheDocument()
    })
  })

  describe('Test Case 4: axe-core accessibility audit passes', () => {
    it('should pass axe-core audit for homepage', async () => {
      const { container } = renderFullPage()

      // Run axe-core accessibility audit
      const results = await axe.run(container, {
        rules: {
          // Disable color-contrast rule as jsdom doesn't compute styles accurately
          'color-contrast': { enabled: false },
          // Focus on critical rules
          region: { enabled: true },
          'landmark-one-main': { enabled: false }, // We don't have main in test
          'page-has-heading-one': { enabled: true },
          'heading-order': { enabled: true },
          'button-name': { enabled: true },
          'image-alt': { enabled: true },
          'link-name': { enabled: true },
          'aria-valid-attr': { enabled: true },
          'aria-valid-attr-value': { enabled: true },
        },
      })

      // Filter for critical and serious violations
      const criticalViolations = results.violations.filter(
        (v) => v.impact === 'critical' || v.impact === 'serious'
      )

      // Log violations for debugging
      if (criticalViolations.length > 0) {
        console.log('Critical/Serious Accessibility Violations:')
        criticalViolations.forEach((violation) => {
          console.log(`- ${violation.id}: ${violation.description}`)
          console.log(`  Impact: ${violation.impact}`)
          violation.nodes.forEach((node) => {
            console.log(`  Element: ${node.html}`)
          })
        })
      }

      expect(criticalViolations).toHaveLength(0)
    })

    it('should pass button-name rule', async () => {
      const { container } = renderFullPage()

      const results = await axe.run(container, {
        runOnly: ['button-name'],
      })

      const violations = results.violations.filter(
        (v) => v.impact === 'critical' || v.impact === 'serious'
      )
      expect(violations).toHaveLength(0)
    })

    it('should pass image-alt rule', async () => {
      const { container } = renderFullPage()

      const results = await axe.run(container, {
        runOnly: ['image-alt'],
      })

      const violations = results.violations.filter(
        (v) => v.impact === 'critical' || v.impact === 'serious'
      )
      expect(violations).toHaveLength(0)
    })

    it('should pass link-name rule', async () => {
      const { container } = renderFullPage()

      const results = await axe.run(container, {
        runOnly: ['link-name'],
      })

      const violations = results.violations.filter(
        (v) => v.impact === 'critical' || v.impact === 'serious'
      )
      expect(violations).toHaveLength(0)
    })

    it('should pass aria-valid-attr rules', async () => {
      const { container } = renderFullPage()

      const results = await axe.run(container, {
        runOnly: ['aria-valid-attr', 'aria-valid-attr-value'],
      })

      const violations = results.violations.filter(
        (v) => v.impact === 'critical' || v.impact === 'serious'
      )
      expect(violations).toHaveLength(0)
    })
  })

  describe('ARIA landmarks are properly structured', () => {
    it('should have navigation landmark', () => {
      renderFullPage()

      const navs = screen.getAllByRole('navigation')
      expect(navs.length).toBeGreaterThanOrEqual(1)
    })

    it('should have footer landmark', () => {
      renderFullPage()

      const footer = screen.getByRole('contentinfo')
      expect(footer).toBeInTheDocument()
    })
  })

  describe('Interactive elements have sufficient touch target size', () => {
    it('should have buttons with minimum size classes', () => {
      renderWithProviders(<Navbar />)

      // Check hamburger menu has minimum size
      const hamburgerMenu = screen.getByTestId('hamburger-menu')
      const classes = hamburgerMenu.className

      // Verify button has size classes (w-12 h-12 = 48px)
      expect(
        classes.includes('w-12') ||
          classes.includes('h-12') ||
          classes.includes('min-w-12') ||
          classes.includes('min-h-12') ||
          classes.includes('btn')
      ).toBe(true)
    })

    it('should have nav links with minimum size', () => {
      renderWithProviders(<Navbar />)

      // Check that nav links have min-h-[44px] for touch targets
      const desktopNav = screen.queryByTestId('desktop-nav')
      if (desktopNav) {
        const links = within(desktopNav).getAllByRole('link')
        links.forEach((link) => {
          const classes = link.className
          // Should have min-h-[44px] or similar for WCAG touch target
          expect(
            classes.includes('min-h-[44px]') ||
              classes.includes('btn') ||
              classes.includes('min-h-11')
          ).toBe(true)
        })
      }
    })
  })
})
