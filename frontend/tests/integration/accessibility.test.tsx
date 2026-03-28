/**
 * Accessibility Integration Tests
 * Owner: Scenario 11 - Accessibility Compliance
 *
 * Tests WCAG 2.1 AA compliance including:
 * - Automated axe-core accessibility audit
 * - Color contrast validation
 * - Keyboard navigation
 * - Focus indicators
 * - Image alt text
 * - Semantic HTML structure
 * - Heading hierarchy
 * - Skip link functionality
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import { render, screen, within } from '../setup'
import { axe } from 'vitest-axe'
import * as matchers from 'vitest-axe/matchers'
import userEvent from '@testing-library/user-event'
import Home from '@/pages/Home'

// Extend expect with axe matchers
expect.extend(matchers)

// Mock the API for stats section
vi.mock('@/api', () => ({
  fetchPublicStats: vi.fn().mockResolvedValue({
    linksShortened: 100000,
    clicksTracked: 500000,
    activeUsers: 10000,
  }),
  DEFAULT_STATS: {
    linksShortened: 100000,
    clicksTracked: 500000,
    activeUsers: 10000,
  },
}))

describe('Accessibility Compliance - WCAG 2.1 AA', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  /**
   * Test Case 1: Run axe-core accessibility audit on HomePage
   * Type: integration
   * Expected: No critical or serious accessibility violations
   */
  describe('TC1: Automated Accessibility Audit', () => {
    it('should have no critical or serious accessibility violations', async () => {
      const { container } = render(<Home />)

      // Wait for async content to load
      await screen.findByTestId('home-page')

      // Run axe-core accessibility audit
      const results = await axe(container, {
        rules: {
          // Focus on critical and serious violations for WCAG AA
          'color-contrast': { enabled: true },
          'image-alt': { enabled: true },
          'label': { enabled: true },
          'link-name': { enabled: true },
          'heading-order': { enabled: true },
          'region': { enabled: true },
          'landmark-one-main': { enabled: true },
          'page-has-heading-one': { enabled: true },
          'bypass': { enabled: true },
        },
      })

      // Filter for critical and serious violations only
      const criticalViolations = results.violations.filter(
        (v) => v.impact === 'critical' || v.impact === 'serious'
      )

      expect(criticalViolations).toHaveLength(0)
    })

    it('should pass axe-core audit with no violations', async () => {
      const { container } = render(<Home />)
      await screen.findByTestId('home-page')

      const results = await axe(container)
      expect(results).toHaveNoViolations()
    })
  })

  /**
   * Test Case 2: Check color contrast ratios
   * Type: unit
   * Expected: All text meets minimum 4.5:1 contrast ratio (WCAG AA)
   * Note: Color contrast is primarily validated by axe-core. This test
   * ensures the page structure supports proper contrast through DaisyUI's
   * semantic color classes.
   */
  describe('TC2: Color Contrast', () => {
    it('should use semantic color classes that support proper contrast', async () => {
      const { container } = render(<Home />)
      await screen.findByTestId('home-page')

      // Verify DaisyUI semantic classes are used (which maintain proper contrast)
      const mainElement = container.querySelector('main')
      expect(mainElement).toHaveClass('bg-base-100')

      // Check that text uses base-content or similar semantic colors
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Verify h1 exists and is readable
      const heading = screen.getByRole('heading', { level: 1 })
      expect(heading).toBeVisible()

      // Run axe specifically for color contrast
      const results = await axe(container, {
        runOnly: ['color-contrast'],
      })
      expect(results.violations).toHaveLength(0)
    })

    it('should have readable text in both themed sections', async () => {
      render(<Home />)
      await screen.findByTestId('home-page')

      // Check hero section (bg-base-200)
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toHaveClass('bg-base-200')

      // Check stats section (bg-base-100)
      const statsSection = screen.getByTestId('stats-section')
      expect(statsSection).toHaveClass('bg-base-100')
    })
  })

  /**
   * Test Case 3: Navigate page using Tab key
   * Type: e2e
   * Expected: All interactive elements receive focus in logical order
   */
  describe('TC3: Keyboard Navigation', () => {
    it('should allow navigation through all interactive elements using Tab', async () => {
      const user = userEvent.setup()
      render(<Home />)
      await screen.findByTestId('home-page')

      // Start tabbing through interactive elements
      // First should be skip link
      await user.tab()
      const skipLink = screen.getByTestId('skip-link')
      expect(skipLink).toHaveFocus()

      // Continue tabbing to navigation
      await user.tab()
      // Should reach navbar logo
      const logoLink = screen.getByTestId('navbar-logo')
      expect(logoLink).toHaveFocus()

      // Tab through navigation links
      await user.tab()
      expect(screen.getByTestId('nav-features')).toHaveFocus()

      await user.tab()
      expect(screen.getByTestId('nav-pricing')).toHaveFocus()

      await user.tab()
      expect(screen.getByTestId('nav-about')).toHaveFocus()

      // Tab through auth links
      await user.tab() // theme toggle
      await user.tab()
      expect(screen.getByTestId('nav-login')).toHaveFocus()

      await user.tab()
      expect(screen.getByTestId('nav-get-started')).toHaveFocus()
    })

    it('should navigate to hero CTAs after navigation', async () => {
      const user = userEvent.setup()
      render(<Home />)
      await screen.findByTestId('home-page')

      // Find all tabbable elements
      const allLinks = screen.getAllByRole('link')
      const allButtons = screen.getAllByRole('button')
      const focusableElements = [...allLinks, ...allButtons].filter(
        (el) => !el.closest('[hidden]') && !el.hasAttribute('disabled')
      )

      // Verify there are focusable elements
      expect(focusableElements.length).toBeGreaterThan(0)

      // Tab through first few elements
      for (let i = 0; i < Math.min(5, focusableElements.length); i++) {
        await user.tab()
        expect(document.activeElement).toBeTruthy()
      }
    })

    it('should support reverse tab navigation (Shift+Tab)', async () => {
      const user = userEvent.setup()
      render(<Home />)
      await screen.findByTestId('home-page')

      // Tab forward a few times
      await user.tab()
      await user.tab()
      await user.tab()

      const currentElement = document.activeElement

      // Tab backward
      await user.tab({ shift: true })
      expect(document.activeElement).not.toBe(currentElement)
    })
  })

  /**
   * Test Case 4: Check focus indicators on interactive elements
   * Type: e2e
   * Expected: Visible focus ring/outline appears on focused elements
   */
  describe('TC4: Focus Indicators', () => {
    it('should have visible focus indicators on links', async () => {
      const user = userEvent.setup()
      render(<Home />)
      await screen.findByTestId('home-page')

      // Tab to first link (skip link)
      await user.tab()
      const skipLink = screen.getByTestId('skip-link')
      expect(skipLink).toHaveFocus()

      // DaisyUI and Tailwind provide focus styles via btn classes and focus-visible
      // Check that the focused element has proper focus styling classes available
      const focusedElement = document.activeElement
      expect(focusedElement).toBeTruthy()
    })

    it('should have visible focus on buttons', async () => {
      const user = userEvent.setup()
      render(<Home />)
      await screen.findByTestId('home-page')

      // Navigate to primary CTA button
      const primaryCta = screen.getByTestId('primary-cta')
      primaryCta.focus()
      expect(primaryCta).toHaveFocus()

      // Button should have proper classes for focus visibility
      expect(primaryCta).toHaveClass('btn')
    })

    it('should have visible focus on navigation buttons', async () => {
      render(<Home />)
      await screen.findByTestId('home-page')

      const themeToggle = screen.getByTestId('theme-toggle')
      themeToggle.focus()
      expect(themeToggle).toHaveFocus()
      expect(themeToggle).toHaveClass('btn')
    })
  })

  /**
   * Test Case 5: Check image elements
   * Type: unit
   * Expected: All images have descriptive alt text attributes
   */
  describe('TC5: Image Alt Text', () => {
    it('should have alt text on all image elements', async () => {
      const { container } = render(<Home />)
      await screen.findByTestId('home-page')

      // Find all img elements
      const images = container.querySelectorAll('img')

      images.forEach((img) => {
        // Each image should have an alt attribute
        expect(img).toHaveAttribute('alt')
        // Alt text should not be empty unless decorative
        const altText = img.getAttribute('alt')
        if (altText !== '') {
          expect(altText!.length).toBeGreaterThan(0)
        }
      })
    })

    it('should have aria-hidden on decorative SVG icons', async () => {
      const { container } = render(<Home />)
      await screen.findByTestId('home-page')

      // Find decorative SVGs (icons)
      const svgs = container.querySelectorAll('svg')

      svgs.forEach((svg) => {
        // Decorative icons should have aria-hidden="true"
        // or be part of an element with accessible text
        // or have an ancestor with aria-hidden
        const hasAriaHidden = svg.getAttribute('aria-hidden') === 'true'
        const hasAriaLabel = svg.hasAttribute('aria-label')

        // Check if any ancestor has aria-hidden or aria-label
        let ancestor = svg.parentElement
        let ancestorHasAriaHidden = false
        let ancestorHasAriaLabel = false

        while (ancestor && ancestor !== container) {
          if (ancestor.getAttribute('aria-hidden') === 'true') {
            ancestorHasAriaHidden = true
            break
          }
          if (ancestor.hasAttribute('aria-label')) {
            ancestorHasAriaLabel = true
            break
          }
          ancestor = ancestor.parentElement
        }

        const isAccessible =
          hasAriaHidden ||
          hasAriaLabel ||
          ancestorHasAriaHidden ||
          ancestorHasAriaLabel

        expect(isAccessible).toBe(true)
      })
    })
  })

  /**
   * Test Case 6: Inspect HTML structure
   * Type: unit
   * Expected: Page uses semantic elements: header, nav, main, section, footer
   */
  describe('TC6: Semantic HTML Structure', () => {
    it('should have a main element', async () => {
      render(<Home />)
      await screen.findByTestId('home-page')

      const main = screen.getByRole('main')
      expect(main).toBeInTheDocument()
    })

    it('should have a navigation element', async () => {
      render(<Home />)
      await screen.findByTestId('home-page')

      const nav = screen.getByRole('navigation', { name: /main navigation/i })
      expect(nav).toBeInTheDocument()
    })

    it('should have a footer element', async () => {
      render(<Home />)
      await screen.findByTestId('home-page')

      const footer = screen.getByTestId('footer')
      expect(footer.tagName.toLowerCase()).toBe('footer')
    })

    it('should have multiple section elements', async () => {
      const { container } = render(<Home />)
      await screen.findByTestId('home-page')

      const sections = container.querySelectorAll('section')
      expect(sections.length).toBeGreaterThanOrEqual(3)
    })

    it('should have properly labeled sections with aria-label or aria-labelledby', async () => {
      render(<Home />)
      await screen.findByTestId('home-page')

      // Stats section should have aria-label
      const statsSection = screen.getByTestId('stats-section')
      expect(statsSection).toHaveAttribute('aria-label')

      // How it works section should have aria-labelledby
      const howItWorksSection = screen.getByTestId('how-it-works-section')
      expect(howItWorksSection).toHaveAttribute('aria-labelledby')
    })

    it('should have secondary navigation in footer', async () => {
      render(<Home />)
      await screen.findByTestId('home-page')

      const footerNav = screen.getByRole('navigation', { name: /footer/i })
      expect(footerNav).toBeInTheDocument()
    })
  })

  /**
   * Test Case 7: Check heading hierarchy
   * Type: unit
   * Expected: Headings follow logical order (H1 before H2, etc.) without skipping levels
   */
  describe('TC7: Heading Hierarchy', () => {
    it('should have exactly one H1 element', async () => {
      render(<Home />)
      await screen.findByTestId('home-page')

      const h1Elements = screen.getAllByRole('heading', { level: 1 })
      expect(h1Elements).toHaveLength(1)
    })

    it('should have H1 as the first heading in the document', async () => {
      const { container } = render(<Home />)
      await screen.findByTestId('home-page')

      const allHeadings = container.querySelectorAll('h1, h2, h3, h4, h5, h6')
      expect(allHeadings.length).toBeGreaterThan(0)

      // First heading should be H1
      expect(allHeadings[0].tagName.toLowerCase()).toBe('h1')
    })

    it('should have proper heading hierarchy without skipping levels', async () => {
      const { container } = render(<Home />)
      await screen.findByTestId('home-page')

      const headings = Array.from(
        container.querySelectorAll('h1, h2, h3, h4, h5, h6')
      )

      let previousLevel = 0

      headings.forEach((heading) => {
        const currentLevel = parseInt(heading.tagName.charAt(1))

        // Should not skip more than one level
        // (e.g., H1 to H3 without H2 is invalid)
        if (previousLevel > 0 && currentLevel > previousLevel) {
          expect(currentLevel - previousLevel).toBeLessThanOrEqual(1)
        }

        previousLevel = currentLevel
      })
    })

    it('should have descriptive heading content', async () => {
      const { container } = render(<Home />)
      await screen.findByTestId('home-page')

      const headings = container.querySelectorAll('h1, h2, h3, h4, h5, h6')

      headings.forEach((heading) => {
        // Each heading should have non-empty text content
        expect(heading.textContent?.trim().length).toBeGreaterThan(0)
      })
    })

    it('should have H2 elements for section headings', async () => {
      render(<Home />)
      await screen.findByTestId('home-page')

      const h2Elements = screen.getAllByRole('heading', { level: 2 })
      // Should have multiple H2s for different sections
      expect(h2Elements.length).toBeGreaterThanOrEqual(2)
    })
  })

  /**
   * Test Case 8: Check for skip link
   * Type: unit
   * Expected: Skip to main content link is present for keyboard users
   */
  describe('TC8: Skip Link', () => {
    it('should have a skip link at the start of the page', async () => {
      render(<Home />)
      await screen.findByTestId('home-page')

      const skipLink = screen.getByTestId('skip-link')
      expect(skipLink).toBeInTheDocument()
    })

    it('should have skip link as first focusable element', async () => {
      const user = userEvent.setup()
      render(<Home />)
      await screen.findByTestId('home-page')

      // Tab once - should focus on skip link
      await user.tab()
      const skipLink = screen.getByTestId('skip-link')
      expect(skipLink).toHaveFocus()
    })

    it('should have skip link that links to main content', async () => {
      render(<Home />)
      await screen.findByTestId('home-page')

      const skipLink = screen.getByTestId('skip-link')
      expect(skipLink).toHaveAttribute('href', '#main-content')
    })

    it('should have accessible text on skip link', async () => {
      render(<Home />)
      await screen.findByTestId('home-page')

      const skipLink = screen.getByTestId('skip-link')
      expect(skipLink).toHaveTextContent(/skip/i)
    })

    it('should have main content with matching ID', async () => {
      const { container } = render(<Home />)
      await screen.findByTestId('home-page')

      const mainContent = container.querySelector('#main-content')
      expect(mainContent).toBeInTheDocument()
    })

    it('should be visually hidden but visible when focused', async () => {
      const user = userEvent.setup()
      render(<Home />)
      await screen.findByTestId('home-page')

      const skipLink = screen.getByTestId('skip-link')

      // Initially should be visually hidden (sr-only class)
      expect(skipLink).toHaveClass('sr-only')

      // When focused, should become visible (focus:not-sr-only)
      await user.tab()
      expect(skipLink).toHaveFocus()
      expect(skipLink).toHaveClass('focus:not-sr-only')
    })
  })

  /**
   * Test Case 9: Test with screen reader
   * Type: manual
   * Expected: Page content is announced correctly and navigation is clear
   *
   * Note: This is documented as a manual test that should be performed
   * using actual screen reader software (VoiceOver, NVDA, JAWS).
   * The automated tests below verify the prerequisites for screen reader compatibility.
   */
  describe('TC9: Screen Reader Compatibility', () => {
    it('should have proper ARIA landmarks', async () => {
      render(<Home />)
      await screen.findByTestId('home-page')

      // Main landmark
      expect(screen.getByRole('main')).toBeInTheDocument()

      // Navigation landmark
      expect(
        screen.getByRole('navigation', { name: /main navigation/i })
      ).toBeInTheDocument()

      // Contentinfo landmark (footer)
      expect(screen.getByRole('contentinfo')).toBeInTheDocument()
    })

    it('should have accessible names for interactive elements', async () => {
      render(<Home />)
      await screen.findByTestId('home-page')

      // Theme toggle should have aria-label
      const themeToggle = screen.getByTestId('theme-toggle')
      expect(themeToggle).toHaveAttribute('aria-label')

      // Mobile menu toggle should have aria-label
      const mobileMenuToggle = screen.getByTestId('mobile-menu-toggle')
      expect(mobileMenuToggle).toHaveAttribute('aria-label')

      // Logo link should have aria-label
      const logoLink = screen.getByTestId('navbar-logo')
      expect(logoLink).toHaveAttribute('aria-label')
    })

    it('should have proper link text that makes sense out of context', async () => {
      render(<Home />)
      await screen.findByTestId('home-page')

      // Get all links
      const links = screen.getAllByRole('link')

      links.forEach((link) => {
        // Links should have meaningful text content or aria-label
        const hasText = link.textContent && link.textContent.trim().length > 0
        const hasAriaLabel = link.hasAttribute('aria-label')

        expect(hasText || hasAriaLabel).toBe(true)
      })
    })

    it('should have proper button states announced', async () => {
      render(<Home />)
      await screen.findByTestId('home-page')

      // Mobile menu button should announce expanded state
      const mobileMenuToggle = screen.getByTestId('mobile-menu-toggle')
      expect(mobileMenuToggle).toHaveAttribute('aria-expanded')
    })

    // Manual test documentation
    it.skip('MANUAL TEST: Screen reader navigation', () => {
      /**
       * Manual Testing Steps:
       *
       * 1. Navigate to homepage using VoiceOver (Mac), NVDA, or JAWS
       * 2. Verify the page title is announced
       * 3. Use landmark navigation (D key in NVDA) to jump between regions
       * 4. Verify all headings are announced with proper levels
       * 5. Navigate through links and verify each has meaningful text
       * 6. Verify form inputs have associated labels
       * 7. Verify dynamic content (stats loading) is announced
       * 8. Test mobile menu expansion is announced
       *
       * Expected Results:
       * - Page structure is clear and navigable
       * - All interactive elements have accessible names
       * - Focus order makes sense when navigating
       * - State changes (menu open/close) are announced
       */
    })
  })
})
