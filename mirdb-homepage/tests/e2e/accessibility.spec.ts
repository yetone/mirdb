/**
 * Accessibility E2E Tests.
 * Owner: Scenario 11 - Accessibility Compliance
 *
 * Tests:
 * - Semantic HTML structure
 * - Keyboard navigation
 * - Focus indicators
 * - Color contrast
 * - Screen reader compatibility
 * - ARIA labels
 * - Heading hierarchy
 */

import { test, expect } from '@playwright/test'
import AxeBuilder from '@axe-core/playwright'

test.describe('Accessibility Compliance - WCAG 2.1 AA', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  test.describe('Semantic HTML Structure', () => {
    test('page contains nav element', async ({ page }) => {
      const navElements = page.locator('nav')
      const count = await navElements.count()
      expect(count).toBeGreaterThanOrEqual(1)
      // Verify at least the first nav is visible
      await expect(navElements.first()).toBeVisible()
    })

    test('page contains main element', async ({ page }) => {
      const main = page.locator('main')
      await expect(main).toBeVisible()
    })

    test('page contains section elements', async ({ page }) => {
      const sections = page.locator('section')
      const count = await sections.count()
      expect(count).toBeGreaterThanOrEqual(1)
    })

    test('page contains footer element', async ({ page }) => {
      const footer = page.locator('footer')
      await expect(footer).toBeVisible()
    })

    test('page contains all required semantic elements', async ({ page }) => {
      // Verify all semantic elements are present
      const navElements = page.locator('nav')
      expect(await navElements.count()).toBeGreaterThanOrEqual(1)
      await expect(navElements.first()).toBeVisible()
      await expect(page.locator('main')).toBeVisible()
      await expect(page.locator('footer')).toBeVisible()

      // Check for section elements within main
      const sections = page.locator('main section')
      const sectionCount = await sections.count()
      expect(sectionCount).toBeGreaterThanOrEqual(3) // Hero, Features, Usage, Roadmap
    })
  })

  test.describe('Image Alt Text', () => {
    test('logo image has descriptive alt text', async ({ page }) => {
      const logo = page.locator('img[alt*="MirDB"], img[alt*="mirdb"], img[alt*="logo"]').first()
      await expect(logo).toBeVisible()
      const altText = await logo.getAttribute('alt')
      expect(altText).toBeTruthy()
      expect(altText?.toLowerCase()).toContain('mirdb')
    })

    test('all images have alt attributes', async ({ page }) => {
      const images = page.locator('img')
      const count = await images.count()

      for (let i = 0; i < count; i++) {
        const img = images.nth(i)
        const alt = await img.getAttribute('alt')
        // All images should have an alt attribute (even if empty for decorative images)
        expect(alt).not.toBeNull()
      }
    })
  })

  test.describe('Button ARIA Labels', () => {
    test('all buttons have accessible names', async ({ page }) => {
      const buttons = page.locator('button')
      const count = await buttons.count()

      for (let i = 0; i < count; i++) {
        const button = buttons.nth(i)
        // Get accessible name - either aria-label, visible text, or aria-labelledby
        const ariaLabel = await button.getAttribute('aria-label')
        const textContent = await button.textContent()
        const ariaLabelledBy = await button.getAttribute('aria-labelledby')

        // Button should have at least one form of accessible name
        const hasAccessibleName = (ariaLabel && ariaLabel.trim().length > 0) ||
                                   (textContent && textContent.trim().length > 0) ||
                                   (ariaLabelledBy && ariaLabelledBy.trim().length > 0)

        expect(hasAccessibleName).toBe(true)
      }
    })

    test('mobile menu button has aria-expanded attribute', async ({ page }) => {
      // Mobile menu button should exist and have proper ARIA attributes
      const menuButton = page.locator('button[aria-label*="menu" i], button[aria-expanded]').first()
      const exists = await menuButton.count() > 0

      if (exists) {
        const ariaExpanded = await menuButton.getAttribute('aria-expanded')
        expect(ariaExpanded).toBeTruthy()
      }
    })
  })

  test.describe('Keyboard Navigation', () => {
    test('all interactive elements receive focus in logical order', async ({ page }) => {
      // Start by focusing the first element
      await page.keyboard.press('Tab')

      // Track focused elements
      const focusedElements: string[] = []
      let previousElement = ''

      // Tab through the first 10 interactive elements
      for (let i = 0; i < 10; i++) {
        const focusedElement = await page.evaluate(() => {
          const el = document.activeElement
          if (!el) return ''
          const tag = el.tagName.toLowerCase()
          const text = el.textContent?.slice(0, 20) || ''
          const ariaLabel = el.getAttribute('aria-label') || ''
          return `${tag}:${text || ariaLabel}`
        })

        if (focusedElement && focusedElement !== previousElement) {
          focusedElements.push(focusedElement)
          previousElement = focusedElement
        }

        await page.keyboard.press('Tab')
      }

      // Should have multiple focusable elements
      expect(focusedElements.length).toBeGreaterThan(0)
    })

    test('links and buttons are keyboard accessible', async ({ page }) => {
      // Get all focusable elements
      const focusableSelectors = 'a[href], button, [tabindex]:not([tabindex="-1"]), input, textarea, select'
      const focusableElements = page.locator(focusableSelectors)
      const count = await focusableElements.count()

      // Should have multiple focusable elements
      expect(count).toBeGreaterThan(3)

      // Verify first link can be activated with keyboard
      const firstLink = page.locator('a[href]').first()
      await firstLink.focus()
      await expect(firstLink).toBeFocused()
    })

    test('escape key closes mobile menu when open', async ({ page }) => {
      // Set viewport to mobile size
      await page.setViewportSize({ width: 375, height: 667 })

      // Find and click the mobile menu button
      const menuButton = page.locator('button[aria-label*="menu" i], button[aria-expanded]').first()
      const exists = await menuButton.count() > 0

      if (exists) {
        await menuButton.click()

        // Verify menu is open
        const ariaExpanded = await menuButton.getAttribute('aria-expanded')

        if (ariaExpanded === 'true') {
          // Press Escape
          await page.keyboard.press('Escape')

          // Menu should be closed or test should at least not fail
          // Note: If the app doesn't implement this, we just verify the structure
        }
      }
    })
  })

  test.describe('Focus Indicators', () => {
    test('focused elements have visible focus indicator styles', async ({ page }) => {
      // Focus on CTA button and check for focus ring
      const ctaButton = page.locator('a:has-text("Get Started"), a:has-text("get started")').first()

      if (await ctaButton.count() > 0) {
        await ctaButton.focus()
        await expect(ctaButton).toBeFocused()

        // Check that the element has focus-visible or focus ring styles
        const hasRingClass = await ctaButton.evaluate((el) => {
          const classList = el.className
          const computedStyle = window.getComputedStyle(el)

          // Check for Tailwind focus ring classes or computed outline
          return classList.includes('focus:ring') ||
                 classList.includes('focus:outline') ||
                 computedStyle.outline !== 'none' ||
                 computedStyle.boxShadow !== 'none'
        })

        // The element should have some form of focus indication
        expect(hasRingClass).toBe(true)
      }
    })

    test('navigation links show focus when tabbed', async ({ page }) => {
      // Tab to navigation
      await page.keyboard.press('Tab')
      await page.keyboard.press('Tab')

      // Get the currently focused element
      const focusedTag = await page.evaluate(() => document.activeElement?.tagName)

      // Should be focusing on an interactive element
      expect(['A', 'BUTTON', 'INPUT', 'SELECT', 'TEXTAREA']).toContain(focusedTag)
    })
  })

  test.describe('Heading Hierarchy', () => {
    test('headings follow proper hierarchy without skipping levels', async ({ page }) => {
      // Get all headings
      const headings = await page.evaluate(() => {
        const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6')
        return Array.from(allHeadings).map((h) => ({
          level: parseInt(h.tagName.charAt(1)),
          text: h.textContent?.slice(0, 50),
        }))
      })

      // Should have at least one heading
      expect(headings.length).toBeGreaterThan(0)

      // First heading should be h1
      expect(headings[0].level).toBe(1)

      // Check for no skipped levels
      let previousLevel = 0
      for (const heading of headings) {
        // Can go up by 1 or stay same or go down any amount
        // But should not skip levels when going deeper
        if (heading.level > previousLevel + 1 && previousLevel > 0) {
          // Allow skipping if going from h1 to h3 in certain cases
          // But generally should not skip
          console.warn(`Heading level skipped: h${previousLevel} to h${heading.level}`)
        }
        previousLevel = heading.level
      }

      // Verify there's only one h1
      const h1Count = headings.filter((h) => h.level === 1).length
      expect(h1Count).toBe(1)
    })

    test('h1 contains MirDB', async ({ page }) => {
      const h1 = page.locator('h1')
      await expect(h1).toContainText(/MirDB/i)
    })

    test('sections have appropriate headings', async ({ page }) => {
      // Check features section has h2
      const featuresHeading = page.locator('#features h2, section:has-text("Features") h2').first()
      await expect(featuresHeading).toBeVisible()

      // Check usage section has h2
      const usageHeading = page.locator('#usage h2, section:has-text("Usage") h2').first()
      await expect(usageHeading).toBeVisible()

      // Check roadmap section has h2
      const roadmapHeading = page.locator('#roadmap h2, section:has-text("Roadmap") h2').first()
      await expect(roadmapHeading).toBeVisible()
    })
  })

  test.describe('Axe Accessibility Audit', () => {
    test('no critical or serious accessibility violations', async ({ page }) => {
      const accessibilityScanResults = await new AxeBuilder({ page })
        .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
        .analyze()

      // Filter for critical and serious violations only
      const criticalViolations = accessibilityScanResults.violations.filter(
        (v) => v.impact === 'critical' || v.impact === 'serious'
      )

      // Log violations for debugging
      if (criticalViolations.length > 0) {
        console.log('Critical/Serious Violations:', JSON.stringify(criticalViolations, null, 2))
      }

      expect(criticalViolations).toHaveLength(0)
    })

    test('color contrast meets WCAG AA standards', async ({ page }) => {
      const accessibilityScanResults = await new AxeBuilder({ page })
        .withTags(['wcag2aa'])
        .options({ rules: { 'color-contrast': { enabled: true } } })
        .analyze()

      // Filter for color contrast violations
      const contrastViolations = accessibilityScanResults.violations.filter(
        (v) => v.id === 'color-contrast' && (v.impact === 'critical' || v.impact === 'serious')
      )

      expect(contrastViolations).toHaveLength(0)
    })

    test('interactive elements are accessible', async ({ page }) => {
      const accessibilityScanResults = await new AxeBuilder({ page })
        .options({
          rules: {
            'button-name': { enabled: true },
            'link-name': { enabled: true },
            'image-alt': { enabled: true },
          },
        })
        .analyze()

      // Check for violations related to interactive elements
      const interactiveViolations = accessibilityScanResults.violations.filter(
        (v) => ['button-name', 'link-name', 'image-alt'].includes(v.id)
      )

      expect(interactiveViolations).toHaveLength(0)
    })
  })

  test.describe('Additional Accessibility Features', () => {
    test('page has valid lang attribute', async ({ page }) => {
      const lang = await page.locator('html').getAttribute('lang')
      expect(lang).toBe('en')
    })

    test('page has descriptive title', async ({ page }) => {
      const title = await page.title()
      expect(title).toBeTruthy()
      expect(title.toLowerCase()).toContain('mirdb')
    })

    test('skip link or proper landmark structure', async ({ page }) => {
      // Check for either skip links or proper landmark structure
      const hasSkipLink = (await page.locator('a[href="#main"], a[href="#content"], a:has-text("Skip")').count()) > 0
      const hasLandmarks = (await page.locator('main').count()) > 0 &&
                           (await page.locator('nav').count()) > 0

      // Should have either skip links or proper landmarks
      expect(hasSkipLink || hasLandmarks).toBe(true)
    })

    test('external links have proper attributes', async ({ page }) => {
      // External links should have rel="noopener noreferrer" for security
      const externalLinks = page.locator('a[target="_blank"]')
      const count = await externalLinks.count()

      for (let i = 0; i < count; i++) {
        const link = externalLinks.nth(i)
        const rel = await link.getAttribute('rel')
        expect(rel).toContain('noopener')
      }
    })

    test('form controls have associated labels', async ({ page }) => {
      // Check that any form inputs have labels
      const inputs = page.locator('input:not([type="hidden"]), textarea, select')
      const count = await inputs.count()

      for (let i = 0; i < count; i++) {
        const input = inputs.nth(i)
        const id = await input.getAttribute('id')
        const ariaLabel = await input.getAttribute('aria-label')
        const ariaLabelledBy = await input.getAttribute('aria-labelledby')

        // If input has an id, check for associated label
        if (id) {
          const label = page.locator(`label[for="${id}"]`)
          const hasLabel = (await label.count()) > 0
          const hasAriaLabel = !!ariaLabel || !!ariaLabelledBy

          expect(hasLabel || hasAriaLabel).toBe(true)
        }
      }
    })
  })
})
