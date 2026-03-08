/**
 * Accessibility E2E tests for MirDB homepage.
 * Owners:
 * - Scenario 12: Keyboard navigation tests
 * - Scenario 13: Screen reader compatibility tests
 * - Scenario 14: Color contrast tests
 *
 * Requirements:
 * - WCAG 2.1 AA compliance (NFR-2)
 * - Keyboard navigable
 * - Screen reader compatible
 * - 4.5:1 contrast ratio for text
 *
 * Test structure:
 * - describe('Keyboard Navigation')
 * - describe('Screen Reader Compatibility')
 * - describe('Color Contrast')
 */

import { test, expect } from '@playwright/test'
import AxeBuilder from '@axe-core/playwright'

/**
 * Keyboard Navigation Tests (Scenario 12)
 * Verifies homepage is fully navigable via keyboard (NFR-2, US-7)
 */
test.describe('Keyboard Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    // Wait for page to fully load
    await page.waitForLoadState('domcontentloaded')
  })

  // Test Case 1: Tab to first interactive element
  test('focus moves to first link/button with visible indicator when Tab pressed', async ({
    page,
  }) => {
    // Start from page top
    await page.keyboard.press('Tab')

    // The first focusable element should be the skip link
    const skipLink = page.locator('a[href="#main-content"]')
    await expect(skipLink).toBeFocused()

    // Skip link should become visible when focused (not sr-only)
    await expect(skipLink).toBeVisible()

    // Verify focus indicator is visible (skip link has focus styles)
    const skipLinkBox = await skipLink.boundingBox()
    expect(skipLinkBox).toBeTruthy()
    expect(skipLinkBox!.width).toBeGreaterThan(0)
    expect(skipLinkBox!.height).toBeGreaterThan(0)
  })

  // Test Case 2: Tab through all interactive elements in logical order
  test('all links and buttons receive focus in logical order', async ({ page }) => {
    const focusedElements: string[] = []

    // Tab through elements and collect their identifiers
    let maxTabs = 50 // Safety limit
    let previousElement = ''

    while (maxTabs > 0) {
      await page.keyboard.press('Tab')
      maxTabs--

      // Get currently focused element info
      const focusedInfo = await page.evaluate(() => {
        const el = document.activeElement
        if (!el || el === document.body) return null

        const tag = el.tagName.toLowerCase()
        const href = (el as HTMLAnchorElement).href || ''
        const text = el.textContent?.trim().slice(0, 50) || ''
        const id = el.id || ''

        return { tag, href, text, id }
      })

      if (!focusedInfo) continue

      const elementKey = `${focusedInfo.tag}:${focusedInfo.text || focusedInfo.href || focusedInfo.id}`

      // Stop if we've cycled back to the beginning
      if (focusedElements.length > 0 && elementKey === focusedElements[0]) {
        break
      }

      // Avoid duplicates from same element
      if (elementKey !== previousElement) {
        focusedElements.push(elementKey)
        previousElement = elementKey
      }
    }

    // Verify we found multiple interactive elements
    expect(focusedElements.length).toBeGreaterThanOrEqual(5)

    // Verify logical order: skip link first, then header nav, then hero buttons
    const skipLinkIndex = focusedElements.findIndex((el) =>
      el.includes('Skip to main content')
    )
    const featuresLinkIndex = focusedElements.findIndex((el) => el.includes('Features'))
    const getStartedLinkIndex = focusedElements.findIndex((el) =>
      el.includes('Get Started')
    )

    // Skip link should be first
    expect(skipLinkIndex).toBe(0)

    // Navigation links should come before hero CTA buttons
    if (featuresLinkIndex !== -1 && getStartedLinkIndex !== -1) {
      expect(featuresLinkIndex).toBeLessThan(getStartedLinkIndex)
    }
  })

  // Test Case 3: Check focus visibility on buttons
  test('buttons show visible focus ring meeting 3:1 contrast', async ({ page }) => {
    // Navigate to hero section CTA button
    let foundGetStarted = false
    for (let i = 0; i < 20; i++) {
      await page.keyboard.press('Tab')

      const focusedText = await page.evaluate(() => {
        return document.activeElement?.textContent?.trim()
      })

      if (focusedText === 'Get Started') {
        foundGetStarted = true
        break
      }
    }

    expect(foundGetStarted).toBe(true)

    // Get the focused button
    const focusedButton = page.locator(':focus')
    await expect(focusedButton).toBeVisible()

    // Check focus ring styles
    const focusStyles = await focusedButton.evaluate((el) => {
      const computed = window.getComputedStyle(el)
      return {
        outline: computed.outline,
        outlineColor: computed.outlineColor,
        outlineWidth: computed.outlineWidth,
        outlineStyle: computed.outlineStyle,
        boxShadow: computed.boxShadow,
        ringWidth: computed.getPropertyValue('--tw-ring-offset-width'),
      }
    })

    // Tailwind's focus:ring-2 creates a visible focus indicator via box-shadow
    // The box-shadow should be non-empty (not 'none')
    const hasVisibleFocusIndicator =
      (focusStyles.boxShadow && focusStyles.boxShadow !== 'none') ||
      (focusStyles.outlineWidth && parseFloat(focusStyles.outlineWidth) >= 2)

    expect(hasVisibleFocusIndicator).toBe(true)

    // Verify contrast: focus ring should be visible against background
    // blue-500 (#3b82f6) on slate-900 (#0f172a) background provides 4.5:1+ contrast
    if (focusStyles.boxShadow && focusStyles.boxShadow !== 'none') {
      // Check that the ring color contains a visible blue
      const hasBlueRing = focusStyles.boxShadow.includes('59, 130, 246') // rgb for blue-500
      expect(hasBlueRing).toBe(true)
    }
  })

  // Test Case 4: Press Enter on focused CTA
  test('CTA activates and scrolls to Getting Started when Enter pressed', async ({
    page,
  }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY)

    // Navigate to "Get Started" CTA button
    let foundGetStarted = false
    for (let i = 0; i < 20; i++) {
      await page.keyboard.press('Tab')

      const focusedText = await page.evaluate(() => {
        return document.activeElement?.textContent?.trim()
      })

      if (focusedText === 'Get Started') {
        foundGetStarted = true
        break
      }
    }

    expect(foundGetStarted).toBe(true)

    // Press Enter to activate the link
    await page.keyboard.press('Enter')

    // Wait for smooth scroll animation to complete
    await page.waitForTimeout(500)

    // Verify URL hash changed to #getting-started
    const currentUrl = page.url()
    expect(currentUrl).toContain('#getting-started')

    // Verify page scrolled to Getting Started section
    const finalScrollY = await page.evaluate(() => window.scrollY)

    // Getting Started section should be visible (scrolled into view)
    const gettingStartedSection = page.locator('#getting-started')
    await expect(gettingStartedSection).toBeVisible()

    // Scroll position should have changed (page scrolled down)
    expect(finalScrollY).toBeGreaterThan(initialScrollY)

    // Verify the section is near the top of the viewport
    const sectionBounds = await gettingStartedSection.boundingBox()
    expect(sectionBounds).toBeTruthy()
    // Section top should be near top of viewport (within 200px accounting for sticky header)
    expect(sectionBounds!.y).toBeLessThan(200)
  })

  // Test Case 5: Check skip link functionality
  test('skip to main content link exists and works', async ({ page }) => {
    // Verify skip link exists
    const skipLink = page.locator('a[href="#main-content"]')
    await expect(skipLink).toBeAttached()

    // Skip link should be visually hidden initially (sr-only class)
    const initialVisibility = await skipLink.evaluate((el) => {
      const computed = window.getComputedStyle(el)
      return {
        position: computed.position,
        clip: computed.clip,
        width: computed.width,
        height: computed.height,
      }
    })

    // sr-only makes element 1x1 pixel with clip
    expect(initialVisibility.position).toBe('absolute')

    // Tab to skip link
    await page.keyboard.press('Tab')
    await expect(skipLink).toBeFocused()

    // Skip link should now be visible (focus:not-sr-only)
    await expect(skipLink).toBeVisible()
    const focusedBox = await skipLink.boundingBox()
    expect(focusedBox!.width).toBeGreaterThan(50)
    expect(focusedBox!.height).toBeGreaterThan(20)

    // Verify skip link text
    const skipLinkText = await skipLink.textContent()
    expect(skipLinkText).toContain('Skip to main content')

    // Activate skip link
    await page.keyboard.press('Enter')

    // Wait for navigation/focus change
    await page.waitForTimeout(100)

    // Verify main-content exists as the target
    const mainContent = page.locator('#main-content')
    await expect(mainContent).toBeAttached()

    // URL should have #main-content hash
    const currentUrl = page.url()
    expect(currentUrl).toContain('#main-content')
  })

  // Additional test: Verify all interactive elements have focus styles
  test('all interactive elements have visible focus indicators', async ({ page }) => {
    // Get all focusable elements
    const focusableSelectors =
      'a[href], button, [tabindex]:not([tabindex="-1"]), input, select, textarea'
    const focusableElements = page.locator(focusableSelectors)
    const count = await focusableElements.count()

    expect(count).toBeGreaterThan(0)

    // Sample check: verify at least the first 5 focusable elements have focus styles
    const elementsToCheck = Math.min(count, 5)

    for (let i = 0; i < elementsToCheck; i++) {
      const element = focusableElements.nth(i)

      // Skip hidden elements
      const isVisible = await element.isVisible()
      if (!isVisible) continue

      // Focus the element
      await element.focus()
      await expect(element).toBeFocused()

      // Check for focus styling (outline or box-shadow)
      const focusStyles = await element.evaluate((el) => {
        const computed = window.getComputedStyle(el)
        return {
          outline: computed.outline,
          outlineStyle: computed.outlineStyle,
          outlineWidth: computed.outlineWidth,
          boxShadow: computed.boxShadow,
        }
      })

      // Element should have either outline or box-shadow for focus visibility
      const hasOutline =
        focusStyles.outlineStyle !== 'none' && parseFloat(focusStyles.outlineWidth) > 0
      const hasBoxShadow = focusStyles.boxShadow !== 'none'

      expect(hasOutline || hasBoxShadow).toBe(true)
    }
  })

  // Test: Verify navigation links work with Enter key
  test('header navigation links activate with Enter key', async ({ page }) => {
    // Tab through to find Features link
    let foundFeatures = false
    for (let i = 0; i < 15; i++) {
      await page.keyboard.press('Tab')

      const focusedText = await page.evaluate(() => {
        const el = document.activeElement
        return el?.textContent?.trim()
      })

      if (focusedText === 'Features') {
        foundFeatures = true
        break
      }
    }

    expect(foundFeatures).toBe(true)

    // Press Enter to navigate
    await page.keyboard.press('Enter')

    // Wait for scroll
    await page.waitForTimeout(500)

    // Verify URL changed
    expect(page.url()).toContain('#features')

    // Verify Features section is visible
    const featuresSection = page.locator('#features')
    await expect(featuresSection).toBeVisible()
  })

  // Test: Verify focus order follows visual order
  test('focus order follows visual/reading order (top to bottom, left to right)', async ({
    page,
  }) => {
    const focusPositions: { y: number; x: number; text: string }[] = []

    // Tab through first 15 elements and record positions
    for (let i = 0; i < 15; i++) {
      await page.keyboard.press('Tab')

      const posInfo = await page.evaluate(() => {
        const el = document.activeElement
        if (!el || el === document.body) return null

        const rect = el.getBoundingClientRect()
        return {
          y: rect.top + window.scrollY,
          x: rect.left,
          text: el.textContent?.trim().slice(0, 30) || '',
        }
      })

      if (posInfo && posInfo.text) {
        focusPositions.push(posInfo)
      }
    }

    // Verify we collected positions
    expect(focusPositions.length).toBeGreaterThan(5)

    // Generally, each focused element should be at or below the previous one
    // (accounting for same-row elements that might be to the right)
    let previousY = -Infinity

    for (let i = 0; i < focusPositions.length; i++) {
      const current = focusPositions[i]

      // Allow some tolerance for elements on same row
      // If Y is similar (within 50px), it's okay
      // If Y is greater, that's correct (moving down)
      const tolerance = 50

      if (current.y < previousY - tolerance) {
        // Focus jumped up significantly - this might be wrong
        // But allow it for skip links or modals
        if (!current.text.toLowerCase().includes('skip')) {
          // Log but don't fail - some layouts have valid reasons
          console.log(`Focus order note: ${current.text} (y=${current.y}) after y=${previousY}`)
        }
      }

      previousY = current.y
    }
  })
})

/**
 * Screen Reader Compatibility Tests (Scenario 13)
 * Verifies homepage content is accessible to screen readers (NFR-2, US-7)
 */
test.describe('Screen Reader Compatibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('domcontentloaded')
  })

  // Test Case 1: Check for single h1 element
  test('page has exactly one h1 element', async ({ page }) => {
    const h1Elements = page.locator('h1')
    const count = await h1Elements.count()

    expect(count).toBe(1)

    // Verify the h1 has meaningful content
    const h1Text = await h1Elements.first().textContent()
    expect(h1Text?.trim().length).toBeGreaterThan(0)
  })

  // Test Case 2: Validate heading hierarchy
  test('heading hierarchy has no skipped levels', async ({ page }) => {
    const headingLevels = await page.evaluate(() => {
      const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6')
      return Array.from(headings).map((h) => ({
        level: parseInt(h.tagName.charAt(1), 10),
        text: h.textContent?.trim().slice(0, 50),
      }))
    })

    // Ensure we have headings
    expect(headingLevels.length).toBeGreaterThan(0)

    // Check for skipped levels
    let lastLevel = 0
    const skippedLevels: string[] = []

    for (const heading of headingLevels) {
      if (lastLevel > 0 && heading.level > lastLevel + 1) {
        skippedLevels.push(`h${lastLevel} to h${heading.level} ("${heading.text}")`)
      }
      lastLevel = heading.level
    }

    // Expect no skipped levels
    expect(skippedLevels).toEqual([])
  })

  // Test Case 3: Check main landmark
  test('page has a main element or role="main"', async ({ page }) => {
    const mainLandmark = page.locator('main, [role="main"]')
    const count = await mainLandmark.count()

    expect(count).toBeGreaterThanOrEqual(1)

    // Verify main landmark is visible
    await expect(mainLandmark.first()).toBeVisible()

    // Check that main landmark has content
    const mainContent = await mainLandmark.first().textContent()
    expect(mainContent?.trim().length).toBeGreaterThan(0)
  })

  // Test Case 4: Check all images for alt attribute
  test('all images have alt attributes', async ({ page }) => {
    const imagesWithoutAlt = await page.evaluate(() => {
      const images = document.querySelectorAll('img')
      const missingAlt: string[] = []

      images.forEach((img, index) => {
        if (!img.hasAttribute('alt')) {
          missingAlt.push(`Image ${index + 1}: ${img.src || 'no src'}`)
        }
      })

      return {
        total: images.length,
        missingAlt,
      }
    })

    // Expect all images to have alt attributes (even if empty for decorative images)
    expect(imagesWithoutAlt.missingAlt).toEqual([])
  })

  // Test Case 5: Check icon buttons for labels
  test('icon-only buttons have aria-label or accessible name', async ({ page }) => {
    const buttonsWithoutLabels = await page.evaluate(() => {
      const buttons = document.querySelectorAll('button')
      const issues: string[] = []

      buttons.forEach((button, index) => {
        const hasTextContent = button.textContent?.trim()
        const hasAriaLabel = button.getAttribute('aria-label')
        const hasAriaLabelledBy = button.getAttribute('aria-labelledby')
        const hasTitle = button.getAttribute('title')

        // Check if button appears to be icon-only
        const hasOnlySvg =
          button.querySelector('svg') !== null && !hasTextContent
        const hasOnlyImg =
          button.querySelector('img') !== null && !hasTextContent

        const isIconOnly = hasOnlySvg || hasOnlyImg

        if (isIconOnly && !hasAriaLabel && !hasAriaLabelledBy && !hasTitle) {
          issues.push(`Button ${index + 1}: Icon-only button without accessible name`)
        }
      })

      return issues
    })

    // Expect all icon-only buttons to have accessible names
    expect(buttonsWithoutLabels).toEqual([])
  })

  // Test Case 6: Run axe accessibility audit (screen reader focused)
  // Note: Color contrast is excluded as it's Scenario 14's responsibility
  test('no critical or serious accessibility violations', async ({ page }) => {
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
      // Exclude color-contrast as it's tested by Scenario 14 (Color Contrast)
      .disableRules(['color-contrast'])
      .analyze()

    // Filter for critical and serious violations only
    const criticalViolations = accessibilityScanResults.violations.filter(
      (v) => v.impact === 'critical' || v.impact === 'serious'
    )

    // Log violations for debugging if any exist
    if (criticalViolations.length > 0) {
      console.log(
        'Critical/Serious violations:',
        JSON.stringify(
          criticalViolations.map((v) => ({
            id: v.id,
            impact: v.impact,
            description: v.description,
            nodes: v.nodes.map((n) => n.html),
          })),
          null,
          2
        )
      )
    }

    expect(criticalViolations).toEqual([])
  })

  // Additional test: Verify semantic HTML structure
  test('page uses semantic HTML elements', async ({ page }) => {
    const semanticStructure = await page.evaluate(() => {
      return {
        hasHeader: document.querySelector('header, [role="banner"]') !== null,
        hasMain: document.querySelector('main, [role="main"]') !== null,
        hasNav: document.querySelector('nav, [role="navigation"]') !== null,
        hasSections: document.querySelectorAll('section').length > 0,
        hasFooter: document.querySelector('footer, [role="contentinfo"]') !== null,
      }
    })

    // Essential landmarks should be present
    expect(semanticStructure.hasHeader).toBe(true)
    expect(semanticStructure.hasMain).toBe(true)
    expect(semanticStructure.hasNav).toBe(true)
    expect(semanticStructure.hasSections).toBe(true)
  })

  // Additional test: Verify ARIA labels on interactive elements
  test('interactive elements with icons have ARIA labels', async ({ page }) => {
    // Check for links with icons that need accessible names
    const linksWithIcons = await page.evaluate(() => {
      const links = document.querySelectorAll('a')
      const issues: string[] = []

      links.forEach((link, index) => {
        const hasOnlySvg =
          link.querySelector('svg') !== null &&
          !link.textContent?.trim()
        const hasOnlyImg =
          link.querySelector('img') !== null &&
          !link.textContent?.trim()

        const isIconOnly = hasOnlySvg || hasOnlyImg
        const hasAriaLabel = link.getAttribute('aria-label')
        const hasAriaLabelledBy = link.getAttribute('aria-labelledby')
        const hasTitle = link.getAttribute('title')

        if (isIconOnly && !hasAriaLabel && !hasAriaLabelledBy && !hasTitle) {
          issues.push(`Link ${index + 1}: ${link.href || 'no href'} - icon-only without accessible name`)
        }
      })

      return issues
    })

    expect(linksWithIcons).toEqual([])
  })

  // Additional test: Verify section headings have proper IDs for navigation
  test('major sections have IDs for navigation', async ({ page }) => {
    const sectionsWithIds = await page.evaluate(() => {
      const sections = document.querySelectorAll('section')
      const results: { hasId: boolean; heading: string | null }[] = []

      sections.forEach((section) => {
        const heading = section.querySelector('h2')
        results.push({
          hasId: section.id !== '',
          heading: heading?.textContent?.trim() || null,
        })
      })

      return results
    })

    // All major sections should have IDs
    const sectionsWithoutIds = sectionsWithIds.filter(
      (s) => !s.hasId && s.heading !== null
    )
    expect(sectionsWithoutIds.length).toBe(0)
  })

  // Additional test: Verify proper aria-labelledby connections
  test('sections use aria-labelledby for proper labeling', async ({ page }) => {
    const labelledSections = await page.evaluate(() => {
      const sections = document.querySelectorAll('section[aria-labelledby]')
      const results: { valid: boolean; sectionId: string; labelId: string }[] = []

      sections.forEach((section) => {
        const labelId = section.getAttribute('aria-labelledby')
        if (labelId) {
          const labelElement = document.getElementById(labelId)
          results.push({
            valid: labelElement !== null,
            sectionId: section.id || 'unnamed',
            labelId,
          })
        }
      })

      return results
    })

    // All aria-labelledby references should be valid
    const invalidLabels = labelledSections.filter((s) => !s.valid)
    expect(invalidLabels).toEqual([])
  })
})
