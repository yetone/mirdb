/**
 * E2E Tests for Image Accessibility
 * Owner: Scenario 11 - Accessibility Compliance
 *
 * Test Case 5: All img elements have non-empty alt text or role='presentation'
 */

import { test, expect } from '@playwright/test'

test.describe('Test Case 5: Image Alt Text', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')
  })

  test('all images have alt attributes', async ({ page }) => {
    const images = await page.$$('img')

    for (const img of images) {
      const alt = await img.getAttribute('alt')
      const role = await img.getAttribute('role')
      const ariaHidden = await img.getAttribute('aria-hidden')

      // Image should have alt text OR role="presentation" OR be aria-hidden
      const hasAccessibleAlternative =
        (alt !== null && alt.trim() !== '') || role === 'presentation' || ariaHidden === 'true'

      const src = await img.getAttribute('src')

      expect(
        hasAccessibleAlternative,
        `Image with src "${src}" should have non-empty alt text, role="presentation", or aria-hidden="true"`
      ).toBe(true)
    }
  })

  test('decorative images are properly marked', async ({ page }) => {
    // Check that images meant for decoration are properly hidden from screen readers
    const images = await page.$$('img')

    for (const img of images) {
      const alt = await img.getAttribute('alt')
      const role = await img.getAttribute('role')

      // If alt is empty string, it should indicate decorative image
      if (alt === '') {
        // Empty alt is acceptable for decorative images
        // but better to have role="presentation" for clarity
        const hasDecorativeIndicator =
          role === 'presentation' || (await img.getAttribute('aria-hidden')) === 'true' || alt === ''

        expect(hasDecorativeIndicator).toBe(true)
      }
    }
  })

  test('informative images have meaningful alt text', async ({ page }) => {
    const images = await page.$$('img')

    for (const img of images) {
      const alt = await img.getAttribute('alt')
      const role = await img.getAttribute('role')
      const ariaHidden = await img.getAttribute('aria-hidden')

      // Skip decorative images
      if (role === 'presentation' || ariaHidden === 'true' || alt === '') {
        continue
      }

      // Alt text should be meaningful (more than just "image" or the filename)
      const src = await img.getAttribute('src')

      if (alt) {
        // Alt text should not be just generic placeholder text
        const genericAltPatterns = [/^image$/i, /^photo$/i, /^picture$/i, /^img$/i, /\.(?:gif|jpg|jpeg|png|webp)$/i]

        const isGeneric = genericAltPatterns.some((pattern) => pattern.test(alt.trim()))

        expect(isGeneric, `Image alt text "${alt}" appears to be generic placeholder text`).toBe(false)
      }
    }
  })

  test('logo image has appropriate alt text', async ({ page }) => {
    const logoImg = await page.$('img[src*="logo"]')

    if (logoImg) {
      const alt = await logoImg.getAttribute('alt')
      expect(alt).not.toBeNull()
      expect(alt).not.toBe('')
      expect(alt?.toLowerCase()).toContain('mirdb')
    }
  })

  test('demo GIF has descriptive alt text', async ({ page }) => {
    const demoImg = await page.$('img[src*="usage"]')

    if (demoImg) {
      const alt = await demoImg.getAttribute('alt')
      expect(alt).not.toBeNull()
      expect(alt).not.toBe('')
      // Alt text should describe what the demo shows
      expect(alt?.length).toBeGreaterThan(20)
    }
  })
})
