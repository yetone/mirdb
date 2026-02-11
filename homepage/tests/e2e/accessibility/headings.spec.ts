/**
 * E2E Tests for Heading Hierarchy
 * Owner: Scenario 11 - Accessibility Compliance
 *
 * Test Case 6: Headings follow logical order (h1 > h2 > h3), single h1 per page
 */

import { test, expect } from '@playwright/test'

test.describe('Test Case 6: Heading Hierarchy', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')
  })

  test('page has exactly one h1 element', async ({ page }) => {
    const h1Elements = await page.$$('h1')
    expect(h1Elements.length).toBe(1)
  })

  test('h1 is the first heading on the page', async ({ page }) => {
    const firstHeading = await page.evaluate(() => {
      const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6')
      if (headings.length === 0) return null
      return headings[0].tagName.toLowerCase()
    })

    expect(firstHeading).toBe('h1')
  })

  test('heading hierarchy is logical (no skipping levels)', async ({ page }) => {
    const headingLevels = await page.evaluate(() => {
      const headings = Array.from(document.querySelectorAll('h1, h2, h3, h4, h5, h6'))
      return headings.map((h) => ({
        level: parseInt(h.tagName.replace('H', '')),
        text: h.textContent?.trim().substring(0, 50) || '',
      }))
    })

    // Check that we don't skip heading levels
    let previousLevel = 0

    for (const heading of headingLevels) {
      // We can go from any level to h1, or to the next level, or same level, or back up
      // But we shouldn't skip down (e.g., h1 to h3 without h2)
      if (previousLevel === 0) {
        expect(heading.level).toBe(1)
      } else {
        // Going deeper shouldn't skip more than one level
        const isValidProgression =
          heading.level <= previousLevel + 1 || // Going same or one level deeper
          heading.level <= previousLevel // Going up or staying same

        expect(
          isValidProgression,
          `Invalid heading progression: h${previousLevel} to h${heading.level} ("${heading.text}")`
        ).toBe(true)
      }

      previousLevel = heading.level
    }
  })

  test('all headings have meaningful content', async ({ page }) => {
    const headings = await page.$$('h1, h2, h3, h4, h5, h6')

    for (const heading of headings) {
      const text = await heading.textContent()
      const trimmedText = text?.trim() || ''

      expect(trimmedText.length, 'Heading should not be empty').toBeGreaterThan(0)
    }
  })

  test('h2 elements are used for main section headings', async ({ page }) => {
    // Verify sections have h2 headings
    const sectionsWithH2 = await page.evaluate(() => {
      const sections = document.querySelectorAll('section')
      let count = 0
      sections.forEach((section) => {
        if (section.querySelector('h2')) {
          count++
        }
      })
      return count
    })

    // At least some sections should have h2 headings
    expect(sectionsWithH2).toBeGreaterThan(0)
  })
})
