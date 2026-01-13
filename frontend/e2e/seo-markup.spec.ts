import { test, expect } from '@playwright/test'

/**
 * E2E SEO Markup Tests for Homepage
 * Validates NFR-4: SEO-friendly markup with appropriate meta tags
 * Scenario: SEO Markup
 */

test.describe('SEO Markup E2E Tests - NFR-4 Compliance', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')
  })

  /**
   * Test Case 1: Query for page title meta tag
   * Input: Query for page title meta tag
   * Expected: Page has a descriptive title tag
   */
  test.describe('Test Case 1: Page Title Tag', () => {
    test('should have a descriptive title tag', async ({ page }) => {
      const title = await page.title()

      // Title should be present and descriptive
      expect(title).toBeTruthy()
      expect(title.length).toBeGreaterThan(0)
    })

    test('should have a title that describes the service', async ({ page }) => {
      const title = await page.title()

      // Title should contain relevant keywords for a URL shortener service
      expect(title.toLowerCase()).toMatch(/url|shortener|link/i)
    })

    test('should have a title with appropriate length for SEO (under 70 characters)', async ({
      page,
    }) => {
      const title = await page.title()

      // SEO best practice: title should be under 70 characters
      expect(title.length).toBeLessThanOrEqual(70)
      expect(title.length).toBeGreaterThan(10)
    })
  })

  /**
   * Test Case 2: Query for meta description tag
   * Input: Query for meta description tag
   * Expected: Page has a meta description with relevant content
   */
  test.describe('Test Case 2: Meta Description Tag', () => {
    test('should have a meta description tag present', async ({ page }) => {
      const metaDescription = await page.locator('meta[name="description"]')
      await expect(metaDescription).toHaveCount(1)
    })

    test('should have a meta description with relevant content', async ({
      page,
    }) => {
      const metaDescription = page.locator('meta[name="description"]')
      const content = await metaDescription.getAttribute('content')

      expect(content).toBeTruthy()
      expect(content!.length).toBeGreaterThan(0)

      // Meta description should mention URL shortening or link management
      expect(content!.toLowerCase()).toMatch(/url|shorten|link|analytics/i)
    })

    test('should have a meta description with appropriate length for SEO', async ({
      page,
    }) => {
      const metaDescription = page.locator('meta[name="description"]')
      const content = await metaDescription.getAttribute('content')

      // SEO best practice: meta description should be 50-160 characters
      expect(content!.length).toBeLessThanOrEqual(160)
      expect(content!.length).toBeGreaterThan(50)
    })
  })

  /**
   * Test Case 3: Query for h1 heading on page
   * Input: Query for h1 heading on page
   * Expected: Page has exactly one h1 heading with relevant content
   */
  test.describe('Test Case 3: H1 Heading', () => {
    test('should have exactly one h1 heading on the page', async ({ page }) => {
      const h1Elements = await page.locator('h1').all()
      expect(h1Elements.length).toBe(1)
    })

    test('should have an h1 heading with relevant content', async ({ page }) => {
      const h1 = page.locator('h1')
      await expect(h1).toBeVisible()

      const text = await h1.textContent()
      expect(text).toBeTruthy()
      expect(text!.length).toBeGreaterThan(0)
    })

    test('should have an h1 heading that communicates the product value', async ({
      page,
    }) => {
      const h1 = page.locator('h1')
      const text = await h1.textContent()

      // H1 should relate to the URL shortening service's value proposition
      expect(text!.toLowerCase()).toMatch(/shorten|track|share|url|link/i)
    })
  })

  /**
   * Test Case 4: Check heading hierarchy
   * Input: Check heading hierarchy
   * Expected: Headings follow proper hierarchy (h1 > h2 > h3) without skipping levels
   */
  test.describe('Test Case 4: Heading Hierarchy', () => {
    test('should have headings in proper hierarchical order starting with h1', async ({
      page,
    }) => {
      // Get all headings
      const headings = await page.locator('h1, h2, h3, h4, h5, h6').all()

      expect(headings.length).toBeGreaterThan(0)

      // First heading should be h1
      const firstHeadingTag = await headings[0].evaluate((el) => el.tagName)
      expect(firstHeadingTag).toBe('H1')
    })

    test('should not skip heading levels', async ({ page }) => {
      const headings = await page.locator('h1, h2, h3, h4, h5, h6').all()
      const headingLevels: number[] = []

      for (const heading of headings) {
        const tagName = await heading.evaluate((el) => el.tagName)
        const level = parseInt(tagName.charAt(1))
        headingLevels.push(level)
      }

      // Check that heading levels don't skip more than 1 level going deeper
      let minLevelSeen = 1

      for (let i = 1; i < headingLevels.length; i++) {
        const currentLevel = headingLevels[i]

        // If going deeper, should not skip levels
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

    test('should have proper section structure with h2 headings', async ({
      page,
    }) => {
      const h2Elements = await page.locator('h2').all()

      // Homepage should have multiple sections with h2 headings
      expect(h2Elements.length).toBeGreaterThanOrEqual(3)

      // Verify h2 headings have meaningful content
      for (const h2 of h2Elements) {
        const text = await h2.textContent()
        expect(text).toBeTruthy()
        expect(text!.length).toBeGreaterThan(0)
      }
    })

    test('should have h3 headings only after h2 headings have been established', async ({
      page,
    }) => {
      const headings = await page.locator('h1, h2, h3').all()
      let h2Found = false
      let h3BeforeH2 = false

      for (const heading of headings) {
        const tagName = await heading.evaluate((el) => el.tagName)
        if (tagName === 'H2') {
          h2Found = true
        }
        if (tagName === 'H3' && !h2Found) {
          h3BeforeH2 = true
        }
      }

      // H3 should not appear before any H2
      expect(h3BeforeH2).toBe(false)
    })

    test('should have feature cards using h3 headings', async ({ page }) => {
      const h3Elements = await page.locator('h3').all()

      // Features section should have h3 headings for individual features
      expect(h3Elements.length).toBeGreaterThanOrEqual(4)
    })
  })
})

test.describe('Additional SEO Requirements', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')
  })

  test('should have viewport meta tag for mobile responsiveness', async ({
    page,
  }) => {
    const viewportMeta = page.locator('meta[name="viewport"]')
    await expect(viewportMeta).toHaveCount(1)

    const content = await viewportMeta.getAttribute('content')
    expect(content).toContain('width=device-width')
  })

  test('should have charset meta tag', async ({ page }) => {
    const charsetMeta = page.locator('meta[charset]')
    await expect(charsetMeta).toHaveCount(1)

    const charset = await charsetMeta.getAttribute('charset')
    expect(charset?.toLowerCase()).toBe('utf-8')
  })

  test('should have html lang attribute for accessibility and SEO', async ({
    page,
  }) => {
    const htmlLang = await page.evaluate(() =>
      document.documentElement.getAttribute('lang')
    )
    expect(htmlLang).toBeTruthy()
    expect(htmlLang).toBe('en')
  })

  test('should have semantic HTML structure', async ({ page }) => {
    // Navigation landmark
    const nav = await page.locator('nav').count()
    expect(nav).toBeGreaterThanOrEqual(1)

    // Footer landmark
    const footer = await page.locator('footer').count()
    expect(footer).toBe(1)

    // Section elements
    const sections = await page.locator('section').count()
    expect(sections).toBeGreaterThanOrEqual(3)
  })
})
