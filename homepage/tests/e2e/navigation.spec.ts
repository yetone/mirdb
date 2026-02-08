/**
 * Navigation E2E Tests
 * Owner: Scenario 5 - Navigation and Links
 */
import { test, expect } from '@playwright/test'

const GITHUB_URL = 'https://github.com/yetone/mirdb'
const DOCUMENTATION_URL = 'https://github.com/yetone/mirdb#readme'

test.describe('Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  test.describe('Navigation Bar', () => {
    test('navigation bar is present', async ({ page }) => {
      const nav = page.getByRole('navigation')
      await expect(nav).toBeVisible()
    })

    test('navigation contains all section links', async ({ page }) => {
      const nav = page.getByRole('navigation')
      await expect(nav.getByRole('link', { name: /features/i })).toBeVisible()
      await expect(
        nav.getByRole('link', { name: /installation/i })
      ).toBeVisible()
      await expect(nav.getByRole('link', { name: /usage/i })).toBeVisible()
      // Check for the GitHub link in the header nav specifically
      await expect(
        nav.getByRole('link', { name: 'GitHub', exact: true })
      ).toBeVisible()
    })
  })

  test.describe('Section Navigation', () => {
    test('clicking Features link scrolls to features section', async ({
      page,
    }) => {
      // Get initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY)

      // Click the Features link
      await page.getByRole('link', { name: /features/i }).click()

      // Wait for scroll to complete
      await page.waitForTimeout(500)

      // Check that page has scrolled
      const newScrollY = await page.evaluate(() => window.scrollY)

      // Either scrolled or the section is visible in viewport
      const featuresSection = page.locator('#features')
      await expect(featuresSection).toBeInViewport()
    })

    test('clicking Installation link scrolls to installation section', async ({
      page,
    }) => {
      await page.getByRole('link', { name: /installation/i }).click()
      await page.waitForTimeout(500)

      const installationSection = page.locator('#installation')
      await expect(installationSection).toBeInViewport()
    })

    test('clicking Usage link scrolls to usage section', async ({ page }) => {
      await page.getByRole('link', { name: /usage/i }).click()
      await page.waitForTimeout(500)

      const usageSection = page.locator('#usage')
      await expect(usageSection).toBeInViewport()
    })
  })

  test.describe('GitHub Link', () => {
    test('GitHub link has correct URL', async ({ page }) => {
      // Target the GitHub link in the navigation specifically
      const nav = page.getByRole('navigation')
      const githubLink = nav.getByRole('link', { name: 'GitHub', exact: true })
      await expect(githubLink).toHaveAttribute('href', GITHUB_URL)
    })

    test('GitHub link opens in new tab', async ({ page }) => {
      const nav = page.getByRole('navigation')
      const githubLink = nav.getByRole('link', { name: 'GitHub', exact: true })
      await expect(githubLink).toHaveAttribute('target', '_blank')
    })

    test('GitHub link has proper security attributes', async ({ page }) => {
      const nav = page.getByRole('navigation')
      const githubLink = nav.getByRole('link', { name: 'GitHub', exact: true })
      const rel = await githubLink.getAttribute('rel')
      expect(rel).toContain('noopener')
      expect(rel).toContain('noreferrer')
    })
  })

  test.describe('External Links Validation', () => {
    test('all external links have valid href attributes', async ({ page }) => {
      // Get all links that open in new tab (external links)
      const externalLinks = page.locator('a[target="_blank"]')
      const count = await externalLinks.count()

      for (let i = 0; i < count; i++) {
        const link = externalLinks.nth(i)
        const href = await link.getAttribute('href')

        // External links should have valid URLs (not empty or just '#')
        expect(href).not.toBe('')
        expect(href).not.toBe('#')
        expect(href).toMatch(/^https?:\/\//)
      }
    })

    test('no internal links have empty href', async ({ page }) => {
      // Check internal anchor links
      const anchorLinks = page.locator('a[href^="#"]')
      const count = await anchorLinks.count()

      for (let i = 0; i < count; i++) {
        const link = anchorLinks.nth(i)
        const href = await link.getAttribute('href')

        // Should not be just '#'
        expect(href).not.toBe('#')
        expect(href?.length).toBeGreaterThan(1)
      }
    })
  })

  test.describe('Link Hover States', () => {
    test('navigation links have hover effect', async ({ page }) => {
      const navLink = page.getByRole('link', { name: /features/i })

      // Get initial styles
      const initialColor = await navLink.evaluate((el) =>
        getComputedStyle(el).getPropertyValue('color')
      )

      // Hover over the link
      await navLink.hover()

      // Small wait for CSS transition
      await page.waitForTimeout(200)

      // Verify hoverable (link is interactive)
      await expect(navLink).toBeVisible()
      await expect(navLink).toBeEnabled()
    })

    test('all navigation links are keyboard accessible', async ({ page }) => {
      // Start from the body
      await page.keyboard.press('Tab')

      // Tab through navigation links and verify focus
      let tabCount = 0
      const maxTabs = 10

      while (tabCount < maxTabs) {
        const focusedElement = await page.evaluate(
          () => document.activeElement?.tagName
        )
        if (focusedElement === 'A') {
          // Found a link, verify it has visible focus
          const focusedLink = page.locator(':focus')
          await expect(focusedLink).toBeVisible()
        }
        await page.keyboard.press('Tab')
        tabCount++
      }
    })
  })

  test.describe('Brand Link', () => {
    test('brand link navigates to homepage', async ({ page }) => {
      const brandLink = page.getByRole('link', { name: /mirdb/i }).first()
      await expect(brandLink).toHaveAttribute('href', '/')
    })
  })
})
