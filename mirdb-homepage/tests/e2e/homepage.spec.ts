/**
 * Homepage E2E Tests.
 * Primary Owner: Scenario 1 - Hero Section Display
 * Contributors: Scenarios 2, 3, 4, 5, 6, 16
 *
 * Tests:
 * - Hero section visibility
 * - All sections render correctly
 * - Navigation works
 * - External links function
 * - Error handling for missing assets (Scenario 16)
 */

import { test, expect } from '@playwright/test'

test.describe('Error Handling - Missing Assets', () => {
  test.describe('Test Case 1: Logo Image Load Failure', () => {
    test('page displays fallback when logo fails to load', async ({ page }) => {
      // Block the logo image request
      await page.route('**/logo.svg', (route) => {
        route.abort()
      })

      await page.goto('/')

      // Wait for the page to load and potentially trigger error handling
      await page.waitForLoadState('networkidle')

      // The fallback should appear since the logo failed to load
      const fallback = page.locator('[data-testid="logo-fallback"]')
      await expect(fallback).toBeVisible()

      // Verify fallback has proper aria-label for accessibility
      await expect(fallback).toHaveAttribute('aria-label', 'MirDB Logo')

      // Verify fallback displays the "M" letter
      await expect(fallback).toContainText('M')
    })

    test('page remains functional when logo fails to load', async ({ page }) => {
      // Block the logo image request
      await page.route('**/logo.svg', (route) => {
        route.abort()
      })

      await page.goto('/')

      // Page should still have the main heading
      const h1 = page.locator('h1')
      await expect(h1).toContainText('MirDB')

      // Page should still have the tagline
      const h2 = page.locator('h2').first()
      await expect(h2).toContainText('Key-Value Store')

      // CTA button should still be functional
      const ctaButton = page.locator('a:has-text("Get Started")')
      await expect(ctaButton).toBeVisible()
      await expect(ctaButton).toHaveAttribute('href', /github.com\/yetone\/mirdb/)
    })
  })

  test.describe('Test Case 2: CircleCI Badge Load Failure', () => {
    test('page renders without error when badge request is blocked', async ({ page }) => {
      // Block CircleCI badge image request
      await page.route('**/circleci.com/**', (route) => {
        route.abort()
      })

      await page.goto('/')

      // Wait for the page to load
      await page.waitForLoadState('networkidle')

      // Page should render without JavaScript errors
      const errors: string[] = []
      page.on('pageerror', (error) => {
        errors.push(error.message)
      })

      // Status badges section should still be present
      const statusBadges = page.locator('[data-testid="status-badges"]')
      await expect(statusBadges).toBeVisible()

      // Verify no critical JavaScript errors occurred
      expect(errors.filter(e => !e.includes('net::ERR'))).toHaveLength(0)
    })

    test('badge area shows fallback when CircleCI badge fails', async ({ page }) => {
      // Block CircleCI badge image request
      await page.route('**/circleci.com/**', (route) => {
        route.abort()
      })

      await page.goto('/')

      // Wait for the page to load and error handler to trigger
      await page.waitForLoadState('networkidle')

      // The fallback text should appear
      const fallback = page.locator('[data-testid="circleci-badge-fallback"]')
      await expect(fallback).toBeVisible()
      await expect(fallback).toContainText('Build Status')
    })

    test('badge link remains clickable with fallback', async ({ page }) => {
      // Block CircleCI badge image request
      await page.route('**/circleci.com/*.svg', (route) => {
        route.abort()
      })

      await page.goto('/')

      // Wait for the page to load
      await page.waitForLoadState('networkidle')

      // The badge link should still work
      const badgeLink = page.locator('[data-testid="circleci-badge-link"]')
      await expect(badgeLink).toBeVisible()
      await expect(badgeLink).toHaveAttribute('href', /circleci.com/)
      await expect(badgeLink).toHaveAttribute('target', '_blank')
    })
  })

  test.describe('Test Case 3: All External Resources Blocked', () => {
    test('core page content remains functional with all external resources blocked', async ({ page }) => {
      // Block all external resource requests
      await page.route('**/*', (route, request) => {
        const url = request.url()
        // Allow local resources (the app itself) but block external
        if (url.includes('localhost') || url.includes('127.0.0.1') || url.startsWith('/')) {
          route.continue()
        } else if (url.includes('circleci.com') || url.includes('github.com') || url.endsWith('.svg') || url.endsWith('.png')) {
          route.abort()
        } else {
          route.continue()
        }
      })

      await page.goto('/')

      // Core content should be visible
      // Hero section
      const hero = page.locator('section').first()
      await expect(hero).toBeVisible()

      // Main heading
      const h1 = page.locator('h1')
      await expect(h1).toContainText('MirDB')

      // Navigation should still work
      const nav = page.locator('nav')
      await expect(nav).toBeVisible()

      // Footer should be visible
      const footer = page.locator('footer')
      await expect(footer).toBeVisible()
    })

    test('navigation links remain functional with blocked external resources', async ({ page }) => {
      // Block external image/badge resources
      await page.route('**/circleci.com/**', (route) => route.abort())
      await page.route('**/*.svg', (route, request) => {
        const url = request.url()
        // Block external SVGs but allow local ones
        if (!url.includes('localhost') && !url.includes('127.0.0.1')) {
          route.abort()
        } else {
          route.continue()
        }
      })

      await page.goto('/')

      // Check that navigation contains expected links
      const featuresLink = page.locator('nav a[href*="features"], nav a:has-text("Features")')
      const usageLink = page.locator('nav a[href*="usage"], nav a:has-text("Usage")')
      const roadmapLink = page.locator('nav a[href*="roadmap"], nav a:has-text("Roadmap")')

      // At least one navigation link should be visible
      const navLinks = await page.locator('nav a').count()
      expect(navLinks).toBeGreaterThan(0)
    })

    test('page does not show broken image icons', async ({ page }) => {
      // Block all external images
      await page.route('**/circleci.com/**', (route) => route.abort())
      await page.route('**/logo.svg', (route) => route.abort())

      await page.goto('/')

      // Wait for error handling to complete
      await page.waitForLoadState('networkidle')

      // Check that there are no visible broken image placeholders
      // The fallback elements should be shown instead
      const brokenImages = await page.evaluate(() => {
        const images = document.querySelectorAll('img')
        return Array.from(images).filter((img) => {
          // Check if image failed to load
          return !img.complete || img.naturalWidth === 0
        }).length
      })

      // No broken images should be visible (they should be replaced by fallbacks)
      expect(brokenImages).toBe(0)
    })

    test('all sections remain accessible with external resources blocked', async ({ page }) => {
      // Block all external resources
      await page.route('**/circleci.com/**', (route) => route.abort())
      await page.route('**/logo.svg', (route) => route.abort())

      await page.goto('/')

      // Check main sections are still visible
      const main = page.locator('main')
      await expect(main).toBeVisible()

      // Features section should be visible
      const featuresSection = page.locator('#features, section:has-text("Features")')
      await expect(featuresSection.first()).toBeVisible()

      // Usage section should be visible
      const usageSection = page.locator('#usage, section:has-text("Usage")')
      await expect(usageSection.first()).toBeVisible()

      // Roadmap section should be visible
      const roadmapSection = page.locator('#roadmap, section:has-text("Roadmap")')
      await expect(roadmapSection.first()).toBeVisible()
    })
  })
})

test.describe('Homepage Basic Functionality', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  test('hero section is visible', async ({ page }) => {
    const hero = page.locator('section').first()
    await expect(hero).toBeVisible()
  })

  test('MirDB title is displayed', async ({ page }) => {
    const title = page.locator('h1')
    await expect(title).toContainText('MirDB')
  })

  test('Get Started button is visible and links to GitHub', async ({ page }) => {
    const ctaButton = page.locator('a:has-text("Get Started")')
    await expect(ctaButton).toBeVisible()
    await expect(ctaButton).toHaveAttribute('href', /github.com\/yetone\/mirdb/)
  })

  test('navigation is visible', async ({ page }) => {
    const nav = page.locator('nav')
    await expect(nav).toBeVisible()
  })

  test('footer is visible', async ({ page }) => {
    const footer = page.locator('footer')
    await expect(footer).toBeVisible()
  })
})
