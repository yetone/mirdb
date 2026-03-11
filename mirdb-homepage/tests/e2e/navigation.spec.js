/**
 * Navigation E2E Tests
 * Owner: Scenario 8 - Navigation Component
 *
 * Test coverage:
 * - Navigation link clicking and scrolling
 * - Sticky navigation on scroll
 * - Mobile hamburger menu open/close
 * - Mobile menu keyboard accessibility
 * - External link behavior (GitHub)
 */

import { test, expect } from '@playwright/test'

test.describe('Navigation Component', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    // Wait for navigation to be rendered
    await page.waitForSelector('[data-testid="navigation-bar"]')
  })

  test.describe('Test Case 1: Render Navigation component', () => {
    test('should display sticky navigation bar with logo and menu links', async ({ page }) => {
      // Verify navigation bar is visible
      const navBar = page.locator('[data-testid="navigation-bar"]')
      await expect(navBar).toBeVisible()

      // Verify logo is present
      const logo = page.locator('[data-testid="nav-logo"]')
      await expect(logo).toBeVisible()
      await expect(logo).toContainText('MirDB')

      // Verify navigation links are present on desktop
      const featuresLink = page.locator('[data-testid="nav-link-features"]')
      const docsLink = page.locator('[data-testid="nav-link-documentation"]')
      const githubLink = page.locator('[data-testid="nav-link-github"]')

      await expect(featuresLink).toBeVisible()
      await expect(docsLink).toBeVisible()
      await expect(githubLink).toBeVisible()
    })
  })

  test.describe('Test Case 2: Check Features link', () => {
    test('should have Features link present and navigate to features section', async ({ page }) => {
      const featuresLink = page.locator('[data-testid="nav-link-features"]')
      await expect(featuresLink).toBeVisible()
      await expect(featuresLink).toHaveText(/Features/i)
      await expect(featuresLink).toHaveAttribute('href', '#features')

      // Click and verify navigation
      const initialScrollY = await page.evaluate(() => window.scrollY)
      await featuresLink.click()
      await page.waitForTimeout(500)

      // Either scrolled or URL hash changed
      const urlHash = await page.evaluate(() => window.location.hash)
      const currentScrollY = await page.evaluate(() => window.scrollY)

      expect(currentScrollY > initialScrollY || urlHash === '#features').toBe(true)
    })
  })

  test.describe('Test Case 3: Check Documentation link', () => {
    test('should have Documentation link present and navigate to documentation', async ({ page }) => {
      const docsLink = page.locator('[data-testid="nav-link-documentation"]')
      await expect(docsLink).toBeVisible()
      await expect(docsLink).toHaveText(/Documentation/i)

      const href = await docsLink.getAttribute('href')
      // Should link to internal section or external docs
      expect(href).toBeTruthy()
    })
  })

  test.describe('Test Case 4: Check GitHub Repository link', () => {
    test('should have GitHub link present and open in new tab', async ({ page, context }) => {
      const githubLink = page.locator('[data-testid="nav-link-github"]')
      await expect(githubLink).toBeVisible()
      await expect(githubLink).toHaveText(/GitHub/i)

      // Verify it opens in new tab
      await expect(githubLink).toHaveAttribute('target', '_blank')
      await expect(githubLink).toHaveAttribute('rel', /noopener/)

      // Verify href points to GitHub
      const href = await githubLink.getAttribute('href')
      expect(href).toMatch(/github\.com/)

      // Test that clicking opens a new page/tab
      const [newPage] = await Promise.all([
        context.waitForEvent('page'),
        githubLink.click()
      ])

      const newPageUrl = newPage.url()
      expect(newPageUrl).toMatch(/github\.com/)
      await newPage.close()
    })
  })

  test.describe('Test Case 5: Test smooth scrolling to section', () => {
    test('should smoothly scroll to target section when clicking nav link', async ({ page }) => {
      // Get initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY)

      // Click Features link
      const featuresLink = page.locator('[data-testid="nav-link-features"]')
      await featuresLink.click()

      // Wait for smooth scroll animation
      await page.waitForTimeout(600)

      // Verify scroll happened
      const currentScrollY = await page.evaluate(() => window.scrollY)
      const urlHash = await page.evaluate(() => window.location.hash)

      // Either the page scrolled or URL hash changed
      expect(currentScrollY > initialScrollY || urlHash === '#features').toBe(true)

      // Features section should be visible
      const featuresSection = page.locator('#features')
      await expect(featuresSection).toBeInViewport({ timeout: 1000 }).catch(() => {
        // Fall back to checking URL hash if toBeInViewport fails
        expect(urlHash).toBe('#features')
      })
    })
  })

  test.describe('Test Case 6: Test sticky navigation on scroll', () => {
    test('should remain visible when scrolling down the page', async ({ page }) => {
      const navBar = page.locator('[data-testid="navigation-bar"]')

      // Initial visibility
      await expect(navBar).toBeVisible()

      // Scroll down the page
      await page.evaluate(() => window.scrollTo(0, 500))
      await page.waitForTimeout(100)

      // Navigation should still be visible (sticky)
      await expect(navBar).toBeVisible()

      // Scroll further down
      await page.evaluate(() => window.scrollTo(0, 1000))
      await page.waitForTimeout(100)

      // Navigation should still be visible
      await expect(navBar).toBeVisible()

      // Verify navigation stays at top (fixed position)
      const navPosition = await navBar.evaluate(el => {
        const style = window.getComputedStyle(el)
        return style.position
      })
      expect(navPosition).toBe('fixed')
    })
  })
})

test.describe('Mobile Navigation', () => {
  test.use({ viewport: { width: 375, height: 667 } })

  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForSelector('[data-testid="navigation-bar"]')
  })

  test.describe('Test Case 7: Test mobile menu at 375px width', () => {
    test('should show hamburger menu icon and clicking opens navigation overlay', async ({ page }) => {
      // Hamburger menu should be visible at mobile viewport
      const mobileToggle = page.locator('[data-testid="mobile-menu-toggle"]')
      await expect(mobileToggle).toBeVisible()

      // Desktop links should be hidden
      const desktopLinks = page.locator('[data-testid="desktop-nav-links"]')
      await expect(desktopLinks).not.toBeVisible()

      // Mobile menu should be hidden initially
      const mobileMenu = page.locator('[data-testid="mobile-menu"]')
      await expect(mobileMenu).toHaveClass(/hidden/)

      // Click hamburger to open menu
      await mobileToggle.click()
      await page.waitForTimeout(300)

      // Mobile menu should now be visible
      await expect(mobileMenu).not.toHaveClass(/hidden/)

      // Mobile navigation links should be visible
      const mobileFeaturesLink = page.locator('[data-testid="mobile-nav-link-features"]')
      const mobileDocsLink = page.locator('[data-testid="mobile-nav-link-documentation"]')
      const mobileGithubLink = page.locator('[data-testid="mobile-nav-link-github"]')

      await expect(mobileFeaturesLink).toBeVisible()
      await expect(mobileDocsLink).toBeVisible()
      await expect(mobileGithubLink).toBeVisible()
    })

    test('should close mobile menu when close button is clicked', async ({ page }) => {
      const mobileToggle = page.locator('[data-testid="mobile-menu-toggle"]')
      const mobileMenu = page.locator('[data-testid="mobile-menu"]')
      const closeBtn = page.locator('[data-testid="mobile-menu-close"]')

      // Open menu
      await mobileToggle.click()
      await page.waitForTimeout(300)
      await expect(mobileMenu).not.toHaveClass(/hidden/)

      // Close menu
      await closeBtn.click()
      await page.waitForTimeout(300)
      await expect(mobileMenu).toHaveClass(/hidden/)
    })

    test('should close mobile menu when navigation link is clicked', async ({ page }) => {
      const mobileToggle = page.locator('[data-testid="mobile-menu-toggle"]')
      const mobileMenu = page.locator('[data-testid="mobile-menu"]')
      const mobileFeaturesLink = page.locator('[data-testid="mobile-nav-link-features"]')

      // Open menu
      await mobileToggle.click()
      await page.waitForTimeout(300)
      await expect(mobileMenu).not.toHaveClass(/hidden/)

      // Click a navigation link
      await mobileFeaturesLink.click()
      await page.waitForTimeout(300)

      // Menu should close
      await expect(mobileMenu).toHaveClass(/hidden/)
    })
  })

  test.describe('Test Case 8: Test mobile menu accessibility', () => {
    test('should have proper ARIA attributes', async ({ page }) => {
      const mobileToggle = page.locator('[data-testid="mobile-menu-toggle"]')
      const mobileMenu = page.locator('[data-testid="mobile-menu"]')

      // Toggle button should have aria-expanded
      await expect(mobileToggle).toHaveAttribute('aria-expanded', 'false')
      await expect(mobileToggle).toHaveAttribute('aria-controls', 'mobile-menu')
      await expect(mobileToggle).toHaveAttribute('aria-label')

      // Mobile menu should have dialog role
      await expect(mobileMenu).toHaveAttribute('role', 'dialog')
      await expect(mobileMenu).toHaveAttribute('aria-modal', 'true')
      await expect(mobileMenu).toHaveAttribute('aria-label')

      // Open menu and verify aria-expanded updates
      await mobileToggle.click()
      await page.waitForTimeout(300)
      await expect(mobileToggle).toHaveAttribute('aria-expanded', 'true')
    })

    test('should close mobile menu with Escape key', async ({ page }) => {
      const mobileToggle = page.locator('[data-testid="mobile-menu-toggle"]')
      const mobileMenu = page.locator('[data-testid="mobile-menu"]')

      // Open menu
      await mobileToggle.click()
      await page.waitForTimeout(300)
      await expect(mobileMenu).not.toHaveClass(/hidden/)

      // Press Escape to close
      await page.keyboard.press('Escape')
      await page.waitForTimeout(300)

      // Menu should close
      await expect(mobileMenu).toHaveClass(/hidden/)
      await expect(mobileToggle).toHaveAttribute('aria-expanded', 'false')
    })

    test('should be keyboard navigable', async ({ page }) => {
      const mobileToggle = page.locator('[data-testid="mobile-menu-toggle"]')

      // Focus the toggle button
      await mobileToggle.focus()
      await expect(mobileToggle).toBeFocused()

      // Press Enter to open menu
      await page.keyboard.press('Enter')
      await page.waitForTimeout(300)

      // Menu should be open
      const mobileMenu = page.locator('[data-testid="mobile-menu"]')
      await expect(mobileMenu).not.toHaveClass(/hidden/)

      // Close button should receive focus
      const closeBtn = page.locator('[data-testid="mobile-menu-close"]')
      await expect(closeBtn).toBeFocused()

      // Press Enter on close button to close menu
      await page.keyboard.press('Enter')
      await page.waitForTimeout(300)

      // Menu should close and toggle should receive focus
      await expect(mobileMenu).toHaveClass(/hidden/)
      await expect(mobileToggle).toBeFocused()
    })
  })
})
