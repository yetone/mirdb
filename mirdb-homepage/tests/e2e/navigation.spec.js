/**
 * Navigation E2E Tests
 * Owner: Scenario 8 - Navigation Component
 *
 * Test coverage:
 * - Navigation link clicking and smooth scrolling
 * - Sticky navigation on scroll
 * - Mobile hamburger menu open/close
 * - Mobile menu keyboard accessibility
 * - External link behavior (GitHub)
 */

import { test, expect } from '@playwright/test'

test.describe('Navigation Component', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    // Wait for the Navigation component to be rendered
    await page.waitForSelector('[data-testid="navigation"]')
  })

  test('Test Case 1: Render Navigation component - sticky bar with logo and links', async ({ page }) => {
    // Verify navigation is visible
    const nav = page.locator('[data-testid="navigation"]')
    await expect(nav).toBeVisible()

    // Verify logo
    const logo = page.locator('[data-testid="nav-logo"]')
    await expect(logo).toBeVisible()
    await expect(logo).toContainText('MirDB')

    // Verify desktop links are visible on wide viewport
    await page.setViewportSize({ width: 1200, height: 800 })
    const desktopLinks = page.locator('[data-testid="nav-desktop-links"]')
    await expect(desktopLinks).toBeVisible()
  })

  test('Test Case 2: Features link navigates to features section', async ({ page }) => {
    await page.setViewportSize({ width: 1200, height: 800 })

    // Use desktop container to avoid finding mobile links too
    const desktopLinks = page.locator('[data-testid="nav-desktop-links"]')
    const featuresLink = desktopLinks.locator('[data-testid="nav-link-features"]')
    await expect(featuresLink).toBeVisible()
    await expect(featuresLink).toHaveAttribute('href', '#features')

    // Click the features link
    await featuresLink.click()

    // Wait for scroll animation
    await page.waitForTimeout(500)

    // Verify URL hash changed
    const hash = await page.evaluate(() => window.location.hash)
    expect(hash).toBe('#features')

    // Verify features section is in view
    const featuresSection = page.locator('#features')
    await expect(featuresSection).toBeInViewport()
  })

  test('Test Case 3: Documentation/Quick Start link navigates correctly', async ({ page }) => {
    await page.setViewportSize({ width: 1200, height: 800 })

    // Use desktop container to avoid finding mobile links too
    const desktopLinks = page.locator('[data-testid="nav-desktop-links"]')
    const quickStartLink = desktopLinks.locator('[data-testid="nav-link-quick-start"]')
    await expect(quickStartLink).toBeVisible()
    await expect(quickStartLink).toHaveAttribute('href', '#quickstart')

    // Click the quick start link
    await quickStartLink.click()

    // Wait for scroll
    await page.waitForTimeout(500)

    // Verify hash
    const hash = await page.evaluate(() => window.location.hash)
    expect(hash).toBe('#quickstart')
  })

  test('Test Case 4: GitHub link opens repository in new tab', async ({ page, context }) => {
    await page.setViewportSize({ width: 1200, height: 800 })

    // Use desktop container to avoid finding mobile links too
    const desktopLinks = page.locator('[data-testid="nav-desktop-links"]')
    const githubLink = desktopLinks.locator('[data-testid="nav-link-github"]')
    await expect(githubLink).toBeVisible()
    await expect(githubLink).toHaveText(/GitHub/i)

    // Verify link attributes for new tab
    await expect(githubLink).toHaveAttribute('target', '_blank')
    await expect(githubLink).toHaveAttribute('rel', /noopener/)

    // Verify href points to GitHub
    const href = await githubLink.getAttribute('href')
    expect(href).toContain('github.com')

    // Test that clicking opens new page
    const [newPage] = await Promise.all([
      context.waitForEvent('page'),
      githubLink.click()
    ])

    const newPageUrl = newPage.url()
    expect(newPageUrl).toContain('github.com')
    await newPage.close()
  })

  test('Test Case 5: Smooth scrolling to section', async ({ page }) => {
    await page.setViewportSize({ width: 1200, height: 800 })

    // Get initial scroll position
    const initialScroll = await page.evaluate(() => window.scrollY)

    // Use desktop container to avoid finding mobile links too
    const desktopLinks = page.locator('[data-testid="nav-desktop-links"]')
    const demoLink = desktopLinks.locator('[data-testid="nav-link-demo"]')
    await demoLink.click()

    // Wait for smooth scroll animation
    await page.waitForTimeout(500)

    // Verify scroll happened
    const finalScroll = await page.evaluate(() => window.scrollY)
    expect(finalScroll).toBeGreaterThan(initialScroll)

    // Verify target section is visible
    const demoSection = page.locator('#demo')
    await expect(demoSection).toBeInViewport()
  })

  test('Test Case 6: Sticky navigation on scroll', async ({ page }) => {
    await page.setViewportSize({ width: 1200, height: 800 })

    const nav = page.locator('[data-testid="navigation"]')

    // Verify nav is fixed
    const position = await nav.evaluate(el => window.getComputedStyle(el).position)
    expect(position).toBe('fixed')

    // Scroll down the page
    await page.evaluate(() => window.scrollTo(0, 500))
    await page.waitForTimeout(100)

    // Verify nav is still visible
    await expect(nav).toBeVisible()

    // Verify nav stays at top
    const navTop = await nav.evaluate(el => el.getBoundingClientRect().top)
    expect(navTop).toBe(0)

    // Scroll even more
    await page.evaluate(() => window.scrollTo(0, 1500))
    await page.waitForTimeout(100)

    // Nav should still be at top
    await expect(nav).toBeVisible()
    const navTopAfterMoreScroll = await nav.evaluate(el => el.getBoundingClientRect().top)
    expect(navTopAfterMoreScroll).toBe(0)
  })

  test('Test Case 7: Mobile menu at 375px width', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 })

    // Hamburger menu should be visible
    const toggleBtn = page.locator('[data-testid="mobile-menu-toggle"]')
    await expect(toggleBtn).toBeVisible()

    // Desktop links should be hidden
    const desktopLinks = page.locator('[data-testid="nav-desktop-links"]')
    await expect(desktopLinks).toBeHidden()

    // Mobile menu should be hidden initially
    const mobileMenu = page.locator('[data-testid="mobile-menu"]')
    await expect(mobileMenu).toHaveAttribute('aria-hidden', 'true')

    // Click hamburger to open menu
    await toggleBtn.click()

    // Menu should now be visible
    await expect(mobileMenu).toHaveAttribute('aria-hidden', 'false')

    // Verify navigation links are visible in mobile menu
    const mobileLinks = mobileMenu.locator('a')
    const count = await mobileLinks.count()
    expect(count).toBeGreaterThan(0)

    // Close button should be visible
    const closeBtn = page.locator('[data-testid="mobile-menu-close"]')
    await expect(closeBtn).toBeVisible()

    // Click close button
    await closeBtn.click()

    // Menu should be hidden again
    await expect(mobileMenu).toHaveAttribute('aria-hidden', 'true')
  })

  test('Test Case 8: Mobile menu accessibility', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 })

    const toggleBtn = page.locator('[data-testid="mobile-menu-toggle"]')
    const mobileMenu = page.locator('[data-testid="mobile-menu"]')
    const closeBtn = page.locator('[data-testid="mobile-menu-close"]')

    // Verify ARIA attributes on toggle button
    await expect(toggleBtn).toHaveAttribute('aria-label', 'Open menu')
    await expect(toggleBtn).toHaveAttribute('aria-expanded', 'false')
    await expect(toggleBtn).toHaveAttribute('aria-controls', 'mobile-menu')

    // Verify menu ARIA attributes
    await expect(mobileMenu).toHaveAttribute('role', 'dialog')
    await expect(mobileMenu).toHaveAttribute('aria-modal', 'true')
    await expect(mobileMenu).toHaveAttribute('aria-label', 'Navigation menu')

    // Open menu with keyboard
    await toggleBtn.focus()
    await page.keyboard.press('Enter')

    // Menu should open
    await expect(mobileMenu).toHaveAttribute('aria-hidden', 'false')
    await expect(toggleBtn).toHaveAttribute('aria-expanded', 'true')

    // Close menu with Escape key
    await page.keyboard.press('Escape')

    // Menu should close
    await expect(mobileMenu).toHaveAttribute('aria-hidden', 'true')
    await expect(toggleBtn).toHaveAttribute('aria-expanded', 'false')

    // Open menu again
    await toggleBtn.click()
    await expect(mobileMenu).toHaveAttribute('aria-hidden', 'false')

    // Close with close button via keyboard
    await closeBtn.focus()
    await page.keyboard.press('Enter')

    await expect(mobileMenu).toHaveAttribute('aria-hidden', 'true')
  })

  test('Mobile menu closes when clicking nav link', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 })

    const toggleBtn = page.locator('[data-testid="mobile-menu-toggle"]')
    const mobileMenu = page.locator('[data-testid="mobile-menu"]')

    // Open menu
    await toggleBtn.click()
    await expect(mobileMenu).toHaveAttribute('aria-hidden', 'false')

    // Click a navigation link in mobile menu
    const featuresLinkMobile = mobileMenu.locator('[data-testid="nav-link-features"]')
    await featuresLinkMobile.click()

    // Menu should close
    await expect(mobileMenu).toHaveAttribute('aria-hidden', 'true')

    // Wait for scroll
    await page.waitForTimeout(500)

    // Verify scrolled to section
    const hash = await page.evaluate(() => window.location.hash)
    expect(hash).toBe('#features')
  })
})

test.describe('Navigation responsive behavior', () => {
  test('should show desktop links on wide viewport', async ({ page }) => {
    await page.goto('/')
    await page.setViewportSize({ width: 1200, height: 800 })

    const desktopLinks = page.locator('[data-testid="nav-desktop-links"]')
    const mobileToggle = page.locator('[data-testid="mobile-menu-toggle"]')

    await expect(desktopLinks).toBeVisible()
    await expect(mobileToggle).toBeHidden()
  })

  test('should show mobile toggle on narrow viewport', async ({ page }) => {
    await page.goto('/')
    await page.setViewportSize({ width: 600, height: 800 })

    const desktopLinks = page.locator('[data-testid="nav-desktop-links"]')
    const mobileToggle = page.locator('[data-testid="mobile-menu-toggle"]')

    await expect(desktopLinks).toBeHidden()
    await expect(mobileToggle).toBeVisible()
  })
})
