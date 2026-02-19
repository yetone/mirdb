/**
 * Navigation E2E Tests
 * Owner: Scenario 2 - Navigation & Header
 *
 * Test cases:
 * 2. Click 'Features' nav link - Page scrolls to #features section smoothly
 * 3. Click 'Quick Start' nav link - Page scrolls to #quick-start section smoothly
 * 4. Click 'Usage' nav link - Page scrolls to #usage section smoothly
 * 5. Set viewport to 375px width (mobile) - Hamburger menu icon visible, desktop nav hidden
 * 6. Click hamburger menu icon on mobile - Mobile navigation drawer slides open with all nav links
 * 7. Click nav link in mobile menu - Menu closes, page scrolls to target section
 * 8. Click outside mobile menu when open - Mobile menu closes
 * 9. Press Escape key when mobile menu open - Mobile menu closes and focus returns to hamburger button
 */

import { test, expect } from '@playwright/test'

test.describe('Navigation & Header', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    // Wait for header to be visible
    await page.waitForSelector('header[role="banner"]')
  })

  test('header renders with logo, nav links, and theme toggle slot', async ({ page }) => {
    // Test Case 1: Render Header component
    // Expected: Header renders with logo, nav links, and theme toggle

    // Check header is visible
    const header = page.locator('header[role="banner"]')
    await expect(header).toBeVisible()

    // Check logo is visible
    const logoText = page.locator('header').getByText('MirDB')
    await expect(logoText).toBeVisible()

    // Check navigation links are visible on desktop (links have role="menuitem")
    const featuresLink = page.locator('header nav').getByRole('menuitem', { name: /features/i })
    await expect(featuresLink).toBeVisible()

    const quickStartLink = page.locator('header nav').getByRole('menuitem', { name: /quick start/i })
    await expect(quickStartLink).toBeVisible()

    const usageLink = page.locator('header nav').getByRole('menuitem', { name: /usage/i })
    await expect(usageLink).toBeVisible()

    const githubLink = page.locator('header nav').getByRole('menuitem', { name: /github/i })
    await expect(githubLink).toBeVisible()

    // Check theme toggle slot exists
    const themeToggleSlot = page.getByTestId('theme-toggle-slot')
    await expect(themeToggleSlot).toBeAttached()
  })

  test("click 'Features' nav link scrolls to #features section smoothly", async ({ page }) => {
    // Test Case 2: Click 'Features' nav link
    // Expected: Page scrolls to #features section smoothly

    const featuresLink = page.locator('header nav').getByRole('menuitem', { name: /features/i })
    await featuresLink.click()

    // Wait for smooth scroll
    await page.waitForTimeout(1000)

    // Verify features section is in viewport
    const featuresSection = page.locator('#features')
    await expect(featuresSection).toBeInViewport()
  })

  test("click 'Quick Start' nav link scrolls to #quick-start section smoothly", async ({ page }) => {
    // Test Case 3: Click 'Quick Start' nav link
    // Expected: Page scrolls to #quick-start section smoothly

    const quickStartLink = page.locator('header nav').getByRole('menuitem', { name: /quick start/i })
    await quickStartLink.click()

    // Wait for smooth scroll
    await page.waitForTimeout(1000)

    // Verify quick-start section is in viewport
    const quickStartSection = page.locator('#quick-start')
    await expect(quickStartSection).toBeInViewport()
  })

  test("click 'Usage' nav link scrolls to #usage section smoothly", async ({ page }) => {
    // Test Case 4: Click 'Usage' nav link
    // Expected: Page scrolls to #usage section smoothly

    const usageLink = page.locator('header nav').getByRole('menuitem', { name: /usage/i })
    await usageLink.click()

    // Wait for smooth scroll
    await page.waitForTimeout(1000)

    // Verify usage section is in viewport
    const usageSection = page.locator('#usage')
    await expect(usageSection).toBeInViewport()
  })

  test('hamburger menu icon visible, desktop nav hidden on mobile viewport (375px)', async ({ page }) => {
    // Test Case 5: Set viewport to 375px width (mobile)
    // Expected: Hamburger menu icon visible, desktop nav hidden

    await page.setViewportSize({ width: 375, height: 667 })

    // Wait for layout to update
    await page.waitForTimeout(300)

    // Hamburger button should be visible
    const hamburgerButton = page.getByTestId('hamburger-button')
    await expect(hamburgerButton).toBeVisible()

    // Desktop navigation should be hidden
    const desktopNav = page.locator('header nav[aria-label="Main navigation"]')
    await expect(desktopNav).toBeHidden()
  })

  test('click hamburger menu icon opens mobile navigation drawer', async ({ page }) => {
    // Test Case 6: Click hamburger menu icon on mobile
    // Expected: Mobile navigation drawer slides open with all nav links

    await page.setViewportSize({ width: 375, height: 667 })
    await page.waitForTimeout(300)

    const hamburgerButton = page.getByTestId('hamburger-button')
    await hamburgerButton.click()

    // Wait for menu to open
    await page.waitForTimeout(400)

    // Mobile menu should be visible
    const mobileMenu = page.getByRole('dialog', { name: /mobile navigation menu/i })
    await expect(mobileMenu).toBeVisible()

    // All nav links should be in the mobile menu (links have role="menuitem")
    const mobileNav = mobileMenu.locator('nav')
    await expect(mobileNav.getByRole('menuitem', { name: /features/i })).toBeVisible()
    await expect(mobileNav.getByRole('menuitem', { name: /quick start/i })).toBeVisible()
    await expect(mobileNav.getByRole('menuitem', { name: /usage/i })).toBeVisible()
    await expect(mobileNav.getByRole('menuitem', { name: /github/i })).toBeVisible()
  })

  test('click nav link in mobile menu closes menu and scrolls to section', async ({ page }) => {
    // Test Case 7: Click nav link in mobile menu
    // Expected: Menu closes, page scrolls to target section

    await page.setViewportSize({ width: 375, height: 667 })
    await page.waitForTimeout(300)

    // Open mobile menu
    const hamburgerButton = page.getByTestId('hamburger-button')
    await hamburgerButton.click()
    await page.waitForTimeout(400)

    // Click Features link in mobile menu (links have role="menuitem")
    const mobileMenu = page.getByRole('dialog', { name: /mobile navigation menu/i })
    const featuresLink = mobileMenu.getByRole('menuitem', { name: /features/i })
    await featuresLink.click()

    // Wait for menu close animation and scroll
    await page.waitForTimeout(1000)

    // Menu should be closed (hidden)
    await expect(mobileMenu).toBeHidden()

    // Features section should be in viewport
    const featuresSection = page.locator('#features')
    await expect(featuresSection).toBeInViewport()
  })

  test('click outside mobile menu closes the menu', async ({ page }) => {
    // Test Case 8: Click outside mobile menu when open
    // Expected: Mobile menu closes

    await page.setViewportSize({ width: 375, height: 667 })
    await page.waitForTimeout(300)

    // Open mobile menu
    const hamburgerButton = page.getByTestId('hamburger-button')
    await hamburgerButton.click()
    await page.waitForTimeout(400)

    // Verify menu is open
    const mobileMenu = page.getByRole('dialog', { name: /mobile navigation menu/i })
    await expect(mobileMenu).toBeVisible()

    // Click outside the menu (on the backdrop)
    await page.mouse.click(50, 300)

    // Wait for menu close animation
    await page.waitForTimeout(400)

    // Menu should be closed
    await expect(mobileMenu).toBeHidden()
  })

  test('press Escape key closes mobile menu and returns focus to hamburger', async ({ page }) => {
    // Test Case 9: Press Escape key when mobile menu open
    // Expected: Mobile menu closes and focus returns to hamburger button

    await page.setViewportSize({ width: 375, height: 667 })
    await page.waitForTimeout(300)

    // Open mobile menu
    const hamburgerButton = page.getByTestId('hamburger-button')
    await hamburgerButton.click()
    await page.waitForTimeout(400)

    // Verify menu is open
    const mobileMenu = page.getByRole('dialog', { name: /mobile navigation menu/i })
    await expect(mobileMenu).toBeVisible()

    // Press Escape
    await page.keyboard.press('Escape')

    // Wait for menu close animation
    await page.waitForTimeout(400)

    // Menu should be closed
    await expect(mobileMenu).toBeHidden()

    // Focus should return to hamburger button
    await expect(hamburgerButton).toBeFocused()
  })

  test('header is fixed/sticky at top of viewport', async ({ page }) => {
    // Additional test: Verify header stays fixed during scroll
    const header = page.locator('header[role="banner"]')

    // Get header position before scroll
    const initialBox = await header.boundingBox()
    expect(initialBox).not.toBeNull()

    // Scroll down
    await page.evaluate(() => window.scrollBy(0, 500))
    await page.waitForTimeout(300)

    // Header should still be at top
    const afterScrollBox = await header.boundingBox()
    expect(afterScrollBox).not.toBeNull()
    expect(afterScrollBox!.y).toBe(0)
  })

  test('GitHub link opens in new tab with correct attributes', async ({ page }) => {
    // Links have role="menuitem" for accessible navigation
    const githubLink = page.locator('header nav').getByRole('menuitem', { name: /github/i })

    await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb')
    await expect(githubLink).toHaveAttribute('target', '_blank')
    await expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer')
  })
})
