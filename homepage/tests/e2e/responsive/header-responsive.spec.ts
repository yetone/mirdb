/**
 * E2E tests for responsive header and navigation
 * Scenario 1 - Header and Navigation
 */

import { test, expect } from '@playwright/test'

test.describe('Header Responsive Design', () => {
  test('navigation collapses to mobile menu at 375px width', async ({ page }) => {
    // Set viewport to mobile width
    await page.setViewportSize({ width: 375, height: 667 })
    await page.goto('/')

    // Desktop navigation should be hidden
    const desktopNav = page.locator('.navigation')
    await expect(desktopNav).toBeHidden()

    // Hamburger button should be visible
    const hamburgerButton = page.getByRole('button', { name: /open menu/i })
    await expect(hamburgerButton).toBeVisible()
  })

  test('hamburger icon opens mobile menu on click', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 })
    await page.goto('/')

    // Click hamburger button
    const hamburgerButton = page.getByRole('button', { name: /open menu/i })
    await hamburgerButton.click()

    // Mobile menu should be visible
    const mobileMenu = page.locator('.mobile-menu--open')
    await expect(mobileMenu).toBeVisible()

    // Navigation links should be visible in mobile menu
    await expect(page.locator('.mobile-menu__link').filter({ hasText: 'Documentation' })).toBeVisible()
    await expect(page.locator('.mobile-menu__link').filter({ hasText: 'Examples' })).toBeVisible()
    await expect(page.locator('.mobile-menu__link').filter({ hasText: 'GitHub' })).toBeVisible()
    await expect(page.locator('.mobile-menu__link').filter({ hasText: 'About' })).toBeVisible()
  })

  test('desktop navigation is visible at larger viewports', async ({ page }) => {
    // Set viewport to tablet/desktop width
    await page.setViewportSize({ width: 1024, height: 768 })
    await page.goto('/')

    // Desktop navigation should be visible
    const desktopNav = page.locator('.navigation')
    await expect(desktopNav).toBeVisible()

    // Hamburger button should be hidden
    const hamburgerButton = page.getByRole('button', { name: /open menu/i })
    await expect(hamburgerButton).toBeHidden()
  })

  test('logo is always visible on mobile', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 })
    await page.goto('/')

    // Use the header-specific logo class to be more specific
    const logo = page.locator('.header__logo')
    await expect(logo).toBeVisible()
  })

  test('theme toggle is always visible on mobile', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 })
    await page.goto('/')

    const themeToggle = page.getByRole('button', { name: /switch to .* mode/i })
    await expect(themeToggle).toBeVisible()
  })

  test('mobile menu closes when a link is clicked', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 })
    await page.goto('/')

    // Open mobile menu
    const hamburgerButton = page.getByRole('button', { name: /open menu/i })
    await hamburgerButton.click()

    // Click a link
    const aboutLink = page.locator('.mobile-menu__link').filter({ hasText: 'About' })
    await aboutLink.click()

    // Mobile menu should be closed
    const mobileMenu = page.locator('.mobile-menu--open')
    await expect(mobileMenu).not.toBeVisible()
  })
})
