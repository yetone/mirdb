/**
 * E2E tests for Header component.
 * Owner: Scenario 1 - Header Section Implementation
 *
 * Test cases:
 * - Header renders at desktop viewport (1440px)
 * - Header renders at mobile viewport (375px)
 * - Hamburger menu expands on click
 * - Header sticky behavior on scroll
 * - Logo has alt text
 */

import { test, expect } from '@playwright/test'

test.describe('Header Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  test('Header displays correctly at desktop viewport (1440px)', async ({
    page,
  }) => {
    // Set desktop viewport
    await page.setViewportSize({ width: 1440, height: 900 })

    // Verify header is visible
    const header = page.locator('header[role="banner"]')
    await expect(header).toBeVisible()

    // Verify logo is on the left side
    const logo = page.getByTestId('header-logo')
    await expect(logo).toBeVisible()

    // Verify navigation links are visible (desktop)
    const nav = page.getByRole('navigation', { name: 'Main navigation' })
    await expect(nav).toBeVisible()

    // Verify CTA button is visible
    const ctaButton = page.getByTestId('header-cta')
    await expect(ctaButton).toBeVisible()

    // Verify hamburger menu is hidden on desktop
    const hamburgerButton = page.getByTestId('hamburger-menu-button')
    await expect(hamburgerButton).toBeHidden()

    // Check header has fixed positioning
    const headerStyle = await header.evaluate((el) => {
      const style = window.getComputedStyle(el)
      return {
        position: style.position,
        top: style.top,
      }
    })
    expect(headerStyle.position).toBe('fixed')
    expect(headerStyle.top).toBe('0px')
  })

  test('Header displays correctly at mobile viewport (375px)', async ({
    page,
  }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 })

    // Verify header is visible
    const header = page.locator('header[role="banner"]')
    await expect(header).toBeVisible()

    // Verify logo is visible
    const logo = page.getByTestId('header-logo')
    await expect(logo).toBeVisible()

    // Verify hamburger menu button is visible
    const hamburgerButton = page.getByTestId('hamburger-menu-button')
    await expect(hamburgerButton).toBeVisible()

    // Verify desktop navigation is hidden
    const desktopNav = page.getByRole('navigation', {
      name: 'Main navigation',
    })
    await expect(desktopNav).toBeHidden()

    // Verify mobile menu is not visible initially
    const mobileMenu = page.getByTestId('mobile-menu')
    await expect(mobileMenu).not.toBeVisible()
  })

  test('Mobile hamburger menu expands and shows navigation links', async ({
    page,
  }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 })

    // Click hamburger menu button
    const hamburgerButton = page.getByTestId('hamburger-menu-button')
    await hamburgerButton.click()

    // Verify mobile menu is now visible
    const mobileMenu = page.getByTestId('mobile-menu')
    await expect(mobileMenu).toBeVisible()

    // Verify navigation links are visible in mobile menu
    await expect(
      page.getByTestId('mobile-nav-link-features')
    ).toBeVisible()
    await expect(page.getByTestId('mobile-nav-link-pricing')).toBeVisible()
    await expect(page.getByTestId('mobile-nav-link-about')).toBeVisible()
    await expect(page.getByTestId('mobile-nav-link-contact')).toBeVisible()

    // Verify mobile CTA button is visible
    const mobileCta = page.getByTestId('mobile-menu-cta')
    await expect(mobileCta).toBeVisible()
  })

  test('Mobile menu closes when clicking a link', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 })

    // Open mobile menu
    const hamburgerButton = page.getByTestId('hamburger-menu-button')
    await hamburgerButton.click()

    // Verify mobile menu is visible
    const mobileMenu = page.getByTestId('mobile-menu')
    await expect(mobileMenu).toBeVisible()

    // Click a navigation link
    await page.getByTestId('mobile-nav-link-features').click()

    // Verify mobile menu is closed
    await expect(mobileMenu).not.toBeVisible()
  })

  test('Mobile menu closes when clicking backdrop', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 })

    // Open mobile menu
    const hamburgerButton = page.getByTestId('hamburger-menu-button')
    await hamburgerButton.click()

    // Verify mobile menu is visible
    const mobileMenu = page.getByTestId('mobile-menu')
    await expect(mobileMenu).toBeVisible()

    // Click backdrop
    const backdrop = page.getByTestId('mobile-menu-backdrop')
    await backdrop.click({ force: true, position: { x: 10, y: 10 } })

    // Verify mobile menu is closed
    await expect(mobileMenu).not.toBeVisible()
  })

  test('Header remains fixed at top when scrolling down', async ({ page }) => {
    // Set viewport
    await page.setViewportSize({ width: 1440, height: 900 })

    // Get initial header position
    const header = page.locator('header[role="banner"]')
    const initialBoundingBox = await header.boundingBox()
    expect(initialBoundingBox?.y).toBe(0)

    // Scroll down the page
    await page.evaluate(() => {
      window.scrollTo(0, 500)
    })

    // Wait for scroll to complete
    await page.waitForTimeout(200)

    // Verify header is still at the top of the viewport
    const afterScrollBoundingBox = await header.boundingBox()
    expect(afterScrollBoundingBox?.y).toBe(0)

    // Verify header is still visible
    await expect(header).toBeVisible()
  })

  test('Product logo has descriptive alt text for accessibility', async ({
    page,
  }) => {
    await page.setViewportSize({ width: 1440, height: 900 })

    // Get the logo element
    const logo = page.getByTestId('header-logo')
    await expect(logo).toBeVisible()

    // Verify alt attribute exists and is not empty
    const altText = await logo.getAttribute('alt')
    expect(altText).toBeTruthy()
    expect(altText?.length).toBeGreaterThan(0)
    expect(altText).toContain('logo')
  })

  test('Navigation links have correct href attributes', async ({ page }) => {
    await page.setViewportSize({ width: 1440, height: 900 })

    // Check Features link
    const featuresLink = page.getByRole('link', { name: 'Features' })
    await expect(featuresLink).toHaveAttribute('href', '#features')

    // Check Pricing link
    const pricingLink = page.getByRole('link', { name: 'Pricing' })
    await expect(pricingLink).toHaveAttribute('href', '#pricing')

    // Check About link
    const aboutLink = page.getByRole('link', { name: 'About' })
    await expect(aboutLink).toHaveAttribute('href', '#about')

    // Check Contact link
    const contactLink = page.getByRole('link', { name: 'Contact' })
    await expect(contactLink).toHaveAttribute('href', '#contact')
  })

  test('CTA button has correct styling and is clickable', async ({ page }) => {
    await page.setViewportSize({ width: 1440, height: 900 })

    const ctaButton = page.getByTestId('header-cta')
    await expect(ctaButton).toBeVisible()
    await expect(ctaButton).toHaveAttribute('href', '#signup')

    // Verify button is clickable
    await expect(ctaButton).toBeEnabled()
  })

  test('Hamburger menu button has correct aria attributes', async ({
    page,
  }) => {
    await page.setViewportSize({ width: 375, height: 667 })

    const hamburgerButton = page.getByTestId('hamburger-menu-button')

    // Initially aria-expanded should be false
    await expect(hamburgerButton).toHaveAttribute('aria-expanded', 'false')

    // Click to open menu
    await hamburgerButton.click()

    // After opening, aria-expanded should be true
    await expect(hamburgerButton).toHaveAttribute('aria-expanded', 'true')
  })
})
