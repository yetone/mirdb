/**
 * Hero Section E2E Tests
 * Owner: Scenario 1 - Hero Section Display and Value Proposition
 *
 * E2E tests to verify:
 * - Hero section visibility at desktop viewport
 * - Headline and CTA buttons visible without scrolling
 * - Navigation functionality
 */

import { test, expect } from '@playwright/test'
import { HomePage, viewports } from './fixtures'

test.describe('Hero Section Display and Value Proposition', () => {
  test('hero section renders with headline, subheadline, and CTA buttons visible', async ({ page }) => {
    const homePage = new HomePage(page)
    await homePage.goto()

    await expect(homePage.heroSection).toBeVisible()
    await expect(homePage.heroHeadline).toBeVisible()
    await expect(homePage.heroSubheadline).toBeVisible()
    await expect(homePage.getStartedButton).toBeVisible()
    await expect(homePage.signInButton).toBeVisible()
  })

  test('headline displays product tagline', async ({ page }) => {
    const homePage = new HomePage(page)
    await homePage.goto()

    await expect(homePage.heroHeadline).toHaveText('Shorten. Share. Track.')
  })

  test('subheadline explains the service value', async ({ page }) => {
    const homePage = new HomePage(page)
    await homePage.goto()

    await expect(homePage.heroSubheadline).toContainText('Transform your long URLs')
    await expect(homePage.heroSubheadline).toContainText('short links')
  })

  test('all hero content is visible without vertical scrolling at 1280x720', async ({ page }) => {
    await page.setViewportSize(viewports.desktop)
    const homePage = new HomePage(page)
    await homePage.goto()

    await expect(homePage.heroSection).toBeVisible()
    await expect(homePage.heroHeadline).toBeVisible()
    await expect(homePage.heroSubheadline).toBeVisible()
    await expect(homePage.getStartedButton).toBeVisible()
    await expect(homePage.signInButton).toBeVisible()

    const heroBox = await homePage.heroSection.boundingBox()
    const getStartedBox = await homePage.getStartedButton.boundingBox()
    const signInBox = await homePage.signInButton.boundingBox()

    expect(heroBox).not.toBeNull()
    expect(getStartedBox).not.toBeNull()
    expect(signInBox).not.toBeNull()

    if (getStartedBox && signInBox) {
      expect(getStartedBox.y + getStartedBox.height).toBeLessThan(viewports.desktop.height)
      expect(signInBox.y + signInBox.height).toBeLessThan(viewports.desktop.height)
    }
  })

  test('Get Started button navigates to register page', async ({ page }) => {
    const homePage = new HomePage(page)
    await homePage.goto()

    await homePage.clickGetStarted()

    await expect(page).toHaveURL(/\/register/)
  })

  test('Sign In button navigates to login page', async ({ page }) => {
    const homePage = new HomePage(page)
    await homePage.goto()

    await homePage.clickSignIn()

    await expect(page).toHaveURL(/\/login/)
  })

  test('CTA buttons are properly styled and identifiable', async ({ page }) => {
    const homePage = new HomePage(page)
    await homePage.goto()

    await expect(homePage.getStartedButton).toHaveText('Get Started Free')
    await expect(homePage.signInButton).toHaveText('Sign In')

    await expect(homePage.getStartedButton).toBeEnabled()
    await expect(homePage.signInButton).toBeEnabled()
  })

  test('hero section is accessible with proper ARIA attributes', async ({ page }) => {
    const homePage = new HomePage(page)
    await homePage.goto()

    await expect(homePage.heroSection).toHaveAttribute('aria-labelledby', 'hero-heading')

    const heading = page.locator('h1#hero-heading')
    await expect(heading).toBeVisible()
  })
})
