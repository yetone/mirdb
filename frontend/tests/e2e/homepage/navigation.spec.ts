/**
 * Navigation E2E Tests
 * Owner: Scenario 6 - Navigation Header, Scenario 13 - Anchor Link Scrolling
 *
 * E2E tests to verify:
 * - Navigation header visibility and elements
 * - Anchor link smooth scrolling to sections
 */

import { test, expect } from '@playwright/test'
import { HomePage, viewports } from './fixtures'

test.describe('Navigation Header', () => {
  test('navigation header is visible at the top of the page', async ({ page }) => {
    const homePage = new HomePage(page)
    await homePage.goto()

    await expect(homePage.navbar).toBeVisible()

    const navbarBox = await homePage.navbar.boundingBox()
    expect(navbarBox).not.toBeNull()
    if (navbarBox) {
      expect(navbarBox.y).toBeLessThan(50)
    }
  })

  test('logo/brand name is displayed on the left side of header', async ({ page }) => {
    const homePage = new HomePage(page)
    await homePage.goto()

    const brandLink = page.getByRole('link', { name: /urlshortener/i })
    await expect(brandLink).toBeVisible()

    const navbarBox = await homePage.navbar.boundingBox()
    const brandBox = await brandLink.boundingBox()

    expect(navbarBox).not.toBeNull()
    expect(brandBox).not.toBeNull()

    if (navbarBox && brandBox) {
      const navbarLeftEdge = navbarBox.x
      const brandLeftEdge = brandBox.x
      expect(brandLeftEdge - navbarLeftEdge).toBeLessThan(200)
    }
  })

  test('Features anchor link is present in navigation', async ({ page }) => {
    await page.setViewportSize(viewports.desktop)
    const homePage = new HomePage(page)
    await homePage.goto()

    const featuresLink = page.getByRole('link', { name: /features/i })
    await expect(featuresLink).toBeVisible()
    await expect(featuresLink).toHaveAttribute('href', '#features')
  })

  test('How It Works anchor link is present in navigation', async ({ page }) => {
    await page.setViewportSize(viewports.desktop)
    const homePage = new HomePage(page)
    await homePage.goto()

    const howItWorksLink = page.getByRole('link', { name: /how it works/i })
    await expect(howItWorksLink).toBeVisible()
    await expect(howItWorksLink).toHaveAttribute('href', '#how-it-works')
  })

  test('Sign In button is present and on the right side', async ({ page }) => {
    const homePage = new HomePage(page)
    await homePage.goto()

    const signInButton = page.getByTestId('sign-in-nav')
    await expect(signInButton).toBeVisible()
    await expect(signInButton).toHaveText('Sign In')

    const navbarBox = await homePage.navbar.boundingBox()
    const buttonBox = await signInButton.boundingBox()

    expect(navbarBox).not.toBeNull()
    expect(buttonBox).not.toBeNull()

    if (navbarBox && buttonBox) {
      const navbarCenter = navbarBox.x + navbarBox.width / 2
      expect(buttonBox.x).toBeGreaterThan(navbarCenter)
    }
  })

  test('Get Started button is present and on the right side', async ({ page }) => {
    const homePage = new HomePage(page)
    await homePage.goto()

    const getStartedButton = page.getByTestId('get-started-nav')
    await expect(getStartedButton).toBeVisible()
    await expect(getStartedButton).toHaveText('Get Started')

    const navbarBox = await homePage.navbar.boundingBox()
    const buttonBox = await getStartedButton.boundingBox()

    expect(navbarBox).not.toBeNull()
    expect(buttonBox).not.toBeNull()

    if (navbarBox && buttonBox) {
      const navbarCenter = navbarBox.x + navbarBox.width / 2
      expect(buttonBox.x).toBeGreaterThan(navbarCenter)
    }
  })
})

test.describe('Anchor Link Scrolling', () => {
  test('clicking Features link updates URL hash and attempts to scroll to features section', async ({ page }) => {
    await page.setViewportSize(viewports.desktop)
    const homePage = new HomePage(page)
    await homePage.goto()

    const featuresLink = page.getByRole('link', { name: /features/i })
    await expect(featuresLink).toHaveAttribute('href', '#features')

    await featuresLink.click()

    await page.waitForTimeout(500)

    await expect(page).toHaveURL(/#features/)
  })
})
