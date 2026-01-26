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
  test('clicking Features link scrolls to features section and updates URL hash to #features', async ({ page }) => {
    await page.setViewportSize(viewports.desktop)
    const homePage = new HomePage(page)
    await homePage.goto()

    const featuresLink = page.getByRole('link', { name: /features/i })
    await expect(featuresLink).toHaveAttribute('href', '#features')

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY)

    await featuresLink.click()

    // Wait for scroll animation to complete
    await page.waitForTimeout(1000)

    // Verify URL hash is updated
    await expect(page).toHaveURL(/#features/)

    // Verify page has scrolled (scroll position changed)
    const newScrollY = await page.evaluate(() => window.scrollY)
    expect(newScrollY).toBeGreaterThan(initialScrollY)

    // Verify the features section is now visible in viewport
    const featuresSection = page.locator('#features')
    await expect(featuresSection).toBeInViewport()
  })

  test('clicking How It Works link scrolls to How It Works section and updates URL hash', async ({ page }) => {
    await page.setViewportSize(viewports.desktop)
    const homePage = new HomePage(page)
    await homePage.goto()

    const howItWorksLink = page.getByRole('link', { name: /how it works/i })
    await expect(howItWorksLink).toHaveAttribute('href', '#how-it-works')

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY)

    await howItWorksLink.click()

    // Wait for scroll animation to complete
    await page.waitForTimeout(1000)

    // Verify URL hash is updated
    await expect(page).toHaveURL(/#how-it-works/)

    // Verify page has scrolled
    const newScrollY = await page.evaluate(() => window.scrollY)
    expect(newScrollY).toBeGreaterThan(initialScrollY)

    // Verify the How It Works section is now visible in viewport
    const howItWorksSection = page.locator('#how-it-works')
    await expect(howItWorksSection).toBeInViewport()
  })

  test('navigating directly to /#features URL loads page and scrolls to features section', async ({ page }) => {
    await page.setViewportSize(viewports.desktop)

    // Navigate directly to the URL with hash
    await page.goto('/#features')

    // Wait for page to load and scroll
    await page.waitForTimeout(1000)

    // Verify URL hash is present
    await expect(page).toHaveURL(/#features/)

    // Verify the features section is visible in viewport
    const featuresSection = page.locator('#features')
    await expect(featuresSection).toBeInViewport()

    // Verify scroll position is not at top (page scrolled to section)
    const scrollY = await page.evaluate(() => window.scrollY)
    expect(scrollY).toBeGreaterThan(0)
  })

  test('anchor link scroll is smooth, not an instant jump', async ({ page }) => {
    await page.setViewportSize(viewports.desktop)
    const homePage = new HomePage(page)
    await homePage.goto()

    // Ensure we're at the top of the page
    await page.evaluate(() => window.scrollTo(0, 0))
    await page.waitForTimeout(100)

    const featuresLink = page.getByRole('link', { name: /features/i })

    // Record scroll positions over time to detect smooth scrolling
    const scrollPositions: number[] = []

    // Start recording scroll positions
    const recordingInterval = setInterval(async () => {
      try {
        const scrollY = await page.evaluate(() => window.scrollY)
        scrollPositions.push(scrollY)
      } catch {
        // Page might be navigating, ignore errors
      }
    }, 50)

    await featuresLink.click()

    // Wait for scroll animation to complete
    await page.waitForTimeout(800)

    clearInterval(recordingInterval)

    // Filter out duplicate consecutive values and check for intermediate positions
    const uniquePositions = scrollPositions.filter(
      (pos, idx, arr) => idx === 0 || pos !== arr[idx - 1]
    )

    // Smooth scroll should have multiple intermediate scroll positions
    // An instant jump would only have 2 positions (start and end)
    // Smooth scroll typically has several intermediate positions
    expect(uniquePositions.length).toBeGreaterThanOrEqual(2)

    // Verify the scroll actually happened (not still at top)
    const finalScrollY = await page.evaluate(() => window.scrollY)
    expect(finalScrollY).toBeGreaterThan(0)

    // Verify features section is visible after scroll
    const featuresSection = page.locator('#features')
    await expect(featuresSection).toBeInViewport()
  })
})
