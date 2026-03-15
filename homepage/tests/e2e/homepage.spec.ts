import { test, expect } from '@playwright/test'

test.describe('Homepage Hero Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  // Test Case 4: Click 'Get Started' button navigates to documentation section
  test('Get Started button navigates to documentation section', async ({ page }) => {
    const getStartedButton = page.getByRole('link', { name: /get started/i })

    await expect(getStartedButton).toBeVisible()
    await expect(getStartedButton).toHaveAttribute('href', '#quick-start')

    await getStartedButton.click()

    // Verify URL hash changed to #quick-start
    await expect(page).toHaveURL(/#quick-start/)
  })

  test('displays MirDB product name prominently', async ({ page }) => {
    const heading = page.getByRole('heading', { level: 1 })
    await expect(heading).toContainText('MirDB')
  })

  test('displays tagline with key value proposition', async ({ page }) => {
    await expect(page.getByText(/persistent key-value store/i)).toBeVisible()
  })

  test('displays logo image', async ({ page }) => {
    const logo = page.getByRole('img', { name: /mirdb logo/i })
    await expect(logo).toBeVisible()
  })

  test('hero section is accessible', async ({ page }) => {
    const heroSection = page.getByRole('region', { name: /hero/i })
    await expect(heroSection).toBeVisible()
  })
})
