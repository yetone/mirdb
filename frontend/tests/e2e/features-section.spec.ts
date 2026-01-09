import { test, expect } from '@playwright/test'

test.describe('Features Section E2E Tests', () => {
  // Test Case 6: Scroll to features section on homepage
  test('features section is visible with all feature cards rendered correctly', async ({ page }) => {
    await page.goto('/')

    // Wait for page to load
    await page.waitForLoadState('networkidle')

    // Scroll to features section
    const featuresSection = page.locator('#features')
    await featuresSection.scrollIntoViewIfNeeded()

    // Verify features section is visible
    await expect(featuresSection).toBeVisible()

    // Verify features grid is present
    const featuresGrid = page.getByTestId('features-grid')
    await expect(featuresGrid).toBeVisible()

    // Verify at least 3 feature cards are visible
    const featureCards = page.locator('[data-testid^="feature-card-"]')
    const cardCount = await featureCards.count()
    expect(cardCount).toBeGreaterThanOrEqual(3)

    // Verify each feature card structure
    const expectedFeatures = [
      { id: 'url-shortening', title: 'Quick URL Shortening' },
      { id: 'analytics', title: 'Detailed Analytics' },
      { id: 'link-management', title: 'Easy Link Management' },
      { id: 'security', title: 'Secure & Reliable' },
    ]

    for (const feature of expectedFeatures) {
      // Check card exists
      const card = page.getByTestId(`feature-card-${feature.id}`)
      await expect(card).toBeVisible()

      // Check icon exists
      const icon = page.getByTestId(`feature-icon-${feature.id}`)
      await expect(icon).toBeVisible()
      await expect(icon.locator('svg')).toBeVisible()

      // Check title
      const title = page.getByTestId(`feature-title-${feature.id}`)
      await expect(title).toHaveText(feature.title)

      // Check description exists
      const description = page.getByTestId(`feature-description-${feature.id}`)
      await expect(description).toBeVisible()
      const descriptionText = await description.textContent()
      expect(descriptionText?.length).toBeGreaterThan(20)
    }
  })

  test('features section has correct heading', async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    const featuresSection = page.locator('#features')
    await featuresSection.scrollIntoViewIfNeeded()

    const heading = page.locator('#features-heading')
    await expect(heading).toHaveText('Powerful Features')
  })

  test('feature cards are responsive', async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    const featuresGrid = page.getByTestId('features-grid')
    await featuresGrid.scrollIntoViewIfNeeded()

    // Desktop: should have grid layout
    await expect(featuresGrid).toHaveClass(/grid/)

    // Check cards exist and are visible
    const featureCards = page.locator('[data-testid^="feature-card-"]')
    const cardCount = await featureCards.count()
    expect(cardCount).toBe(4)
  })
})
