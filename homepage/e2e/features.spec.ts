import { test, expect } from '@playwright/test'

test.describe('Features Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  // Test Case 9: Feature cards show subtle visual feedback on hover
  test('feature cards show subtle visual feedback on hover', async ({ page }) => {
    // Wait for the features section to load
    await page.waitForSelector('[data-testid="features-grid"]')

    // Get the first feature card
    const featureCard = page.locator('[data-testid="feature-card"]').first()

    // Get initial computed styles
    const initialTransform = await featureCard.evaluate((el) => {
      return window.getComputedStyle(el).transform
    })

    // Hover over the card
    await featureCard.hover()

    // Wait for transition to complete
    await page.waitForTimeout(300)

    // Verify the card has hover styles applied (transform should change)
    const hoverTransform = await featureCard.evaluate((el) => {
      return window.getComputedStyle(el).transform
    })

    // The transform should change on hover (translate-y-1 applies translateY)
    // Initial might be 'none' or 'matrix(1, 0, 0, 1, 0, 0)'
    // Hover state should show different transform due to -translate-y-1
    expect(hoverTransform).not.toBe(initialTransform)
  })

  test('all 6 feature cards are visible', async ({ page }) => {
    const featureCards = page.locator('[data-testid="feature-card"]')
    await expect(featureCards).toHaveCount(6)
  })

  test('features section has proper heading', async ({ page }) => {
    const heading = page.getByRole('heading', { name: /key features/i })
    await expect(heading).toBeVisible()
  })

  test('feature cards contain expected feature titles', async ({ page }) => {
    const expectedTitles = [
      'Memcached Protocol Support',
      'Data Persistence via SSTables',
      'LSM Tree Architecture',
      'Async Networking with Tokio',
      'Skip List Memtable',
      'Multi-level Compaction',
    ]

    for (const title of expectedTitles) {
      await expect(page.getByText(title)).toBeVisible()
    }
  })

  test('feature cards have transition classes for visual feedback', async ({ page }) => {
    const featureCard = page.locator('[data-testid="feature-card"]').first()
    const className = await featureCard.getAttribute('class')

    // Verify card has transition-related classes
    expect(className).toContain('transition')
    expect(className).toContain('hover:')
  })
})
