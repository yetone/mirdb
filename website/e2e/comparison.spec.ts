import { test, expect } from '@playwright/test'

test.describe('Memcached Comparison Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  test('displays comparison section with memcached content', async ({ page }) => {
    // Test Case 1: Section or content comparing MirDB to memcached is present
    const comparisonSection = page.locator('.comparison')
    await expect(comparisonSection).toBeVisible()

    // Verify the section mentions both MirDB and memcached
    const mirdbMention = comparisonSection.getByText(/mirdb/i)
    await expect(mirdbMention.first()).toBeVisible()

    const memcachedMention = comparisonSection.getByText(/memcached/i)
    await expect(memcachedMention.first()).toBeVisible()
  })

  test('highlights persistence advantage - data survives restarts', async ({ page }) => {
    // Test Case 2: Content explicitly states data survives restarts/persistence benefit
    const comparisonSection = page.locator('.comparison')
    await expect(comparisonSection).toBeVisible()

    // Check for persistence-related messaging
    const persistencePatterns = [
      /data survives restarts/i,
      /persistent/i,
      /persists.*data/i,
      /durability/i,
    ]

    let persistenceFound = false
    for (const pattern of persistencePatterns) {
      const persistenceText = comparisonSection.getByText(pattern)
      if (await persistenceText.first().isVisible().catch(() => false)) {
        persistenceFound = true
        break
      }
    }

    expect(persistenceFound).toBeTruthy()

    // Specifically check for the key differentiator message about data surviving restarts
    const restartMessage = page.getByText(/unlike memcached|survives restarts|data.*persist/i)
    await expect(restartMessage.first()).toBeVisible()
  })

  test('mentions drop-in replacement and compatibility with existing memcached clients', async ({ page }) => {
    // Test Case 3: Content mentions compatibility with existing memcached clients
    const comparisonSection = page.locator('.comparison')
    await expect(comparisonSection).toBeVisible()

    // Check for drop-in replacement or compatibility messaging
    const compatibilityPatterns = [
      /drop-in replacement/i,
      /existing.*clients/i,
      /compatible.*memcached/i,
      /memcached.*protocol/i,
      /protocol.*compat/i,
    ]

    let compatibilityFound = false
    for (const pattern of compatibilityPatterns) {
      const compatibilityText = comparisonSection.getByText(pattern)
      if (await compatibilityText.first().isVisible().catch(() => false)) {
        compatibilityFound = true
        break
      }
    }

    expect(compatibilityFound).toBeTruthy()
  })

  test('comparison section is navigable', async ({ page }) => {
    // Verify users can navigate to the comparison section
    // This could be via a nav link or by scrolling
    const comparisonSection = page.locator('.comparison')

    // Check if there's a navigation link to the comparison section
    const navLink = page.getByRole('link', { name: /comparison|why mirdb|features/i })
    if (await navLink.first().isVisible().catch(() => false)) {
      await navLink.first().click()
      await expect(comparisonSection).toBeInViewport()
    } else {
      // If no nav link, just verify the section exists and is visible on the page
      await expect(comparisonSection).toBeVisible()
    }
  })

  test('comparison section has accessible structure', async ({ page }) => {
    // Verify accessibility: section should have a heading
    const comparisonSection = page.locator('.comparison')
    await expect(comparisonSection).toBeVisible()

    // Check for a heading within the comparison section
    const heading = comparisonSection.locator('h2, h3')
    await expect(heading.first()).toBeVisible()
  })
})
