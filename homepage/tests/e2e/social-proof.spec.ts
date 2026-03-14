/**
 * E2E tests for Social Proof section.
 * Owner: Scenario 4 - Social Proof Section Implementation
 *
 * Test cases:
 * - Test Case 4: Test logo carousel/grid at mobile viewport - Logos display appropriately on small screens
 * - Social proof section renders correctly
 * - Customer logos display with proper accessibility
 * - Testimonials display correctly
 * - Statistics display correctly
 */

import { test, expect } from '@playwright/test'

test.describe('Social Proof Section E2E Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  // Basic rendering tests
  test('Social proof section renders correctly', async ({ page }) => {
    const socialProofSection = page.getByTestId('social-proof-section')
    await expect(socialProofSection).toBeVisible()

    const heading = page.getByTestId('social-proof-heading')
    await expect(heading).toBeVisible()
    await expect(heading).toHaveText(/Trusted by Teams Worldwide/)

    const description = page.getByTestId('social-proof-description')
    await expect(description).toBeVisible()
  })

  test('Social proof section has proper semantic structure', async ({ page }) => {
    const section = page.getByTestId('social-proof-section')
    await expect(section).toHaveAttribute('aria-labelledby', 'social-proof-heading')
    await expect(section).toHaveAttribute('id', 'social-proof')
  })

  test('Social proof heading is h2 element', async ({ page }) => {
    const heading = page.getByTestId('social-proof-heading')
    await expect(heading).toBeVisible()

    const tagName = await heading.evaluate((el) => el.tagName)
    expect(tagName).toBe('H2')
  })

  // Logo tests
  test('Customer logos are displayed', async ({ page }) => {
    const logosContainer = page.getByTestId('logos-container')
    await expect(logosContainer).toBeVisible()

    const logoGrid = page.getByTestId('logo-grid')
    await expect(logoGrid).toBeVisible()

    const logoItems = page.getByTestId('logo-item')
    const count = await logoItems.count()
    expect(count).toBeGreaterThan(0)
  })

  test('Logo grid has proper accessibility attributes', async ({ page }) => {
    const logoGrid = page.getByTestId('logo-grid')
    await expect(logoGrid).toHaveAttribute('role', 'list')
    await expect(logoGrid).toHaveAttribute('aria-label', 'Trusted by these companies')
  })

  test('Logo items display company names for accessibility', async ({ page }) => {
    const logoGrid = page.getByTestId('logo-grid')
    const logoItems = page.getByTestId('logo-item')

    const count = await logoItems.count()
    expect(count).toBeGreaterThan(0)

    // Each logo item should contain either an image with alt text or fallback text
    // When images fail to load, the component falls back to showing company name as text
    for (let i = 0; i < Math.min(count, 3); i++) {
      const logoItem = logoItems.nth(i)
      await expect(logoItem).toBeVisible()

      // Check that the logo item has accessible content (either img with alt or text fallback)
      const textContent = await logoItem.textContent()
      const img = logoItem.locator('img')
      const imgCount = await img.count()

      if (imgCount > 0) {
        // If image exists, check alt text
        const altText = await img.first().getAttribute('alt')
        expect(altText).toBeTruthy()
      } else {
        // If no image, check fallback text exists
        expect(textContent!.trim().length).toBeGreaterThan(0)
      }
    }
  })

  test('Logos have grayscale effect by default', async ({ page }) => {
    const logoItem = page.getByTestId('logo-item').first()
    await expect(logoItem).toBeVisible()

    // Check grayscale class is applied
    await expect(logoItem).toHaveClass(/grayscale/)
  })

  // Test Case 4: Test logo carousel/grid at mobile viewport
  test('Test Case 4: Logo grid displays appropriately at mobile viewport (375px)', async ({
    page,
  }) => {
    // Set viewport to mobile
    await page.setViewportSize({ width: 375, height: 812 })
    await page.goto('/')

    const logoGrid = page.getByTestId('logo-grid')
    await expect(logoGrid).toBeVisible()

    // Check grid layout
    const gridStyles = await logoGrid.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        display: styles.display,
        gridTemplateColumns: styles.gridTemplateColumns,
      }
    })

    expect(gridStyles.display).toBe('grid')

    // At 375px (mobile), should have 2 columns
    const columnCount = gridStyles.gridTemplateColumns
      .split(' ')
      .filter((col) => col !== '').length
    expect(columnCount).toBe(2)
  })

  test('Logo grid displays 3 columns at small tablet (640px+)', async ({ page }) => {
    // Set viewport to small tablet
    await page.setViewportSize({ width: 640, height: 800 })
    await page.goto('/')

    const logoGrid = page.getByTestId('logo-grid')
    await expect(logoGrid).toBeVisible()

    // Check grid layout
    const gridStyles = await logoGrid.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        display: styles.display,
        gridTemplateColumns: styles.gridTemplateColumns,
      }
    })

    expect(gridStyles.display).toBe('grid')

    // At 640px+ (sm breakpoint), should have 3 columns
    const columnCount = gridStyles.gridTemplateColumns
      .split(' ')
      .filter((col) => col !== '').length
    expect(columnCount).toBe(3)
  })

  test('Logo grid displays 5 columns at desktop (768px+)', async ({ page }) => {
    // Set viewport to desktop
    await page.setViewportSize({ width: 1024, height: 800 })
    await page.goto('/')

    const logoGrid = page.getByTestId('logo-grid')
    await expect(logoGrid).toBeVisible()

    // Check grid layout
    const gridStyles = await logoGrid.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        display: styles.display,
        gridTemplateColumns: styles.gridTemplateColumns,
      }
    })

    expect(gridStyles.display).toBe('grid')

    // At 768px+ (md breakpoint), should have 5 columns
    const columnCount = gridStyles.gridTemplateColumns
      .split(' ')
      .filter((col) => col !== '').length
    expect(columnCount).toBe(5)
  })

  // Testimonial tests
  test('Testimonials are displayed', async ({ page }) => {
    const testimonialsContainer = page.getByTestId('testimonials-container')
    await expect(testimonialsContainer).toBeVisible()

    const testimonialCards = page.getByTestId('testimonial-card')
    const count = await testimonialCards.count()
    expect(count).toBeGreaterThan(0)
  })

  test('Each testimonial card has quote, author, role, and company', async ({ page }) => {
    const testimonialCards = page.getByTestId('testimonial-card')
    const firstCard = testimonialCards.first()

    await expect(firstCard.getByTestId('testimonial-quote')).toBeVisible()
    await expect(firstCard.getByTestId('testimonial-author')).toBeVisible()
    await expect(firstCard.getByTestId('testimonial-role')).toBeVisible()
    await expect(firstCard.getByTestId('testimonial-company')).toBeVisible()
  })

  test('Testimonial cards are keyboard focusable', async ({ page }) => {
    const firstCard = page.getByTestId('testimonial-card').first()

    // Focus the card
    await firstCard.focus()
    await expect(firstCard).toBeFocused()
  })

  test('Testimonial card shows focus indicator', async ({ page }) => {
    const firstCard = page.getByTestId('testimonial-card').first()

    // Focus the card
    await firstCard.focus()
    await page.waitForTimeout(100)

    // Check for focus indicator (ring/outline/shadow)
    const focusStyles = await firstCard.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        outline: styles.outline,
        boxShadow: styles.boxShadow,
      }
    })

    // Should have some focus indicator
    const hasFocusIndicator =
      focusStyles.boxShadow !== 'none' || focusStyles.outline !== 'none'

    expect(hasFocusIndicator).toBeTruthy()
  })

  test('Testimonial card displays hover effect', async ({ page }) => {
    const testimonialCard = page.getByTestId('testimonial-card').first()
    await expect(testimonialCard).toBeVisible()

    // Get initial styles
    const initialStyles = await testimonialCard.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        boxShadow: styles.boxShadow,
        borderColor: styles.borderColor,
      }
    })

    // Hover over the card
    await testimonialCard.hover()
    await page.waitForTimeout(350)

    // Get hover styles
    const hoverStyles = await testimonialCard.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        boxShadow: styles.boxShadow,
        borderColor: styles.borderColor,
      }
    })

    // Verify at least one visual property changed on hover
    const hasVisibleChange =
      initialStyles.boxShadow !== hoverStyles.boxShadow ||
      initialStyles.borderColor !== hoverStyles.borderColor

    expect(hasVisibleChange).toBeTruthy()
  })

  test('Testimonials grid displays properly at mobile viewport', async ({ page }) => {
    // Set viewport to mobile
    await page.setViewportSize({ width: 375, height: 812 })
    await page.goto('/')

    const testimonialsGrid = page.getByTestId('testimonials-grid')
    await expect(testimonialsGrid).toBeVisible()

    // Check grid layout - should be single column on mobile
    const gridStyles = await testimonialsGrid.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        display: styles.display,
        gridTemplateColumns: styles.gridTemplateColumns,
      }
    })

    expect(gridStyles.display).toBe('grid')
    const columnCount = gridStyles.gridTemplateColumns
      .split(' ')
      .filter((col) => col !== '').length
    expect(columnCount).toBe(1)
  })

  // Statistics tests
  test('Statistics are displayed', async ({ page }) => {
    const statisticsContainer = page.getByTestId('statistics-container')
    await expect(statisticsContainer).toBeVisible()

    const statisticItems = page.getByTestId('statistic-item')
    const count = await statisticItems.count()
    expect(count).toBeGreaterThan(0)
  })

  test('Each statistic has value and label', async ({ page }) => {
    const statisticItems = page.getByTestId('statistic-item')
    const firstStat = statisticItems.first()

    await expect(firstStat.getByTestId('statistic-value')).toBeVisible()
    await expect(firstStat.getByTestId('statistic-label')).toBeVisible()
  })

  test('Statistics display icons', async ({ page }) => {
    const iconContainers = page.getByTestId('statistic-icon')
    const count = await iconContainers.count()

    expect(count).toBeGreaterThan(0)

    const firstIcon = iconContainers.first()
    await expect(firstIcon).toBeVisible()

    // Check icon has SVG
    const svg = firstIcon.locator('svg')
    await expect(svg).toBeVisible()
  })

  test('Statistics grid has proper accessibility attributes', async ({ page }) => {
    const statsGrid = page.getByTestId('statistics-grid')
    await expect(statsGrid).toHaveAttribute('role', 'list')
    await expect(statsGrid).toHaveAttribute('aria-label', 'Key statistics')
  })

  test('Statistics grid displays 2 columns at mobile viewport', async ({ page }) => {
    // Set viewport to mobile
    await page.setViewportSize({ width: 375, height: 812 })
    await page.goto('/')

    const statsGrid = page.getByTestId('statistics-grid')
    await expect(statsGrid).toBeVisible()

    // Check grid layout
    const gridStyles = await statsGrid.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        display: styles.display,
        gridTemplateColumns: styles.gridTemplateColumns,
      }
    })

    expect(gridStyles.display).toBe('grid')
    const columnCount = gridStyles.gridTemplateColumns
      .split(' ')
      .filter((col) => col !== '').length
    expect(columnCount).toBe(2)
  })

  test('Statistics grid displays 4 columns at desktop viewport', async ({ page }) => {
    // Set viewport to desktop
    await page.setViewportSize({ width: 1024, height: 800 })
    await page.goto('/')

    const statsGrid = page.getByTestId('statistics-grid')
    await expect(statsGrid).toBeVisible()

    // Check grid layout
    const gridStyles = await statsGrid.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        display: styles.display,
        gridTemplateColumns: styles.gridTemplateColumns,
      }
    })

    expect(gridStyles.display).toBe('grid')
    const columnCount = gridStyles.gridTemplateColumns
      .split(' ')
      .filter((col) => col !== '').length
    expect(columnCount).toBe(4)
  })

  // Navigation test
  test('Social proof section is navigable via anchor link', async ({ page }) => {
    // Navigate directly to #social-proof
    await page.goto('/#social-proof')

    const socialProofSection = page.getByTestId('social-proof-section')
    await expect(socialProofSection).toBeInViewport()
  })
})
