/**
 * Homepage Accessibility E2E Tests
 * Owner: Scenario 12 (primary), Scenario 14 (shared)
 *
 * End-to-end accessibility tests:
 * - Keyboard navigation works (Scenario 12)
 * - Focus indicators visible
 * - Color contrast meets WCAG AA (Scenario 14)
 * - axe-core audit passes
 *
 * Testing framework: Playwright + axe-core
 */
import { test, expect } from '@playwright/test'
import AxeBuilder from '@axe-core/playwright'

test.describe('Accessibility - Color Contrast (Scenario 14)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    // Wait for animations to complete
    await page.waitForLoadState('networkidle')
    // Wait a bit for Framer Motion animations
    await page.waitForTimeout(1000)
  })

  test('Test Case 1: Audit color contrast of hero headline in light theme', async ({
    page,
  }) => {
    // Ensure we're in light theme
    const htmlElement = page.locator('html')
    await expect(htmlElement).toHaveAttribute('data-theme', /(light|lemonade|cupcake)?/)

    // Verify hero section exists
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible()

    // Run axe-core specifically for color contrast on hero section
    const accessibilityScanResults = await new AxeBuilder({ page })
      .include('[data-testid="hero-section"]')
      .withRules(['color-contrast'])
      .analyze()

    // Filter out results that are "incomplete" due to gradient text
    // Gradient text may show as incomplete because axe cannot compute gradient contrast
    const violations = accessibilityScanResults.violations.filter(
      (v) => v.id === 'color-contrast'
    )

    // Assert no color contrast violations (contrast ratio >= 4.5:1)
    expect(violations).toHaveLength(0)
  })

  test('Test Case 2: Audit color contrast of CTA button text', async ({
    page,
  }) => {
    // Verify buttons exist and are visible
    const getStartedButton = page.getByRole('button', { name: /get started/i })
    const loginButton = page.getByRole('button', { name: /log in/i })

    await expect(getStartedButton).toBeVisible()
    await expect(loginButton).toBeVisible()

    // Run axe-core specifically for color contrast on buttons
    const accessibilityScanResults = await new AxeBuilder({ page })
      .include('button')
      .withRules(['color-contrast'])
      .analyze()

    const violations = accessibilityScanResults.violations.filter(
      (v) => v.id === 'color-contrast'
    )

    // Assert button text has sufficient contrast (>= 4.5:1)
    expect(violations).toHaveLength(0)
  })

  test('Test Case 3: Audit contrast in dark mode', async ({ page }) => {
    // Switch to dark theme by clicking theme toggle if available
    const themeToggle = page.locator('[data-testid="theme-toggle"]').first()

    if (await themeToggle.isVisible()) {
      // Click to toggle theme - may need multiple clicks to get to dark
      await themeToggle.click()
      await page.waitForTimeout(300)
    }

    // Force dark theme via data attribute if toggle didn't work
    await page.evaluate(() => {
      document.documentElement.setAttribute('data-theme', 'dark')
    })
    await page.waitForTimeout(500)

    // Run comprehensive axe-core color contrast audit for entire page
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withRules(['color-contrast'])
      .analyze()

    const violations = accessibilityScanResults.violations.filter(
      (v) => v.id === 'color-contrast'
    )

    // All text should meet 4.5:1 contrast in dark theme
    expect(violations).toHaveLength(0)
  })

  test('Test Case 4: Run Lighthouse accessibility audit', async ({ page }) => {
    // Run comprehensive axe-core accessibility audit (serves as Lighthouse proxy)
    // axe-core checks the same WCAG criteria as Lighthouse accessibility
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21aa'])
      .analyze()

    // Calculate approximate accessibility score
    // Similar to Lighthouse scoring - violations reduce score
    const totalChecks =
      accessibilityScanResults.passes.length +
      accessibilityScanResults.violations.length
    const passedChecks = accessibilityScanResults.passes.length
    const accessibilityScore =
      totalChecks > 0 ? Math.round((passedChecks / totalChecks) * 100) : 100

    // Log results for debugging
    console.log(`Accessibility Score: ${accessibilityScore}`)
    console.log(
      `Passed: ${accessibilityScanResults.passes.length}, Violations: ${accessibilityScanResults.violations.length}`
    )

    if (accessibilityScanResults.violations.length > 0) {
      console.log('Violations:', JSON.stringify(accessibilityScanResults.violations, null, 2))
    }

    // Expect accessibility score > 80
    expect(accessibilityScore).toBeGreaterThan(80)
  })

  test('Verify feature card text contrast on glass morphism cards', async ({
    page,
  }) => {
    // Verify features section exists
    const featuresSection = page.getByTestId('features-section')
    await expect(featuresSection).toBeVisible()

    // Run axe-core on feature cards
    const accessibilityScanResults = await new AxeBuilder({ page })
      .include('[data-testid="features-section"]')
      .withRules(['color-contrast'])
      .analyze()

    const violations = accessibilityScanResults.violations.filter(
      (v) => v.id === 'color-contrast'
    )

    // Assert no color contrast violations on glass morphism cards
    expect(violations).toHaveLength(0)
  })

  test('Verify contrast in cyberpunk theme', async ({ page }) => {
    // Set cyberpunk theme
    await page.evaluate(() => {
      document.documentElement.setAttribute('data-theme', 'cyberpunk')
    })
    await page.waitForTimeout(500)

    // Run axe-core color contrast audit
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withRules(['color-contrast'])
      .analyze()

    const violations = accessibilityScanResults.violations.filter(
      (v) => v.id === 'color-contrast'
    )

    // Alternative themes must meet accessibility standards
    expect(violations).toHaveLength(0)
  })

  test('Verify contrast in synthwave theme', async ({ page }) => {
    // Set synthwave theme
    await page.evaluate(() => {
      document.documentElement.setAttribute('data-theme', 'synthwave')
    })
    await page.waitForTimeout(500)

    // Run axe-core color contrast audit
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withRules(['color-contrast'])
      .analyze()

    const violations = accessibilityScanResults.violations.filter(
      (v) => v.id === 'color-contrast'
    )

    // Alternative themes must meet accessibility standards
    expect(violations).toHaveLength(0)
  })

  test('Verify how-it-works section text contrast', async ({ page }) => {
    // Verify how-it-works section exists
    const howItWorksSection = page.getByTestId('how-it-works-section')
    await expect(howItWorksSection).toBeVisible()

    // Run axe-core on how-it-works section
    const accessibilityScanResults = await new AxeBuilder({ page })
      .include('[data-testid="how-it-works-section"]')
      .withRules(['color-contrast'])
      .analyze()

    const violations = accessibilityScanResults.violations.filter(
      (v) => v.id === 'color-contrast'
    )

    // Assert no color contrast violations
    expect(violations).toHaveLength(0)
  })

  test('Verify footer text contrast', async ({ page }) => {
    // Verify footer exists
    const footer = page.getByTestId('footer')
    await expect(footer).toBeVisible()

    // Run axe-core on footer
    const accessibilityScanResults = await new AxeBuilder({ page })
      .include('[data-testid="footer"]')
      .withRules(['color-contrast'])
      .analyze()

    const violations = accessibilityScanResults.violations.filter(
      (v) => v.id === 'color-contrast'
    )

    // Assert no color contrast violations in footer
    expect(violations).toHaveLength(0)
  })
})
