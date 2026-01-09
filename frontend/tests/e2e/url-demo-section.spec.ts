import { test, expect } from '@playwright/test'

test.describe('URL Demo Section E2E Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')
  })

  // Test Case 1: Check for URL demo section on homepage
  test('demo section is visible on homepage', async ({ page }) => {
    // Scroll to demo section
    const demoSection = page.getByTestId('url-demo-section')
    await demoSection.scrollIntoViewIfNeeded()

    // Verify demo section is visible
    await expect(demoSection).toBeVisible()

    // Verify heading is present
    const heading = page.getByTestId('demo-heading')
    await expect(heading).toBeVisible()
    await expect(heading).toHaveText('Try It Out')

    // Verify description is present
    const description = page.getByTestId('demo-description')
    await expect(description).toBeVisible()
    await expect(description).toContainText('shorten')
  })

  // Test Case 2: Interact with URL demo input field
  test('input field accepts text entry', async ({ page }) => {
    const demoSection = page.getByTestId('url-demo-section')
    await demoSection.scrollIntoViewIfNeeded()

    // Find the input field
    const input = page.getByTestId('demo-url-input')
    await expect(input).toBeVisible()

    // Type a URL into the input
    await input.fill('https://example.com/very-long-url-that-needs-shortening')

    // Verify the input contains the URL
    await expect(input).toHaveValue('https://example.com/very-long-url-that-needs-shortening')
  })

  // Test Case 3: View demo output
  test('demo shows example shortened URL format when URL is entered', async ({ page }) => {
    const demoSection = page.getByTestId('url-demo-section')
    await demoSection.scrollIntoViewIfNeeded()

    // Enter a URL
    const input = page.getByTestId('demo-url-input')
    await input.fill('https://example.com/test')

    // Check that the shortened URL is displayed
    const shortenedUrl = page.getByTestId('demo-shortened-url')
    await expect(shortenedUrl).toBeVisible()

    // Verify the format matches expected pattern
    const urlText = await shortenedUrl.textContent()
    expect(urlText).toMatch(/https:\/\/short\.url\/[a-z0-9]+/)
  })

  test('shows placeholder text initially', async ({ page }) => {
    const demoSection = page.getByTestId('url-demo-section')
    await demoSection.scrollIntoViewIfNeeded()

    // Check placeholder is visible
    const placeholder = page.getByTestId('demo-output-placeholder')
    await expect(placeholder).toBeVisible()
    await expect(placeholder).toHaveText('Your short URL will appear here')
  })

  test('placeholder disappears when URL is entered', async ({ page }) => {
    const demoSection = page.getByTestId('url-demo-section')
    await demoSection.scrollIntoViewIfNeeded()

    const input = page.getByTestId('demo-url-input')
    await input.fill('https://test.com')

    // Placeholder should not be visible
    const placeholder = page.getByTestId('demo-output-placeholder')
    await expect(placeholder).not.toBeVisible()

    // Shortened URL should be visible
    const shortenedUrl = page.getByTestId('demo-shortened-url')
    await expect(shortenedUrl).toBeVisible()
  })

  test('copy button is disabled when no URL is entered', async ({ page }) => {
    const demoSection = page.getByTestId('url-demo-section')
    await demoSection.scrollIntoViewIfNeeded()

    const copyButton = page.getByTestId('demo-copy-button')
    await expect(copyButton).toBeDisabled()
  })

  test('copy button is enabled when URL is entered', async ({ page }) => {
    const demoSection = page.getByTestId('url-demo-section')
    await demoSection.scrollIntoViewIfNeeded()

    const input = page.getByTestId('demo-url-input')
    await input.fill('https://example.com')

    const copyButton = page.getByTestId('demo-copy-button')
    await expect(copyButton).toBeEnabled()
  })

  test('clicking copy button changes its label', async ({ page }) => {
    const demoSection = page.getByTestId('url-demo-section')
    await demoSection.scrollIntoViewIfNeeded()

    const input = page.getByTestId('demo-url-input')
    await input.fill('https://example.com')

    const copyButton = page.getByTestId('demo-copy-button')

    // Check initial state
    await expect(copyButton).toHaveAttribute('aria-label', 'Copy shortened URL')

    // Click copy
    await copyButton.click()

    // Check copied state
    await expect(copyButton).toHaveAttribute('aria-label', 'Copied to clipboard')
  })

  test('demo section includes disclaimer text', async ({ page }) => {
    const demoSection = page.getByTestId('url-demo-section')
    await demoSection.scrollIntoViewIfNeeded()

    const disclaimer = page.getByTestId('demo-disclaimer')
    await expect(disclaimer).toBeVisible()
    await expect(disclaimer).toContainText('demo preview')
  })

  test('demo section includes sign up CTA', async ({ page }) => {
    const demoSection = page.getByTestId('url-demo-section')
    await demoSection.scrollIntoViewIfNeeded()

    const ctaSection = page.getByTestId('demo-cta-section')
    await expect(ctaSection).toBeVisible()

    const signupCta = page.getByTestId('demo-signup-cta')
    await expect(signupCta).toBeVisible()
    await expect(signupCta).toHaveAttribute('href', '/register')
  })

  test('generates different shortened URLs for different inputs', async ({ page }) => {
    const demoSection = page.getByTestId('url-demo-section')
    await demoSection.scrollIntoViewIfNeeded()

    const input = page.getByTestId('demo-url-input')

    // Enter first URL
    await input.fill('https://first.com')
    const firstUrl = await page.getByTestId('demo-shortened-url').textContent()

    // Enter second URL
    await input.fill('https://second.com')
    const secondUrl = await page.getByTestId('demo-shortened-url').textContent()

    expect(firstUrl).not.toBe(secondUrl)
  })

  test('demo section has correct id for anchor navigation', async ({ page }) => {
    const demoSection = page.getByTestId('url-demo-section')
    await expect(demoSection).toHaveAttribute('id', 'demo')

    // Test anchor navigation
    await page.goto('/#demo')
    await page.waitForLoadState('networkidle')

    // Demo section should be in view
    await expect(demoSection).toBeInViewport()
  })

  test('URL updates in real-time as user types', async ({ page }) => {
    const demoSection = page.getByTestId('url-demo-section')
    await demoSection.scrollIntoViewIfNeeded()

    const input = page.getByTestId('demo-url-input')

    // Type character by character and verify URL updates
    await input.type('https://t', { delay: 50 })
    const url1 = await page.getByTestId('demo-shortened-url').textContent()

    await input.type('est.com', { delay: 50 })
    const url2 = await page.getByTestId('demo-shortened-url').textContent()

    // URLs should be different after typing more
    expect(url1).not.toBe(url2)
  })
})
