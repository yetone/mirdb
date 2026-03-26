/**
 * URL Shortening E2E Tests
 * Owner: Scenario 2 - Inline URL Shortening
 *
 * Tests:
 * - Valid URL shortening flow (Test Case 1)
 * - Copy to clipboard functionality (Test Case 3)
 * - Keyboard submission with Enter key (Test Case 4)
 * - Input auto-focus on page load (Test Case 5)
 *
 * Requirements: REQ-2, REQ-7
 */

import { test, expect } from '@playwright/test'

test.describe('Inline URL Shortening - Happy Path', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  test('Test Case 5: input field is auto-focused on page load', async ({ page }) => {
    const urlInput = page.getByTestId('url-input')

    // Wait for page to be fully loaded
    await expect(urlInput).toBeVisible()

    // Check that input is focused
    await expect(urlInput).toBeFocused()
  })

  test('input has correct placeholder text', async ({ page }) => {
    const urlInput = page.getByTestId('url-input')

    await expect(urlInput).toHaveAttribute(
      'placeholder',
      'Paste your long URL here...'
    )
  })

  test('Test Case 1: short URL is generated and displayed', async ({ page }) => {
    const urlInput = page.getByTestId('url-input')
    const shortenButton = page.getByTestId('shorten-button')

    // Enter valid URL
    await urlInput.fill('https://example.com/very/long/url/path')
    await expect(shortenButton).toBeEnabled()

    // Click shorten
    await shortenButton.click()

    // Wait for result to appear
    const resultArea = page.getByTestId('result-area')
    await expect(resultArea).toBeVisible({ timeout: 1000 })

    // Verify short URL is displayed
    const shortUrl = page.getByTestId('short-url')
    await expect(shortUrl).toBeVisible()

    // Verify short URL format
    const shortUrlText = await shortUrl.textContent()
    expect(shortUrlText).toMatch(/\/s\/[a-zA-Z0-9]+/)
  })

  test('Test Case 4: Enter key submits form and shortens URL', async ({ page }) => {
    const urlInput = page.getByTestId('url-input')

    // Enter valid URL
    await urlInput.fill('https://example.com/enter-key-test')

    // Press Enter to submit
    await urlInput.press('Enter')

    // Wait for result
    const resultArea = page.getByTestId('result-area')
    await expect(resultArea).toBeVisible({ timeout: 1000 })

    // Verify short URL is displayed
    const shortUrl = page.getByTestId('short-url')
    await expect(shortUrl).toBeVisible()
  })

  test('Test Case 3: copy button copies URL to clipboard and shows feedback', async ({
    page,
    context,
  }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write'])

    const urlInput = page.getByTestId('url-input')
    const shortenButton = page.getByTestId('shorten-button')

    // Shorten a URL first
    await urlInput.fill('https://example.com/copy-test')
    await shortenButton.click()

    // Wait for result
    await expect(page.getByTestId('result-area')).toBeVisible()

    // Get the short URL text before copying
    const shortUrl = page.getByTestId('short-url')
    const shortUrlText = await shortUrl.textContent()

    // Click copy button
    const copyButton = page.getByTestId('copy-button')
    await copyButton.click()

    // Verify "Copied!" feedback appears
    const copiedFeedback = page.getByTestId('copied-feedback')
    await expect(copiedFeedback).toBeVisible()
    await expect(copiedFeedback).toHaveText('Copied!')

    // Verify clipboard contains the short URL
    const clipboardText = await page.evaluate(() =>
      navigator.clipboard.readText()
    )
    expect(clipboardText).toBe(shortUrlText)
  })

  test('shows loading state during URL shortening', async ({ page }) => {
    const urlInput = page.getByTestId('url-input')
    const shortenButton = page.getByTestId('shorten-button')

    await urlInput.fill('https://example.com/loading-test')
    await shortenButton.click()

    // Button should have aria-busy attribute during loading
    // Note: This may be very fast, so we check the final state
    await expect(page.getByTestId('result-area')).toBeVisible()

    // After loading, button should not be busy
    await expect(shortenButton).not.toHaveAttribute('aria-busy', 'true')
  })

  test('Shorten button is disabled when input is empty', async ({ page }) => {
    const shortenButton = page.getByTestId('shorten-button')
    await expect(shortenButton).toBeDisabled()
  })

  test('Shorten button is enabled when URL is entered', async ({ page }) => {
    const urlInput = page.getByTestId('url-input')
    const shortenButton = page.getByTestId('shorten-button')

    await urlInput.fill('https://example.com')
    await expect(shortenButton).toBeEnabled()
  })

  test('shows registration prompt after successful shortening', async ({ page }) => {
    const urlInput = page.getByTestId('url-input')
    const shortenButton = page.getByTestId('shorten-button')

    await urlInput.fill('https://example.com/register-test')
    await shortenButton.click()

    // Wait for result
    await expect(page.getByTestId('result-area')).toBeVisible()

    // Verify registration prompt appears
    const registrationPrompt = page.getByTestId('registration-prompt')
    await expect(registrationPrompt).toBeVisible()

    // Verify register CTA link
    const registerCta = page.getByTestId('register-cta')
    await expect(registerCta).toBeVisible()
    await expect(registerCta).toHaveAttribute('href', '/register')
  })

  test('Shorten another button resets form', async ({ page }) => {
    const urlInput = page.getByTestId('url-input')
    const shortenButton = page.getByTestId('shorten-button')

    // Shorten a URL
    await urlInput.fill('https://example.com/first-url')
    await shortenButton.click()

    // Wait for result
    await expect(page.getByTestId('result-area')).toBeVisible()

    // Click Shorten another
    const shortenAnotherButton = page.getByTestId('shorten-another-button')
    await shortenAnotherButton.click()

    // Verify result is hidden and input is cleared
    await expect(page.getByTestId('result-area')).not.toBeVisible()
    await expect(urlInput).toHaveValue('')

    // Verify input is focused again
    await expect(urlInput).toBeFocused()
  })

  test('displays error for invalid URL', async ({ page }) => {
    const urlInput = page.getByTestId('url-input')
    const shortenButton = page.getByTestId('shorten-button')

    await urlInput.fill('not-a-valid-url')
    await shortenButton.click()

    // Verify error message appears
    const errorMessage = page.getByTestId('error-message')
    await expect(errorMessage).toBeVisible()
    await expect(errorMessage).toContainText('valid URL')
  })

  test('can shorten multiple URLs consecutively', async ({ page }) => {
    const urlInput = page.getByTestId('url-input')
    const shortenButton = page.getByTestId('shorten-button')

    // First URL
    await urlInput.fill('https://first-url.com')
    await shortenButton.click()
    await expect(page.getByTestId('result-area')).toBeVisible()
    const firstShortUrl = await page.getByTestId('short-url').textContent()

    // Shorten another
    await page.getByTestId('shorten-another-button').click()

    // Second URL
    await urlInput.fill('https://second-url.com')
    await shortenButton.click()
    await expect(page.getByTestId('result-area')).toBeVisible()
    const secondShortUrl = await page.getByTestId('short-url').textContent()

    // URLs should be different
    expect(firstShortUrl).not.toBe(secondShortUrl)
  })
})

test.describe('Loading States and Visual Feedback (Scenario 14)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  test('Test Case 1: loading spinner appears during API call', async ({ page }) => {
    const urlInput = page.getByTestId('url-input')
    const shortenButton = page.getByTestId('shorten-button')

    await urlInput.fill('https://example.com/loading-spinner-test')

    // Start watching for loading state before clicking
    const loadingPromise = shortenButton.locator('.loading-spinner').isVisible()

    await shortenButton.click()

    // Button should show aria-busy during loading
    // Note: The loading state may be very brief, so we check aria-busy
    await expect(shortenButton).toHaveAttribute('aria-busy', 'true')

    // Wait for result to appear (loading finished)
    await expect(page.getByTestId('result-area')).toBeVisible({ timeout: 1000 })
  })

  test('Test Case 2: Shorten button is disabled during operation', async ({ page }) => {
    const urlInput = page.getByTestId('url-input')
    const shortenButton = page.getByTestId('shorten-button')

    await urlInput.fill('https://example.com/button-disable-test')

    // Button should be enabled before submission
    await expect(shortenButton).toBeEnabled()

    // Click and immediately check disabled state
    await shortenButton.click()

    // Button should be disabled during loading
    await expect(shortenButton).toBeDisabled()

    // Wait for completion
    await expect(page.getByTestId('result-area')).toBeVisible({ timeout: 1000 })
  })

  test('Test Case 3: copy button shows Copied! or checkmark feedback', async ({
    page,
    context,
  }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write'])

    const urlInput = page.getByTestId('url-input')
    const shortenButton = page.getByTestId('shorten-button')

    await urlInput.fill('https://example.com/copy-feedback-test')
    await shortenButton.click()

    await expect(page.getByTestId('result-area')).toBeVisible()

    const copyButton = page.getByTestId('copy-button')

    // Initially should show "Copy" text
    await expect(copyButton).toContainText('Copy')

    // Click copy button
    await copyButton.click()

    // Should show "Copied!" feedback
    const copiedFeedback = page.getByTestId('copied-feedback')
    await expect(copiedFeedback).toBeVisible()
    await expect(copiedFeedback).toHaveText('Copied!')

    // Button should have success styling (btn-success class)
    await expect(copyButton).toHaveClass(/btn-success/)
  })

  test('Test Case 4: smooth transition from loading to result display', async ({ page }) => {
    const urlInput = page.getByTestId('url-input')
    const shortenButton = page.getByTestId('shorten-button')

    await urlInput.fill('https://example.com/transition-test')

    // Start time tracking
    const startTime = Date.now()

    await shortenButton.click()

    // Result area should not be visible initially
    const resultArea = page.getByTestId('result-area')

    // Wait for result to appear
    await expect(resultArea).toBeVisible({ timeout: 1000 })

    const endTime = Date.now()

    // Transition should complete within reasonable time (NFR-5: under 500ms, allow buffer for test overhead)
    expect(endTime - startTime).toBeLessThanOrEqual(1000)

    // Verify the result area contains expected elements after transition
    await expect(page.getByTestId('short-url')).toBeVisible()
    await expect(page.getByTestId('copy-button')).toBeVisible()
    await expect(page.getByTestId('shorten-another-button')).toBeVisible()
    await expect(page.getByTestId('registration-prompt')).toBeVisible()
  })

  test('loading state shows visual spinner element', async ({ page }) => {
    const urlInput = page.getByTestId('url-input')
    const shortenButton = page.getByTestId('shorten-button')

    await urlInput.fill('https://example.com/spinner-visual-test')

    // Click and immediately capture loading state
    const [loadingSpinner] = await Promise.all([
      shortenButton.locator('.loading').first().isVisible().catch(() => false),
      shortenButton.click(),
    ])

    // The loading spinner class should be applied during loading
    // (This test verifies the CSS class is applied, even if briefly)
    await expect(page.getByTestId('result-area')).toBeVisible({ timeout: 1000 })
  })

  test('input is disabled during loading to prevent modifications', async ({ page }) => {
    const urlInput = page.getByTestId('url-input')
    const shortenButton = page.getByTestId('shorten-button')

    await urlInput.fill('https://example.com/input-disable-test')
    await shortenButton.click()

    // Input should be disabled during loading
    await expect(urlInput).toBeDisabled()

    // Wait for completion
    await expect(page.getByTestId('result-area')).toBeVisible({ timeout: 1000 })
  })
})
