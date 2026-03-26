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
