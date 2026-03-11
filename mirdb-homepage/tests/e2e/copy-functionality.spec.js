/**
 * Copy Functionality E2E Tests
 * Owner: Scenario 3 - Interactive Demo Section
 *
 * Test coverage:
 * - Test Case 5: Copy button click in demo section
 * - Copy button click in quick start (if present)
 * - Clipboard content verification
 * - Visual feedback display (toast/checkmark)
 */

import { test, expect } from '@playwright/test'

test.describe('Copy Functionality E2E', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    // Wait for the demo section to be rendered
    await page.waitForSelector('[data-testid="interactive-demo"]')
  })

  test.describe('Test Case 5: Click copy button on SET example', () => {
    test('should show visual feedback when copy button is clicked', async ({ page }) => {
      // Scroll to demo section
      const demoSection = page.locator('[data-testid="interactive-demo"]')
      await demoSection.scrollIntoViewIfNeeded()

      // Find the SET command copy button
      const setExample = page.locator('[data-testid="command-example-set"]')
      await expect(setExample).toBeVisible()

      // Hover over code block to show copy button
      const codeBlock = setExample.locator('[data-testid="code-block-set"]')
      await codeBlock.hover()

      // Find and click the copy button
      const copyButton = setExample.locator('[data-testid="copy-button-set"]')
      await expect(copyButton).toBeVisible()

      // Grant clipboard permissions
      await page.context().grantPermissions(['clipboard-write', 'clipboard-read'])

      // Click the copy button
      await copyButton.click()

      // Verify visual feedback - toast should appear
      const toast = page.locator('[data-testid="copy-toast"]')
      await expect(toast).toBeVisible()
      await expect(toast).toContainText('Copied to clipboard')

      // Toast should disappear after timeout
      await expect(toast).toBeHidden({ timeout: 5000 })
    })

    test('should show checkmark icon after copying', async ({ page }) => {
      const setExample = page.locator('[data-testid="command-example-set"]')
      await setExample.scrollIntoViewIfNeeded()

      const codeBlock = setExample.locator('[data-testid="code-block-set"]')
      await codeBlock.hover()

      const copyButton = setExample.locator('[data-testid="copy-button-set"]')
      await page.context().grantPermissions(['clipboard-write', 'clipboard-read'])

      await copyButton.click()

      // Check for success class or checkmark icon
      await expect(copyButton).toHaveClass(/copy-success/)

      // Icon should change to checkmark
      const checkIcon = copyButton.locator('.check-icon')
      await expect(checkIcon).toBeVisible()
    })

    test('should copy correct command to clipboard', async ({ page }) => {
      const setExample = page.locator('[data-testid="command-example-set"]')
      await setExample.scrollIntoViewIfNeeded()

      const codeBlock = setExample.locator('[data-testid="code-block-set"]')
      await codeBlock.hover()

      const copyButton = setExample.locator('[data-testid="copy-button-set"]')

      // Grant clipboard permissions
      await page.context().grantPermissions(['clipboard-write', 'clipboard-read'])

      await copyButton.click()

      // Read clipboard content
      const clipboardContent = await page.evaluate(async () => {
        return await navigator.clipboard.readText()
      })

      // Verify the copied content matches expected format
      expect(clipboardContent).toContain('set mykey')
      expect(clipboardContent).toContain('myval')
    })
  })

  test.describe('Copy functionality for all commands', () => {
    test('should copy GET command when copy button is clicked', async ({ page }) => {
      const getExample = page.locator('[data-testid="command-example-get"]')
      await getExample.scrollIntoViewIfNeeded()

      const codeBlock = getExample.locator('[data-testid="code-block-get"]')
      await codeBlock.hover()

      const copyButton = getExample.locator('[data-testid="copy-button-get"]')
      await page.context().grantPermissions(['clipboard-write', 'clipboard-read'])

      await copyButton.click()

      const clipboardContent = await page.evaluate(async () => {
        return await navigator.clipboard.readText()
      })

      expect(clipboardContent).toContain('get mykey')
    })

    test('should copy DELETE command when copy button is clicked', async ({ page }) => {
      const deleteExample = page.locator('[data-testid="command-example-delete"]')
      await deleteExample.scrollIntoViewIfNeeded()

      const codeBlock = deleteExample.locator('[data-testid="code-block-delete"]')
      await codeBlock.hover()

      const copyButton = deleteExample.locator('[data-testid="copy-button-delete"]')
      await page.context().grantPermissions(['clipboard-write', 'clipboard-read'])

      await copyButton.click()

      const clipboardContent = await page.evaluate(async () => {
        return await navigator.clipboard.readText()
      })

      expect(clipboardContent).toContain('delete mykey')
    })
  })

  test.describe('Accessibility and keyboard navigation', () => {
    test('copy button should be focusable and have accessible label', async ({ page }) => {
      const copyButton = page.locator('[data-testid="copy-button-set"]')

      // Should be focusable via keyboard
      await copyButton.focus()
      await expect(copyButton).toBeFocused()

      // Should have aria-label
      const ariaLabel = await copyButton.getAttribute('aria-label')
      expect(ariaLabel).toContain('Copy')
    })

    test('copy button should be activatable via keyboard', async ({ page }) => {
      const setExample = page.locator('[data-testid="command-example-set"]')
      await setExample.scrollIntoViewIfNeeded()

      const copyButton = setExample.locator('[data-testid="copy-button-set"]')
      await page.context().grantPermissions(['clipboard-write', 'clipboard-read'])

      // Focus and press Enter
      await copyButton.focus()
      await page.keyboard.press('Enter')

      // Visual feedback should appear
      const toast = page.locator('[data-testid="copy-toast"]')
      await expect(toast).toBeVisible()
    })
  })

  test.describe('Demo section visibility', () => {
    test('demo section should be labeled and visible', async ({ page }) => {
      const demoSection = page.locator('[data-testid="interactive-demo"]')
      await expect(demoSection).toBeVisible()

      // Check section heading
      const heading = demoSection.locator('.demo-title')
      await expect(heading).toHaveText('Try It Out')
    })

    test('all three command examples should be visible', async ({ page }) => {
      const setExample = page.locator('[data-testid="command-example-set"]')
      const getExample = page.locator('[data-testid="command-example-get"]')
      const deleteExample = page.locator('[data-testid="command-example-delete"]')

      await expect(setExample).toBeVisible()
      await expect(getExample).toBeVisible()
      await expect(deleteExample).toBeVisible()
    })
  })
})
