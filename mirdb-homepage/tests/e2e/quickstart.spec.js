/**
 * QuickStart Section E2E Tests
 * Owner: Scenario 4 - Quick Start Guide Section
 *
 * Test coverage:
 * - Test Case 6: Link to full documentation is visible and functional
 * - Test Case 7: Click copy on installation command - copied with visual feedback
 */

import { test, expect } from '@playwright/test'

test.describe('QuickStart Section E2E', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    // Wait for the quickstart section to be rendered
    await page.waitForSelector('[data-testid="quickstart-section"]')
  })

  test.describe('Test Case 6: Check link to full documentation', () => {
    test('documentation link should be visible', async ({ page }) => {
      const quickstartSection = page.locator('[data-testid="quickstart-section"]')
      await quickstartSection.scrollIntoViewIfNeeded()

      const docsLink = page.locator('[data-testid="quickstart-docs-link"]')
      await expect(docsLink).toBeVisible()
    })

    test('documentation link should have correct href', async ({ page }) => {
      const docsLink = page.locator('[data-testid="quickstart-docs-link"]')
      const href = await docsLink.getAttribute('href')

      expect(href).toBeTruthy()
      expect(href).toMatch(/github|docs|readme/i)
    })

    test('documentation link should have descriptive text', async ({ page }) => {
      const docsLink = page.locator('[data-testid="quickstart-docs-link"]')
      const text = await docsLink.textContent()

      expect(text.toLowerCase()).toMatch(/documentation|docs|learn more|read more/)
    })

    test('documentation link should open in new tab', async ({ page }) => {
      const docsLink = page.locator('[data-testid="quickstart-docs-link"]')
      const target = await docsLink.getAttribute('target')
      const rel = await docsLink.getAttribute('rel')

      expect(target).toBe('_blank')
      expect(rel).toContain('noopener')
    })

    test('documentation link should be clickable', async ({ page, context }) => {
      const quickstartSection = page.locator('[data-testid="quickstart-section"]')
      await quickstartSection.scrollIntoViewIfNeeded()

      const docsLink = page.locator('[data-testid="quickstart-docs-link"]')

      // Listen for new page/tab
      const pagePromise = context.waitForEvent('page')

      await docsLink.click()

      // Verify new page was opened
      const newPage = await pagePromise
      expect(newPage.url()).toMatch(/github/i)
      await newPage.close()
    })
  })

  test.describe('Test Case 7: Click copy on installation command', () => {
    test('should show visual feedback when copy button is clicked', async ({ page }) => {
      const quickstartSection = page.locator('[data-testid="quickstart-section"]')
      await quickstartSection.scrollIntoViewIfNeeded()

      // Find the installation code block
      const installBlock = page.locator('[data-testid="code-block-install"]')
      await expect(installBlock).toBeVisible()

      // Hover to show copy button
      await installBlock.hover()

      // Find and click the copy button
      const copyButton = installBlock.locator('.copy-button')
      await expect(copyButton).toBeVisible()

      // Grant clipboard permissions
      await page.context().grantPermissions(['clipboard-write', 'clipboard-read'])

      // Click the copy button
      await copyButton.click()

      // Verify visual feedback - toast should appear
      const toast = page.locator('[data-testid="copy-toast"]')
      await expect(toast).toBeVisible()
      await expect(toast).toContainText('Copied to clipboard')
    })

    test('should copy correct installation command to clipboard', async ({ page }) => {
      const quickstartSection = page.locator('[data-testid="quickstart-section"]')
      await quickstartSection.scrollIntoViewIfNeeded()

      const installBlock = page.locator('[data-testid="code-block-install"]')
      await installBlock.hover()

      const copyButton = installBlock.locator('.copy-button')

      // Grant clipboard permissions
      await page.context().grantPermissions(['clipboard-write', 'clipboard-read'])

      await copyButton.click()

      // Read clipboard content
      const clipboardContent = await page.evaluate(async () => {
        return await navigator.clipboard.readText()
      })

      // Verify the copied content matches expected install command
      expect(clipboardContent).toContain('cargo install mirdb')
    })

    test('copy button should show checkmark after successful copy', async ({ page }) => {
      const quickstartSection = page.locator('[data-testid="quickstart-section"]')
      await quickstartSection.scrollIntoViewIfNeeded()

      const installBlock = page.locator('[data-testid="code-block-install"]')
      await installBlock.hover()

      const copyButton = installBlock.locator('.copy-button')
      await page.context().grantPermissions(['clipboard-write', 'clipboard-read'])

      await copyButton.click()

      // Check for success class
      await expect(copyButton).toHaveClass(/copy-success/)

      // Icon should change to checkmark
      const checkIcon = copyButton.locator('.check-icon')
      await expect(checkIcon).toBeVisible()
    })

    test('toast should disappear after timeout', async ({ page }) => {
      const quickstartSection = page.locator('[data-testid="quickstart-section"]')
      await quickstartSection.scrollIntoViewIfNeeded()

      const installBlock = page.locator('[data-testid="code-block-install"]')
      await installBlock.hover()

      const copyButton = installBlock.locator('.copy-button')
      await page.context().grantPermissions(['clipboard-write', 'clipboard-read'])

      await copyButton.click()

      const toast = page.locator('[data-testid="copy-toast"]')
      await expect(toast).toBeVisible()

      // Toast should disappear after timeout
      await expect(toast).toBeHidden({ timeout: 5000 })
    })
  })

  test.describe('QuickStart section visibility and content', () => {
    test('quickstart section should be visible and properly labeled', async ({ page }) => {
      const quickstartSection = page.locator('[data-testid="quickstart-section"]')
      await expect(quickstartSection).toBeVisible()

      // Check section heading
      const heading = quickstartSection.locator('h2')
      await expect(heading).toContainText('Quick Start')
    })

    test('installation section should be visible', async ({ page }) => {
      const installSection = page.locator('[data-testid="quickstart-install"]')
      await expect(installSection).toBeVisible()
    })

    test('usage section should be visible', async ({ page }) => {
      const usageSection = page.locator('[data-testid="quickstart-usage"]')
      await expect(usageSection).toBeVisible()
    })

    test('should display cargo install command', async ({ page }) => {
      const quickstartSection = page.locator('[data-testid="quickstart-section"]')
      const text = await quickstartSection.textContent()

      expect(text).toContain('cargo install mirdb')
    })

    test('should display connection example with localhost:12333', async ({ page }) => {
      const quickstartSection = page.locator('[data-testid="quickstart-section"]')
      const text = await quickstartSection.textContent()

      expect(text).toContain('localhost')
      expect(text).toContain('12333')
    })

    test('should display configuration file example', async ({ page }) => {
      const quickstartSection = page.locator('[data-testid="quickstart-section"]')
      const text = await quickstartSection.textContent()

      expect(text).toContain('mirdb.toml')
    })
  })

  test.describe('Accessibility', () => {
    test('copy buttons should be keyboard accessible', async ({ page }) => {
      const quickstartSection = page.locator('[data-testid="quickstart-section"]')
      await quickstartSection.scrollIntoViewIfNeeded()

      const installBlock = page.locator('[data-testid="code-block-install"]')
      const copyButton = installBlock.locator('.copy-button')

      // Should be focusable
      await copyButton.focus()
      await expect(copyButton).toBeFocused()

      // Should have aria-label
      const ariaLabel = await copyButton.getAttribute('aria-label')
      expect(ariaLabel).toBeTruthy()
    })

    test('copy button should be activatable via keyboard', async ({ page }) => {
      const quickstartSection = page.locator('[data-testid="quickstart-section"]')
      await quickstartSection.scrollIntoViewIfNeeded()

      const installBlock = page.locator('[data-testid="code-block-install"]')
      const copyButton = installBlock.locator('.copy-button')

      await page.context().grantPermissions(['clipboard-write', 'clipboard-read'])

      // Focus and press Enter
      await copyButton.focus()
      await page.keyboard.press('Enter')

      // Visual feedback should appear
      const toast = page.locator('[data-testid="copy-toast"]')
      await expect(toast).toBeVisible()
    })

    test('documentation link should be keyboard accessible', async ({ page }) => {
      const docsLink = page.locator('[data-testid="quickstart-docs-link"]')

      // Should be focusable
      await docsLink.focus()
      await expect(docsLink).toBeFocused()
    })
  })

  test.describe('Copy all code blocks', () => {
    test('server start command copy should work', async ({ page }) => {
      const quickstartSection = page.locator('[data-testid="quickstart-section"]')
      await quickstartSection.scrollIntoViewIfNeeded()

      const serverBlock = page.locator('[data-testid="code-block-server-start"]')
      await serverBlock.hover()

      const copyButton = serverBlock.locator('.copy-button')
      await page.context().grantPermissions(['clipboard-write', 'clipboard-read'])

      await copyButton.click()

      const clipboardContent = await page.evaluate(async () => {
        return await navigator.clipboard.readText()
      })

      expect(clipboardContent).toContain('mirdb')
    })

    test('connection command copy should work', async ({ page }) => {
      const quickstartSection = page.locator('[data-testid="quickstart-section"]')
      await quickstartSection.scrollIntoViewIfNeeded()

      const connectBlock = page.locator('[data-testid="code-block-connect"]')
      await connectBlock.hover()

      const copyButton = connectBlock.locator('.copy-button')
      await page.context().grantPermissions(['clipboard-write', 'clipboard-read'])

      await copyButton.click()

      const clipboardContent = await page.evaluate(async () => {
        return await navigator.clipboard.readText()
      })

      expect(clipboardContent).toContain('telnet localhost 12333')
    })

    test('config file copy should work', async ({ page }) => {
      const quickstartSection = page.locator('[data-testid="quickstart-section"]')
      await quickstartSection.scrollIntoViewIfNeeded()

      const configBlock = page.locator('[data-testid="code-block-config"]')
      await configBlock.hover()

      const copyButton = configBlock.locator('.copy-button')
      await page.context().grantPermissions(['clipboard-write', 'clipboard-read'])

      await copyButton.click()

      const clipboardContent = await page.evaluate(async () => {
        return await navigator.clipboard.readText()
      })

      expect(clipboardContent).toContain('mirdb.toml')
      expect(clipboardContent).toContain('port')
      expect(clipboardContent).toContain('data_dir')
    })
  })
})
