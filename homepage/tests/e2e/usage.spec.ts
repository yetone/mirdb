/**
 * Usage Section E2E Tests
 * Owner: Scenario 5 - Usage Demonstration
 *
 * Tests for the usage section functionality including:
 * - Test case 3: Verify usage.gif loads successfully without 404 error
 * - Test case 8: Verify example command copy buttons work correctly
 */
import { test, expect } from '@playwright/test'

test.describe('Usage Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    // Scroll to usage section
    await page.evaluate(() => {
      document.getElementById('usage')?.scrollIntoView()
    })
  })

  // Test case 3: Verify usage.gif loads successfully without 404 error
  test('usage.gif image loads successfully without 404 error', async ({ page }) => {
    // Get the usage GIF image
    const usageGif = page.getByTestId('usage-gif')
    await expect(usageGif).toBeVisible()

    // Verify the src attribute
    await expect(usageGif).toHaveAttribute('src', '/assets/usage.gif')

    // Check that the image actually loaded (no 404)
    const imgSrc = await usageGif.getAttribute('src')
    const response = await page.request.get(imgSrc!)
    expect(response.status()).toBe(200)
    expect(response.headers()['content-type']).toContain('image')
  })

  test('usage section renders with correct heading', async ({ page }) => {
    const usageSection = page.locator('#usage')
    await expect(usageSection).toBeVisible()

    // Check for Usage heading
    const heading = usageSection.locator('h2', { hasText: 'Usage' })
    await expect(heading).toBeVisible()
  })

  test('usage section displays command table with all 10 commands', async ({ page }) => {
    const commandTable = page.getByTestId('command-table')
    await expect(commandTable).toBeVisible()

    // Check for all 10 commands
    const commands = ['get', 'gets', 'set', 'add', 'replace', 'append', 'prepend', 'delete', 'info', 'major_compaction']

    for (const cmd of commands) {
      const row = page.getByTestId(`command-row-${cmd}`)
      await expect(row).toBeVisible()
    }
  })

  test('command table displays correct syntax for get command', async ({ page }) => {
    const getRow = page.getByTestId('command-row-get')
    await expect(getRow).toContainText('get <key1> <key2> ...')
    await expect(getRow).toContainText('Retrieve values by key')
  })

  test('command table displays correct syntax for set command', async ({ page }) => {
    const setRow = page.getByTestId('command-row-set')
    await expect(setRow).toContainText('set <key> <flags> <ttl> <bytes>')
    await expect(setRow).toContainText('Set a key-value pair')
  })

  test('command table displays correct syntax for delete command', async ({ page }) => {
    const deleteRow = page.getByTestId('command-row-delete')
    await expect(deleteRow).toContainText('delete <key>')
    await expect(deleteRow).toContainText('Delete a key')
  })

  // Test case 8: Verify example command copy buttons work correctly
  test('example commands have copy buttons that work correctly', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write'])

    // Find a copy button in the example blocks
    const setExampleCopyButton = page.getByTestId('example-set-example-copy-button')
    await expect(setExampleCopyButton).toBeVisible()

    // Click the copy button
    await setExampleCopyButton.click()

    // Wait for the button to change to "Copied!" state
    await expect(setExampleCopyButton).toContainText('Copied!')

    // Verify clipboard content
    const clipboardContent = await page.evaluate(() => navigator.clipboard.readText())
    expect(clipboardContent).toContain('set mykey')
  })

  test('config copy button works correctly', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write'])

    // Find the config copy button
    const configCopyButton = page.getByTestId('config-block-copy-button')
    await expect(configCopyButton).toBeVisible()

    // Click the copy button
    await configCopyButton.click()

    // Wait for the button to change to "Copied!" state
    await expect(configCopyButton).toContainText('Copied!')

    // Verify clipboard content contains TOML config
    const clipboardContent = await page.evaluate(() => navigator.clipboard.readText())
    expect(clipboardContent).toContain('addr = "0.0.0.0:12333"')
    expect(clipboardContent).toContain('work_dir = "/tmp/mirdb"')
  })

  test('usage section is accessible via navigation', async ({ page }) => {
    // Navigate to top of page
    await page.goto('/')

    // Scroll to usage section using anchor link behavior
    await page.evaluate(() => {
      window.location.hash = '#usage'
    })

    // Wait for scroll
    await page.waitForTimeout(500)

    // Verify usage section is in viewport
    const usageSection = page.locator('#usage')
    await expect(usageSection).toBeInViewport()
  })

  test('usage section has proper accessibility attributes', async ({ page }) => {
    const usageSection = page.locator('#usage')
    await expect(usageSection).toHaveAttribute('aria-labelledby', 'usage-heading')

    const heading = page.locator('h2#usage-heading')
    await expect(heading).toBeVisible()
  })

  test('copy buttons have accessible labels', async ({ page }) => {
    const copyButtons = page.getByRole('button', { name: /copy/i })
    const count = await copyButtons.count()
    expect(count).toBeGreaterThan(0)

    // Check first copy button has aria-label
    const firstButton = copyButtons.first()
    await expect(firstButton).toHaveAttribute('aria-label')
  })

  test('usage section is responsive on mobile viewport', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 })

    // Reload page with mobile viewport
    await page.goto('/')
    await page.evaluate(() => {
      document.getElementById('usage')?.scrollIntoView()
    })

    // Verify usage elements are still visible
    const usageGif = page.getByTestId('usage-gif')
    await expect(usageGif).toBeVisible()

    const commandTable = page.getByTestId('command-table')
    await expect(commandTable).toBeVisible()

    // Verify table is scrollable (wrapper exists)
    const tableWrapper = page.getByTestId('command-table-wrapper')
    await expect(tableWrapper).toBeVisible()
  })
})
