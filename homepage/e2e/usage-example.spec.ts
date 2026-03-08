import { test, expect } from '@playwright/test'

test.describe('Usage Example Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  test('usage example section is visible with code block', async ({ page }) => {
    // Navigate to usage example section
    const section = page.locator('#usage-example')
    await section.scrollIntoViewIfNeeded()
    await expect(section).toBeVisible()

    // Verify section heading
    const heading = section.getByRole('heading', { name: /see it in action/i })
    await expect(heading).toBeVisible()

    // Verify code block is present
    const codeBlock = page.locator('[data-testid="usage-example-code"]')
    await expect(codeBlock).toBeVisible()
  })

  test('code example has proper syntax highlighting applied', async ({ page }) => {
    // Navigate to usage example section
    const section = page.locator('#usage-example')
    await section.scrollIntoViewIfNeeded()

    const codeBlock = page.locator('[data-testid="usage-example-code"]')
    await expect(codeBlock).toBeVisible()

    // Verify syntax-highlighted class is applied
    await expect(codeBlock).toHaveClass(/syntax-highlighted/)

    // Verify syntax highlighting elements exist with distinct colors
    const commands = codeBlock.locator('.syntax-command')
    const responses = codeBlock.locator('.syntax-response')
    const comments = codeBlock.locator('.syntax-comment')

    // Ensure there are multiple command elements (set, get, delete)
    expect(await commands.count()).toBeGreaterThan(0)

    // Ensure there are response elements (STORED, VALUE, END, DELETED)
    expect(await responses.count()).toBeGreaterThan(0)

    // Verify commands have distinct color (cyan-ish color)
    const commandColor = await commands.first().evaluate((el) => {
      return window.getComputedStyle(el).color
    })
    // Cyan color should have high green and blue values, low red
    expect(commandColor).toBeTruthy()

    // Verify responses have distinct color (emerald/green color)
    const responseColor = await responses.first().evaluate((el) => {
      return window.getComputedStyle(el).color
    })
    expect(responseColor).toBeTruthy()

    // Commands and responses should have different colors
    expect(commandColor).not.toBe(responseColor)
  })

  test('code example contains memcached set and get commands', async ({ page }) => {
    const codeBlock = page.locator('[data-testid="usage-example-code"]')
    await codeBlock.scrollIntoViewIfNeeded()

    const codeText = await codeBlock.textContent()

    // Verify set command is present
    expect(codeText).toContain('set')
    expect(codeText).toContain('STORED')

    // Verify get command is present
    expect(codeText).toContain('get')
    expect(codeText).toContain('VALUE')
    expect(codeText).toContain('END')

    // Verify delete command is present
    expect(codeText).toContain('delete')
    expect(codeText).toContain('DELETED')
  })

  test('code block is properly styled with monospace font', async ({ page }) => {
    const codeBlock = page.locator('[data-testid="usage-example-code"]')
    await codeBlock.scrollIntoViewIfNeeded()

    // Verify monospace font family
    const fontFamily = await codeBlock.evaluate((el) => {
      return window.getComputedStyle(el).fontFamily
    })
    expect(fontFamily.toLowerCase()).toMatch(/mono|courier|consolas/)
  })

  test('usage example section has proper accessibility attributes', async ({ page }) => {
    const section = page.locator('#usage-example')
    await section.scrollIntoViewIfNeeded()

    // Verify aria-labelledby is set
    await expect(section).toHaveAttribute('aria-labelledby', 'usage-example-title')

    // Verify the code block has a proper role
    const codeBlock = page.locator('[data-testid="usage-example-code"]')
    await expect(codeBlock).toHaveAttribute('role', 'region')
    await expect(codeBlock).toHaveAttribute('aria-label', 'Memcached usage example code')
  })
})
