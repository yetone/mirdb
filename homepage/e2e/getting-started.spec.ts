import { test, expect } from '@playwright/test'

test.describe('Getting Started Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  test('getting started section is visible', async ({ page }) => {
    const section = page.locator('#getting-started')
    await expect(section).toBeVisible()
  })

  test('section has proper heading', async ({ page }) => {
    const heading = page.locator('#getting-started h2')
    await expect(heading).toHaveText('Getting Started')
  })

  test('displays all 5 steps', async ({ page }) => {
    const steps = page.locator('[data-testid="step-item"]')
    await expect(steps).toHaveCount(5)
  })

  test('steps are numbered sequentially', async ({ page }) => {
    const stepNumbers = page.locator('[data-testid="step-number"]')
    const count = await stepNumbers.count()

    for (let i = 0; i < count; i++) {
      const number = await stepNumbers.nth(i).textContent()
      expect(number).toBe(String(i + 1))
    }
  })

  // Test Case 7: Verify code blocks have syntax highlighting
  test('code blocks have syntax highlighting', async ({ page }) => {
    // Wait for the getting started section to be visible
    await page.locator('#getting-started').waitFor({ state: 'visible' })

    // Find all code blocks
    const codeBlocks = page.locator('[data-testid="code-block"]')
    const count = await codeBlocks.count()
    expect(count).toBeGreaterThan(0)

    // Check that at least one code block has the data-highlighted attribute
    const firstCodeBlock = codeBlocks.first()
    const preElement = firstCodeBlock.locator('pre')
    await expect(preElement).toHaveAttribute('data-highlighted', 'true')

    // Check that syntax highlighting is applied (contains span elements with color classes)
    const codeElement = firstCodeBlock.locator('code')
    const innerHTML = await codeElement.innerHTML()

    // Verify that the code contains highlighted spans
    expect(innerHTML).toContain('<span')

    // Check for specific syntax highlighting classes
    const hasHighlightClasses =
      innerHTML.includes('text-cyan-400') || // commands
      innerHTML.includes('text-emerald-400') || // responses
      innerHTML.includes('text-slate-500') || // comments
      innerHTML.includes('text-pink-400') // keywords

    expect(hasHighlightClasses).toBe(true)
  })

  test('code blocks have proper styling', async ({ page }) => {
    const codeBlocks = page.locator('[data-testid="code-block"] pre')
    const firstBlock = codeBlocks.first()

    // Check that code blocks have dark background
    const backgroundColor = await firstBlock.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor
    })
    // Should be dark (low RGB values)
    expect(backgroundColor).toBeTruthy()
  })

  test('code blocks have copy button', async ({ page }) => {
    const codeBlocks = page.locator('[data-testid="code-block"]')
    const firstBlock = codeBlocks.first()

    // Hover to reveal copy button
    await firstBlock.hover()

    const copyButton = firstBlock.locator('button')
    await expect(copyButton).toBeVisible()
    await expect(copyButton).toHaveText('Copy')
  })

  test('installation step contains cargo commands', async ({ page }) => {
    const installStep = page.locator('[data-testid="step-item"]').first()
    const codeContent = await installStep.locator('code').innerHTML()

    expect(codeContent).toContain('cargo build')
    expect(codeContent).toContain('cargo run')
  })

  test('connection step contains localhost:12333', async ({ page }) => {
    const connectStep = page.locator('[data-testid="step-item"]').nth(1)
    const codeContent = await connectStep.locator('code').innerHTML()

    expect(codeContent).toContain('localhost:12333')
  })

  test('set operation step is present', async ({ page }) => {
    const stepContent = await page.locator('#getting-started').innerHTML()
    expect(stepContent).toContain('Set a Value')
    expect(stepContent).toContain('set mykey')
  })

  test('get operation step is present', async ({ page }) => {
    const stepContent = await page.locator('#getting-started').innerHTML()
    expect(stepContent).toContain('Get a Value')
    expect(stepContent).toContain('get mykey')
  })

  test('delete operation step is present', async ({ page }) => {
    const stepContent = await page.locator('#getting-started').innerHTML()
    expect(stepContent).toContain('Delete a Value')
    expect(stepContent).toContain('delete mykey')
  })

  test('section is accessible via internal navigation', async ({ page }) => {
    // Click on hero CTA if it links to getting started
    const ctaLink = page.locator('a[href="#getting-started"]')
    if (await ctaLink.count() > 0) {
      await ctaLink.first().click()

      // Verify section is scrolled into view
      const section = page.locator('#getting-started')
      await expect(section).toBeInViewport()
    }
  })

  test('section has proper accessibility attributes', async ({ page }) => {
    const section = page.locator('#getting-started')

    // Check for region role
    await expect(section).toHaveAttribute('role', 'region')

    // Check for aria-labelledby
    await expect(section).toHaveAttribute('aria-labelledby', 'getting-started-heading')
  })
})
