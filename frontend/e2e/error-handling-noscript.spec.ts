import { test, expect } from '@playwright/test'

test.describe('Error Handling - JavaScript Disabled', () => {
  test('should display noscript message when JavaScript is disabled', async ({ browser }) => {
    // Create a context with JavaScript disabled
    const context = await browser.newContext({
      javaScriptEnabled: false,
    })
    const page = await context.newPage()

    // Navigate to the homepage
    await page.goto('/')

    // The noscript content should be visible when JS is disabled
    const noscriptContent = page.locator('noscript')
    await expect(noscriptContent).toBeAttached()

    // Verify the noscript message content is in the page HTML
    const pageContent = await page.content()
    expect(pageContent).toContain('JavaScript Required')
    expect(pageContent).toContain('URL Shortener')
    expect(pageContent).toContain('enable JavaScript')

    await context.close()
  })

  test('should show meaningful fallback content structure in noscript', async ({ browser }) => {
    // Create a context with JavaScript disabled
    const context = await browser.newContext({
      javaScriptEnabled: false,
    })
    const page = await context.newPage()

    await page.goto('/')

    // Get the page HTML to check noscript content
    const pageContent = await page.content()

    // Verify key information is present in the noscript block
    expect(pageContent).toContain('URL Shortener')
    expect(pageContent).toContain('JavaScript Required')
    expect(pageContent).toContain('noscript-fallback')

    await context.close()
  })

  test('should have properly styled noscript fallback', async ({ browser }) => {
    // Create a context with JavaScript disabled
    const context = await browser.newContext({
      javaScriptEnabled: false,
    })
    const page = await context.newPage()

    await page.goto('/')

    // Verify inline styles are present for proper display
    const pageContent = await page.content()

    // Check that critical styles are present in noscript
    expect(pageContent).toContain('font-family')
    expect(pageContent).toContain('text-align: center')
    expect(pageContent).toContain('min-height')

    await context.close()
  })
})
