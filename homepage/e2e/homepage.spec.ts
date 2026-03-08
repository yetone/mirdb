import { test, expect } from '@playwright/test'

test.describe('Homepage Header', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  // Test Case 5: Load homepage and inspect header
  test('header is fixed at top with logo and name prominently displayed', async ({ page }) => {
    // Verify header is visible immediately without scrolling
    const header = page.locator('header')
    await expect(header).toBeVisible()

    // Verify header is at the top of the page
    const headerBox = await header.boundingBox()
    expect(headerBox).toBeTruthy()
    expect(headerBox!.y).toBeLessThanOrEqual(10) // Allow small margin

    // Verify header has sticky positioning
    const position = await header.evaluate((el) => {
      return window.getComputedStyle(el).position
    })
    expect(position).toBe('sticky')

    // Verify logo is visible and prominently displayed
    const logo = page.locator('header img[alt*="MirDB"]')
    await expect(logo).toBeVisible()

    // Verify logo has reasonable size (at least 32px)
    const logoBox = await logo.boundingBox()
    expect(logoBox).toBeTruthy()
    expect(logoBox!.width).toBeGreaterThanOrEqual(32)
    expect(logoBox!.height).toBeGreaterThanOrEqual(32)

    // Verify project name "MirDB" is visible
    const projectName = page.locator('header').getByText('MirDB', { exact: true })
    await expect(projectName).toBeVisible()

    // Verify project name is styled appropriately (is text, has larger font)
    const fontSize = await projectName.evaluate((el) => {
      return window.getComputedStyle(el).fontSize
    })
    const fontSizeValue = parseFloat(fontSize)
    expect(fontSizeValue).toBeGreaterThanOrEqual(20) // At least 20px for prominence
  })

  test('header remains visible when scrolling', async ({ page }) => {
    // Add some content to enable scrolling
    await page.evaluate(() => {
      const content = document.createElement('div')
      content.style.height = '2000px'
      document.body.appendChild(content)
    })

    // Scroll down the page
    await page.evaluate(() => window.scrollTo(0, 500))

    // Verify header is still visible
    const header = page.locator('header')
    await expect(header).toBeVisible()

    // Verify header is still at top of viewport
    const headerTop = await header.evaluate((el) => {
      return el.getBoundingClientRect().top
    })
    expect(headerTop).toBe(0)
  })

  test('logo image loads successfully', async ({ page }) => {
    const logo = page.locator('header img[alt*="MirDB"]')
    const srcAttribute = await logo.getAttribute('src')

    // Verify src attribute exists and points to a valid image path
    expect(srcAttribute).toBeTruthy()
    expect(srcAttribute).toContain('logo')

    // Verify the image loads properly (naturalWidth > 0)
    const isLoaded = await logo.evaluate((img: HTMLImageElement) => {
      return img.complete && img.naturalWidth > 0
    })
    expect(isLoaded).toBe(true)
  })
})
