/**
 * E2E Tests for Semantic HTML Structure
 * Owner: Scenario 11 - Accessibility Compliance
 *
 * Test Case 4: Page uses header, main, nav, section, article, footer elements appropriately
 */

import { test, expect } from '@playwright/test'

test.describe('Test Case 4: Semantic HTML Structure', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')
  })

  test('page has a header element', async ({ page }) => {
    const header = await page.$('header')
    expect(header).not.toBeNull()
  })

  test('page has a main element', async ({ page }) => {
    const main = await page.$('main')
    expect(main).not.toBeNull()
  })

  test('page has nav elements', async ({ page }) => {
    const navElements = await page.$$('nav')
    expect(navElements.length).toBeGreaterThan(0)
  })

  test('nav elements have appropriate aria-label', async ({ page }) => {
    const navElements = await page.$$('nav')

    for (const nav of navElements) {
      const ariaLabel = await nav.getAttribute('aria-label')
      expect(ariaLabel).not.toBeNull()
      expect(ariaLabel).not.toBe('')
    }
  })

  test('page has section elements', async ({ page }) => {
    const sections = await page.$$('section')
    expect(sections.length).toBeGreaterThan(0)
  })

  test('page has a footer element', async ({ page }) => {
    const footer = await page.$('footer')
    expect(footer).not.toBeNull()
  })

  test('semantic structure follows logical order', async ({ page }) => {
    // Verify header comes before main
    const headerIndex = await page.evaluate(() => {
      const elements = Array.from(document.querySelectorAll('header, main, footer'))
      const header = elements.find((el) => el.tagName === 'HEADER')
      return header ? elements.indexOf(header) : -1
    })

    const mainIndex = await page.evaluate(() => {
      const elements = Array.from(document.querySelectorAll('header, main, footer'))
      const main = elements.find((el) => el.tagName === 'MAIN')
      return main ? elements.indexOf(main) : -1
    })

    const footerIndex = await page.evaluate(() => {
      const elements = Array.from(document.querySelectorAll('header, main, footer'))
      const footer = elements.find((el) => el.tagName === 'FOOTER')
      return footer ? elements.indexOf(footer) : -1
    })

    expect(headerIndex).toBeLessThan(mainIndex)
    expect(mainIndex).toBeLessThan(footerIndex)
  })

  test('there is exactly one main element', async ({ page }) => {
    const mainElements = await page.$$('main')
    expect(mainElements.length).toBe(1)
  })
})
