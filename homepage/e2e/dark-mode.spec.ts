/**
 * Dark Mode Theme E2E Tests (Scenario 19)
 * Owner: Scenario 19 - Dark Mode Theme
 *
 * Requirements:
 * - NFR-3: Modern, clean aesthetic with dark mode preference
 * - WCAG 2.1 AA: 4.5:1 contrast ratio for normal text
 * - Dark backgrounds with light text
 * - Code blocks with appropriate dark theme styling
 *
 * Test Cases:
 * - TC1: Check page background color (luminance < 50%)
 * - TC2: Check text color (light colored against dark background)
 * - TC3: Verify dark mode contrast ratios (4.5:1)
 * - TC4: Check code block styling in dark mode
 */

import { test, expect } from '@playwright/test'

/**
 * Helper function to calculate relative luminance per WCAG 2.1
 * @see https://www.w3.org/TR/WCAG21/#dfn-relative-luminance
 */
function getRelativeLuminance(r: number, g: number, b: number): number {
  const [rs, gs, bs] = [r, g, b].map((c) => {
    const sRGB = c / 255
    return sRGB <= 0.03928
      ? sRGB / 12.92
      : Math.pow((sRGB + 0.055) / 1.055, 2.4)
  })
  return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs
}

/**
 * Helper function to calculate contrast ratio per WCAG 2.1
 * @see https://www.w3.org/TR/WCAG21/#dfn-contrast-ratio
 */
function getContrastRatio(l1: number, l2: number): number {
  const lighter = Math.max(l1, l2)
  const darker = Math.min(l1, l2)
  return (lighter + 0.05) / (darker + 0.05)
}

/**
 * Helper function to parse CSS color to RGB
 */
function parseColor(color: string): { r: number; g: number; b: number } | null {
  // Match rgb(r, g, b) or rgba(r, g, b, a)
  const rgbMatch = color.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/)
  if (rgbMatch) {
    return {
      r: parseInt(rgbMatch[1], 10),
      g: parseInt(rgbMatch[2], 10),
      b: parseInt(rgbMatch[3], 10),
    }
  }
  return null
}

test.describe('Dark Mode Theme (Scenario 19)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('domcontentloaded')
  })

  /**
   * Test Case 1: Check page background color
   * Expected: Background is dark (luminance < 50%)
   */
  test('page background is dark with luminance less than 50%', async ({ page }) => {
    // Get the main container background color
    const bgInfo = await page.evaluate(() => {
      // Check multiple elements to find the effective background
      const body = document.body
      const root = document.getElementById('root')
      const mainContainer = root?.firstElementChild as HTMLElement

      const getEffectiveBackground = (el: HTMLElement | null): string => {
        if (!el) return 'rgb(0, 0, 0)'
        const computed = window.getComputedStyle(el)
        const bgColor = computed.backgroundColor
        // If transparent, check parent
        if (bgColor === 'rgba(0, 0, 0, 0)' || bgColor === 'transparent') {
          return getEffectiveBackground(el.parentElement)
        }
        return bgColor
      }

      return {
        bodyBg: window.getComputedStyle(body).backgroundColor,
        containerBg: mainContainer ? window.getComputedStyle(mainContainer).backgroundColor : null,
        effectiveBg: getEffectiveBackground(mainContainer || body),
      }
    })

    // Parse the effective background color
    const bgColor = parseColor(bgInfo.effectiveBg) || parseColor(bgInfo.bodyBg)
    expect(bgColor).not.toBeNull()

    // Calculate luminance
    const luminance = getRelativeLuminance(bgColor!.r, bgColor!.g, bgColor!.b)

    // Luminance should be less than 50% (0.5) for dark mode
    // slate-900 (#0f172a) has luminance ~0.03 (3%)
    expect(luminance).toBeLessThan(0.5)

    // Additionally verify it's actually a dark background (luminance < 10%)
    expect(luminance).toBeLessThan(0.1)
  })

  /**
   * Test Case 2: Check text color
   * Expected: Text is light colored against dark background
   */
  test('text is light colored against dark background', async ({ page }) => {
    // Get text colors from various elements
    const textColors = await page.evaluate(() => {
      const results: Array<{
        element: string
        color: string
        text: string
        hasGradientText: boolean
      }> = []

      // Helper to detect gradient text (bg-clip-text text-transparent)
      const hasGradient = (el: Element): boolean => {
        const computed = window.getComputedStyle(el as HTMLElement)
        const isTransparent = computed.color === 'rgba(0, 0, 0, 0)'
        const hasBackgroundGradient = computed.backgroundImage.includes('gradient')
        return isTransparent && hasBackgroundGradient
      }

      // Check body text color
      const body = document.body
      results.push({
        element: 'body',
        color: window.getComputedStyle(body).color,
        text: 'body default',
        hasGradientText: false,
      })

      // Check heading colors (skip h1 which uses gradient text)
      const h2s = document.querySelectorAll('h2')
      h2s.forEach((h2, i) => {
        if (i < 2) {
          results.push({
            element: 'h2',
            color: window.getComputedStyle(h2).color,
            text: h2.textContent?.slice(0, 30) || '',
            hasGradientText: hasGradient(h2),
          })
        }
      })

      // Check paragraph colors
      const paragraphs = document.querySelectorAll('p')
      paragraphs.forEach((p, i) => {
        if (i < 3) {
          results.push({
            element: `p[${i}]`,
            color: window.getComputedStyle(p).color,
            text: p.textContent?.slice(0, 30) || '',
            hasGradientText: hasGradient(p),
          })
        }
      })

      return results
    })

    // Verify we found text elements
    expect(textColors.length).toBeGreaterThan(0)

    // Check that text colors are light (high luminance)
    let checkedCount = 0
    for (const textInfo of textColors) {
      // Skip gradient text - these use gradient fill which provides contrast visually
      if (textInfo.hasGradientText) continue

      const color = parseColor(textInfo.color)
      if (!color) continue

      // Skip transparent colors (from gradient text elements)
      if (color.r === 0 && color.g === 0 && color.b === 0) {
        // Check if text is really black or transparent gradient text
        if (textInfo.color.includes('0)')) continue // rgba with 0 alpha
      }

      const luminance = getRelativeLuminance(color.r, color.g, color.b)

      // Text luminance should be > 50% for light text on dark background
      // white has luminance 1.0, slate-300 has ~0.65
      expect(luminance).toBeGreaterThan(0.5)
      checkedCount++
    }

    // Ensure we checked at least some text elements
    expect(checkedCount).toBeGreaterThan(0)
  })

  /**
   * Test Case 3: Verify dark mode contrast ratios
   * Expected: Text meets 4.5:1 contrast in dark mode
   *
   * Note: This test focuses on main page elements styled by globals.css.
   * Elements inside feature cards (owned by Scenario 3) are excluded as they
   * use component-specific styling.
   */
  test('text meets 4.5:1 contrast ratio in dark mode', async ({ page }) => {
    // Get text and background colors for contrast calculation
    const contrastData = await page.evaluate(() => {
      const results: Array<{
        element: string
        textColor: string
        bgColor: string
        text: string
        fontSize: string
        fontWeight: string
        hasGradientText: boolean
        isInsideCard: boolean
      }> = []

      const getEffectiveBackground = (el: Element | null): string => {
        if (!el || el === document.documentElement) {
          // Default dark background (slate-900)
          return 'rgb(15, 23, 42)'
        }
        const computed = window.getComputedStyle(el as HTMLElement)
        const bgColor = computed.backgroundColor
        if (bgColor === 'rgba(0, 0, 0, 0)' || bgColor === 'transparent') {
          return getEffectiveBackground(el.parentElement)
        }
        return bgColor
      }

      // Helper to detect gradient text (bg-clip-text text-transparent)
      const hasGradient = (el: Element): boolean => {
        const computed = window.getComputedStyle(el as HTMLElement)
        const isTransparent = computed.color === 'rgba(0, 0, 0, 0)'
        const hasBackgroundGradient = computed.backgroundImage.includes('gradient')
        return isTransparent && hasBackgroundGradient
      }

      // Helper to check if element is inside a card (owned by other scenarios)
      const isInsideCard = (el: Element): boolean => {
        return el.closest('[data-testid="feature-card"]') !== null ||
               el.closest('[data-testid="roadmap-item"]') !== null ||
               el.closest('article') !== null
      }

      // Sample various text elements (skip h1 which uses gradient)
      // Focus on section headings and paragraphs directly on the page
      const selectors = ['h2', 'p']

      for (const selector of selectors) {
        const elements = document.querySelectorAll(selector)
        elements.forEach((el, i) => {
          if (i >= 3) return // Sample 3 of each type
          const text = el.textContent?.trim()
          if (!text || text.length === 0) return

          const computed = window.getComputedStyle(el as HTMLElement)

          results.push({
            element: selector,
            textColor: computed.color,
            bgColor: getEffectiveBackground(el),
            text: text.slice(0, 40),
            fontSize: computed.fontSize,
            fontWeight: computed.fontWeight,
            hasGradientText: hasGradient(el),
            isInsideCard: isInsideCard(el),
          })
        })
      }

      return results
    })

    // Verify we have text elements to check
    expect(contrastData.length).toBeGreaterThan(0)

    // Check contrast ratio for each element
    let checkedCount = 0
    for (const item of contrastData) {
      // Skip gradient text elements - they use CSS gradient which provides visual contrast
      if (item.hasGradientText) continue

      // Skip elements inside cards - those are styled by their owner scenarios
      if (item.isInsideCard) continue

      const textColor = parseColor(item.textColor)
      const bgColor = parseColor(item.bgColor)

      if (!textColor || !bgColor) continue

      // Skip transparent text (gradient text effect)
      if (textColor.r === 0 && textColor.g === 0 && textColor.b === 0 &&
          item.textColor.includes('0)')) continue

      const textLuminance = getRelativeLuminance(textColor.r, textColor.g, textColor.b)
      const bgLuminance = getRelativeLuminance(bgColor.r, bgColor.g, bgColor.b)
      const contrastRatio = getContrastRatio(textLuminance, bgLuminance)

      // Determine if large text (>= 18pt or >= 14pt bold)
      const fontSize = parseFloat(item.fontSize)
      const fontWeight = parseInt(item.fontWeight, 10)
      const isLargeText = fontSize >= 24 || (fontSize >= 18.66 && fontWeight >= 700)

      // WCAG AA: 4.5:1 for normal text, 3:1 for large text
      const requiredRatio = isLargeText ? 3 : 4.5

      expect(contrastRatio).toBeGreaterThanOrEqual(requiredRatio)
      checkedCount++
    }

    // Ensure we checked at least some text elements
    expect(checkedCount).toBeGreaterThan(0)
  })

  /**
   * Test Case 4: Check code block styling in dark mode
   * Expected: Code blocks have appropriate dark theme styling
   */
  test('code blocks have appropriate dark theme styling', async ({ page }) => {
    // Scroll to Getting Started section where code blocks are located
    await page.evaluate(() => {
      const gettingStarted = document.getElementById('getting-started')
      if (gettingStarted) {
        gettingStarted.scrollIntoView()
      }
    })

    // Wait for code blocks to be visible
    const codeBlocks = page.locator('pre, [data-testid="code-block"]')
    const codeBlockCount = await codeBlocks.count()

    // Verify code blocks exist
    expect(codeBlockCount).toBeGreaterThan(0)

    // Check styling for each code block
    for (let i = 0; i < Math.min(codeBlockCount, 3); i++) {
      const codeBlock = codeBlocks.nth(i)

      // Check if code block is visible
      const isVisible = await codeBlock.isVisible()
      if (!isVisible) continue

      // Get code block styles
      const codeStyles = await codeBlock.evaluate((el) => {
        const computed = window.getComputedStyle(el)
        const pre = el.tagName === 'PRE' ? el : el.querySelector('pre')
        const preComputed = pre ? window.getComputedStyle(pre) : computed

        return {
          bgColor: preComputed.backgroundColor,
          textColor: preComputed.color,
          borderColor: preComputed.borderColor,
          borderRadius: preComputed.borderRadius,
          fontFamily: preComputed.fontFamily,
        }
      })

      // Parse colors
      const bgColor = parseColor(codeStyles.bgColor)
      const textColor = parseColor(codeStyles.textColor)

      expect(bgColor).not.toBeNull()
      expect(textColor).not.toBeNull()

      // Code block background should be very dark (slate-950 or similar)
      const bgLuminance = getRelativeLuminance(bgColor!.r, bgColor!.g, bgColor!.b)
      expect(bgLuminance).toBeLessThan(0.05) // Very dark background

      // Code text should be light for readability
      const textLuminance = getRelativeLuminance(textColor!.r, textColor!.g, textColor!.b)
      expect(textLuminance).toBeGreaterThan(0.4) // Light text

      // Verify contrast ratio meets WCAG AA
      const contrastRatio = getContrastRatio(textLuminance, bgLuminance)
      expect(contrastRatio).toBeGreaterThanOrEqual(4.5)

      // Verify monospace font for code
      expect(codeStyles.fontFamily.toLowerCase()).toMatch(/mono|consolas|courier/i)
    }
  })

  /**
   * Additional test: Verify dark mode is applied as default
   * PRD specifies "dark mode preference" for developer tools
   */
  test('dark mode is the default theme without toggling', async ({ page }) => {
    // Check that the page loads with dark theme by default
    const themeInfo = await page.evaluate(() => {
      const root = document.getElementById('root')
      const container = root?.firstElementChild as HTMLElement

      return {
        hasClasses: container?.className || '',
        bodyBg: window.getComputedStyle(document.body).backgroundColor,
      }
    })

    // Parse body background
    const bgColor = parseColor(themeInfo.bodyBg)
    expect(bgColor).not.toBeNull()

    // Verify dark background (slate-900 or similar dark color)
    const bgLuminance = getRelativeLuminance(bgColor!.r, bgColor!.g, bgColor!.b)
    expect(bgLuminance).toBeLessThan(0.1) // Dark mode should have low luminance

    // Verify the container has dark mode classes
    expect(themeInfo.hasClasses).toContain('bg-slate')
    expect(themeInfo.hasClasses).toContain('text-white')
  })

  /**
   * Additional test: Verify all sections maintain dark theme
   */
  test('all page sections maintain consistent dark theme', async ({ page }) => {
    // Check each major section for dark theme consistency
    const sections = ['#hero', '#features', '#getting-started', '#roadmap']

    for (const sectionId of sections) {
      // Try to find section, skip if not found
      const section = page.locator(sectionId)
      const exists = await section.count() > 0

      if (!exists) continue

      // Scroll to section
      await section.scrollIntoViewIfNeeded()

      // Get section background
      const sectionBg = await section.evaluate((el) => {
        const getEffectiveBg = (element: Element | null): string => {
          if (!element) return 'rgb(15, 23, 42)'
          const computed = window.getComputedStyle(element as HTMLElement)
          if (computed.backgroundColor === 'rgba(0, 0, 0, 0)' ||
              computed.backgroundColor === 'transparent') {
            return getEffectiveBg(element.parentElement)
          }
          return computed.backgroundColor
        }
        return getEffectiveBg(el)
      })

      const bgColor = parseColor(sectionBg)
      if (!bgColor) continue

      // All sections should have dark background
      const luminance = getRelativeLuminance(bgColor.r, bgColor.g, bgColor.b)
      expect(luminance).toBeLessThan(0.15) // Allow some variation for surface colors
    }
  })

  /**
   * Additional test: Verify link colors in dark mode
   */
  test('links are visible and have sufficient contrast in dark mode', async ({ page }) => {
    // Get link colors
    const linkData = await page.evaluate(() => {
      const links = document.querySelectorAll('a[href]:not(.sr-only)')
      const results: Array<{
        color: string
        text: string
        href: string
      }> = []

      links.forEach((link, i) => {
        if (i >= 5) return // Sample first 5 links
        const computed = window.getComputedStyle(link as HTMLElement)
        results.push({
          color: computed.color,
          text: link.textContent?.trim().slice(0, 30) || '',
          href: (link as HTMLAnchorElement).href,
        })
      })

      return results
    })

    // Verify links exist
    expect(linkData.length).toBeGreaterThan(0)

    // Check link colors for visibility
    for (const link of linkData) {
      const color = parseColor(link.color)
      if (!color) continue

      const luminance = getRelativeLuminance(color.r, color.g, color.b)

      // Links should be visible (light colored) on dark background
      // Allow both light links (white, blue-400) and branded colors
      const darkBgLuminance = 0.03 // slate-900 luminance
      const contrastRatio = getContrastRatio(luminance, darkBgLuminance)

      // Links need at least 4.5:1 contrast for accessibility
      expect(contrastRatio).toBeGreaterThanOrEqual(4.5)
    }
  })
})
