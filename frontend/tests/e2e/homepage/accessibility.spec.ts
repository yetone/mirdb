/**
 * Accessibility E2E Tests
 * Owner: Scenario 11 - Keyboard Navigation, Scenario 12 - Screen Reader Compatibility
 *
 * E2E tests to verify:
 * - All interactive elements can be accessed using keyboard
 * - Focus indicators are visible on focused elements
 * - Buttons can be activated with Enter/Space keys
 * - Tab order is logical
 * - Screen reader compatibility (semantic HTML, ARIA attributes, heading hierarchy)
 */

import { test, expect } from '@playwright/test'
import { HomePage, viewports, testUrls } from './fixtures'

test.describe('Accessibility - Keyboard Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(viewports.desktop)
  })

  test('Tab from page start to Get Started button reaches it in logical order', async ({ page }) => {
    const homePage = new HomePage(page)
    await homePage.goto()

    // Wait for the page to be fully loaded and animations complete
    await page.waitForLoadState('networkidle')
    await page.waitForTimeout(500)

    // Start tabbing from the beginning of the page
    // First, focus on the body to ensure we start from the beginning
    await page.keyboard.press('Tab')

    // Tab through elements and collect focused element info
    const focusedElements: string[] = []
    let foundGetStartedButton = false
    let tabCount = 0
    const maxTabs = 20

    while (tabCount < maxTabs && !foundGetStartedButton) {
      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement
        if (!el) return null
        return {
          tagName: el.tagName,
          testId: el.getAttribute('data-testid'),
          text: el.textContent?.trim().slice(0, 50),
          role: el.getAttribute('role'),
        }
      })

      if (focusedElement) {
        focusedElements.push(
          `${focusedElement.tagName}:${focusedElement.testId || focusedElement.text}`
        )

        // Check if we've reached the Get Started button (either hero or nav)
        if (
          focusedElement.testId === 'get-started-button' ||
          focusedElement.testId === 'get-started-nav'
        ) {
          foundGetStartedButton = true
        }
      }

      if (!foundGetStartedButton) {
        await page.keyboard.press('Tab')
        tabCount++
      }
    }

    // Verify we found the Get Started button via Tab navigation
    expect(foundGetStartedButton).toBe(true)
    expect(tabCount).toBeLessThan(maxTabs) // Should find it within reasonable tab count
  })

  test('Focused CTA buttons have visible focus indicators (outline or ring)', async ({ page }) => {
    const homePage = new HomePage(page)
    await homePage.goto()
    await page.waitForLoadState('networkidle')
    await page.waitForTimeout(500)

    // Focus the Get Started button in hero section
    const getStartedButton = page.getByTestId('get-started-button')
    await getStartedButton.focus()

    // Check that the button has a visible focus state
    const focusStyles = await getStartedButton.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        outline: styles.outline,
        outlineWidth: styles.outlineWidth,
        outlineStyle: styles.outlineStyle,
        outlineColor: styles.outlineColor,
        boxShadow: styles.boxShadow,
        // DaisyUI uses ring utility for focus states
        ringColor: styles.getPropertyValue('--tw-ring-color'),
      }
    })

    // Check for visible focus indication (either outline or box-shadow/ring)
    const hasOutline =
      focusStyles.outlineWidth !== '0px' && focusStyles.outlineStyle !== 'none'
    const hasBoxShadow = focusStyles.boxShadow !== 'none' && focusStyles.boxShadow !== ''

    expect(hasOutline || hasBoxShadow).toBe(true)

    // Also test Sign In button
    const signInButton = page.getByTestId('sign-in-button')
    await signInButton.focus()

    const signInFocusStyles = await signInButton.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        outline: styles.outline,
        outlineWidth: styles.outlineWidth,
        outlineStyle: styles.outlineStyle,
        boxShadow: styles.boxShadow,
      }
    })

    const signInHasOutline =
      signInFocusStyles.outlineWidth !== '0px' && signInFocusStyles.outlineStyle !== 'none'
    const signInHasBoxShadow =
      signInFocusStyles.boxShadow !== 'none' && signInFocusStyles.boxShadow !== ''

    expect(signInHasOutline || signInHasBoxShadow).toBe(true)
  })

  test('Get Started button activates on Enter key and navigates to /register', async ({ page }) => {
    const homePage = new HomePage(page)
    await homePage.goto()
    await page.waitForLoadState('networkidle')
    await page.waitForTimeout(500)

    // Focus the Get Started button in hero section
    const getStartedButton = page.getByTestId('get-started-button')
    await getStartedButton.focus()

    // Verify button is focused
    const isFocused = await getStartedButton.evaluate(
      (el) => document.activeElement === el
    )
    expect(isFocused).toBe(true)

    // Press Enter to activate
    await page.keyboard.press('Enter')

    // Wait for navigation
    await page.waitForURL(/\/register/)

    // Verify navigation to register page
    expect(page.url()).toContain(testUrls.register)
  })

  test('All navigation links are focusable in sequence', async ({ page }) => {
    const homePage = new HomePage(page)
    await homePage.goto()
    await page.waitForLoadState('networkidle')
    await page.waitForTimeout(500)

    // Define the expected navigation links we should be able to tab to
    const expectedNavElements = [
      { name: 'URLShortener brand link', pattern: /urlshortener/i },
      { name: 'Features link', pattern: /features/i },
      { name: 'How It Works link', pattern: /how it works/i },
    ]

    // Start tabbing and collect links we encounter
    const foundLinks: string[] = []
    let tabCount = 0
    const maxTabs = 15

    // Press Tab to start
    await page.keyboard.press('Tab')

    while (tabCount < maxTabs) {
      const focusedInfo = await page.evaluate(() => {
        const el = document.activeElement
        if (!el) return null
        const tagName = el.tagName.toLowerCase()
        const isLink = tagName === 'a'
        const isNavLink =
          isLink &&
          (el.getAttribute('href')?.startsWith('#') ||
            el.getAttribute('href') === '/')

        return {
          tagName,
          text: el.textContent?.trim(),
          href: el.getAttribute('href'),
          isNavLink,
        }
      })

      if (focusedInfo && focusedInfo.isNavLink) {
        foundLinks.push(focusedInfo.text || focusedInfo.href || '')
      }

      await page.keyboard.press('Tab')
      tabCount++
    }

    // Verify we found navigation links
    expect(foundLinks.length).toBeGreaterThan(0)

    // Check that we found at least the brand link and anchor links
    const foundBrandLink = foundLinks.some((link) =>
      link.toLowerCase().includes('urlshortener')
    )
    const foundFeaturesLink = foundLinks.some(
      (link) => link.toLowerCase().includes('features') || link.includes('#features')
    )
    const foundHowItWorksLink = foundLinks.some(
      (link) =>
        link.toLowerCase().includes('how it works') || link.includes('#how-it-works')
    )

    expect(foundBrandLink).toBe(true)
    expect(foundFeaturesLink).toBe(true)
    expect(foundHowItWorksLink).toBe(true)
  })

  test('Theme toggle can be focused and activated with keyboard', async ({ page }) => {
    const homePage = new HomePage(page)
    await homePage.goto()
    await page.waitForLoadState('networkidle')
    await page.waitForTimeout(500)

    // Find the theme toggle button
    const themeToggle = page.getByTestId('theme-toggle')
    await expect(themeToggle).toBeVisible()

    // Get initial theme state
    const initialTheme = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme')
    })

    // Focus the theme toggle via Tab navigation
    let foundThemeToggle = false
    let tabCount = 0
    const maxTabs = 20

    await page.keyboard.press('Tab')

    while (tabCount < maxTabs && !foundThemeToggle) {
      const focusedTestId = await page.evaluate(() => {
        return document.activeElement?.getAttribute('data-testid')
      })

      if (focusedTestId === 'theme-toggle') {
        foundThemeToggle = true
      } else {
        await page.keyboard.press('Tab')
        tabCount++
      }
    }

    // Verify theme toggle is focusable
    expect(foundThemeToggle).toBe(true)

    // Verify it has proper aria-label for accessibility
    const ariaLabel = await themeToggle.getAttribute('aria-label')
    expect(ariaLabel).toBeTruthy()
    expect(ariaLabel).toMatch(/switch to (light|dark) mode/i)

    // Activate with Enter key
    await page.keyboard.press('Enter')

    // Wait for theme change to take effect
    await page.waitForTimeout(100)

    // Verify theme changed
    const newTheme = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme')
    })

    expect(newTheme).not.toBe(initialTheme)

    // Also verify Space key works for activation
    await page.keyboard.press('Space')
    await page.waitForTimeout(100)

    const finalTheme = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme')
    })

    // Theme should have toggled back
    expect(finalTheme).toBe(initialTheme)
  })
})

test.describe('Accessibility - Screen Reader Compatibility', () => {
  let homePage: HomePage

  test.beforeEach(async ({ page }) => {
    homePage = new HomePage(page)
    await homePage.goto()
  })

  test('page has exactly one h1 element (main headline)', async ({ page }) => {
    const h1Elements = page.locator('h1')
    await expect(h1Elements).toHaveCount(1)

    const h1Text = await h1Elements.first().textContent()
    expect(h1Text).toContain('Shorten. Share. Track.')
  })

  test('headings follow logical order (h1 > h2 > h3, no skipping)', async ({
    page,
  }) => {
    // Get all heading elements in document order
    const headings = await page
      .locator('h1, h2, h3, h4, h5, h6')
      .evaluateAll((elements) =>
        elements.map((el) => ({
          tag: el.tagName.toLowerCase(),
          level: parseInt(el.tagName[1], 10),
          text: el.textContent?.trim().substring(0, 50) || '',
        }))
      )

    expect(headings.length).toBeGreaterThan(0)

    // First heading should be h1
    expect(headings[0].level).toBe(1)

    // Verify no heading skips more than one level when going deeper
    for (let i = 1; i < headings.length; i++) {
      const currentLevel = headings[i].level
      const previousLevel = headings[i - 1].level

      // When going deeper (to a smaller heading), should not skip levels
      if (currentLevel > previousLevel) {
        expect(currentLevel - previousLevel).toBeLessThanOrEqual(1)
      }
    }
  })

  test('all images have non-empty alt text or are marked decorative', async ({
    page,
  }) => {
    // Check img elements
    const images = page.locator('img')
    const imageCount = await images.count()

    for (let i = 0; i < imageCount; i++) {
      const img = images.nth(i)
      const hasAlt = await img.evaluate(
        (el) =>
          el.hasAttribute('alt') ||
          el.getAttribute('aria-hidden') === 'true' ||
          el.getAttribute('role') === 'presentation'
      )
      expect(hasAlt).toBe(true)
    }

    // Check SVG icons are properly marked as decorative or have accessible labels
    const svgs = page.locator('svg')
    const svgCount = await svgs.count()

    for (let i = 0; i < svgCount; i++) {
      const svg = svgs.nth(i)
      const isAccessible = await svg.evaluate(
        (el) =>
          el.getAttribute('aria-hidden') === 'true' ||
          el.hasAttribute('aria-label') ||
          el.hasAttribute('aria-labelledby') ||
          el.getAttribute('role') === 'img'
      )
      expect(isAccessible).toBe(true)
    }
  })

  test('navigation uses <nav> element or role="navigation"', async ({
    page,
  }) => {
    // Check for nav elements or elements with role="navigation"
    const navElements = page.locator('nav, [role="navigation"]')
    const navCount = await navElements.count()

    expect(navCount).toBeGreaterThanOrEqual(1)

    // Verify at least one navigation landmark exists
    const navigationLandmarks = page.getByRole('navigation')
    await expect(navigationLandmarks.first()).toBeVisible()
  })

  test('main content uses <main> element or role="main"', async ({ page }) => {
    // Check for main element or element with role="main"
    const mainElements = page.locator('main, [role="main"]')
    const mainCount = await mainElements.count()

    // There should be exactly one main landmark
    expect(mainCount).toBe(1)

    // Verify main landmark is present
    const mainLandmark = page.getByRole('main')
    await expect(mainLandmark).toBeVisible()
  })

  test('all sections have aria-labelledby attributes', async ({ page }) => {
    // Check that major sections have aria-labelledby for screen reader context
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toHaveAttribute('aria-labelledby', 'hero-heading')

    const featuresSection = page.getByTestId('features-section')
    await expect(featuresSection).toHaveAttribute(
      'aria-labelledby',
      'features-heading'
    )

    const howItWorksSection = page.getByTestId('how-it-works-section')
    await expect(howItWorksSection).toHaveAttribute(
      'aria-labelledby',
      'how-it-works-heading'
    )
  })

  test('interactive elements have accessible names', async ({ page }) => {
    // Check Get Started button has accessible name
    const getStartedButton = page.getByTestId('get-started-button')
    await expect(getStartedButton).toHaveAttribute(
      'aria-label',
      'Get started with URL shortening for free'
    )

    // Check Sign In button has accessible name
    const signInButton = page.getByTestId('sign-in-button')
    await expect(signInButton).toHaveAttribute(
      'aria-label',
      'Sign in to your account'
    )

    // Check hamburger menu button has accessible label
    await page.setViewportSize(viewports.mobile)
    const hamburgerButton = page.getByTestId('hamburger-menu')
    const hamburgerLabel = await hamburgerButton.getAttribute('aria-label')
    expect(hamburgerLabel).toMatch(/open menu|close menu/i)
  })

  test('footer has contentinfo role', async ({ page }) => {
    const footer = page.getByTestId('footer-section')
    await expect(footer).toHaveAttribute('role', 'contentinfo')
  })
})
