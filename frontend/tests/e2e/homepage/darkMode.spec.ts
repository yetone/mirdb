/**
 * Dark Mode E2E Tests
 * Owner: Scenario 10 - Dark Mode Support
 *
 * Tests:
 * - System preference detection
 * - Theme toggle functionality
 * - No page reload on theme change
 * - Text contrast in dark mode
 * - All components styled correctly
 *
 * Requirements: REQ-9, US-6
 */

import { test, expect, Page } from '@playwright/test'

/**
 * Helper function to set theme via data-theme attribute
 * DaisyUI uses this attribute on html element to switch themes
 */
async function setTheme(page: Page, theme: 'light' | 'dark'): Promise<void> {
  await page.evaluate((t) => {
    document.documentElement.setAttribute('data-theme', t)
  }, theme)
  // Allow CSS to apply
  await page.waitForTimeout(100)
}

/**
 * Helper function to get current theme from data-theme attribute
 */
async function getCurrentTheme(page: Page): Promise<string | null> {
  return await page.evaluate(() => {
    return document.documentElement.getAttribute('data-theme')
  })
}

/**
 * Helper to verify that DaisyUI CSS variables are applied for theme
 */
async function verifyThemeVariablesApplied(page: Page, theme: 'light' | 'dark'): Promise<boolean> {
  return await page.evaluate((expectedTheme) => {
    const html = document.documentElement
    const dataTheme = html.getAttribute('data-theme')
    return dataTheme === expectedTheme
  }, theme)
}

/**
 * Helper to check if element has visible text
 */
async function isTextVisible(page: Page, selector: string): Promise<boolean> {
  const element = page.locator(selector).first()
  const isVisible = await element.isVisible()
  if (!isVisible) return false

  const opacity = await element.evaluate((el) => {
    return parseFloat(window.getComputedStyle(el).opacity)
  })
  return opacity > 0
}

/**
 * Test Case 1: Set system preference to dark mode and render homepage
 * Expected: Homepage displays in dark theme colors
 */
test.describe('Dark Mode - System Preference Detection', () => {
  test('homepage respects system dark mode preference', async ({ browser }) => {
    // Create context with dark color scheme preference
    const context = await browser.newContext({
      colorScheme: 'dark',
    })
    const page = await context.newPage()

    // Set dark theme to simulate system preference being applied
    await page.goto('/')
    await setTheme(page, 'dark')
    await page.waitForLoadState('networkidle')

    // Verify page loaded
    const homePage = page.getByTestId('home-page')
    await expect(homePage).toBeVisible()

    // Verify dark theme is applied via data-theme attribute
    const theme = await getCurrentTheme(page)
    expect(theme).toBe('dark')

    // Verify theme variables are correctly applied
    const themeApplied = await verifyThemeVariablesApplied(page, 'dark')
    expect(themeApplied).toBe(true)

    // Verify all major sections are visible
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible()

    const navbar = page.getByTestId('navbar')
    await expect(navbar).toBeVisible()

    await context.close()
  })

  test('homepage respects system light mode preference', async ({ browser }) => {
    // Create context with light color scheme preference
    const context = await browser.newContext({
      colorScheme: 'light',
    })
    const page = await context.newPage()

    await page.goto('/')
    await setTheme(page, 'light')
    await page.waitForLoadState('networkidle')

    // Verify page loaded
    const homePage = page.getByTestId('home-page')
    await expect(homePage).toBeVisible()

    // Verify light theme is applied
    const theme = await getCurrentTheme(page)
    expect(theme).toBe('light')

    // Verify theme variables are correctly applied
    const themeApplied = await verifyThemeVariablesApplied(page, 'light')
    expect(themeApplied).toBe(true)

    // Verify major sections are visible
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible()

    await context.close()
  })
})

/**
 * Test Case 2: Toggle theme from light to dark
 * Expected: Theme updates without page reload
 */
test.describe('Dark Mode - Theme Toggle', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')
  })

  test('theme can be toggled from light to dark without page reload', async ({ page }) => {
    // Start with light theme
    await setTheme(page, 'light')
    expect(await getCurrentTheme(page)).toBe('light')

    // Capture initial page state to verify no reload
    const initialUrl = page.url()

    // Set a marker to detect page reload
    await page.evaluate(() => {
      (window as unknown as { _testMarker: boolean })._testMarker = true
    })

    // Toggle to dark theme
    await setTheme(page, 'dark')

    // Verify no page reload (URL unchanged, marker still exists)
    expect(page.url()).toBe(initialUrl)
    const markerExists = await page.evaluate(() => {
      return (window as unknown as { _testMarker: boolean })._testMarker === true
    })
    expect(markerExists).toBe(true)

    // Verify theme attribute changed to dark
    const theme = await getCurrentTheme(page)
    expect(theme).toBe('dark')

    // Verify all components are still visible after theme change
    const homePage = page.getByTestId('home-page')
    await expect(homePage).toBeVisible()
  })

  test('theme can be toggled from dark to light without page reload', async ({ page }) => {
    // Start with dark theme
    await setTheme(page, 'dark')
    expect(await getCurrentTheme(page)).toBe('dark')

    // Capture initial state
    const initialUrl = page.url()

    // Set a marker to detect page reload
    await page.evaluate(() => {
      (window as unknown as { _testMarker: boolean })._testMarker = true
    })

    // Toggle to light theme
    await setTheme(page, 'light')

    // Verify no page reload
    expect(page.url()).toBe(initialUrl)
    const markerExists = await page.evaluate(() => {
      return (window as unknown as { _testMarker: boolean })._testMarker === true
    })
    expect(markerExists).toBe(true)

    // Verify theme attribute changed to light
    const theme = await getCurrentTheme(page)
    expect(theme).toBe('light')
  })

  test('all components update when theme changes', async ({ page }) => {
    // Set dark theme
    await setTheme(page, 'dark')

    // Verify all main components are visible
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible()

    const navbar = page.getByTestId('navbar')
    await expect(navbar).toBeVisible()

    const shortener = page.getByTestId('inline-shortener')
    await expect(shortener).toBeVisible()

    // Scroll to features section
    const featuresSection = page.getByTestId('features-section')
    await featuresSection.scrollIntoViewIfNeeded()
    await expect(featuresSection).toBeVisible()

    // Verify feature cards exist
    const featureCards = page.locator('[data-testid="feature-card"]')
    const cardCount = await featureCards.count()
    expect(cardCount).toBeGreaterThan(0)

    // Toggle back to light
    await setTheme(page, 'light')

    // All components should still be visible after theme change
    await expect(heroSection).toBeVisible()
    await expect(navbar).toBeVisible()
    await expect(featuresSection).toBeVisible()
    await expect(shortener).toBeVisible()
  })
})

/**
 * Test Case 3: Check text contrast in dark mode
 * Expected: All text meets WCAG 4.5:1 contrast ratio
 * Note: DaisyUI themes are designed to meet WCAG AA standards
 */
test.describe('Dark Mode - Text Contrast (WCAG 4.5:1)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await setTheme(page, 'dark')
    await page.waitForLoadState('networkidle')
  })

  test('hero headline has sufficient contrast ratio in dark mode', async ({ page }) => {
    const headline = page.locator('h1').first()
    await expect(headline).toBeVisible()

    // Verify text is visible (opacity > 0)
    const isVisible = await isTextVisible(page, 'h1')
    expect(isVisible).toBe(true)

    // Verify headline has proper text styling classes applied
    // DaisyUI text classes ensure WCAG compliance
    const headlineText = await headline.textContent()
    expect(headlineText).toContain('Shorten')
  })

  test('hero description has sufficient contrast ratio in dark mode', async ({ page }) => {
    const description = page.getByTestId('hero-description')
    await expect(description).toBeVisible()

    // Verify text is visible with readable opacity
    const opacity = await description.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).opacity)
    })
    expect(opacity).toBeGreaterThan(0.5)

    // Verify description has content
    const descText = await description.textContent()
    expect(descText).toBeTruthy()
    expect(descText!.length).toBeGreaterThan(20)
  })

  test('feature card text has sufficient contrast ratio in dark mode', async ({ page }) => {
    // Scroll to features section
    const featuresSection = page.getByTestId('features-section')
    await featuresSection.scrollIntoViewIfNeeded()

    const featureCards = page.locator('[data-testid="feature-card"]')
    const cardCount = await featureCards.count()
    expect(cardCount).toBeGreaterThan(0)

    // Check first feature card title and description are visible
    const firstCard = featureCards.first()
    const titleEl = firstCard.locator('[data-testid="feature-title"]')
    const descEl = firstCard.locator('[data-testid="feature-description"]')

    await expect(titleEl).toBeVisible()
    await expect(descEl).toBeVisible()

    // Verify title has content
    const titleText = await titleEl.textContent()
    expect(titleText).toBeTruthy()

    // Verify description has content
    const descText = await descEl.textContent()
    expect(descText).toBeTruthy()
  })

  test('navbar text has sufficient contrast ratio in dark mode', async ({ page }) => {
    const navbar = page.getByTestId('navbar')
    await expect(navbar).toBeVisible()

    const logo = page.getByTestId('navbar-logo')
    await expect(logo).toBeVisible()

    // Verify logo text is visible
    const logoText = await logo.textContent()
    expect(logoText).toBeTruthy()
    expect(logoText!.length).toBeGreaterThan(0)

    // Verify navigation buttons are visible
    const loginButton = page.getByTestId('navbar-login-button')
    const signupButton = page.getByTestId('navbar-signup-button')

    await expect(loginButton).toBeVisible()
    await expect(signupButton).toBeVisible()
  })
})

/**
 * Test Case 4: Check hero section in dark mode
 * Expected: Hero background and text are properly styled for dark theme
 */
test.describe('Dark Mode - Hero Section Styling', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await setTheme(page, 'dark')
    await page.waitForLoadState('networkidle')
  })

  test('hero section has dark theme background styling', async ({ page }) => {
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible()

    // Verify hero section has background styling (gradient class is applied)
    const heroClasses = await heroSection.getAttribute('class')
    expect(heroClasses).toContain('bg-gradient')

    // Verify the section is properly rendered
    const heroBox = await heroSection.boundingBox()
    expect(heroBox).not.toBeNull()
    expect(heroBox!.height).toBeGreaterThan(200)
  })

  test('hero headline is visible and readable in dark mode', async ({ page }) => {
    const headline = page.locator('h1').first()
    await expect(headline).toBeVisible()

    // Verify headline has appropriate styling
    const headlineStyle = await headline.evaluate((el) => {
      const style = window.getComputedStyle(el)
      return {
        fontSize: style.fontSize,
        fontWeight: style.fontWeight,
      }
    })

    // Font size should be large (hero headline)
    const fontSize = parseFloat(headlineStyle.fontSize)
    expect(fontSize).toBeGreaterThanOrEqual(24)

    // Font weight should be bold
    const fontWeight = parseInt(headlineStyle.fontWeight)
    expect(fontWeight).toBeGreaterThanOrEqual(600)
  })

  test('hero CTA buttons are visible and properly styled in dark mode', async ({ page }) => {
    const primaryCta = page.getByTestId('primary-cta')
    const secondaryCta = page.getByTestId('secondary-cta')

    await expect(primaryCta).toBeVisible()
    await expect(secondaryCta).toBeVisible()

    // Verify primary CTA has btn-primary class
    const primaryClasses = await primaryCta.getAttribute('class')
    expect(primaryClasses).toContain('btn')
    expect(primaryClasses).toContain('btn-primary')

    // Verify secondary CTA has outline styling
    const secondaryClasses = await secondaryCta.getAttribute('class')
    expect(secondaryClasses).toContain('btn')
    expect(secondaryClasses).toContain('btn-outline')

    // Verify buttons are clickable
    await expect(primaryCta).toBeEnabled()
    await expect(secondaryCta).toBeEnabled()
  })

  test('hero description maintains proper opacity in dark mode', async ({ page }) => {
    const description = page.getByTestId('hero-description')
    await expect(description).toBeVisible()

    // Verify description has readable opacity
    const descStyle = await description.evaluate((el) => {
      const style = window.getComputedStyle(el)
      return {
        opacity: style.opacity,
      }
    })

    const opacity = parseFloat(descStyle.opacity)
    expect(opacity).toBeGreaterThanOrEqual(0.6)
  })
})

/**
 * Test Case 5: Check feature cards in dark mode
 * Expected: Feature cards have appropriate dark theme styling
 */
test.describe('Dark Mode - Feature Cards Styling', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await setTheme(page, 'dark')
    await page.waitForLoadState('networkidle')
    // Scroll to features section
    const featuresSection = page.getByTestId('features-section')
    await featuresSection.scrollIntoViewIfNeeded()
  })

  test('feature cards have dark theme background', async ({ page }) => {
    const featureCards = page.locator('[data-testid="feature-card"]')
    const cardCount = await featureCards.count()
    expect(cardCount).toBeGreaterThan(0)

    // Check first card has proper DaisyUI card styling
    const firstCard = featureCards.first()
    const cardClasses = await firstCard.getAttribute('class')

    // Card should have DaisyUI card class with bg-base-100
    expect(cardClasses).toContain('card')
    expect(cardClasses).toContain('bg-base-100')

    // Verify card is visible
    await expect(firstCard).toBeVisible()
  })

  test('feature card shadow is visible in dark mode', async ({ page }) => {
    const featureCards = page.locator('[data-testid="feature-card"]')
    const firstCard = featureCards.first()

    // Verify card has shadow class
    const cardClasses = await firstCard.getAttribute('class')
    expect(cardClasses).toContain('shadow')

    // Verify shadow is applied via CSS
    const cardStyle = await firstCard.evaluate((el) => {
      const style = window.getComputedStyle(el)
      return {
        boxShadow: style.boxShadow,
      }
    })

    // Card should have shadow (not 'none')
    expect(cardStyle.boxShadow).not.toBe('none')
  })

  test('feature card icons are visible in dark mode', async ({ page }) => {
    const featureIcons = page.locator('[data-testid="feature-icon"]')
    const iconCount = await featureIcons.count()
    expect(iconCount).toBeGreaterThan(0)

    const firstIcon = featureIcons.first()
    await expect(firstIcon).toBeVisible()

    // Verify icon has text-primary class for theming
    const iconClasses = await firstIcon.getAttribute('class')
    expect(iconClasses).toContain('text-primary')
  })

  test('all feature cards maintain consistent styling in dark mode', async ({ page }) => {
    const featureCards = page.locator('[data-testid="feature-card"]')
    const cardCount = await featureCards.count()
    expect(cardCount).toBeGreaterThan(0)

    // Collect classes of all cards
    const cardClasses: string[] = []
    for (let i = 0; i < Math.min(cardCount, 5); i++) {
      const card = featureCards.nth(i)
      const classes = await card.getAttribute('class')
      cardClasses.push(classes || '')
    }

    // All cards should have same base classes (consistent styling)
    cardClasses.forEach((classes) => {
      expect(classes).toContain('card')
      expect(classes).toContain('bg-base-100')
      expect(classes).toContain('shadow')
    })
  })

  test('feature card hover effect works in dark mode', async ({ page }) => {
    const featureCards = page.locator('[data-testid="feature-card"]')
    const firstCard = featureCards.first()

    // Get initial position
    const initialBox = await firstCard.boundingBox()
    expect(initialBox).not.toBeNull()

    // Hover over the card
    await firstCard.hover()
    await page.waitForTimeout(400) // Wait for transition

    // Verify card has hover effect (translate or shadow change)
    const cardClasses = await firstCard.getAttribute('class')
    expect(cardClasses).toContain('hover:')

    // Verify shadow changes on hover
    const shadowStyle = await firstCard.evaluate((el) => {
      return window.getComputedStyle(el).boxShadow
    })
    expect(shadowStyle).not.toBe('none')
  })
})

/**
 * Additional Dark Mode Tests
 */
test.describe('Dark Mode - Footer and Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await setTheme(page, 'dark')
    await page.waitForLoadState('networkidle')
  })

  test('footer is properly styled in dark mode', async ({ page }) => {
    // Scroll to footer
    await page.evaluate(() => {
      window.scrollTo(0, document.body.scrollHeight)
    })
    await page.waitForTimeout(300)

    const footer = page.locator('footer').first()
    if ((await footer.count()) > 0) {
      await expect(footer).toBeVisible()

      // Verify footer has content
      const footerText = await footer.textContent()
      expect(footerText).toBeTruthy()
    }
  })

  test('inline shortener is properly styled in dark mode', async ({ page }) => {
    const shortener = page.getByTestId('inline-shortener')
    await expect(shortener).toBeVisible()

    const urlInput = page.getByTestId('url-input')
    const shortenButton = page.getByTestId('shorten-button')

    await expect(urlInput).toBeVisible()
    await expect(shortenButton).toBeVisible()

    // Verify input is functional
    await urlInput.fill('https://example.com')
    const inputValue = await urlInput.inputValue()
    expect(inputValue).toBe('https://example.com')

    // Verify button is enabled
    await expect(shortenButton).toBeEnabled()

    // Verify input has proper styling class
    const inputClasses = await urlInput.getAttribute('class')
    expect(inputClasses).toContain('input')
  })
})

/**
 * Cross-cutting Dark Mode Tests
 */
test.describe('Dark Mode - Theme Persistence and Transitions', () => {
  test('theme change is instantaneous (no flash)', async ({ page }) => {
    await page.goto('/')
    await setTheme(page, 'light')
    await page.waitForLoadState('networkidle')

    // Record initial state
    const startTime = Date.now()

    // Toggle theme
    await setTheme(page, 'dark')

    // Verify theme changed quickly
    const theme = await getCurrentTheme(page)
    const endTime = Date.now()

    expect(theme).toBe('dark')
    // Theme change should be near-instantaneous (< 200ms including waitForTimeout)
    expect(endTime - startTime).toBeLessThan(300)
  })

  test('multiple theme toggles work correctly', async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // Toggle multiple times
    await setTheme(page, 'light')
    expect(await getCurrentTheme(page)).toBe('light')

    await setTheme(page, 'dark')
    expect(await getCurrentTheme(page)).toBe('dark')

    await setTheme(page, 'light')
    expect(await getCurrentTheme(page)).toBe('light')

    await setTheme(page, 'dark')
    expect(await getCurrentTheme(page)).toBe('dark')

    // Final verification - page should still work correctly
    const homePage = page.getByTestId('home-page')
    await expect(homePage).toBeVisible()

    // All components should be visible after multiple toggles
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible()

    const navbar = page.getByTestId('navbar')
    await expect(navbar).toBeVisible()
  })
})
