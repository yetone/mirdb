/**
 * Responsive Design - Tablet Viewport E2E Tests
 * Owner: Scenario 9 (Responsive Design - Tablet Viewport)
 *
 * Tests homepage responsiveness on tablet devices (768px - 1024px).
 * Verifies:
 * - Layout adapts to tablet breakpoint at 768px
 * - Features may display in 2 or 3 column grid at 1024px
 * - Hero section maintains visual hierarchy
 */

import { test, expect } from '@playwright/test'

test.describe('Responsive Design - Tablet Viewport', () => {
  test.describe('Test Case 1: Layout adapts to tablet breakpoint at 768px', () => {
    test.use({ viewport: { width: 768, height: 1024 } })

    test('homepage layout adapts correctly at 768px tablet viewport', async ({ page }) => {
      await page.goto('/')

      // Verify homepage loads
      await expect(page.locator('body')).toBeVisible()

      // Hero section should be visible and adapt to tablet
      const heroSection = page.getByTestId('hero-section')
      await expect(heroSection).toBeVisible()
      const heroBox = await heroSection.boundingBox()
      expect(heroBox).toBeTruthy()
      // Hero should span full viewport width
      expect(heroBox!.width).toBeGreaterThanOrEqual(760)
      expect(heroBox!.width).toBeLessThanOrEqual(780)

      // Features section should be visible
      const featuresSection = page.getByTestId('features-section')
      await expect(featuresSection).toBeVisible()

      // Features grid should switch to responsive layout
      // At md breakpoint (768px), it should use md:grid-cols-3
      const featuresGrid = page.getByTestId('features-grid')
      await expect(featuresGrid).toBeVisible()
      const gridBox = await featuresGrid.boundingBox()
      expect(gridBox).toBeTruthy()

      // How It Works section should be visible
      const howItWorksSection = page.getByTestId('how-it-works-section')
      await expect(howItWorksSection).toBeVisible()

      // Steps should display horizontally at md breakpoint
      const stepsContainer = page.getByTestId('steps-container')
      await expect(stepsContainer).toBeVisible()

      // Verify horizontal layout on tablet (md:flex-row)
      const stepsBox = await stepsContainer.boundingBox()
      expect(stepsBox).toBeTruthy()

      // Footer should be visible
      const footer = page.getByTestId('footer')
      await expect(footer).toBeVisible()

      // URL form should be functional
      const urlInput = page.getByTestId('url-input')
      await expect(urlInput).toBeVisible()

      // CTA buttons should be arranged in row at sm breakpoint and above
      const signupButton = page.getByTestId('signup-button')
      const loginButton = page.getByTestId('login-button')
      await expect(signupButton).toBeVisible()
      await expect(loginButton).toBeVisible()
    })

    test('all page sections render within tablet viewport width', async ({ page }) => {
      await page.goto('/')

      // Check that no horizontal scroll is needed
      const viewportWidth = 768

      // Hero section
      const heroSection = page.getByTestId('hero-section')
      await expect(heroSection).toBeVisible()
      const heroBox = await heroSection.boundingBox()
      expect(heroBox!.width).toBeLessThanOrEqual(viewportWidth)

      // Features section
      const featuresSection = page.getByTestId('features-section')
      await expect(featuresSection).toBeVisible()
      const featuresBox = await featuresSection.boundingBox()
      expect(featuresBox!.width).toBeLessThanOrEqual(viewportWidth)

      // How It Works section
      const howItWorksSection = page.getByTestId('how-it-works-section')
      await expect(howItWorksSection).toBeVisible()
      const howItWorksBox = await howItWorksSection.boundingBox()
      expect(howItWorksBox!.width).toBeLessThanOrEqual(viewportWidth)

      // Footer
      const footer = page.getByTestId('footer')
      await expect(footer).toBeVisible()
      const footerBox = await footer.boundingBox()
      expect(footerBox!.width).toBeLessThanOrEqual(viewportWidth)
    })
  })

  test.describe('Test Case 2: Features display in proper column grid at 1024px', () => {
    test.use({ viewport: { width: 1024, height: 768 } })

    test('features section displays in 3-column grid at 1024px tablet viewport', async ({ page }) => {
      await page.goto('/')

      // Verify features section loads
      const featuresSection = page.getByTestId('features-section')
      await expect(featuresSection).toBeVisible()

      // Verify features grid is present
      const featuresGrid = page.getByTestId('features-grid')
      await expect(featuresGrid).toBeVisible()

      // Count feature cards
      const featureCards = page.getByTestId('feature-card')
      await expect(featureCards).toHaveCount(3)

      // Get positions of all feature cards
      const cards = await featureCards.all()
      const cardPositions = await Promise.all(
        cards.map(async (card) => {
          const box = await card.boundingBox()
          return { x: box!.x, y: box!.y, width: box!.width, height: box!.height }
        })
      )

      // At 1024px (md breakpoint active), cards should be in a 3-column layout
      // This means all cards should have similar Y positions (same row)
      const firstRowY = cardPositions[0].y

      // All cards should be in the same row (horizontal layout)
      cardPositions.forEach((pos) => {
        // Allow some tolerance for alignment differences
        expect(Math.abs(pos.y - firstRowY)).toBeLessThan(10)
      })

      // Cards should be arranged horizontally (different X positions)
      expect(cardPositions[0].x).toBeLessThan(cardPositions[1].x)
      expect(cardPositions[1].x).toBeLessThan(cardPositions[2].x)
    })

    test('how it works steps display horizontally at 1024px', async ({ page }) => {
      await page.goto('/')

      // Verify How It Works section loads
      const howItWorksSection = page.getByTestId('how-it-works-section')
      await expect(howItWorksSection).toBeVisible()

      // Verify steps container is present
      const stepsContainer = page.getByTestId('steps-container')
      await expect(stepsContainer).toBeVisible()

      // Count step cards
      const stepCards = page.getByTestId('step-card')
      await expect(stepCards).toHaveCount(3)

      // Get positions of all step cards
      const cards = await stepCards.all()
      const cardPositions = await Promise.all(
        cards.map(async (card) => {
          const box = await card.boundingBox()
          return { x: box!.x, y: box!.y }
        })
      )

      // At md breakpoint, steps should be horizontal (md:flex-row)
      // All cards should have similar Y positions
      const firstRowY = cardPositions[0].y
      cardPositions.forEach((pos) => {
        expect(Math.abs(pos.y - firstRowY)).toBeLessThan(50)
      })

      // Steps should be arranged left to right
      expect(cardPositions[0].x).toBeLessThan(cardPositions[1].x)
      expect(cardPositions[1].x).toBeLessThan(cardPositions[2].x)

      // Connector arrows should be visible at md breakpoint
      const connectorArrows = page.getByTestId('step-connector-arrow')
      await expect(connectorArrows.first()).toBeVisible()
    })
  })

  test.describe('Test Case 3: Hero section maintains visual hierarchy at 768px', () => {
    test.use({ viewport: { width: 768, height: 1024 } })

    test('hero section maintains proper visual hierarchy at tablet viewport', async ({ page }) => {
      await page.goto('/')

      // Verify hero section is visible
      const heroSection = page.getByTestId('hero-section')
      await expect(heroSection).toBeVisible()

      // Product name should be prominent and visible
      const productName = page.getByTestId('product-name')
      await expect(productName).toBeVisible()
      await expect(productName).toHaveText('URL Shortening Service')

      // Tagline should be visible below product name
      const tagline = page.getByTestId('tagline')
      await expect(tagline).toBeVisible()
      await expect(tagline).toContainText('Shorten Links')

      // Verify visual hierarchy: product name above tagline
      const productNameBox = await productName.boundingBox()
      const taglineBox = await tagline.boundingBox()
      expect(productNameBox!.y).toBeLessThan(taglineBox!.y)

      // URL form should be visible and below tagline
      const urlForm = page.getByTestId('url-form')
      await expect(urlForm).toBeVisible()
      const urlFormBox = await urlForm.boundingBox()
      expect(taglineBox!.y).toBeLessThan(urlFormBox!.y)

      // URL input and button should be functional
      const urlInput = page.getByTestId('url-input')
      const shortenButton = page.getByTestId('shorten-url-button')
      await expect(urlInput).toBeVisible()
      await expect(shortenButton).toBeVisible()

      // CTA buttons should be visible and properly positioned
      const signupButton = page.getByTestId('signup-button')
      const loginButton = page.getByTestId('login-button')
      await expect(signupButton).toBeVisible()
      await expect(loginButton).toBeVisible()

      // Get CTA button positions
      const signupBox = await signupButton.boundingBox()
      const loginBox = await loginButton.boundingBox()

      // At sm breakpoint and above, buttons should be in a row (sm:flex-row)
      // So they should have similar Y positions
      expect(Math.abs(signupBox!.y - loginBox!.y)).toBeLessThan(10)
    })

    test('hero section text is readable at tablet viewport', async ({ page }) => {
      await page.goto('/')

      // Product name should use responsive text size (md:text-6xl at tablet)
      const productName = page.getByTestId('product-name')
      await expect(productName).toBeVisible()

      // Tagline should use responsive text size (md:text-2xl at tablet)
      const tagline = page.getByTestId('tagline')
      await expect(tagline).toBeVisible()

      // Verify product name has adequate size for readability
      const productNameBox = await productName.boundingBox()
      expect(productNameBox!.height).toBeGreaterThan(40)

      // Verify tagline has adequate size for readability
      const taglineBox = await tagline.boundingBox()
      expect(taglineBox!.height).toBeGreaterThan(20)
    })

    test('hero section has proper spacing and padding at tablet viewport', async ({ page }) => {
      await page.goto('/')

      const heroSection = page.getByTestId('hero-section')
      await expect(heroSection).toBeVisible()

      // Hero should have significant height (min-h-[80vh])
      const heroBox = await heroSection.boundingBox()
      const viewportHeight = 1024
      expect(heroBox!.height).toBeGreaterThanOrEqual(viewportHeight * 0.7)

      // Content should be centered
      const heroContent = heroSection.locator('.hero-content')
      const contentBox = await heroContent.boundingBox()
      expect(contentBox).toBeTruthy()

      // Content should have horizontal padding/centering (at least 16px at tablet viewport)
      expect(contentBox!.x).toBeGreaterThan(15)
    })
  })

  test.describe('Full tablet viewport range tests', () => {
    test('homepage is responsive across tablet range (768px to 1024px)', async ({ page }) => {
      // Test at multiple tablet viewport widths
      const tabletWidths = [768, 834, 900, 1024]

      for (const width of tabletWidths) {
        await page.setViewportSize({ width, height: 1024 })
        await page.goto('/')

        // Verify all main sections are visible
        await expect(page.getByTestId('hero-section')).toBeVisible()
        await expect(page.getByTestId('features-section')).toBeVisible()
        await expect(page.getByTestId('how-it-works-section')).toBeVisible()
        await expect(page.getByTestId('footer')).toBeVisible()

        // Verify no horizontal overflow
        const body = page.locator('body')
        const bodyBox = await body.boundingBox()
        expect(bodyBox!.width).toBeLessThanOrEqual(width)
      }
    })
  })
})
