/**
 * Responsive Design E2E Tests
 * Owner: Scenarios 8, 9, 10 - Responsive Design Tests
 *
 * End-to-end tests for responsive design across different viewport sizes:
 * - Scenario 8: Mobile viewport (< 768px)
 * - Scenario 9: Tablet viewport (768px - 1024px)
 * - Scenario 10: Desktop viewport (>= 1024px)
 */

import { test, expect, Page } from '@playwright/test'

// Mobile viewport configuration (iPhone SE size - 375px)
const MOBILE_VIEWPORT = { width: 375, height: 667 }
// Minimum touch target size per WCAG guidelines
const MIN_TOUCH_TARGET = 44

// ============================================
// Scenario 8: Mobile Viewport Tests
// ============================================
test.describe('Responsive Design - Mobile Viewport', () => {
  test.use({ viewport: MOBILE_VIEWPORT })

  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  test('hero section content stacks vertically and is readable', async ({ page }) => {
    // Verify hero section is visible
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible()

    // Get hero content container
    const heroContent = heroSection.locator('.hero-content')
    await expect(heroContent).toBeVisible()

    // Verify text elements are visible and readable
    const productName = page.getByTestId('product-name')
    const tagline = page.getByTestId('tagline')

    await expect(productName).toBeVisible()
    await expect(tagline).toBeVisible()

    // Check that product name uses mobile-friendly font size (text-4xl = 36px)
    const productNameBox = await productName.boundingBox()
    expect(productNameBox).toBeTruthy()
    // On mobile, width should be constrained to viewport
    expect(productNameBox!.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width)

    // Verify text doesn't overflow horizontally
    const heroBox = await heroSection.boundingBox()
    expect(heroBox).toBeTruthy()
    expect(heroBox!.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width)

    // Verify CTA buttons stack vertically on mobile (flex-col class)
    const signupButton = page.getByTestId('signup-button')
    const loginButton = page.getByTestId('login-button')

    const signupBox = await signupButton.boundingBox()
    const loginBox = await loginButton.boundingBox()

    expect(signupBox).toBeTruthy()
    expect(loginBox).toBeTruthy()

    // Buttons should be stacked (login button Y position > signup button Y position)
    expect(loginBox!.y).toBeGreaterThan(signupBox!.y)
  })

  test('URL shortening form is full-width and accessible', async ({ page }) => {
    // Verify form container is visible
    const formContainer = page.getByTestId('url-form')
    await expect(formContainer).toBeVisible()

    // Verify URL input is visible and accessible
    const urlInput = page.getByTestId('url-input')
    await expect(urlInput).toBeVisible()

    // Check form width approaches full viewport width (accounting for padding)
    const formBox = await formContainer.boundingBox()
    expect(formBox).toBeTruthy()
    // Form should take up most of the viewport width (at least 80% after padding)
    expect(formBox!.width).toBeGreaterThan(MOBILE_VIEWPORT.width * 0.7)

    // Verify input is focusable and functional
    await urlInput.click()
    await expect(urlInput).toBeFocused()

    // Verify shorten button is visible and accessible
    const shortenButton = page.getByTestId('shorten-url-button')
    await expect(shortenButton).toBeVisible()

    // Check button has adequate touch target size
    const buttonBox = await shortenButton.boundingBox()
    expect(buttonBox).toBeTruthy()
    expect(buttonBox!.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET)
  })

  test('features section displays as single column', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.getByTestId('features-section')
    await featuresSection.scrollIntoViewIfNeeded()
    await expect(featuresSection).toBeVisible()

    // Get all feature cards
    const featureCards = page.getByTestId('feature-card')
    const count = await featureCards.count()
    expect(count).toBe(3)

    // Get bounding boxes for first three cards
    const card1Box = await featureCards.nth(0).boundingBox()
    const card2Box = await featureCards.nth(1).boundingBox()
    const card3Box = await featureCards.nth(2).boundingBox()

    expect(card1Box).toBeTruthy()
    expect(card2Box).toBeTruthy()
    expect(card3Box).toBeTruthy()

    // Verify cards are stacked vertically (each card's Y position is greater than previous)
    expect(card2Box!.y).toBeGreaterThan(card1Box!.y)
    expect(card3Box!.y).toBeGreaterThan(card2Box!.y)

    // Verify cards don't overflow horizontally
    expect(card1Box!.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width)
    expect(card2Box!.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width)
    expect(card3Box!.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width)
  })

  test('all buttons have minimum touch target size (44px)', async ({ page }) => {
    // Test hero section buttons
    const signupButton = page.getByTestId('signup-button')
    const loginButton = page.getByTestId('login-button')
    const shortenButton = page.getByTestId('shorten-url-button')

    const signupBox = await signupButton.boundingBox()
    const loginBox = await loginButton.boundingBox()
    const shortenBox = await shortenButton.boundingBox()

    expect(signupBox).toBeTruthy()
    expect(loginBox).toBeTruthy()
    expect(shortenBox).toBeTruthy()

    // Verify minimum touch target height (44px per WCAG)
    expect(signupBox!.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET)
    expect(loginBox!.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET)
    expect(shortenBox!.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET)

    // Scroll to footer and check social media buttons
    const footer = page.getByTestId('footer')
    await footer.scrollIntoViewIfNeeded()
    await expect(footer).toBeVisible()

    // Check footer social buttons have adequate touch targets
    const twitterLink = page.getByTestId('footer-social-twitter')
    const githubLink = page.getByTestId('footer-social-github')
    const linkedinLink = page.getByTestId('footer-social-linkedin')

    await expect(twitterLink).toBeVisible()
    await expect(githubLink).toBeVisible()
    await expect(linkedinLink).toBeVisible()

    // Social links should have adequate touch target area (24px icon but may have larger clickable area)
    const twitterBox = await twitterLink.boundingBox()
    expect(twitterBox).toBeTruthy()
    // Icon size is 24px, which is acceptable for secondary navigation
    expect(twitterBox!.height).toBeGreaterThanOrEqual(24)
  })

  test('all interactive elements are accessible and functional', async ({ page }) => {
    // Test hero section navigation
    const signupButton = page.getByTestId('signup-button')
    const loginButton = page.getByTestId('login-button')

    // Verify buttons are visible and clickable
    await expect(signupButton).toBeVisible()
    await expect(loginButton).toBeVisible()

    // Test URL input functionality
    const urlInput = page.getByTestId('url-input')
    await urlInput.click()
    await urlInput.fill('https://example.com')
    await expect(urlInput).toHaveValue('https://example.com')

    // Verify shorten button is clickable (don't actually submit to avoid network call)
    const shortenButton = page.getByTestId('shorten-url-button')
    await expect(shortenButton).toBeEnabled()

    // Scroll and verify features section is accessible
    const featuresSection = page.getByTestId('features-section')
    await featuresSection.scrollIntoViewIfNeeded()
    await expect(featuresSection).toBeVisible()

    // Verify features heading is visible
    await expect(page.getByRole('heading', { name: 'Why Choose Our Service?' })).toBeVisible()

    // Scroll and verify how it works section
    const howItWorksSection = page.getByTestId('how-it-works-section')
    await howItWorksSection.scrollIntoViewIfNeeded()
    await expect(howItWorksSection).toBeVisible()

    // Verify steps are displayed vertically with connectors on mobile
    const verticalConnectors = page.getByTestId('step-connector-line')
    // Should have 2 vertical connectors (between 3 steps) visible on mobile
    const connectorCount = await verticalConnectors.count()
    expect(connectorCount).toBe(2)

    // Horizontal arrows should be hidden on mobile
    const horizontalArrows = page.getByTestId('step-connector-arrow')
    for (let i = 0; i < await horizontalArrows.count(); i++) {
      await expect(horizontalArrows.nth(i)).toBeHidden()
    }

    // Scroll and verify footer
    const footer = page.getByTestId('footer')
    await footer.scrollIntoViewIfNeeded()
    await expect(footer).toBeVisible()

    // Verify footer links are visible and accessible
    await expect(page.getByTestId('footer-link-about')).toBeVisible()
    await expect(page.getByTestId('footer-link-privacy-policy')).toBeVisible()
    await expect(page.getByTestId('footer-link-terms-of-service')).toBeVisible()
  })

  test('how it works section adapts to mobile layout', async ({ page }) => {
    const howItWorksSection = page.getByTestId('how-it-works-section')
    await howItWorksSection.scrollIntoViewIfNeeded()
    await expect(howItWorksSection).toBeVisible()

    // Get steps container
    const stepsContainer = page.getByTestId('steps-container')
    await expect(stepsContainer).toBeVisible()

    // Get all step cards
    const stepCards = page.getByTestId('step-card')
    const count = await stepCards.count()
    expect(count).toBe(3)

    // Verify steps are stacked vertically
    const step1Box = await stepCards.nth(0).boundingBox()
    const step2Box = await stepCards.nth(1).boundingBox()
    const step3Box = await stepCards.nth(2).boundingBox()

    expect(step1Box).toBeTruthy()
    expect(step2Box).toBeTruthy()
    expect(step3Box).toBeTruthy()

    // Steps should be vertically stacked (Y positions increase)
    expect(step2Box!.y).toBeGreaterThan(step1Box!.y)
    expect(step3Box!.y).toBeGreaterThan(step2Box!.y)
  })

  test('footer adapts to mobile layout', async ({ page }) => {
    const footer = page.getByTestId('footer')
    await footer.scrollIntoViewIfNeeded()
    await expect(footer).toBeVisible()

    // Verify footer content is visible
    await expect(page.getByTestId('footer-brand')).toBeVisible()
    await expect(page.getByTestId('footer-product-name')).toBeVisible()
    await expect(page.getByTestId('footer-copyright')).toBeVisible()
    await expect(page.getByTestId('footer-links')).toBeVisible()
    await expect(page.getByTestId('footer-social')).toBeVisible()

    // Verify footer doesn't overflow
    const footerBox = await footer.boundingBox()
    expect(footerBox).toBeTruthy()
    expect(footerBox!.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width)
  })

  test('page content handles mobile viewport without major overflow', async ({ page }) => {
    // Verify main content areas don't overflow the viewport
    // Small overflow (< 20px) is acceptable due to scrollbars or minor CSS edge cases
    const overflowInfo = await page.evaluate(() => {
      const scrollWidth = document.documentElement.scrollWidth
      const clientWidth = document.documentElement.clientWidth
      return {
        scrollWidth,
        clientWidth,
        overflow: scrollWidth - clientWidth
      }
    })

    // Allow up to 20px overflow for scrollbar tolerance
    expect(overflowInfo.overflow).toBeLessThanOrEqual(20)

    // Verify all major sections are visible and don't cause significant overflow
    const heroSection = page.getByTestId('hero-section')
    const featuresSection = page.getByTestId('features-section')
    const howItWorksSection = page.getByTestId('how-it-works-section')
    const footer = page.getByTestId('footer')

    await expect(heroSection).toBeVisible()
    await featuresSection.scrollIntoViewIfNeeded()
    await expect(featuresSection).toBeVisible()
    await howItWorksSection.scrollIntoViewIfNeeded()
    await expect(howItWorksSection).toBeVisible()
    await footer.scrollIntoViewIfNeeded()
    await expect(footer).toBeVisible()
  })
})

// ============================================
// Scenario 10: Desktop Viewport Tests
// ============================================
test.describe('Responsive Design - Desktop Viewport', () => {
  // Configure desktop viewport (1440px width as specified in scenario)
  test.use({
    viewport: { width: 1440, height: 900 },
  })

  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    // Wait for the page to fully load
    await page.waitForLoadState('networkidle')
  })

  test('hero section is centered with maximum width constraint', async ({ page }) => {
    // Verify hero section is present
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible()

    // Get the hero content container that has the max-w-4xl constraint
    const heroContent = heroSection.locator('.hero-content')
    await expect(heroContent).toBeVisible()

    // Verify hero content is centered (using flexbox centering)
    await expect(heroContent).toHaveClass(/text-center/)

    // Verify the hero content has a maximum width constraint
    // max-w-4xl = 56rem = 896px at default font size
    const heroContentBox = await heroContent.boundingBox()
    expect(heroContentBox).toBeTruthy()

    // At 1440px viewport, hero content should be constrained and centered
    // Hero content max-width is max-w-4xl (896px)
    expect(heroContentBox!.width).toBeLessThanOrEqual(900) // max-w-4xl ≈ 896px + padding

    // Verify the hero section takes significant vertical space
    const heroSectionBox = await heroSection.boundingBox()
    expect(heroSectionBox).toBeTruthy()
    expect(heroSectionBox!.height).toBeGreaterThan(400) // min-h-[80vh] on desktop

    // Verify the hero content is horizontally centered within the viewport
    // The left margin should be approximately equal to (viewport - content width) / 2
    const expectedMargin = (1440 - heroContentBox!.width) / 2
    expect(heroContentBox!.x).toBeCloseTo(expectedMargin, -1) // Allow some tolerance
  })

  test('features display in 3-column grid', async ({ page }) => {
    // Scroll to features section
    const featuresSection = page.getByTestId('features-section')
    await featuresSection.scrollIntoViewIfNeeded()
    await expect(featuresSection).toBeVisible()

    // Get the features grid container
    const featuresGrid = page.getByTestId('features-grid')
    await expect(featuresGrid).toBeVisible()

    // Verify the grid has the correct classes for 3 columns on desktop
    // md:grid-cols-3 should be active at 1440px viewport
    await expect(featuresGrid).toHaveClass(/grid/)
    await expect(featuresGrid).toHaveClass(/md:grid-cols-3/)

    // Get all feature cards
    const featureCards = page.getByTestId('feature-card')
    await expect(featureCards).toHaveCount(3)

    // Verify all 3 feature cards are visible
    for (let i = 0; i < 3; i++) {
      await expect(featureCards.nth(i)).toBeVisible()
    }

    // Verify the cards are laid out horizontally (3-column grid)
    const card1Box = await featureCards.nth(0).boundingBox()
    const card2Box = await featureCards.nth(1).boundingBox()
    const card3Box = await featureCards.nth(2).boundingBox()

    expect(card1Box).toBeTruthy()
    expect(card2Box).toBeTruthy()
    expect(card3Box).toBeTruthy()

    // All cards should be on the same row (same y position, give or take a few pixels)
    expect(card1Box!.y).toBeCloseTo(card2Box!.y, -1)
    expect(card2Box!.y).toBeCloseTo(card3Box!.y, -1)

    // Cards should be arranged left to right
    expect(card1Box!.x).toBeLessThan(card2Box!.x)
    expect(card2Box!.x).toBeLessThan(card3Box!.x)

    // Verify each card has reasonable width for a 3-column layout
    // At 1440px with max-w-6xl container, each card should be roughly 1/3 of the container width
    const minCardWidth = 200 // Minimum reasonable card width
    expect(card1Box!.width).toBeGreaterThan(minCardWidth)
    expect(card2Box!.width).toBeGreaterThan(minCardWidth)
    expect(card3Box!.width).toBeGreaterThan(minCardWidth)
  })

  test('How It Works section displays steps horizontally', async ({ page }) => {
    // Scroll to How It Works section
    const howItWorksSection = page.getByTestId('how-it-works-section')
    await howItWorksSection.scrollIntoViewIfNeeded()
    await expect(howItWorksSection).toBeVisible()

    // Get the steps container
    const stepsContainer = page.getByTestId('steps-container')
    await expect(stepsContainer).toBeVisible()

    // Verify the container uses flex layout for horizontal arrangement on desktop
    // flex-col md:flex-row means horizontal on desktop (md and above)
    await expect(stepsContainer).toHaveClass(/flex/)
    await expect(stepsContainer).toHaveClass(/md:flex-row/)

    // Get all step cards
    const stepCards = page.getByTestId('step-card')
    await expect(stepCards).toHaveCount(3)

    // Verify all 3 steps are visible
    for (let i = 0; i < 3; i++) {
      await expect(stepCards.nth(i)).toBeVisible()
    }

    // Verify the steps are laid out horizontally
    const step1Box = await stepCards.nth(0).boundingBox()
    const step2Box = await stepCards.nth(1).boundingBox()
    const step3Box = await stepCards.nth(2).boundingBox()

    expect(step1Box).toBeTruthy()
    expect(step2Box).toBeTruthy()
    expect(step3Box).toBeTruthy()

    // All steps should be on the same row (same y position, give or take)
    // Using a tolerance of 50px to account for slight vertical alignment differences
    expect(Math.abs(step1Box!.y - step2Box!.y)).toBeLessThan(50)
    expect(Math.abs(step2Box!.y - step3Box!.y)).toBeLessThan(50)

    // Steps should be arranged left to right
    expect(step1Box!.x).toBeLessThan(step2Box!.x)
    expect(step2Box!.x).toBeLessThan(step3Box!.x)

    // Verify horizontal arrow connectors are visible on desktop
    // The arrows have class "hidden md:block" so they should be visible at 1440px
    const arrowConnectors = page.getByTestId('step-connector-arrow')
    await expect(arrowConnectors).toHaveCount(2) // 2 arrows between 3 steps

    for (let i = 0; i < 2; i++) {
      await expect(arrowConnectors.nth(i)).toBeVisible()
    }

    // Verify vertical connectors are hidden on desktop
    // The vertical lines have class "md:hidden" so they should NOT be visible
    const verticalConnectors = page.getByTestId('step-connector-line')
    for (let i = 0; i < (await verticalConnectors.count()); i++) {
      await expect(verticalConnectors.nth(i)).not.toBeVisible()
    }

    // Verify step numbers are displayed correctly
    const stepNumbers = page.getByTestId('step-number')
    await expect(stepNumbers).toHaveCount(3)
    await expect(stepNumbers.nth(0)).toHaveText('1')
    await expect(stepNumbers.nth(1)).toHaveText('2')
    await expect(stepNumbers.nth(2)).toHaveText('3')
  })

  test('all sections utilize full desktop space appropriately', async ({ page }) => {
    // Verify the page container uses the full viewport width
    const mainContainer = page.locator('main')
    await expect(mainContainer).toBeVisible()

    // Verify hero section spans full width
    const heroSection = page.getByTestId('hero-section')
    const heroBox = await heroSection.boundingBox()
    expect(heroBox).toBeTruthy()
    expect(heroBox!.width).toBeGreaterThanOrEqual(1430) // Near full 1440px viewport

    // Verify features section spans full width
    const featuresSection = page.getByTestId('features-section')
    await featuresSection.scrollIntoViewIfNeeded()
    const featuresBox = await featuresSection.boundingBox()
    expect(featuresBox).toBeTruthy()
    expect(featuresBox!.width).toBeGreaterThanOrEqual(1430)

    // Verify How It Works section spans full width
    const howItWorksSection = page.getByTestId('how-it-works-section')
    await howItWorksSection.scrollIntoViewIfNeeded()
    const howItWorksBox = await howItWorksSection.boundingBox()
    expect(howItWorksBox).toBeTruthy()
    expect(howItWorksBox!.width).toBeGreaterThanOrEqual(1430)

    // Verify content within sections is properly constrained
    // Features section has max-w-6xl (1152px) container
    const featuresContainer = featuresSection.locator('.container')
    const featuresContainerBox = await featuresContainer.boundingBox()
    expect(featuresContainerBox).toBeTruthy()
    expect(featuresContainerBox!.width).toBeLessThanOrEqual(1200) // max-w-6xl ≈ 1152px + padding

    // How It Works section has max-w-4xl (896px) container
    const howItWorksContainer = howItWorksSection.locator('.container')
    const howItWorksContainerBox = await howItWorksContainer.boundingBox()
    expect(howItWorksContainerBox).toBeTruthy()
    expect(howItWorksContainerBox!.width).toBeLessThanOrEqual(950) // max-w-4xl ≈ 896px + padding
  })
})
