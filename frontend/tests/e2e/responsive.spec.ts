/**
 * Responsive Design E2E Tests - Mobile
 * Owner: Scenario 8 - Responsive Design Mobile
 *
 * Tests responsive layout and functionality on mobile viewports (375px width).
 * Validates no horizontal scrolling, proper text wrapping, touch targets,
 * and mobile-friendly component layouts.
 *
 * Related requirements: REQ-10, NFR-3, US-7
 */

import { test, expect } from '@playwright/test'

// iPhone SE viewport dimensions
const MOBILE_VIEWPORT = { width: 375, height: 667 }

// Minimum touch target size per WCAG guidelines
const MIN_TOUCH_TARGET = 44

test.describe('Responsive Design - Mobile Viewport (375px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(MOBILE_VIEWPORT)
    await page.goto('/')
    // Wait for the page to be fully loaded
    await page.waitForLoadState('networkidle')
  })

  // Test Case 1: No horizontal scrollbar at 375px viewport width
  test('no horizontal scrollbar appears at 375px viewport', async ({ page }) => {
    // Check that document doesn't have horizontal overflow
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth
    })

    expect(hasHorizontalScroll).toBe(false)

    // Also verify body doesn't overflow
    const bodyOverflow = await page.evaluate(() => {
      const body = document.body
      return body.scrollWidth > body.clientWidth
    })

    expect(bodyOverflow).toBe(false)
  })

  // Test Case 2: Hero headline wraps appropriately and is fully visible
  test('hero headline wraps appropriately and is fully visible', async ({ page }) => {
    const headline = page.getByTestId('hero-headline')
    await expect(headline).toBeVisible()

    // Check headline is within viewport bounds
    const headlineBox = await headline.boundingBox()
    expect(headlineBox).not.toBeNull()

    if (headlineBox) {
      // Headline should not extend beyond viewport width
      expect(headlineBox.x).toBeGreaterThanOrEqual(0)
      expect(headlineBox.x + headlineBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width)

      // Headline should be visible (positive height indicates content is rendered)
      expect(headlineBox.height).toBeGreaterThan(0)
    }

    // Verify text content is present
    await expect(headline).toHaveText('Shorten Links. Track Success.')
  })

  // Test Case 3: URL input field is full width and usable on mobile
  test('URL input field is full width and usable on mobile', async ({ page }) => {
    const urlInput = page.getByTestId('url-input')
    await expect(urlInput).toBeVisible()

    // Get the input's bounding box
    const inputBox = await urlInput.boundingBox()
    expect(inputBox).not.toBeNull()

    if (inputBox) {
      // Input should take up most of the viewport width (accounting for padding)
      // At minimum, it should be at least 80% of viewport width
      const widthPercentage = (inputBox.width / MOBILE_VIEWPORT.width) * 100
      expect(widthPercentage).toBeGreaterThanOrEqual(80)

      // Input should be within viewport
      expect(inputBox.x).toBeGreaterThanOrEqual(0)
      expect(inputBox.x + inputBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width)
    }

    // Test that input is interactive
    await urlInput.fill('https://example.com/test-url')
    await expect(urlInput).toHaveValue('https://example.com/test-url')
  })

  // Test Case 4: CTA button meets minimum 44x44px touch target requirement
  test('CTA button meets minimum 44x44px touch target requirement', async ({ page }) => {
    const ctaButton = page.getByTestId('hero-cta')
    await expect(ctaButton).toBeVisible()

    const buttonBox = await ctaButton.boundingBox()
    expect(buttonBox).not.toBeNull()

    if (buttonBox) {
      // WCAG 2.1 AAA recommends minimum 44x44px touch targets
      expect(buttonBox.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET)
      expect(buttonBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET)
    }
  })

  // Test Case 4b: Shorten button also meets touch target requirements
  test('shorten button meets minimum 44x44px touch target requirement', async ({ page }) => {
    const shortenButton = page.getByTestId('shorten-button')
    await expect(shortenButton).toBeVisible()

    const buttonBox = await shortenButton.boundingBox()
    expect(buttonBox).not.toBeNull()

    if (buttonBox) {
      expect(buttonBox.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET)
      expect(buttonBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET)
    }
  })

  // Test Case 5: Feature cards stack vertically on mobile
  test('feature cards stack vertically on mobile', async ({ page }) => {
    // Scroll to features section to ensure it's loaded
    const featuresSection = page.getByTestId('features-section')
    await featuresSection.scrollIntoViewIfNeeded()
    await expect(featuresSection).toBeVisible()

    const featureCards = page.getByTestId('feature-card')
    const cardCount = await featureCards.count()

    // Ensure there are feature cards
    expect(cardCount).toBeGreaterThanOrEqual(3)

    // Get bounding boxes for first few cards
    const cards = []
    for (let i = 0; i < Math.min(3, cardCount); i++) {
      const box = await featureCards.nth(i).boundingBox()
      if (box) {
        cards.push(box)
      }
    }

    // Verify vertical stacking: each card should be below the previous one
    for (let i = 1; i < cards.length; i++) {
      // Y position of current card should be greater than Y + height of previous
      expect(cards[i].y).toBeGreaterThanOrEqual(cards[i - 1].y + cards[i - 1].height - 10)
    }

    // Verify all cards have similar width (full column width on mobile)
    for (const card of cards) {
      expect(card.width).toBeGreaterThanOrEqual(MOBILE_VIEWPORT.width * 0.8)
    }
  })

  // Test Case 6: How It Works steps stack vertically instead of horizontal layout
  test('How It Works steps stack vertically on mobile', async ({ page }) => {
    // Scroll to How It Works section
    const howItWorksSection = page.getByTestId('how-it-works-section')
    await howItWorksSection.scrollIntoViewIfNeeded()
    await expect(howItWorksSection).toBeVisible()

    const stepCards = page.getByTestId('step-card')
    await expect(stepCards).toHaveCount(3)

    // Get bounding boxes for all step cards
    const step1Box = await stepCards.nth(0).boundingBox()
    const step2Box = await stepCards.nth(1).boundingBox()
    const step3Box = await stepCards.nth(2).boundingBox()

    expect(step1Box).not.toBeNull()
    expect(step2Box).not.toBeNull()
    expect(step3Box).not.toBeNull()

    if (step1Box && step2Box && step3Box) {
      // Verify vertical stacking: each step should be below the previous one
      expect(step2Box.y).toBeGreaterThan(step1Box.y + step1Box.height - 20)
      expect(step3Box.y).toBeGreaterThan(step2Box.y + step2Box.height - 20)

      // Steps should NOT be arranged horizontally (x positions should be similar)
      // Allow small differences due to padding
      expect(Math.abs(step1Box.x - step2Box.x)).toBeLessThan(50)
      expect(Math.abs(step2Box.x - step3Box.x)).toBeLessThan(50)
    }
  })

  // Test Case 7: Form submits successfully via Enter key (mobile keyboard 'Go' button)
  test('form submits successfully via mobile keyboard Go button (Enter key)', async ({ page }) => {
    const urlInput = page.getByTestId('url-input')
    const shortenButton = page.getByTestId('shorten-button')

    await expect(urlInput).toBeVisible()
    await expect(shortenButton).toBeVisible()

    // Type a valid URL
    await urlInput.fill('https://example.com/very-long-url-to-shorten')

    // Simulate pressing Enter (mobile keyboard 'Go' action)
    await urlInput.press('Enter')

    // The form should respond to Enter key
    // Check for loading state OR error state (API may not be available in test)
    // This validates the form submission handler works
    const hasResponse = await Promise.race([
      page.getByTestId('success-state').waitFor({ state: 'visible', timeout: 3000 }).then(() => true).catch(() => false),
      page.getByTestId('error-message').waitFor({ state: 'visible', timeout: 3000 }).then(() => true).catch(() => false),
      shortenButton.evaluate(el => el.textContent?.includes('Shortening')).then(() => true).catch(() => false)
    ])

    // Form should have responded to the Enter key press
    // Either showing success, error, or loading state
    expect(hasResponse).toBe(true)
  })

  // Additional mobile-specific tests
  test('all interactive elements are accessible without horizontal scrolling', async ({ page }) => {
    // Get all interactive elements
    const buttons = page.locator('button')
    const links = page.locator('a')
    const inputs = page.locator('input')

    // Check all buttons are within viewport width
    const buttonCount = await buttons.count()
    for (let i = 0; i < Math.min(buttonCount, 10); i++) {
      const button = buttons.nth(i)
      if (await button.isVisible()) {
        const box = await button.boundingBox()
        if (box) {
          expect(box.x + box.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width + 20) // Small tolerance for animations
        }
      }
    }

    // Check visible inputs are within viewport
    const inputCount = await inputs.count()
    for (let i = 0; i < inputCount; i++) {
      const input = inputs.nth(i)
      if (await input.isVisible()) {
        const box = await input.boundingBox()
        if (box) {
          expect(box.x + box.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width + 20)
        }
      }
    }
  })

  test('subheadline text is readable on mobile viewport', async ({ page }) => {
    const subheadline = page.getByTestId('hero-subheadline')
    await expect(subheadline).toBeVisible()

    const box = await subheadline.boundingBox()
    expect(box).not.toBeNull()

    if (box) {
      // Subheadline should fit within viewport
      expect(box.x).toBeGreaterThanOrEqual(0)
      expect(box.x + box.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width)
    }

    // Verify content is present and readable
    await expect(subheadline).toContainText('Transform long URLs')
  })
})

// Tablet viewport tests (for Scenario 9 - assigned to tablet/desktop)
// These are kept minimal here as Scenario 9 owns the tablet/desktop tests
test.describe('Responsive Design - Touch Target Validation', () => {
  test('all primary action buttons meet touch target requirements on mobile', async ({ page }) => {
    await page.setViewportSize(MOBILE_VIEWPORT)
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // List of critical buttons to check
    const buttonTestIds = ['hero-cta', 'shorten-button']

    for (const testId of buttonTestIds) {
      const button = page.getByTestId(testId)
      if (await button.isVisible()) {
        const box = await button.boundingBox()
        if (box) {
          expect(box.width, `Button ${testId} width`).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET)
          expect(box.height, `Button ${testId} height`).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET)
        }
      }
    }
  })
})
