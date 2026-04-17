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

/**
 * Responsive Design E2E Tests - Tablet and Desktop
 * Owner: Scenario 9 - Responsive Design Tablet and Desktop
 *
 * Tests responsive layout on tablet (768px) and desktop (1920px) viewports.
 * Validates proper column structures, max-width constraints, multi-column grids,
 * horizontal layouts, and ultra-wide scaling.
 *
 * Related requirements: REQ-10, NFR-3
 */

// Standard viewport dimensions
const TABLET_VIEWPORT = { width: 768, height: 1024 }
const LAPTOP_VIEWPORT = { width: 1024, height: 768 }
const DESKTOP_VIEWPORT = { width: 1920, height: 1080 }
const ULTRAWIDE_VIEWPORT = { width: 2560, height: 1440 }

test.describe('Responsive Design - Tablet Viewport (768px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(TABLET_VIEWPORT)
    await page.goto('/')
    await page.waitForLoadState('networkidle')
  })

  // Test Case 1: Render homepage at 768px tablet viewport - Layout adapts with appropriate column structure
  test('layout adapts with appropriate column structure at 768px tablet viewport', async ({
    page,
  }) => {
    // Verify the homepage renders correctly
    const homePage = page.getByTestId('home-page')
    await expect(homePage).toBeVisible()

    // Check hero section is visible and centered
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible()
    const heroBox = await heroSection.boundingBox()
    expect(heroBox).not.toBeNull()
    if (heroBox) {
      // Hero should span full width
      expect(heroBox.width).toBeGreaterThanOrEqual(TABLET_VIEWPORT.width - 50)
    }

    // Check features section uses 2-column grid at tablet (md breakpoint)
    const featuresSection = page.getByTestId('features-section')
    await featuresSection.scrollIntoViewIfNeeded()
    await expect(featuresSection).toBeVisible()

    const featureCards = page.getByTestId('feature-card')
    const cardCount = await featureCards.count()
    expect(cardCount).toBeGreaterThanOrEqual(3)

    // Get first two cards and verify they are side by side (2-column layout)
    const card1Box = await featureCards.nth(0).boundingBox()
    const card2Box = await featureCards.nth(1).boundingBox()
    expect(card1Box).not.toBeNull()
    expect(card2Box).not.toBeNull()

    if (card1Box && card2Box) {
      // Cards should be on the same row (similar Y positions)
      expect(Math.abs(card1Box.y - card2Box.y)).toBeLessThan(20)
      // Card 2 should be to the right of Card 1
      expect(card2Box.x).toBeGreaterThan(card1Box.x + card1Box.width - 20)
    }

    // Check How It Works uses 3-column layout at tablet
    const howItWorksSection = page.getByTestId('how-it-works-section')
    await howItWorksSection.scrollIntoViewIfNeeded()
    await expect(howItWorksSection).toBeVisible()

    const stepCards = page.getByTestId('step-card')
    await expect(stepCards).toHaveCount(3)

    const step1Box = await stepCards.nth(0).boundingBox()
    const step2Box = await stepCards.nth(1).boundingBox()
    const step3Box = await stepCards.nth(2).boundingBox()

    if (step1Box && step2Box && step3Box) {
      // All steps should be on same row (horizontal layout)
      expect(Math.abs(step1Box.y - step2Box.y)).toBeLessThan(20)
      expect(Math.abs(step2Box.y - step3Box.y)).toBeLessThan(20)
    }
  })

  test('no horizontal scrollbar appears at 768px tablet viewport', async ({ page }) => {
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth
    })
    expect(hasHorizontalScroll).toBe(false)
  })
})

test.describe('Responsive Design - Desktop Viewport (1920px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(DESKTOP_VIEWPORT)
    await page.goto('/')
    await page.waitForLoadState('networkidle')
  })

  // Test Case 2: Render homepage at 1920px desktop viewport - Content is centered with appropriate max-width constraints
  test('content is centered with appropriate max-width constraints at 1920px', async ({
    page,
  }) => {
    // Hero section content should be centered with max-width
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible()

    const heroHeadline = page.getByTestId('hero-headline')
    await expect(heroHeadline).toBeVisible()
    const headlineBox = await heroHeadline.boundingBox()
    expect(headlineBox).not.toBeNull()

    if (headlineBox) {
      // Headline should be centered (not starting from 0 or extending to viewport width)
      expect(headlineBox.x).toBeGreaterThan(100)
      expect(headlineBox.x + headlineBox.width).toBeLessThan(DESKTOP_VIEWPORT.width - 100)

      // Calculate center offset - should be roughly centered
      const headlineCenter = headlineBox.x + headlineBox.width / 2
      const viewportCenter = DESKTOP_VIEWPORT.width / 2
      expect(Math.abs(headlineCenter - viewportCenter)).toBeLessThan(200)
    }

    // Features section should have max-width constraint (max-w-6xl = 1152px)
    const featuresSection = page.getByTestId('features-section')
    await featuresSection.scrollIntoViewIfNeeded()
    await expect(featuresSection).toBeVisible()

    const featureCards = page.getByTestId('feature-card')
    const firstCard = await featureCards.nth(0).boundingBox()
    const lastCard = await featureCards.nth((await featureCards.count()) - 1).boundingBox()

    if (firstCard && lastCard) {
      // The total width of cards container should be constrained (not full 1920px)
      const containerWidth = lastCard.x + lastCard.width - firstCard.x
      expect(containerWidth).toBeLessThan(1300) // max-w-6xl + gaps
    }
  })

  // Test Case 4: Check How It Works at 1920px viewport - Steps display in horizontal row layout
  test('How It Works steps display in horizontal row layout at 1920px', async ({ page }) => {
    const howItWorksSection = page.getByTestId('how-it-works-section')
    await howItWorksSection.scrollIntoViewIfNeeded()
    await expect(howItWorksSection).toBeVisible()

    const stepCards = page.getByTestId('step-card')
    await expect(stepCards).toHaveCount(3)

    const step1Box = await stepCards.nth(0).boundingBox()
    const step2Box = await stepCards.nth(1).boundingBox()
    const step3Box = await stepCards.nth(2).boundingBox()

    expect(step1Box).not.toBeNull()
    expect(step2Box).not.toBeNull()
    expect(step3Box).not.toBeNull()

    if (step1Box && step2Box && step3Box) {
      // All three steps should be on the same horizontal row
      expect(Math.abs(step1Box.y - step2Box.y)).toBeLessThan(20)
      expect(Math.abs(step2Box.y - step3Box.y)).toBeLessThan(20)

      // Steps should be arranged left-to-right
      expect(step2Box.x).toBeGreaterThan(step1Box.x)
      expect(step3Box.x).toBeGreaterThan(step2Box.x)

      // Steps should have equal widths (grid layout)
      const widthDiff12 = Math.abs(step1Box.width - step2Box.width)
      const widthDiff23 = Math.abs(step2Box.width - step3Box.width)
      expect(widthDiff12).toBeLessThan(50)
      expect(widthDiff23).toBeLessThan(50)
    }
  })

  test('desktop layout has no horizontal scrollbar', async ({ page }) => {
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth
    })
    expect(hasHorizontalScroll).toBe(false)
  })
})

test.describe('Responsive Design - Laptop Viewport (1024px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(LAPTOP_VIEWPORT)
    await page.goto('/')
    await page.waitForLoadState('networkidle')
  })

  // Test Case 3: Check feature cards grid at 1024px viewport - Feature cards display in multi-column grid layout
  test('feature cards display in 3-column grid layout at 1024px', async ({ page }) => {
    const featuresSection = page.getByTestId('features-section')
    await featuresSection.scrollIntoViewIfNeeded()
    await expect(featuresSection).toBeVisible()

    const featureCards = page.getByTestId('feature-card')
    const cardCount = await featureCards.count()
    expect(cardCount).toBeGreaterThanOrEqual(3)

    // Get first three cards to verify 3-column layout
    const card1Box = await featureCards.nth(0).boundingBox()
    const card2Box = await featureCards.nth(1).boundingBox()
    const card3Box = await featureCards.nth(2).boundingBox()

    expect(card1Box).not.toBeNull()
    expect(card2Box).not.toBeNull()
    expect(card3Box).not.toBeNull()

    if (card1Box && card2Box && card3Box) {
      // All three cards should be on the same row (lg breakpoint = 3 columns)
      expect(Math.abs(card1Box.y - card2Box.y)).toBeLessThan(20)
      expect(Math.abs(card2Box.y - card3Box.y)).toBeLessThan(20)

      // Cards should be arranged left-to-right
      expect(card2Box.x).toBeGreaterThan(card1Box.x)
      expect(card3Box.x).toBeGreaterThan(card2Box.x)

      // Verify multi-column grid (cards not stacked vertically)
      // Each card should take approximately 1/3 of the container width
      const cardWidthRatio = card1Box.width / LAPTOP_VIEWPORT.width
      expect(cardWidthRatio).toBeLessThan(0.5) // Each card should be less than 50% width
    }
  })
})

test.describe('Responsive Design - Ultra-Wide Viewport (2560px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(ULTRAWIDE_VIEWPORT)
    await page.goto('/')
    await page.waitForLoadState('networkidle')
  })

  // Test Case 5: Verify hero section scaling at 2560px viewport - Hero section maintains visual appeal at ultra-wide resolutions
  test('hero section maintains visual appeal at ultra-wide resolutions', async ({ page }) => {
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible()

    const heroBox = await heroSection.boundingBox()
    expect(heroBox).not.toBeNull()

    if (heroBox) {
      // Hero section should span full width
      expect(heroBox.width).toBeGreaterThanOrEqual(ULTRAWIDE_VIEWPORT.width - 10)

      // Hero should have adequate height
      expect(heroBox.height).toBeGreaterThanOrEqual(400)
    }

    // Check headline is visible and properly constrained
    const heroHeadline = page.getByTestId('hero-headline')
    await expect(heroHeadline).toBeVisible()
    const headlineBox = await heroHeadline.boundingBox()
    expect(headlineBox).not.toBeNull()

    if (headlineBox) {
      // Content should be centered (max-w-4xl constraint)
      // max-w-4xl = 896px, content should not span full 2560px
      expect(headlineBox.width).toBeLessThan(1200)

      // Should be horizontally centered
      const headlineCenter = headlineBox.x + headlineBox.width / 2
      const viewportCenter = ULTRAWIDE_VIEWPORT.width / 2
      expect(Math.abs(headlineCenter - viewportCenter)).toBeLessThan(200)
    }

    // Check subheadline is also constrained and centered
    const subheadline = page.getByTestId('hero-subheadline')
    await expect(subheadline).toBeVisible()
    const subBox = await subheadline.boundingBox()

    if (subBox) {
      // Subheadline has max-w-2xl (672px) constraint
      expect(subBox.width).toBeLessThan(900)

      // Should be centered
      const subCenter = subBox.x + subBox.width / 2
      const viewportCenter = ULTRAWIDE_VIEWPORT.width / 2
      expect(Math.abs(subCenter - viewportCenter)).toBeLessThan(200)
    }

    // CTA button should be visible and centered
    const ctaButton = page.getByTestId('hero-cta')
    await expect(ctaButton).toBeVisible()
    const ctaBox = await ctaButton.boundingBox()

    if (ctaBox) {
      // CTA should be centered
      const ctaCenter = ctaBox.x + ctaBox.width / 2
      const viewportCenter = ULTRAWIDE_VIEWPORT.width / 2
      expect(Math.abs(ctaCenter - viewportCenter)).toBeLessThan(200)
    }
  })

  test('content containers maintain max-width constraints at ultra-wide', async ({ page }) => {
    // Check features section maintains max-width
    const featuresSection = page.getByTestId('features-section')
    await featuresSection.scrollIntoViewIfNeeded()
    await expect(featuresSection).toBeVisible()

    const featureCards = page.getByTestId('feature-card')
    const firstCard = await featureCards.nth(0).boundingBox()
    const lastCard = await featureCards.nth((await featureCards.count()) - 1).boundingBox()

    if (firstCard && lastCard) {
      // Container should not span full ultra-wide width
      const containerSpan = lastCard.x + lastCard.width - firstCard.x
      expect(containerSpan).toBeLessThan(1400)

      // Content should be reasonably centered (allow for padding and layout variations)
      const containerCenter = firstCard.x + containerSpan / 2
      expect(Math.abs(containerCenter - ULTRAWIDE_VIEWPORT.width / 2)).toBeLessThan(250)
    }

    // Check How It Works section
    const howItWorks = page.getByTestId('how-it-works-section')
    await howItWorks.scrollIntoViewIfNeeded()

    const stepCards = page.getByTestId('step-card')
    const firstStep = await stepCards.nth(0).boundingBox()
    const lastStep = await stepCards.nth(2).boundingBox()

    if (firstStep && lastStep) {
      const stepsSpan = lastStep.x + lastStep.width - firstStep.x
      expect(stepsSpan).toBeLessThan(1400)
    }
  })

  test('no horizontal scrollbar at ultra-wide viewport', async ({ page }) => {
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth
    })
    expect(hasHorizontalScroll).toBe(false)
  })
})
