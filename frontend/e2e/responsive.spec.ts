/**
 * Responsive Design E2E Tests
 * Owner: Scenarios 8, 9, 10 - Responsive Design Tests
 *
 * End-to-end tests for responsive design across different viewport sizes:
 * - Scenario 8: Mobile viewport (< 768px)
 * - Scenario 9: Tablet viewport (768px - 1024px)
 * - Scenario 10: Desktop viewport (>= 1024px)
 *
 * This file focuses on Scenario 10: Desktop Viewport Testing
 */

import { test, expect, Page } from '@playwright/test'

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
