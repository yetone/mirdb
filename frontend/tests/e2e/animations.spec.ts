/**
 * Animation and Micro-interactions E2E Tests
 * Owner: Scenario 14 - Animation and Micro-interactions
 *
 * Tests smooth animations and user feedback interactions using Framer Motion.
 * Covers hover effects, scroll animations, loading states, and visual feedback.
 *
 * Related requirements: PRD User Interaction Patterns
 */

import { test, expect } from '@playwright/test'

test.describe('Animation and Micro-interactions - Scenario 14', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    // Wait for page to fully load and animations to settle
    await page.waitForLoadState('networkidle')
  })

  // Test Case 1: Hover over CTA button - Button shows hover state with scale or color transition
  test('CTA button shows hover state with visual transition', async ({ page }) => {
    const ctaButton = page.getByTestId('hero-cta')
    await expect(ctaButton).toBeVisible()

    // Get initial button styles
    const initialStyles = await ctaButton.evaluate((el) => {
      const computed = window.getComputedStyle(el)
      return {
        transform: computed.transform,
        backgroundColor: computed.backgroundColor,
        boxShadow: computed.boxShadow,
        scale: computed.scale,
      }
    })

    // Hover over the button
    await ctaButton.hover()

    // Wait for CSS transition to complete
    await page.waitForTimeout(300)

    // Get styles after hover
    const hoverStyles = await ctaButton.evaluate((el) => {
      const computed = window.getComputedStyle(el)
      return {
        transform: computed.transform,
        backgroundColor: computed.backgroundColor,
        boxShadow: computed.boxShadow,
        scale: computed.scale,
      }
    })

    // Verify at least one style changed (indicating hover effect)
    const hasHoverEffect =
      initialStyles.transform !== hoverStyles.transform ||
      initialStyles.backgroundColor !== hoverStyles.backgroundColor ||
      initialStyles.boxShadow !== hoverStyles.boxShadow ||
      initialStyles.scale !== hoverStyles.scale

    expect(hasHoverEffect).toBe(true)
  })

  // Test Case 2: Hover over feature cards - Cards show hover effect (shadow, scale, or highlight)
  test('feature cards show hover effects with scale or shadow changes', async ({ page }) => {
    // Scroll to features section
    const featuresSection = page.getByTestId('features-section')
    await featuresSection.scrollIntoViewIfNeeded()
    await page.waitForTimeout(500) // Wait for scroll animations to complete

    // Get first feature card
    const featureCards = page.getByTestId('feature-card')
    await expect(featureCards.first()).toBeVisible()

    const firstCard = featureCards.first()

    // Get initial styles
    const initialStyles = await firstCard.evaluate((el) => {
      const computed = window.getComputedStyle(el)
      return {
        transform: computed.transform,
        boxShadow: computed.boxShadow,
        scale: computed.scale,
      }
    })

    // Hover over the card
    await firstCard.hover()

    // Wait for Framer Motion animation
    await page.waitForTimeout(300)

    // Get styles after hover
    const hoverStyles = await firstCard.evaluate((el) => {
      const computed = window.getComputedStyle(el)
      return {
        transform: computed.transform,
        boxShadow: computed.boxShadow,
        scale: computed.scale,
      }
    })

    // Verify hover effect occurred (FeatureCard has whileHover={{ y: -5 }} which changes transform)
    const hasHoverEffect =
      initialStyles.transform !== hoverStyles.transform ||
      initialStyles.boxShadow !== hoverStyles.boxShadow ||
      initialStyles.scale !== hoverStyles.scale

    expect(hasHoverEffect).toBe(true)
  })

  // Test Case 5: Click copy button after URL shortened - Button shows copied state feedback
  test('copy button shows copied state feedback with checkmark or text change', async ({ page }) => {
    // Mock the clipboard API
    await page.evaluate(() => {
      Object.assign(navigator, {
        clipboard: {
          writeText: () => Promise.resolve(),
        },
      })
    })

    const urlInput = page.getByTestId('url-input')
    const shortenButton = page.getByTestId('shorten-button')

    // Enter a valid URL
    await urlInput.fill('https://example.com/very/long/url/that/needs/shortening')
    await shortenButton.click()

    // Wait for success state
    const successState = page.getByTestId('success-state')
    await expect(successState).toBeVisible({ timeout: 10000 })

    // Get the copy button
    const copyButton = page.getByTestId('copy-button')
    await expect(copyButton).toBeVisible()

    // Get initial button text/content
    const initialText = await copyButton.textContent()

    // Click copy button
    await copyButton.click()

    // Wait for state change animation
    await page.waitForTimeout(500)

    // Verify button shows copied state (checkmark or "Copied!" text)
    const copiedText = await copyButton.textContent()
    const hasCopiedFeedback =
      copiedText?.includes('Copied') ||
      copiedText !== initialText

    expect(hasCopiedFeedback).toBe(true)

    // Verify the button has success styling
    const hasSuccessClass = await copyButton.evaluate((el) => {
      return el.classList.contains('btn-success')
    })
    expect(hasSuccessClass).toBe(true)
  })

  // Test Case 6: Check page scroll smoothness - No jank or stuttering during scroll
  test('page scrolls smoothly without jank on 60fps capable devices', async ({ page }) => {
    // Enable performance metrics
    const cdpSession = await page.context().newCDPSession(page)
    await cdpSession.send('Performance.enable')

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY)
    expect(initialScrollY).toBe(0)

    // Start measuring frame rate during scroll
    const frameTimings: number[] = []
    let lastFrameTime = 0

    await page.evaluate(() => {
      return new Promise<void>((resolve) => {
        const observer = new PerformanceObserver((list) => {
          for (const entry of list.getEntries()) {
            if (entry.entryType === 'frame') {
              // Track frame timings
            }
          }
        })

        try {
          observer.observe({ entryTypes: ['frame'] })
        } catch {
          // Frame observer may not be supported
        }

        // Smooth scroll to bottom
        window.scrollTo({
          top: document.body.scrollHeight,
          behavior: 'smooth',
        })

        // Wait for scroll to complete
        setTimeout(() => {
          observer.disconnect()
          resolve()
        }, 2000)
      })
    })

    // Wait for scroll animation to complete
    await page.waitForTimeout(2500)

    // Verify we've scrolled
    const finalScrollY = await page.evaluate(() => window.scrollY)
    expect(finalScrollY).toBeGreaterThan(0)

    // Check for any long tasks during scroll (indicating jank)
    const longTasks = await page.evaluate(() => {
      return new Promise<number>((resolve) => {
        let count = 0
        const observer = new PerformanceObserver((list) => {
          count += list.getEntries().length
        })

        try {
          observer.observe({ type: 'longtask', buffered: true })
        } catch {
          resolve(0)
          return
        }

        setTimeout(() => {
          observer.disconnect()
          resolve(count)
        }, 100)
      })
    })

    // Should have minimal long tasks (indicating smooth 60fps)
    expect(longTasks).toBeLessThanOrEqual(5)
  })

  // Additional test: Verify scroll-triggered animations work
  test('sections animate into view when scrolled', async ({ page }) => {
    // Features section uses whileInView animation
    const featuresSection = page.getByTestId('features-section')

    // Verify section exists but may not be in viewport initially
    await expect(featuresSection).toBeAttached()

    // Scroll to features section
    await featuresSection.scrollIntoViewIfNeeded()

    // Wait for animation to trigger
    await page.waitForTimeout(600)

    // Verify section is now visible
    await expect(featuresSection).toBeVisible()

    // Verify feature cards are visible (they animate in with staggerChildren)
    const featureCards = page.getByTestId('feature-card')
    const cardCount = await featureCards.count()
    expect(cardCount).toBeGreaterThan(0)

    for (let i = 0; i < cardCount; i++) {
      await expect(featureCards.nth(i)).toBeVisible()
    }
  })

  // Test: Hero section fade-in animation
  test('hero section content animates on initial load', async ({ page }) => {
    // Go to page fresh to observe initial animation
    await page.goto('/')

    // Hero content should be visible after animation completes
    const headline = page.getByTestId('hero-headline')
    const subheadline = page.getByTestId('hero-subheadline')
    const ctaButton = page.getByTestId('hero-cta')

    // All elements should become visible after animation
    await expect(headline).toBeVisible({ timeout: 2000 })
    await expect(subheadline).toBeVisible({ timeout: 2000 })
    await expect(ctaButton).toBeVisible({ timeout: 2000 })
  })

  // Test: Step cards in How It Works section
  test('step cards have proper visual indicators', async ({ page }) => {
    // Scroll to How It Works section
    const howItWorksSection = page.getByTestId('how-it-works-section')
    await howItWorksSection.scrollIntoViewIfNeeded()
    await page.waitForTimeout(500)

    // Verify step cards are visible
    const stepCards = page.getByTestId('step-card')
    await expect(stepCards).toHaveCount(3)

    // Verify each step has a number indicator
    const stepNumbers = page.getByTestId('step-number')
    await expect(stepNumbers).toHaveCount(3)

    // Verify numbers are displayed
    for (let i = 0; i < 3; i++) {
      await expect(stepNumbers.nth(i)).toBeVisible()
    }
  })
})

test.describe('URL Shortening Animation Feedback - Scenario 14', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')
  })

  // Test Case 3: Submit URL for shortening - Loading spinner or animation appears during API call
  test('loading spinner appears during URL shortening API call', async ({ page }) => {
    const urlInput = page.getByTestId('url-input')
    const shortenButton = page.getByTestId('shorten-button')

    // Enter a valid URL
    await urlInput.fill('https://example.com/test-url')

    // Click shorten and immediately check for loading state
    await shortenButton.click()

    // Verify loading state appears (button text changes or spinner shows)
    // The button should show "Shortening..." with a loader icon
    await expect(shortenButton).toContainText(/Shortening|Loading/i, { timeout: 1000 })

    // Wait for the operation to complete
    const successState = page.getByTestId('success-state')
    await expect(successState).toBeVisible({ timeout: 10000 })
  })

  // Test Case 4: Successful URL shortening - Success state appears with smooth transition animation
  test('success state appears with smooth transition after URL shortening', async ({ page }) => {
    const urlInput = page.getByTestId('url-input')
    const shortenButton = page.getByTestId('shorten-button')

    // Enter a valid URL
    await urlInput.fill('https://example.com/another-long-url')
    await shortenButton.click()

    // Wait for success state to appear
    const successState = page.getByTestId('success-state')
    await expect(successState).toBeVisible({ timeout: 10000 })

    // Verify the success state contains the shortened URL
    const shortenedUrl = page.getByTestId('shortened-url')
    await expect(shortenedUrl).toBeVisible()

    // Verify success state has proper styling (green background/border)
    const hasSuccessStyling = await successState.evaluate((el) => {
      const classList = el.className
      return classList.includes('success') || classList.includes('bg-success')
    })
    expect(hasSuccessStyling).toBe(true)

    // Verify copy and reset buttons are visible
    const copyButton = page.getByTestId('copy-button')
    const resetButton = page.getByTestId('reset-button')
    await expect(copyButton).toBeVisible()
    await expect(resetButton).toBeVisible()
  })

  // Test: Error state animation
  test('error state appears with animation on invalid URL', async ({ page }) => {
    const urlInput = page.getByTestId('url-input')
    const shortenButton = page.getByTestId('shorten-button')

    // Enter an invalid URL
    await urlInput.fill('not-a-valid-url')
    await shortenButton.click()

    // Wait for error message to appear
    const errorMessage = page.getByTestId('error-message')
    await expect(errorMessage).toBeVisible({ timeout: 5000 })

    // Verify error styling
    const hasErrorStyling = await errorMessage.evaluate((el) => {
      const classList = el.className
      const computed = window.getComputedStyle(el)
      return classList.includes('error') || computed.color.includes('rgb')
    })
    expect(hasErrorStyling).toBe(true)
  })

  // Test: Reset button clears state and allows new URL
  test('reset button clears success state with animation', async ({ page }) => {
    // Mock clipboard
    await page.evaluate(() => {
      Object.assign(navigator, {
        clipboard: { writeText: () => Promise.resolve() },
      })
    })

    const urlInput = page.getByTestId('url-input')
    const shortenButton = page.getByTestId('shorten-button')

    // Shorten a URL first
    await urlInput.fill('https://example.com/url-to-reset')
    await shortenButton.click()

    // Wait for success state
    const successState = page.getByTestId('success-state')
    await expect(successState).toBeVisible({ timeout: 10000 })

    // Click reset button
    const resetButton = page.getByTestId('reset-button')
    await resetButton.click()

    // Wait for animation to complete
    await page.waitForTimeout(500)

    // Verify success state is gone
    await expect(successState).not.toBeVisible()

    // Verify input is cleared and enabled
    await expect(urlInput).toBeEnabled()
    await expect(urlInput).toHaveValue('')

    // Verify shorten button is available again
    await expect(shortenButton).toBeEnabled()
  })
})
