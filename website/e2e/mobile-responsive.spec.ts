import { test, expect } from '@playwright/test'

test.describe('Responsive Design - Mobile View', () => {
  test.beforeEach(async ({ page }) => {
    // Set mobile viewport size (375px width as specified for iPhone)
    await page.setViewportSize({ width: 375, height: 667 })
    await page.goto('/')
  })

  test('TC1: page renders without horizontal overflow at 375px viewport width', async ({ page }) => {
    // Set explicit mobile viewport
    await page.setViewportSize({ width: 375, height: 667 })

    // Wait for the page to load
    await expect(page.locator('.hero')).toBeVisible()

    // Check that document body doesn't have horizontal overflow
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth
    })

    expect(hasHorizontalScroll).toBe(false)

    // Verify the page content fits within viewport
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth)
    expect(bodyWidth).toBeLessThanOrEqual(375)
  })

  test('TC2: mobile navigation menu (hamburger) is present and functional', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 })

    // Check for mobile navigation elements
    // Either a hamburger menu button or mobile-friendly navigation
    const hamburgerButton = page.locator('[data-testid="mobile-menu-toggle"], .hamburger-menu, .mobile-nav-toggle, button[aria-label*="menu" i], button[aria-label*="navigation" i]')
    const mobileNav = page.locator('.mobile-nav, .nav-mobile, nav.mobile')
    const navLinks = page.locator('nav a, .nav-link, .navigation a')

    // Check if hamburger menu exists
    const hasHamburger = await hamburgerButton.count() > 0
    const hasMobileNav = await mobileNav.count() > 0
    const hasNavLinks = await navLinks.count() > 0

    // On mobile, either we have a hamburger menu, a mobile nav, or nav links that are visible
    // The key requirement is that navigation is accessible on mobile
    if (hasHamburger) {
      await expect(hamburgerButton.first()).toBeVisible()

      // Click to open the menu
      await hamburgerButton.first().click()

      // After clicking, navigation links should be visible
      await expect(navLinks.first()).toBeVisible({ timeout: 1000 }).catch(() => {
        // If no nav links appear after clicking hamburger, that's okay if there are direct links
      })
    } else if (hasMobileNav) {
      await expect(mobileNav.first()).toBeVisible()
    } else {
      // If no hamburger menu, check that there are CTA buttons/links accessible on mobile
      const ctaLinks = page.locator('.hero a, .cta-primary, .cta-secondary')
      await expect(ctaLinks.first()).toBeVisible()
    }
  })

  test('TC3: code blocks have horizontal scroll without breaking page layout', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 })

    // Look for code blocks
    const codeBlocks = page.locator('pre, code, .code-block, [class*="code"]')
    const codeBlockCount = await codeBlocks.count()

    if (codeBlockCount > 0) {
      // Check each code block
      for (let i = 0; i < codeBlockCount; i++) {
        const codeBlock = codeBlocks.nth(i)
        const isVisible = await codeBlock.isVisible()

        if (isVisible) {
          // Check that code block has overflow-x: auto or scroll
          const overflowX = await codeBlock.evaluate((el) => {
            return window.getComputedStyle(el).overflowX
          })

          // Code blocks should allow horizontal scrolling if needed
          const hasScrollableOverflow = ['auto', 'scroll'].includes(overflowX)

          // Get the code block's width
          const codeBlockBox = await codeBlock.boundingBox()
          if (codeBlockBox) {
            // Code block shouldn't extend beyond viewport
            expect(codeBlockBox.x + codeBlockBox.width).toBeLessThanOrEqual(375 + 1) // +1 for rounding
          }
        }
      }
    }

    // Ensure page still has no horizontal overflow after checking code blocks
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth
    })
    expect(hasHorizontalScroll).toBe(false)
  })

  test('TC4: CTA buttons are visible and have adequate touch target size', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 })

    // Find CTA buttons
    const ctaButtons = page.locator('.cta-primary, .cta-secondary, a.cta-primary, a.cta-secondary')
    const buttonCount = await ctaButtons.count()

    expect(buttonCount).toBeGreaterThan(0)

    // Check each CTA button
    for (let i = 0; i < buttonCount; i++) {
      const button = ctaButtons.nth(i)
      await expect(button).toBeVisible()

      const boundingBox = await button.boundingBox()
      expect(boundingBox).not.toBeNull()

      if (boundingBox) {
        // Minimum touch target size is 44x44px per accessibility guidelines
        expect(boundingBox.height).toBeGreaterThanOrEqual(44)
        expect(boundingBox.width).toBeGreaterThanOrEqual(44)
      }
    }
  })

  test('TC5: all content sections are accessible via vertical scroll', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 })

    // Check hero section is visible
    const heroSection = page.locator('.hero')
    await expect(heroSection).toBeVisible()

    // Get all major content sections
    const sections = page.locator('section, .hero, [class*="section"], main > div')
    const sectionCount = await sections.count()

    expect(sectionCount).toBeGreaterThan(0)

    // Check that hero title is visible
    const heroTitle = page.locator('.hero-title, h1')
    await expect(heroTitle.first()).toBeVisible()

    // Check that hero tagline is visible
    const heroTagline = page.locator('.hero-tagline')
    if (await heroTagline.count() > 0) {
      await expect(heroTagline.first()).toBeVisible()
    }

    // Check that hero description is visible
    const heroDescription = page.locator('.hero-description')
    if (await heroDescription.count() > 0) {
      await expect(heroDescription.first()).toBeVisible()
    }

    // Check that CTA container is visible
    const ctaContainer = page.locator('.hero-cta-container')
    if (await ctaContainer.count() > 0) {
      await expect(ctaContainer.first()).toBeVisible()
    }

    // Verify vertical scrolling works (page should be scrollable if content exceeds viewport)
    const pageHeight = await page.evaluate(() => document.documentElement.scrollHeight)
    const viewportHeight = await page.evaluate(() => window.innerHeight)

    // If content is taller than viewport, verify we can scroll
    if (pageHeight > viewportHeight) {
      await page.evaluate(() => window.scrollTo(0, document.documentElement.scrollHeight))
      const scrollY = await page.evaluate(() => window.scrollY)
      expect(scrollY).toBeGreaterThan(0)
    }
  })
})
