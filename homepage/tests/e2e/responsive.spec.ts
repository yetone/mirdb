/**
 * E2E Tests for Responsive Design.
 * Owner: Scenario 8 - Responsive Design
 *
 * Tests:
 * - Mobile viewport (375px) - no horizontal overflow
 * - Mobile navigation hamburger menu visibility and functionality
 * - Tablet viewport (768px) - layout adaptation
 * - Desktop viewport (1280px) - full layout
 * - Touch targets (44x44 minimum)
 * - Code blocks scrollable on mobile
 *
 * Requirements:
 * - REQ-6: Responsive display on mobile
 * - US-5: Mobile accessibility
 * - NFR-1: Page load under 2 seconds
 */

import { test, expect } from '@playwright/test'

// Mobile viewport - iPhone width
const MOBILE_VIEWPORT = { width: 375, height: 812 }

// Tablet viewport - iPad width
const TABLET_VIEWPORT = { width: 768, height: 1024 }

// Desktop viewport - Standard desktop
const DESKTOP_VIEWPORT = { width: 1280, height: 800 }

// Minimum touch target size per WCAG
const MIN_TOUCH_TARGET = 44

test.describe('Responsive Design - Mobile, Tablet, Desktop', () => {
  test.describe('Mobile Viewport (375px)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(MOBILE_VIEWPORT)
    })

    test('TC1: Page renders without horizontal overflow at 375px', async ({ page }) => {
      await page.goto('/')
      await page.waitForLoadState('networkidle')

      // Check that document body doesn't have horizontal scroll
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth
      })

      expect(hasHorizontalScroll).toBe(false)

      // Additional check: critical elements don't cause horizontal overflow
      // Note: Some fixed-position elements may extend beyond viewport boundaries
      // but should not cause visible scrollbars
      const overflowInfo = await page.evaluate(() => {
        const viewportWidth = document.documentElement.clientWidth
        const body = document.body
        const html = document.documentElement

        return {
          hasHorizontalScroll: html.scrollWidth > html.clientWidth,
          bodyOverflowX: window.getComputedStyle(body).overflowX,
          htmlOverflowX: window.getComputedStyle(html).overflowX,
          scrollWidth: html.scrollWidth,
          clientWidth: html.clientWidth
        }
      })

      // The main check is whether there's actual horizontal scroll capability
      expect(overflowInfo.hasHorizontalScroll).toBe(false)
    })

    test('TC2: Mobile navigation menu (hamburger) is visible at 375px', async ({ page }) => {
      await page.goto('/')
      await page.waitForLoadState('networkidle')

      // Find the mobile menu button using data-testid
      const mobileMenuButton = page.locator('[data-testid="mobile-menu-button"]')

      // Button should be visible
      await expect(mobileMenuButton).toBeVisible()

      // Button should be accessible
      await expect(mobileMenuButton).toHaveAttribute('aria-label', /navigation menu/i)
      await expect(mobileMenuButton).toHaveAttribute('aria-expanded', 'false')
    })

    test('TC3: Navigation menu expands when clicking mobile menu button', async ({ page }) => {
      await page.goto('/')
      await page.waitForLoadState('networkidle')

      // Find and click the mobile menu button
      const mobileMenuButton = page.locator('[data-testid="mobile-menu-button"]')
      await mobileMenuButton.click()

      // Wait for menu to appear
      const mobileNavMenu = page.locator('#mobile-nav-menu')
      await expect(mobileNavMenu).toBeVisible()

      // Check that navigation links are visible
      await expect(page.locator('.mobile-nav-link').first()).toBeVisible()

      // Verify expected navigation items
      await expect(page.locator('.mobile-nav-link:has-text("Features")')).toBeVisible()
      await expect(page.locator('.mobile-nav-link:has-text("Quick Start")')).toBeVisible()
      await expect(page.locator('.mobile-nav-link:has-text("Architecture")')).toBeVisible()

      // Verify aria-expanded is updated
      await expect(mobileMenuButton).toHaveAttribute('aria-expanded', 'true')

      // Test closing the menu
      const closeButton = page.locator('[data-testid="mobile-menu-close"]')
      await closeButton.click()

      await expect(mobileNavMenu).not.toBeVisible()
    })

    test('TC6: Interactive elements have minimum 44x44 touch targets on mobile', async ({
      page,
    }) => {
      await page.goto('/')
      await page.waitForLoadState('networkidle')

      // Check mobile menu button touch target
      const mobileMenuButton = page.locator('[data-testid="mobile-menu-button"]')
      const buttonBox = await mobileMenuButton.boundingBox()

      expect(buttonBox).not.toBeNull()
      if (buttonBox) {
        expect(buttonBox.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET)
        expect(buttonBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET)
      }

      // Open menu and check navigation link touch targets
      await mobileMenuButton.click()
      await page.waitForSelector('.mobile-nav-link')

      const navLinks = page.locator('.mobile-nav-link')
      const linkCount = await navLinks.count()

      for (let i = 0; i < linkCount; i++) {
        const link = navLinks.nth(i)
        const linkBox = await link.boundingBox()

        expect(linkBox).not.toBeNull()
        if (linkBox) {
          expect(linkBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET)
        }
      }
    })

    test('TC7: Code blocks are scrollable within container on mobile', async ({ page }) => {
      await page.goto('/')
      await page.waitForLoadState('networkidle')

      // Navigate to Quick Start section where code blocks should be
      const quickStartSection = page.locator('#quick-start')
      if (await quickStartSection.isVisible()) {
        await quickStartSection.scrollIntoViewIfNeeded()
      }

      // Check code blocks (pre elements with code)
      const codeBlocks = page.locator('pre[class*="language-"]')
      const codeBlockCount = await codeBlocks.count()

      if (codeBlockCount > 0) {
        for (let i = 0; i < codeBlockCount; i++) {
          const codeBlock = codeBlocks.nth(i)

          // Check that the code block has overflow-x-auto or similar scrolling
          const overflowStyle = await codeBlock.evaluate((el) => {
            const computed = window.getComputedStyle(el)
            return {
              overflowX: computed.overflowX,
              overflowY: computed.overflowY,
              scrollWidth: el.scrollWidth,
              clientWidth: el.clientWidth,
            }
          })

          // Code block should be scrollable (overflow-x: auto or scroll)
          expect(['auto', 'scroll']).toContain(overflowStyle.overflowX)
        }
      }

      // Verify page itself has no horizontal overflow
      const pageHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth
      })

      expect(pageHorizontalScroll).toBe(false)
    })
  })

  test.describe('Tablet Viewport (768px)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(TABLET_VIEWPORT)
    })

    test('TC4: Page layout adapts for tablet view at 768px', async ({ page }) => {
      await page.goto('/')
      await page.waitForLoadState('networkidle')

      // Page should not have horizontal scroll
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth
      })

      expect(hasHorizontalScroll).toBe(false)

      // Content should be visible and properly laid out
      const heroSection = page.locator('section[aria-label="Hero"]')
      await expect(heroSection).toBeVisible()

      // At tablet width, navigation may be either desktop or mobile style
      // Check that at least one navigation approach is available
      const desktopNav = page.locator('nav[aria-label="Main navigation"] ul')
      const mobileMenuButton = page.locator('[data-testid="mobile-menu-button"]')

      // Either desktop nav should be visible OR mobile button should be visible
      const desktopNavVisible = await desktopNav.isVisible()
      const mobileButtonVisible = await mobileMenuButton.isVisible()

      expect(desktopNavVisible || mobileButtonVisible).toBe(true)

      // Container should adapt to tablet width
      const container = page.locator('.container').first()
      const containerBox = await container.boundingBox()

      expect(containerBox).not.toBeNull()
      if (containerBox) {
        expect(containerBox.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width)
      }
    })
  })

  test.describe('Desktop Viewport (1280px)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(DESKTOP_VIEWPORT)
    })

    test('TC5: Page displays full desktop layout at 1280px', async ({ page }) => {
      await page.goto('/')
      await page.waitForLoadState('networkidle')

      // Page should not have horizontal scroll
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth
      })

      expect(hasHorizontalScroll).toBe(false)

      // Desktop navigation should be visible
      const desktopNav = page.locator('nav[aria-label="Main navigation"] ul')
      await expect(desktopNav).toBeVisible()

      // Navigation links should be visible
      await expect(page.locator('nav[aria-label="Main navigation"] a:has-text("Features")')).toBeVisible()
      await expect(page.locator('nav[aria-label="Main navigation"] a:has-text("Quick Start")')).toBeVisible()
      await expect(page.locator('nav[aria-label="Main navigation"] a:has-text("Architecture")')).toBeVisible()

      // Mobile menu button should be hidden
      const mobileMenuButton = page.locator('[data-testid="mobile-menu-button"]')
      await expect(mobileMenuButton).not.toBeVisible()

      // All sections should be present
      await expect(page.locator('section[aria-label="Hero"]')).toBeVisible()
      await expect(page.locator('#features')).toBeVisible()

      // Footer should be present
      await expect(page.locator('footer[role="contentinfo"]')).toBeVisible()
    })
  })

  test.describe('Cross-viewport Consistency', () => {
    test('Navigation functionality across viewports', async ({ page }) => {
      // Test at mobile size
      await page.setViewportSize(MOBILE_VIEWPORT)
      await page.goto('/')
      await page.waitForLoadState('networkidle')

      // Open mobile menu
      const mobileMenuButton = page.locator('[data-testid="mobile-menu-button"]')
      await mobileMenuButton.click()

      // Click on a navigation link
      const featuresLink = page.locator('.mobile-nav-link:has-text("Features")')
      await featuresLink.click()

      // Menu should close
      const mobileNavMenu = page.locator('#mobile-nav-menu')
      await expect(mobileNavMenu).not.toBeVisible()

      // Resize to desktop
      await page.setViewportSize(DESKTOP_VIEWPORT)

      // Desktop navigation should now be visible
      const desktopNav = page.locator('nav[aria-label="Main navigation"] ul')
      await expect(desktopNav).toBeVisible()
    })

    test('Content readability across all viewports', async ({ page }) => {
      const viewports = [MOBILE_VIEWPORT, TABLET_VIEWPORT, DESKTOP_VIEWPORT]

      for (const viewport of viewports) {
        await page.setViewportSize(viewport)
        await page.goto('/')
        await page.waitForLoadState('networkidle')

        // Hero title should be visible
        const heroTitle = page.locator('h1:has-text("MirDB")')
        await expect(heroTitle).toBeVisible()

        // CTA buttons should be visible
        const ctaButton = page.locator('a:has-text("Get Started")')
        await expect(ctaButton).toBeVisible()

        // No horizontal overflow
        const hasHorizontalScroll = await page.evaluate(() => {
          return document.documentElement.scrollWidth > document.documentElement.clientWidth
        })

        expect(hasHorizontalScroll).toBe(false)
      }
    })
  })
})
