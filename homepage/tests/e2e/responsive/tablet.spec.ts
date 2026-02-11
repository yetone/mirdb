/**
 * E2E tests for tablet responsive design
 * Scenario 9 - Responsive Design - Tablet
 *
 * Requirements:
 * - REQ-13: Responsive on tablet devices
 * - Grid layouts should adapt
 * - Content should be readable without zooming
 */

import { test, expect } from '@playwright/test'

// Standard tablet viewport (768px width, common for iPad Portrait)
const TABLET_VIEWPORT = { width: 768, height: 1024 }

test.describe('Tablet Responsive Design', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(TABLET_VIEWPORT)
    await page.goto('/')
  })

  test('layout adapts to tablet view with feature cards in 2 columns', async ({ page }) => {
    // Test Case 1: Render homepage at 768px width
    // Expected: Layout adapts to tablet view, feature cards may show 2 columns

    // Navigate to the features section
    const featuresSection = page.locator('#features')
    await expect(featuresSection).toBeVisible()

    // Check that the features grid exists
    const featuresGrid = page.locator('.features__grid')
    await expect(featuresGrid).toBeVisible()

    // Verify the grid is using the tablet layout (2 columns)
    // At 768px, grid should have 2 columns (based on Features.css @media max-width: 992px)
    const gridStyle = await featuresGrid.evaluate((el) => {
      const style = window.getComputedStyle(el)
      return style.gridTemplateColumns
    })

    // Grid should have 2 columns at tablet width (verify it's not 1 or 3 columns)
    // The exact value will be computed based on the content, but it should have 2 track values
    const columnCount = gridStyle.split(' ').filter(v => v && v !== 'none').length
    expect(columnCount).toBe(2)
  })

  test('navigation displays appropriately at tablet viewport', async ({ page }) => {
    // Test Case 2: Check navigation at tablet viewport
    // Expected: Navigation displays appropriately (full nav or simplified)

    // At 768px, desktop navigation should be visible (based on Navigation.css @media min-width: 768px)
    const desktopNav = page.locator('.navigation')
    await expect(desktopNav).toBeVisible()

    // Navigation links should be visible
    const navLinks = page.locator('.navigation__link')
    const linkCount = await navLinks.count()
    expect(linkCount).toBeGreaterThan(0)

    // All nav links should be interactive
    const documentationLink = page.locator('.navigation__link').filter({ hasText: 'Documentation' })
    await expect(documentationLink).toBeVisible()

    const githubLink = page.locator('.navigation__link').filter({ hasText: 'GitHub' })
    await expect(githubLink).toBeVisible()

    // Hamburger menu should be hidden at tablet size (768px is the breakpoint)
    const hamburgerButton = page.getByRole('button', { name: /open menu/i })
    await expect(hamburgerButton).toBeHidden()
  })

  test('hero section content and CTAs are clearly visible and accessible', async ({ page }) => {
    // Test Case 3: Check hero section at tablet viewport
    // Expected: Hero content and CTAs are clearly visible and accessible

    // Hero section should be visible
    const heroSection = page.locator('#hero')
    await expect(heroSection).toBeVisible()

    // Hero title should be visible and readable
    const heroTitle = page.locator('.hero__title')
    await expect(heroTitle).toBeVisible()

    // Hero subtitle should be visible
    const heroSubtitle = page.locator('.hero__subtitle')
    await expect(heroSubtitle).toBeVisible()

    // Hero description should be visible
    const heroDescription = page.locator('.hero__description')
    await expect(heroDescription).toBeVisible()

    // Primary CTA (Get Started) should be visible and clickable
    const primaryCTA = page.getByRole('button', { name: 'Get Started' })
    await expect(primaryCTA).toBeVisible()
    await expect(primaryCTA).toBeEnabled()

    // Secondary CTA (View on GitHub) should be visible
    const secondaryCTA = page.getByRole('link', { name: 'View on GitHub' })
    await expect(secondaryCTA).toBeVisible()

    // CTAs should be accessible (have proper focus states)
    await primaryCTA.focus()
    const primaryFocusVisible = await primaryCTA.evaluate((el) => {
      const style = window.getComputedStyle(el, ':focus-visible')
      return el.matches(':focus-visible') || el.matches(':focus')
    })
    expect(primaryFocusVisible).toBeTruthy()
  })

  test('Quick Start section code blocks are readable and copy buttons accessible', async ({ page }) => {
    // Test Case 4: Check Quick Start section at tablet viewport
    // Expected: Code blocks are readable and copy buttons accessible

    // Navigate to Quick Start section
    const quickStartSection = page.locator('#quick-start')
    await expect(quickStartSection).toBeVisible()

    // Section heading should be visible
    const heading = page.locator('.quick-start__heading')
    await expect(heading).toBeVisible()
    await expect(heading).toContainText('Quick Start')

    // Code blocks should be visible
    const codeBlocks = page.locator('.code-block')
    const codeBlockCount = await codeBlocks.count()
    expect(codeBlockCount).toBeGreaterThan(0)

    // Check first code block is readable
    const firstCodeBlock = codeBlocks.first()
    await expect(firstCodeBlock).toBeVisible()

    // Code content should be visible
    const codeContent = firstCodeBlock.locator('.code-block__code')
    await expect(codeContent).toBeVisible()

    // Verify code block width is appropriate for tablet (not overflowing)
    const codeBlockBox = await firstCodeBlock.boundingBox()
    expect(codeBlockBox).not.toBeNull()
    if (codeBlockBox) {
      // Code block should fit within viewport with some margin
      expect(codeBlockBox.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width)
    }

    // Copy button should be visible and accessible
    const copyButton = firstCodeBlock.locator('.code-block__copy')
    await expect(copyButton).toBeVisible()
    await expect(copyButton).toBeEnabled()

    // Copy button should have accessible label
    const ariaLabel = await copyButton.getAttribute('aria-label')
    expect(ariaLabel).toBeTruthy()
    expect(ariaLabel).toMatch(/copy|clipboard/i)
  })

  test('content is readable without horizontal scrolling', async ({ page }) => {
    // Additional test to verify REQ-13 compliance
    // Content should be readable without horizontal scrolling

    // Check page doesn't require horizontal scroll
    const hasHorizontalOverflow = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth
    })
    expect(hasHorizontalOverflow).toBeFalsy()

    // All main content containers should be within viewport
    const container = page.locator('.container').first()
    const containerBox = await container.boundingBox()
    expect(containerBox).not.toBeNull()
    if (containerBox) {
      expect(containerBox.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width)
    }
  })

  test('text sizes are appropriate for tablet viewing', async ({ page }) => {
    // Verify content readability at tablet viewport

    // Hero title should have appropriate font size
    const heroTitle = page.locator('.hero__title')
    const heroTitleFontSize = await heroTitle.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize)
    })
    // Title should be large enough to be readable (at least 24px for tablet)
    expect(heroTitleFontSize).toBeGreaterThanOrEqual(24)

    // Body text should be readable
    const heroDescription = page.locator('.hero__description')
    const descriptionFontSize = await heroDescription.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize)
    })
    // Description should be at least 14px for readability
    expect(descriptionFontSize).toBeGreaterThanOrEqual(14)
  })

  test('interactive elements have adequate touch target size', async ({ page }) => {
    // Verify that interactive elements are large enough to be usable on tablet

    // Primary CTA button should have adequate width for touch
    const primaryCTA = page.getByRole('button', { name: 'Get Started' })
    const ctaBox = await primaryCTA.boundingBox()
    expect(ctaBox).not.toBeNull()
    if (ctaBox) {
      // Button should be wide enough for easy tapping
      expect(ctaBox.width).toBeGreaterThanOrEqual(100)
      // Button should have reasonable height for touch interaction
      expect(ctaBox.height).toBeGreaterThanOrEqual(20)
    }

    // Navigation links should be visible and interactive
    const navLink = page.locator('.navigation__link').first()
    const navLinkBox = await navLink.boundingBox()
    expect(navLinkBox).not.toBeNull()
    if (navLinkBox) {
      // Links should be visible and have some height
      expect(navLinkBox.height).toBeGreaterThanOrEqual(16)
    }
  })

  test('logo is visible at tablet viewport', async ({ page }) => {
    // Verify logo is displayed correctly
    const logo = page.locator('.header__logo')
    await expect(logo).toBeVisible()

    // Logo should be appropriately sized
    const logoBox = await logo.boundingBox()
    expect(logoBox).not.toBeNull()
    if (logoBox) {
      expect(logoBox.width).toBeGreaterThan(0)
      expect(logoBox.height).toBeGreaterThan(0)
    }
  })

  test('theme toggle is visible and functional at tablet viewport', async ({ page }) => {
    // Theme toggle should be visible
    const themeToggle = page.getByRole('button', { name: /switch to .* mode/i })
    await expect(themeToggle).toBeVisible()

    // Theme toggle should be clickable
    await expect(themeToggle).toBeEnabled()
  })
})
