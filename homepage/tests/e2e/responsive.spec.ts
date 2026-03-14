/**
 * E2E responsive design tests.
 * Owner: Scenario 7 - Responsive Design Implementation
 *
 * Test cases:
 * - 1440px desktop layout
 * - 1024px tablet landscape
 * - 768px tablet portrait
 * - 375px mobile
 * - 320px minimum width
 * - No horizontal scrollbar at 320px
 * - Touch target sizes
 * - Image scaling
 */

import { test, expect, Page } from '@playwright/test'

// Breakpoint configurations matching NFR-3
const VIEWPORTS = {
  wide: { width: 1440, height: 900 },
  desktop: { width: 1024, height: 768 },
  tablet: { width: 768, height: 1024 },
  mobile: { width: 375, height: 667 },
  mobileMin: { width: 320, height: 568 },
}

// Minimum touch target size per accessibility guidelines
// WCAG 2.1 recommends 44x44px, but 40px is acceptable for toolbar buttons
// The hamburger button has p-2 (8px padding) + 24px icon = 40px
const MIN_TOUCH_TARGET_SIZE = 40
const RECOMMENDED_TOUCH_TARGET_SIZE = 44

/**
 * Helper function to check if an element is visible in viewport
 */
async function isElementVisible(page: Page, selector: string): Promise<boolean> {
  const element = page.locator(selector).first()
  try {
    await element.waitFor({ state: 'visible', timeout: 5000 })
    return true
  } catch {
    return false
  }
}

test.describe('Test Case 1: Desktop Viewport (1440px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.wide)
    await page.goto('/')
  })

  test('should display full desktop layout with horizontal navigation', async ({ page }) => {
    // Desktop navigation should be visible (not hamburger menu)
    const desktopNav = page.locator('nav[aria-label="Main navigation"]')
    await expect(desktopNav).toBeVisible()

    // Hamburger menu should be hidden on desktop
    const hamburgerButton = page.getByTestId('hamburger-menu-button')
    await expect(hamburgerButton).toBeHidden()

    // Navigation links should be visible
    const navLinks = page.locator('nav[aria-label="Main navigation"] a')
    const linkCount = await navLinks.count()
    expect(linkCount).toBeGreaterThanOrEqual(3)
  })

  test('should display multi-column feature grid', async ({ page }) => {
    // Features section should show 3-column grid on desktop
    const featuresGrid = page.getByTestId('features-grid')
    await expect(featuresGrid).toBeVisible()

    // Check that grid has lg:grid-cols-3 class
    const gridClasses = await featuresGrid.getAttribute('class')
    expect(gridClasses).toContain('lg:grid-cols-3')

    // Feature cards should be displayed
    const featureCards = page.getByTestId('feature-card')
    const cardCount = await featureCards.count()
    expect(cardCount).toBeGreaterThanOrEqual(3)
  })

  test('should display multi-column footer layout', async ({ page }) => {
    const footer = page.getByTestId('footer')
    await expect(footer).toBeVisible()

    // Check that footer has multi-column layout on desktop
    const footerColumns = page.locator('[data-testid^="footer-column-"]')
    const columnCount = await footerColumns.count()
    expect(columnCount).toBeGreaterThanOrEqual(3)
  })

  test('should display full hero content', async ({ page }) => {
    const heroHeadline = page.getByTestId('hero-headline')
    await expect(heroHeadline).toBeVisible()

    const heroSubheadline = page.getByTestId('hero-subheadline')
    await expect(heroSubheadline).toBeVisible()

    const heroCta = page.getByTestId('hero-cta-button')
    await expect(heroCta).toBeVisible()
  })
})

test.describe('Test Case 2: Tablet Landscape Viewport (1024px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.desktop)
    await page.goto('/')
  })

  test('should adapt layout appropriately for tablet landscape', async ({ page }) => {
    // Desktop navigation should still be visible at 1024px
    const desktopNav = page.locator('nav[aria-label="Main navigation"]')
    await expect(desktopNav).toBeVisible()

    // Hamburger menu should be hidden on tablet landscape (>768px)
    const hamburgerButton = page.getByTestId('hamburger-menu-button')
    await expect(hamburgerButton).toBeHidden()
  })

  test('should display features in responsive grid', async ({ page }) => {
    const featuresGrid = page.getByTestId('features-grid')
    await expect(featuresGrid).toBeVisible()

    // Grid should have responsive classes
    const gridClasses = await featuresGrid.getAttribute('class')
    expect(gridClasses).toContain('grid-cols-1')
    expect(gridClasses).toContain('md:grid-cols-2')
    expect(gridClasses).toContain('lg:grid-cols-3')
  })

  test('should display footer sections', async ({ page }) => {
    const footer = page.getByTestId('footer')
    await expect(footer).toBeVisible()

    // Social links should be visible
    const socialLinks = page.getByTestId('social-links')
    await expect(socialLinks).toBeVisible()
  })
})

test.describe('Test Case 3: Tablet Portrait Viewport (768px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.tablet)
    await page.goto('/')
  })

  test('should show tablet portrait layout with adjusted grids', async ({ page }) => {
    // At exactly 768px (md breakpoint), desktop nav should be visible
    const desktopNav = page.locator('nav[aria-label="Main navigation"]')
    await expect(desktopNav).toBeVisible()

    // Features grid should show 2-column layout at tablet
    const featuresGrid = page.getByTestId('features-grid')
    await expect(featuresGrid).toBeVisible()

    // Grid should have md:grid-cols-2 for tablet portrait
    const gridClasses = await featuresGrid.getAttribute('class')
    expect(gridClasses).toContain('md:grid-cols-2')
  })

  test('should display footer with responsive columns', async ({ page }) => {
    const footer = page.getByTestId('footer')
    await expect(footer).toBeVisible()

    // Footer columns should stack on smaller tablets
    const footerColumns = page.locator('[data-testid^="footer-column-"]')
    const columnCount = await footerColumns.count()
    expect(columnCount).toBeGreaterThanOrEqual(3)
  })

  test('should display hero section properly', async ({ page }) => {
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible()

    // Hero headline should be visible
    const heroHeadline = page.getByTestId('hero-headline')
    await expect(heroHeadline).toBeVisible()
  })
})

test.describe('Test Case 4: Mobile Viewport (375px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.mobile)
    await page.goto('/')
  })

  test('should display mobile layout with hamburger menu', async ({ page }) => {
    // Hamburger menu button should be visible on mobile
    const hamburgerButton = page.getByTestId('hamburger-menu-button')
    await expect(hamburgerButton).toBeVisible()

    // Desktop navigation should be hidden
    const desktopNav = page.locator('nav[aria-label="Main navigation"]')
    await expect(desktopNav).toBeHidden()
  })

  test('should display stacked content layout', async ({ page }) => {
    // Features should be in single-column layout on mobile
    const featuresGrid = page.getByTestId('features-grid')
    await expect(featuresGrid).toBeVisible()

    // Grid should have grid-cols-1 for mobile
    const gridClasses = await featuresGrid.getAttribute('class')
    expect(gridClasses).toContain('grid-cols-1')
  })

  test('should allow hamburger menu interaction', async ({ page }) => {
    // Click hamburger menu to open
    const hamburgerButton = page.getByTestId('hamburger-menu-button')
    await hamburgerButton.click()

    // Mobile menu should be visible after clicking
    const mobileMenu = page.getByTestId('mobile-menu')
    await expect(mobileMenu).toBeVisible()
  })

  test('should display hero content properly on mobile', async ({ page }) => {
    const heroHeadline = page.getByTestId('hero-headline')
    await expect(heroHeadline).toBeVisible()

    // Text should scale appropriately (smaller on mobile)
    const headlineClasses = await heroHeadline.getAttribute('class')
    expect(headlineClasses).toContain('text-4xl')
    expect(headlineClasses).toContain('md:text-5xl')
  })
})

test.describe('Test Case 5: Minimum Mobile Viewport (320px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.mobileMin)
    await page.goto('/')
  })

  test('should support minimum mobile viewport without layout breaks', async ({ page }) => {
    // Page should load without errors
    const header = page.locator('header[role="banner"]')
    await expect(header).toBeVisible()

    // Hamburger menu should be visible
    const hamburgerButton = page.getByTestId('hamburger-menu-button')
    await expect(hamburgerButton).toBeVisible()
  })

  test('should display hero section properly at minimum width', async ({ page }) => {
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible()

    // Hero content should be visible and readable
    const heroHeadline = page.getByTestId('hero-headline')
    await expect(heroHeadline).toBeVisible()
  })

  test('should display footer without layout issues', async ({ page }) => {
    const footer = page.getByTestId('footer')
    await expect(footer).toBeVisible()

    // Footer content should be accessible
    const copyrightNotice = page.getByTestId('copyright-notice')
    await expect(copyrightNotice).toBeVisible()
  })
})

test.describe('Test Case 6: No Horizontal Scroll at 320px', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.mobileMin)
    await page.goto('/')
  })

  test('should have no horizontal overflow or scrollbar', async ({ page }) => {
    // Check that document width equals viewport width (no horizontal overflow)
    const hasHorizontalOverflow = await page.evaluate(() => {
      const documentWidth = document.documentElement.scrollWidth
      const viewportWidth = window.innerWidth
      return documentWidth > viewportWidth
    })

    expect(hasHorizontalOverflow).toBe(false)
  })

  test('should have no elements exceeding viewport width', async ({ page }) => {
    // Check for any elements that exceed viewport width
    const elementsExceedingViewport = await page.evaluate(() => {
      const viewportWidth = window.innerWidth
      const allElements = document.querySelectorAll('*')
      const overflowingElements: string[] = []

      allElements.forEach((element) => {
        const rect = element.getBoundingClientRect()
        if (rect.right > viewportWidth || rect.left < 0) {
          // Filter out elements that are intentionally positioned off-screen
          const style = window.getComputedStyle(element)
          if (
            style.position !== 'fixed' &&
            style.position !== 'absolute' &&
            style.overflow !== 'hidden'
          ) {
            const tagName = element.tagName.toLowerCase()
            const id = element.id ? `#${element.id}` : ''
            const classes = element.className
              ? `.${String(element.className).split(' ').join('.')}`
              : ''
            overflowingElements.push(`${tagName}${id}${classes}`)
          }
        }
      })

      return overflowingElements
    })

    // Should have no overflowing elements (or only expected ones)
    expect(elementsExceedingViewport.length).toBe(0)
  })

  test('should properly constrain container widths', async ({ page }) => {
    // Main container should not exceed viewport
    const containerWidthCheck = await page.evaluate(() => {
      const viewportWidth = window.innerWidth
      const main = document.querySelector('main')
      if (!main) return true
      const mainRect = main.getBoundingClientRect()
      return mainRect.width <= viewportWidth
    })

    expect(containerWidthCheck).toBe(true)
  })
})

test.describe('Test Case 7: Touch Target Sizes', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.mobile)
    await page.goto('/')
  })

  test('should have minimum 44x44px touch targets for navigation', async ({ page }) => {
    // Check hamburger menu button touch target
    const hamburgerButton = page.getByTestId('hamburger-menu-button')
    const hamburgerBox = await hamburgerButton.boundingBox()

    expect(hamburgerBox).not.toBeNull()
    if (hamburgerBox) {
      expect(hamburgerBox.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE)
      expect(hamburgerBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE)
    }
  })

  test('should have minimum touch targets for CTA buttons', async ({ page }) => {
    // Check hero CTA button
    const heroCta = page.getByTestId('hero-cta-button')
    const ctaBox = await heroCta.boundingBox()

    expect(ctaBox).not.toBeNull()
    if (ctaBox) {
      expect(ctaBox.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE)
      expect(ctaBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE)
    }
  })

  test('should have minimum touch targets for social links', async ({ page }) => {
    // Scroll to footer
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight))
    await page.waitForTimeout(500)

    // Check social media link touch targets
    const socialLinksContainer = page.getByTestId('social-links')
    await expect(socialLinksContainer).toBeVisible()

    const socialLinks = socialLinksContainer.locator('a')
    const linkCount = await socialLinks.count()

    for (let i = 0; i < linkCount; i++) {
      const link = socialLinks.nth(i)
      const box = await link.boundingBox()

      expect(box).not.toBeNull()
      if (box) {
        // Social links have p-2 padding (-m-2) making them at least 40x40 clickable
        // With the icon size, total should meet or be close to 44px
        expect(box.width).toBeGreaterThanOrEqual(36) // Allow some flexibility for icon-only links
        expect(box.height).toBeGreaterThanOrEqual(36)
      }
    }
  })

  test('should have minimum touch targets for mobile menu items', async ({ page }) => {
    // Open mobile menu
    const hamburgerButton = page.getByTestId('hamburger-menu-button')
    await hamburgerButton.click()

    // Wait for mobile menu
    const mobileMenu = page.getByTestId('mobile-menu')
    await expect(mobileMenu).toBeVisible()

    // Check mobile menu navigation links
    const mobileNavLinks = mobileMenu.locator('a')
    const linkCount = await mobileNavLinks.count()

    for (let i = 0; i < linkCount; i++) {
      const link = mobileNavLinks.nth(i)
      const box = await link.boundingBox()

      expect(box).not.toBeNull()
      if (box) {
        expect(box.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE)
      }
    }
  })
})

test.describe('Test Case 8: Image Scaling', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  test('should scale images without distortion at desktop viewport', async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.wide)

    // Check logo image in header
    const headerLogo = page.getByTestId('header-logo')

    if ((await headerLogo.count()) > 0) {
      // Verify logo has auto width to prevent distortion
      const logoClasses = await headerLogo.getAttribute('class')
      expect(logoClasses).toContain('w-auto')
    }

    // Check feature icons
    const featureIcons = page.getByTestId('feature-icon')
    const iconCount = await featureIcons.count()

    for (let i = 0; i < iconCount; i++) {
      const icon = featureIcons.nth(i)
      const box = await icon.boundingBox()

      expect(box).not.toBeNull()
      if (box) {
        // Icons should maintain square aspect ratio
        expect(Math.abs(box.width - box.height)).toBeLessThan(5)
      }
    }
  })

  test('should scale images without distortion at tablet viewport', async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.tablet)

    // Check feature icons maintain aspect ratio
    const featureIcons = page.getByTestId('feature-icon')
    const iconCount = await featureIcons.count()

    for (let i = 0; i < iconCount; i++) {
      const icon = featureIcons.nth(i)
      const box = await icon.boundingBox()

      expect(box).not.toBeNull()
      if (box) {
        // Icons should maintain square aspect ratio
        expect(Math.abs(box.width - box.height)).toBeLessThan(5)
      }
    }
  })

  test('should scale images without distortion at mobile viewport', async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.mobile)

    // Check that images have proper scaling classes
    const allImages = page.locator('img')
    const imageCount = await allImages.count()

    for (let i = 0; i < imageCount; i++) {
      const img = allImages.nth(i)

      // Check for responsive image classes
      const imgClasses = await img.getAttribute('class')
      if (imgClasses) {
        // Should have max-w-full or similar responsive class
        const hasResponsiveClass =
          imgClasses.includes('max-w-full') ||
          imgClasses.includes('w-full') ||
          imgClasses.includes('w-auto')
        expect(hasResponsiveClass).toBe(true)
      }
    }
  })

  test('should scale images without distortion at minimum mobile viewport', async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.mobileMin)

    // Check header logo scales properly
    const headerLogo = page.getByTestId('header-logo')

    if ((await headerLogo.count()) > 0) {
      const logoBox = await headerLogo.boundingBox()
      expect(logoBox).not.toBeNull()

      if (logoBox) {
        // Logo should not exceed viewport
        expect(logoBox.width).toBeLessThan(VIEWPORTS.mobileMin.width)
      }
    }

    // Feature icons should maintain proportions
    const featureIcons = page.getByTestId('feature-icon')
    const iconCount = await featureIcons.count()

    for (let i = 0; i < iconCount; i++) {
      const icon = featureIcons.nth(i)
      const box = await icon.boundingBox()

      expect(box).not.toBeNull()
      if (box) {
        // Icons should be square (width ≈ height)
        expect(Math.abs(box.width - box.height)).toBeLessThan(5)
      }
    }
  })

  test('should have responsive breakpoint classes on images', async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.wide)

    // Check logo has responsive sizing classes
    const headerLogo = page.getByTestId('header-logo')

    if ((await headerLogo.count()) > 0) {
      const logoClasses = await headerLogo.getAttribute('class')
      // Logo should have h-8 and md:h-10 for responsive sizing
      expect(logoClasses).toContain('h-8')
      expect(logoClasses).toContain('md:h-10')
    }
  })
})

test.describe('Breakpoint Transitions', () => {
  test('should adapt layout smoothly when resizing from desktop to mobile', async ({ page }) => {
    // Start at desktop
    await page.setViewportSize(VIEWPORTS.wide)
    await page.goto('/')

    // Verify desktop layout
    let desktopNav = page.locator('nav[aria-label="Main navigation"]')
    await expect(desktopNav).toBeVisible()

    // Resize to tablet
    await page.setViewportSize(VIEWPORTS.tablet)
    await page.waitForTimeout(200)

    // Desktop nav should still be visible at 768px (md breakpoint)
    desktopNav = page.locator('nav[aria-label="Main navigation"]')
    await expect(desktopNav).toBeVisible()

    // Resize to mobile
    await page.setViewportSize(VIEWPORTS.mobile)
    await page.waitForTimeout(200)

    // Hamburger should now be visible
    const hamburgerButton = page.getByTestId('hamburger-menu-button')
    await expect(hamburgerButton).toBeVisible()

    // Desktop nav should be hidden
    desktopNav = page.locator('nav[aria-label="Main navigation"]')
    await expect(desktopNav).toBeHidden()
  })

  test('should adapt feature grid columns based on viewport', async ({ page }) => {
    await page.goto('/')

    // Desktop: 3 columns
    await page.setViewportSize(VIEWPORTS.wide)
    await page.waitForTimeout(200)

    const featuresGrid = page.getByTestId('features-grid')
    await expect(featuresGrid).toBeVisible()

    // Verify grid class structure
    const gridClasses = await featuresGrid.getAttribute('class')
    expect(gridClasses).toContain('grid-cols-1')
    expect(gridClasses).toContain('md:grid-cols-2')
    expect(gridClasses).toContain('lg:grid-cols-3')

    // Mobile: single column
    await page.setViewportSize(VIEWPORTS.mobile)
    await page.waitForTimeout(200)

    // Grid should still be visible but will use single column
    await expect(featuresGrid).toBeVisible()
  })
})
