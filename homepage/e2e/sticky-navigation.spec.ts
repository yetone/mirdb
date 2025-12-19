import { test, expect } from '@playwright/test'

test.describe('Navigation and Sticky Header', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    // Wait for the page to fully load
    await expect(page.locator('.app')).toBeVisible()
  })

  test('TC1: Navigation remains fixed at top when scrolling', async ({ page }) => {
    // Get the navigation element
    const stickyNav = page.locator('nav.sticky-nav, .sticky-header, [data-testid="sticky-nav"]')
    await expect(stickyNav).toBeVisible()

    // Get initial position of navigation
    const initialNavBox = await stickyNav.boundingBox()
    expect(initialNavBox).not.toBeNull()
    const initialTop = initialNavBox!.y

    // Scroll down past the hero section
    await page.evaluate(() => {
      window.scrollBy(0, 600)
    })

    // Wait for scroll to complete
    await page.waitForTimeout(300)

    // Get navigation position after scrolling
    const afterScrollNavBox = await stickyNav.boundingBox()
    expect(afterScrollNavBox).not.toBeNull()

    // Navigation should still be near the top of the viewport (fixed position)
    // The y position should remain at 0 or close to it when sticky
    expect(afterScrollNavBox!.y).toBeLessThanOrEqual(10)

    // Verify the navigation is still visible
    await expect(stickyNav).toBeVisible()

    // Check that the navigation has sticky/fixed positioning
    const navPosition = await stickyNav.evaluate((el) => {
      const style = window.getComputedStyle(el)
      return style.position
    })
    expect(['fixed', 'sticky']).toContain(navPosition)
  })

  test('TC2: Click navigation link to Features scrolls smoothly to Features section', async ({ page }) => {
    // Get the Features navigation link
    const featuresLink = page.locator('nav a[href="#features"], [data-testid="nav-features"]')
    await expect(featuresLink).toBeVisible()

    // Get the Features section
    const featuresSection = page.locator('#features, [data-testid="features-section"]')
    await expect(featuresSection).toBeAttached()

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY)

    // Click the Features navigation link
    await featuresLink.click()

    // Wait for smooth scroll to complete
    await page.waitForTimeout(800)

    // Verify the page scrolled
    const finalScrollY = await page.evaluate(() => window.scrollY)
    expect(finalScrollY).toBeGreaterThan(initialScrollY)

    // Verify the Features section is now in view
    const featuresSectionBox = await featuresSection.boundingBox()
    expect(featuresSectionBox).not.toBeNull()

    // The section should be at or near the top of the viewport
    // (accounting for sticky nav height)
    expect(featuresSectionBox!.y).toBeLessThan(150)

    // Verify smooth scroll behavior is set on the document
    const scrollBehavior = await page.evaluate(() => {
      return window.getComputedStyle(document.documentElement).scrollBehavior
    })
    expect(scrollBehavior).toBe('smooth')
  })

  test('TC3: Click navigation link to Quick Start scrolls smoothly to Quick Start section', async ({ page }) => {
    // Get the Quick Start navigation link
    const quickStartLink = page.locator('nav a[href="#quick-start"], [data-testid="nav-quick-start"]')
    await expect(quickStartLink).toBeVisible()

    // Get the Quick Start section
    const quickStartSection = page.locator('#quick-start, [data-testid="quick-start-section"]')
    await expect(quickStartSection).toBeAttached()

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY)

    // Click the Quick Start navigation link
    await quickStartLink.click()

    // Wait for smooth scroll to complete
    await page.waitForTimeout(800)

    // Verify the page scrolled
    const finalScrollY = await page.evaluate(() => window.scrollY)
    expect(finalScrollY).toBeGreaterThan(initialScrollY)

    // Verify the Quick Start section is now in view
    const quickStartSectionBox = await quickStartSection.boundingBox()
    expect(quickStartSectionBox).not.toBeNull()

    // The section should be at or near the top of the viewport
    expect(quickStartSectionBox!.y).toBeLessThan(150)
  })

  test('TC4: Navigation links match corresponding sections on page', async ({ page }) => {
    // Define expected navigation items and their corresponding section IDs
    const navItems = [
      { linkSelector: 'nav a[href="#features"]', sectionId: 'features' },
      { linkSelector: 'nav a[href="#quick-start"]', sectionId: 'quick-start' },
      { linkSelector: 'nav a[href="#commands"]', sectionId: 'commands' },
      { linkSelector: 'nav a[href="#configuration"]', sectionId: 'configuration' },
    ]

    for (const item of navItems) {
      // Check that the navigation link exists
      const navLink = page.locator(item.linkSelector).first()
      await expect(navLink).toBeAttached()

      // Check that the corresponding section exists
      const section = page.locator(`#${item.sectionId}`)
      await expect(section).toBeAttached()

      // Verify the link href matches the section id
      const href = await navLink.getAttribute('href')
      expect(href).toBe(`#${item.sectionId}`)
    }

    // Verify the sticky nav has all expected links
    const stickyNav = page.locator('nav.sticky-nav, .sticky-header nav, [data-testid="sticky-nav"]')
    await expect(stickyNav).toBeVisible()

    // Count navigation links (excluding GitHub link)
    const navLinks = stickyNav.locator('a[href^="#"]')
    const linkCount = await navLinks.count()
    expect(linkCount).toBeGreaterThanOrEqual(4)
  })

  test('Navigation scrolls to Commands section when Commands link is clicked', async ({ page }) => {
    // Get the Commands navigation link
    const commandsLink = page.locator('nav a[href="#commands"]').first()
    await expect(commandsLink).toBeVisible()

    // Get the Commands section
    const commandsSection = page.locator('#commands')
    await expect(commandsSection).toBeAttached()

    // Click the Commands navigation link
    await commandsLink.click()

    // Wait for smooth scroll to complete
    await page.waitForTimeout(800)

    // Verify the Commands section is now in view
    const commandsSectionBox = await commandsSection.boundingBox()
    expect(commandsSectionBox).not.toBeNull()
    expect(commandsSectionBox!.y).toBeLessThan(150)
  })

  test('Navigation scrolls to Configuration section when Configuration link is clicked', async ({ page }) => {
    // Get the Configuration navigation link
    const configLink = page.locator('nav a[href="#configuration"]').first()
    await expect(configLink).toBeVisible()

    // Get the Configuration section
    const configSection = page.locator('#configuration')
    await expect(configSection).toBeAttached()

    // Click the Configuration navigation link
    await configLink.click()

    // Wait for smooth scroll to complete
    await page.waitForTimeout(800)

    // Verify the Configuration section is now in view
    const configSectionBox = await configSection.boundingBox()
    expect(configSectionBox).not.toBeNull()
    expect(configSectionBox!.y).toBeLessThan(150)
  })

  test('Sticky navigation has proper z-index and stays on top', async ({ page }) => {
    const stickyNav = page.locator('nav.sticky-nav, .sticky-header, [data-testid="sticky-nav"]')
    await expect(stickyNav).toBeVisible()

    // Check z-index is set high enough to stay on top
    const zIndex = await stickyNav.evaluate((el) => {
      const style = window.getComputedStyle(el)
      return parseInt(style.zIndex) || 0
    })
    expect(zIndex).toBeGreaterThanOrEqual(100)

    // Scroll down and verify nav is still on top
    await page.evaluate(() => {
      window.scrollBy(0, 1000)
    })
    await page.waitForTimeout(300)

    // Navigation should still be visible and accessible
    await expect(stickyNav).toBeVisible()
  })

  test('Navigation contains GitHub link', async ({ page }) => {
    const stickyNav = page.locator('nav.sticky-nav, .sticky-header, [data-testid="sticky-nav"]')
    await expect(stickyNav).toBeVisible()

    // Check for GitHub link in the navigation
    const githubLink = stickyNav.locator('a[href*="github.com"]')
    await expect(githubLink).toBeVisible()
  })
})
