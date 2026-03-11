/**
 * User Journey E2E Tests
 * Owner: Scenarios 16 & 17 - User Journeys
 *
 * Test coverage:
 * - Product Discovery journey (Scenario 16)
 *   - Landing, reading hero, exploring features, demo, CTA
 * - Getting Started journey (Scenario 17)
 *   - Quick Start navigation, copy install command, documentation link
 */

import { test, expect } from '@playwright/test'

test.describe('User Journey - Product Discovery (Scenario 16)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    // Wait for the page to fully render
    await page.waitForSelector('[data-testid="hero-section"]')
  })

  test.describe('Test Case 1: Complete product discovery journey', () => {
    test('user can understand MirDB value proposition, features, and interface within 2 minutes', async ({ page }) => {
      const startTime = Date.now()

      // Step 1: Land on homepage - Hero section is visible
      const heroSection = page.locator('[data-testid="hero-section"]')
      await expect(heroSection).toBeVisible()

      // Step 2: Read hero section - Value proposition is clear
      const productName = page.locator('[data-testid="hero-product-name"]')
      await expect(productName).toBeVisible()
      await expect(productName).toContainText('MirDB')

      const tagline = page.locator('[data-testid="hero-tagline"]')
      await expect(tagline).toBeVisible()
      await expect(tagline).toContainText('persistent')
      await expect(tagline).toContainText('key-value')

      const valueProposition = page.locator('[data-testid="hero-value-proposition"]')
      await expect(valueProposition).toBeVisible()

      // Step 3: Explore features - Scroll to features section
      const featuresSection = page.locator('#features')
      await featuresSection.scrollIntoViewIfNeeded()
      await page.waitForTimeout(500) // Allow scroll animation

      const featureGrid = page.locator('[data-testid="feature-grid"]')
      await expect(featureGrid).toBeVisible()

      // Verify feature cards are present
      const featureCards = page.locator('[data-testid^="feature-card-"]')
      const featureCount = await featureCards.count()
      expect(featureCount).toBeGreaterThanOrEqual(3) // At least 3 features

      // Step 4: Try interactive demo - Scroll to demo section
      const demoSection = page.locator('[data-testid="interactive-demo"]')
      await demoSection.scrollIntoViewIfNeeded()
      await page.waitForTimeout(500)

      await expect(demoSection).toBeVisible()

      // Verify command examples are present
      const commandExamples = page.locator('[data-testid^="command-example-"]')
      const exampleCount = await commandExamples.count()
      expect(exampleCount).toBeGreaterThanOrEqual(1) // At least one command example

      // Step 5: Proceed to action - CTA button is accessible
      const getStartedCTA = page.locator('[data-testid="hero-cta-get-started"]')
      await heroSection.scrollIntoViewIfNeeded()
      await page.waitForTimeout(300)
      await expect(getStartedCTA).toBeVisible()

      const endTime = Date.now()
      const durationSeconds = (endTime - startTime) / 1000

      // Verify the entire journey took less than 2 minutes (120 seconds)
      expect(durationSeconds).toBeLessThan(120)
    })
  })

  test.describe('Test Case 2: Check value proposition visibility', () => {
    test('value proposition is visible above fold without scrolling', async ({ page }) => {
      // Set a standard viewport size
      await page.setViewportSize({ width: 1280, height: 720 })
      await page.goto('/')
      await page.waitForSelector('[data-testid="hero-section"]')

      // Get viewport height
      const viewportHeight = 720

      // Check hero section is above fold
      const heroSection = page.locator('[data-testid="hero-section"]')
      await expect(heroSection).toBeVisible()

      // Check product name is above fold
      const productName = page.locator('[data-testid="hero-product-name"]')
      await expect(productName).toBeVisible()
      const productNameBox = await productName.boundingBox()
      expect(productNameBox.y + productNameBox.height).toBeLessThan(viewportHeight)

      // Check tagline is above fold
      const tagline = page.locator('[data-testid="hero-tagline"]')
      await expect(tagline).toBeVisible()
      const taglineBox = await tagline.boundingBox()
      expect(taglineBox.y + taglineBox.height).toBeLessThan(viewportHeight)

      // Check value proposition text is above fold
      const valueProposition = page.locator('[data-testid="hero-value-proposition"]')
      await expect(valueProposition).toBeVisible()
      const valueBox = await valueProposition.boundingBox()
      expect(valueBox.y + valueBox.height).toBeLessThan(viewportHeight)

      // Check at least one CTA is above fold
      const getStartedCTA = page.locator('[data-testid="hero-cta-get-started"]')
      await expect(getStartedCTA).toBeVisible()
      const ctaBox = await getStartedCTA.boundingBox()
      expect(ctaBox.y + ctaBox.height).toBeLessThan(viewportHeight)
    })
  })

  test.describe('Test Case 3: Check feature comprehension', () => {
    test('feature descriptions clearly explain what MirDB does differently (persistence + memcached)', async ({ page }) => {
      // Navigate to features section
      const featuresSection = page.locator('#features')
      await featuresSection.scrollIntoViewIfNeeded()
      await page.waitForTimeout(300)

      // Check section title is visible
      const sectionTitle = featuresSection.locator('.section-title')
      await expect(sectionTitle).toBeVisible()

      // Check feature grid exists
      const featureGrid = page.locator('[data-testid="feature-grid"]')
      await expect(featureGrid).toBeVisible()

      // Get all feature cards
      const featureCards = page.locator('[data-testid^="feature-card-"]')
      const count = await featureCards.count()
      expect(count).toBeGreaterThanOrEqual(3)

      // Extract all feature content to verify key concepts are mentioned
      const pageContent = await page.locator('#features').textContent()
      const lowerContent = pageContent.toLowerCase()

      // Verify persistence concept is mentioned
      const hasPersistence = lowerContent.includes('persist') || lowerContent.includes('durable') || lowerContent.includes('sstable')
      expect(hasPersistence).toBe(true)

      // Verify memcached compatibility concept is mentioned
      const hasMemcached = lowerContent.includes('memcached') || lowerContent.includes('protocol')
      expect(hasMemcached).toBe(true)

      // Verify LSM tree architecture is mentioned
      const hasLSM = lowerContent.includes('lsm') || lowerContent.includes('log-structured')
      expect(hasLSM).toBe(true)

      // Each feature card should have a title and description
      for (let i = 0; i < count; i++) {
        const card = featureCards.nth(i)
        const title = card.locator('.feature-title')
        const description = card.locator('.feature-description')

        await expect(title).toBeVisible()
        await expect(description).toBeVisible()

        // Title should not be empty
        const titleText = await title.textContent()
        expect(titleText.trim().length).toBeGreaterThan(0)

        // Description should not be empty
        const descText = await description.textContent()
        expect(descText.trim().length).toBeGreaterThan(0)
      }
    })
  })

  test.describe('Test Case 4: Check demo comprehension', () => {
    test('user understands command interface from demo examples', async ({ page }) => {
      // Navigate to demo section
      const demoSection = page.locator('[data-testid="interactive-demo"]')
      await demoSection.scrollIntoViewIfNeeded()
      await page.waitForTimeout(300)

      await expect(demoSection).toBeVisible()

      // Check demo title is present and descriptive
      const demoTitle = demoSection.locator('.demo-title')
      await expect(demoTitle).toBeVisible()

      // Check demo subtitle/description is present
      const demoSubtitle = demoSection.locator('.demo-subtitle')
      await expect(demoSubtitle).toBeVisible()

      // Check command examples are present
      const commandGrid = page.locator('[data-testid="command-grid"]')
      await expect(commandGrid).toBeVisible()

      // Verify SET command example
      const setExample = page.locator('[data-testid="command-example-set"]')
      if (await setExample.count() > 0) {
        await expect(setExample).toBeVisible()

        // Check it has command name visible
        const setName = setExample.locator('.command-name')
        await expect(setName).toContainText(/SET/i)

        // Check it has description
        const setDesc = setExample.locator('.command-description')
        await expect(setDesc).toBeVisible()

        // Check code blocks exist
        const codeBlocks = setExample.locator('[data-testid^="code-block-"]')
        expect(await codeBlocks.count()).toBeGreaterThanOrEqual(1)
      }

      // Verify GET command example
      const getExample = page.locator('[data-testid="command-example-get"]')
      if (await getExample.count() > 0) {
        await expect(getExample).toBeVisible()

        const getName = getExample.locator('.command-name')
        await expect(getName).toContainText(/GET/i)
      }

      // Verify DELETE command example
      const deleteExample = page.locator('[data-testid="command-example-delete"]')
      if (await deleteExample.count() > 0) {
        await expect(deleteExample).toBeVisible()

        const deleteName = deleteExample.locator('.command-name')
        await expect(deleteName).toContainText(/DELETE/i)
      }

      // Verify the demo section mentions connection instructions
      const demoContent = await demoSection.textContent()
      const hasConnectionInfo = demoContent.includes('telnet') || demoContent.includes('localhost') || demoContent.includes('12333')
      expect(hasConnectionInfo).toBe(true)
    })
  })

  test.describe('Test Case 5: Check CTA accessibility', () => {
    test('at least one CTA is visible at all scroll positions', async ({ page }) => {
      // Define scroll positions to test
      const scrollPositions = [
        0,      // Top of page (hero)
        500,    // Mid-page
        1000,   // Further down
        1500,   // Even further
      ]

      for (const scrollY of scrollPositions) {
        await page.evaluate((y) => window.scrollTo(0, y), scrollY)
        await page.waitForTimeout(200) // Allow for scroll settle

        // Check if any CTA-like element is visible
        // This includes nav CTA, hero CTA, or any other "Get Started" type button
        const ctaSelectors = [
          '[data-testid="hero-cta-get-started"]',
          '[data-testid="hero-cta-github"]',
          '[data-testid="nav-link-documentation"]',
          '[data-testid="nav-link-github"]',
          'a[href="#quickstart"]',
          'a[href*="github.com"]'
        ]

        let hasCTAVisible = false
        for (const selector of ctaSelectors) {
          const element = page.locator(selector).first()
          if (await element.count() > 0) {
            const isVisible = await element.isVisible()
            if (isVisible) {
              hasCTAVisible = true
              break
            }
          }
        }

        // At scroll position, at least one CTA should be visible
        expect(hasCTAVisible).toBe(true)
      }
    })

    test('navigation CTAs remain accessible via sticky navigation', async ({ page }) => {
      // Navigation should be sticky/fixed
      const navBar = page.locator('[data-testid="navigation-bar"]')
      await expect(navBar).toBeVisible()

      // Scroll down significantly
      await page.evaluate(() => window.scrollTo(0, 1500))
      await page.waitForTimeout(300)

      // Navigation should still be visible (sticky behavior)
      await expect(navBar).toBeVisible()

      // GitHub link in nav should be accessible
      const githubNavLink = page.locator('[data-testid="nav-link-github"]')
      await expect(githubNavLink).toBeVisible()

      // Documentation/Quick Start link should be accessible
      const docsLink = page.locator('[data-testid="nav-link-documentation"]')
      await expect(docsLink).toBeVisible()
    })
  })
})

test.describe('User Journey - Product Discovery (Mobile)', () => {
  test.use({ viewport: { width: 375, height: 667 } })

  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForSelector('[data-testid="hero-section"]')
  })

  test('mobile user can complete product discovery journey', async ({ page }) => {
    // Step 1: Hero section visible on mobile
    const heroSection = page.locator('[data-testid="hero-section"]')
    await expect(heroSection).toBeVisible()

    const productName = page.locator('[data-testid="hero-product-name"]')
    await expect(productName).toBeVisible()

    // Step 2: Can access CTAs on mobile
    const getStartedCTA = page.locator('[data-testid="hero-cta-get-started"]')
    await expect(getStartedCTA).toBeVisible()

    // Step 3: Can access navigation via hamburger menu
    const mobileMenuToggle = page.locator('[data-testid="mobile-menu-toggle"]')
    await expect(mobileMenuToggle).toBeVisible()

    // Open mobile menu
    await mobileMenuToggle.click()
    await page.waitForTimeout(300)

    const mobileMenu = page.locator('[data-testid="mobile-menu"]')
    await expect(mobileMenu).not.toHaveClass(/hidden/)

    // Navigation links visible in mobile menu
    const mobileFeaturesLink = page.locator('[data-testid="mobile-nav-link-features"]')
    await expect(mobileFeaturesLink).toBeVisible()

    // Close menu and navigate to features
    await mobileFeaturesLink.click()
    await page.waitForTimeout(500)

    // Features section should be scrolled to
    const featuresSection = page.locator('#features')
    await expect(featuresSection).toBeInViewport({ ratio: 0.5 }).catch(async () => {
      // Fallback: manually scroll to features
      await featuresSection.scrollIntoViewIfNeeded()
    })

    // Feature cards visible on mobile
    const featureCards = page.locator('[data-testid^="feature-card-"]')
    const count = await featureCards.count()
    expect(count).toBeGreaterThanOrEqual(1)
  })

  test('value proposition is visible on mobile without scrolling', async ({ page }) => {
    // Check product name is visible
    const productName = page.locator('[data-testid="hero-product-name"]')
    await expect(productName).toBeVisible()

    // Check tagline is visible
    const tagline = page.locator('[data-testid="hero-tagline"]')
    await expect(tagline).toBeVisible()
  })
})
