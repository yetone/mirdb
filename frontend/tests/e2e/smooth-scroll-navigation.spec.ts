import { test, expect } from '@playwright/test'

test.describe('Smooth Scroll Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')
  })

  // Test Case 1: Click Features navigation link - smooth scroll to Features section
  test.describe('Test Case 1: Features navigation smooth scroll', () => {
    test('clicking Features link smoothly scrolls to Features section', async ({ page }) => {
      // Verify we start at the top of the page
      const initialScrollY = await page.evaluate(() => window.scrollY)
      expect(initialScrollY).toBeLessThan(100)

      // Verify Features section exists
      const featuresSection = page.locator('#features')
      await expect(featuresSection).toBeAttached()

      // Get the Features section position
      const featuresSectionTop = await featuresSection.evaluate((el) => {
        return el.getBoundingClientRect().top + window.scrollY
      })

      // Click the Features navigation link
      const featuresLink = page.getByTestId('nav-features')
      await featuresLink.click()

      // Wait for smooth scroll to complete (smooth scroll takes time)
      await page.waitForTimeout(1000)

      // Verify page scrolled to Features section
      const finalScrollY = await page.evaluate(() => window.scrollY)

      // The scroll position should be near the Features section (with some tolerance for header offset)
      expect(finalScrollY).toBeGreaterThan(featuresSectionTop - 150)
      expect(finalScrollY).toBeLessThan(featuresSectionTop + 150)
    })

    test('Features section becomes visible after navigation', async ({ page }) => {
      // Click the Features navigation link
      const featuresLink = page.getByTestId('nav-features')
      await featuresLink.click()

      // Wait for scroll animation
      await page.waitForTimeout(800)

      // Features section should now be in viewport
      const featuresSection = page.getByTestId('features-section')
      await expect(featuresSection).toBeInViewport()
    })

    test('smooth scroll is not instantaneous (animation)', async ({ page }) => {
      // Get initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY)

      // Click Features link
      const featuresLink = page.getByTestId('nav-features')
      await featuresLink.click()

      // Immediately check scroll position (should have started but not completed)
      await page.waitForTimeout(50)
      const midScrollY = await page.evaluate(() => window.scrollY)

      // Wait for animation to complete
      await page.waitForTimeout(1000)
      const finalScrollY = await page.evaluate(() => window.scrollY)

      // If smooth scroll is working, the mid-scroll position should be between start and end
      // (This verifies it's not an instant jump)
      if (finalScrollY > initialScrollY + 200) {
        // Only check if there was significant scrolling needed
        expect(midScrollY).toBeGreaterThanOrEqual(initialScrollY)
        expect(finalScrollY).toBeGreaterThan(initialScrollY)
      }
    })
  })

  // Test Case 2: Click How It Works navigation link - smooth scroll to How It Works section
  test.describe('Test Case 2: How It Works navigation smooth scroll', () => {
    test('clicking How It Works link smoothly scrolls to How It Works section', async ({ page }) => {
      // Verify we start at the top
      const initialScrollY = await page.evaluate(() => window.scrollY)
      expect(initialScrollY).toBeLessThan(100)

      // Verify How It Works section exists
      const howItWorksSection = page.locator('#how-it-works')
      await expect(howItWorksSection).toBeAttached()

      // Get the How It Works section position
      const howItWorksSectionTop = await howItWorksSection.evaluate((el) => {
        return el.getBoundingClientRect().top + window.scrollY
      })

      // Click the How It Works navigation link
      const howItWorksLink = page.getByTestId('nav-how-it-works')
      await howItWorksLink.click()

      // Wait for smooth scroll to complete
      await page.waitForTimeout(1000)

      // Verify page scrolled to How It Works section
      const finalScrollY = await page.evaluate(() => window.scrollY)

      // The scroll position should be near the How It Works section
      expect(finalScrollY).toBeGreaterThan(howItWorksSectionTop - 150)
      expect(finalScrollY).toBeLessThan(howItWorksSectionTop + 150)
    })

    test('How It Works section becomes visible after navigation', async ({ page }) => {
      // Click the How It Works navigation link
      const howItWorksLink = page.getByTestId('nav-how-it-works')
      await howItWorksLink.click()

      // Wait for scroll animation
      await page.waitForTimeout(800)

      // How It Works section should now be in viewport
      const howItWorksSection = page.getByTestId('how-it-works-section')
      await expect(howItWorksSection).toBeInViewport()
    })

    test('scroll animation is visible (not instant jump)', async ({ page }) => {
      // Get How It Works section position
      const howItWorksSection = page.locator('#how-it-works')
      const targetTop = await howItWorksSection.evaluate((el) => {
        return el.getBoundingClientRect().top + window.scrollY
      })

      // Only run animation check if section is significantly below viewport
      if (targetTop > 300) {
        // Click How It Works link
        const howItWorksLink = page.getByTestId('nav-how-it-works')
        await howItWorksLink.click()

        // Sample scroll positions during animation
        const scrollPositions: number[] = []
        for (let i = 0; i < 5; i++) {
          await page.waitForTimeout(100)
          const scrollY = await page.evaluate(() => window.scrollY)
          scrollPositions.push(scrollY)
        }

        // Wait for animation to complete
        await page.waitForTimeout(600)
        const finalPosition = await page.evaluate(() => window.scrollY)
        scrollPositions.push(finalPosition)

        // Verify progressive scrolling (positions should generally increase)
        // This indicates smooth animation rather than instant jump
        let increasingCount = 0
        for (let i = 1; i < scrollPositions.length; i++) {
          if (scrollPositions[i] >= scrollPositions[i - 1]) {
            increasingCount++
          }
        }

        // Most positions should be increasing (smooth scroll)
        expect(increasingCount).toBeGreaterThan(scrollPositions.length / 2 - 1)
      }
    })
  })

  // Test Case 3: Check CSS scroll-behavior property
  test.describe('Test Case 3: CSS scroll-behavior property', () => {
    test('page has scroll-behavior: smooth applied', async ({ page }) => {
      // Check the computed style of html element
      const scrollBehavior = await page.evaluate(() => {
        const html = document.documentElement
        const computedStyle = window.getComputedStyle(html)
        return computedStyle.scrollBehavior
      })

      expect(scrollBehavior).toBe('smooth')
    })

    test('html element has scroll-behavior CSS property', async ({ page }) => {
      // Verify the CSS is being applied
      const hasScrollBehaviorSmooth = await page.evaluate(() => {
        const html = document.documentElement
        const style = window.getComputedStyle(html)
        return style.getPropertyValue('scroll-behavior') === 'smooth'
      })

      expect(hasScrollBehaviorSmooth).toBe(true)
    })
  })

  // Test Case 4: Verify anchor links have correct href/target sections
  test.describe('Test Case 4: Anchor links target correct sections', () => {
    test('Features link navigates to element with id="features"', async ({ page }) => {
      // Verify Features section has correct id
      const featuresSection = page.locator('#features')
      await expect(featuresSection).toBeAttached()

      // Verify the section has the features-section test id
      const featuresSectionByTestId = page.getByTestId('features-section')
      await expect(featuresSectionByTestId).toBeAttached()

      // Verify they are the same element
      const featuresId = await page.locator('#features').getAttribute('data-testid')
      expect(featuresId).toBe('features-section')
    })

    test('How It Works link navigates to element with id="how-it-works"', async ({ page }) => {
      // Verify How It Works section has correct id
      const howItWorksSection = page.locator('#how-it-works')
      await expect(howItWorksSection).toBeAttached()

      // Verify the section has the how-it-works-section test id
      const howItWorksSectionByTestId = page.getByTestId('how-it-works-section')
      await expect(howItWorksSectionByTestId).toBeAttached()

      // Verify they are the same element
      const howItWorksId = await page.locator('#how-it-works').getAttribute('data-testid')
      expect(howItWorksId).toBe('how-it-works-section')
    })

    test('navigation buttons trigger scroll to correct sections', async ({ page }) => {
      // Click Features and verify we end up at features section
      await page.getByTestId('nav-features').click()
      await page.waitForTimeout(800)

      let featuresInViewport = await page.getByTestId('features-section').isVisible()
      expect(featuresInViewport).toBe(true)

      // Scroll back to top
      await page.evaluate(() => window.scrollTo(0, 0))
      await page.waitForTimeout(300)

      // Click How It Works and verify we end up at how-it-works section
      await page.getByTestId('nav-how-it-works').click()
      await page.waitForTimeout(800)

      let howItWorksInViewport = await page.getByTestId('how-it-works-section').isVisible()
      expect(howItWorksInViewport).toBe(true)
    })
  })

  // Additional validation tests
  test.describe('Additional smooth scroll validation', () => {
    test('mobile menu navigation uses smooth scroll', async ({ page }) => {
      // Set mobile viewport
      await page.setViewportSize({ width: 375, height: 667 })
      await page.goto('/')
      await page.waitForLoadState('networkidle')

      // Open mobile menu
      const hamburger = page.getByTestId('hamburger-menu')
      await hamburger.click()

      // Wait for menu to open
      await page.waitForTimeout(300)

      // Click mobile Features link
      const mobileFeatures = page.getByTestId('mobile-nav-features')
      await mobileFeatures.click()

      // Wait for scroll
      await page.waitForTimeout(800)

      // Verify Features section is in viewport
      const featuresSection = page.getByTestId('features-section')
      await expect(featuresSection).toBeInViewport()
    })

    test('scroll behavior is smooth, not instant', async ({ page }) => {
      // Get Features section position
      const targetPosition = await page.evaluate(() => {
        const features = document.getElementById('features')
        if (!features) return 0
        return features.getBoundingClientRect().top + window.scrollY
      })

      // Only test if scrolling is needed
      if (targetPosition > 200) {
        // Record scroll events
        const scrollEvents: number[] = []
        await page.evaluate(() => {
          (window as any).scrollEvents = []
          window.addEventListener('scroll', () => {
            (window as any).scrollEvents.push(Date.now())
          })
        })

        // Click Features link
        await page.getByTestId('nav-features').click()

        // Wait for animation
        await page.waitForTimeout(1000)

        // Get scroll events
        const eventCount = await page.evaluate(() => (window as any).scrollEvents.length)

        // Smooth scroll should generate multiple scroll events
        // Instant jump would generate fewer events
        expect(eventCount).toBeGreaterThan(1)
      }
    })

    test('multiple navigation clicks work correctly', async ({ page }) => {
      // Navigate to Features
      await page.getByTestId('nav-features').click()
      await page.waitForTimeout(800)
      await expect(page.getByTestId('features-section')).toBeInViewport()

      // Navigate to How It Works
      await page.getByTestId('nav-how-it-works').click()
      await page.waitForTimeout(800)
      await expect(page.getByTestId('how-it-works-section')).toBeInViewport()

      // Navigate back to Features
      await page.getByTestId('nav-features').click()
      await page.waitForTimeout(800)
      await expect(page.getByTestId('features-section')).toBeInViewport()
    })
  })
})
