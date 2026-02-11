/**
 * E2E tests for Lazy Loading
 * Scenario 12 - Performance and Loading
 *
 * Test Case 5: Images below fold have loading='lazy' attribute
 * This is an E2E test that verifies lazy loading implementation in the DOM
 */

import { test, expect } from '@playwright/test'

test.describe('Test Case 5: Lazy Loading Images', () => {
  test('large content images below the fold have loading="lazy" attribute', async ({ page }) => {
    await page.goto('/', { waitUntil: 'domcontentloaded' })

    // Get viewport height to determine fold position
    const viewportHeight = await page.evaluate(() => window.innerHeight)

    // Get all images on the page
    const images = page.locator('img')
    const imageCount = await images.count()

    expect(imageCount).toBeGreaterThan(0)

    let belowFoldContentImages = 0
    let belowFoldContentImagesWithLazy = 0

    for (let i = 0; i < imageCount; i++) {
      const img = images.nth(i)
      const boundingBox = await img.boundingBox()
      const src = await img.getAttribute('src')

      if (boundingBox && src) {
        // Image is below the fold if its top position is below viewport height
        const isBelowFold = boundingBox.y > viewportHeight

        // Only check content images (GIF, PNG, JPG) that would benefit from lazy loading
        // Exclude small external badges (like CI status SVGs) which don't need lazy loading
        const isContentImage =
          src.includes('.gif') ||
          src.includes('.png') ||
          src.includes('.jpg') ||
          src.includes('.jpeg') ||
          src.includes('.webp')
        const isExternalBadge = src.includes('circleci') || src.includes('badge')

        if (isBelowFold && isContentImage && !isExternalBadge) {
          belowFoldContentImages++
          const loadingAttr = await img.getAttribute('loading')

          // Content images below fold should have loading="lazy"
          if (loadingAttr === 'lazy') {
            belowFoldContentImagesWithLazy++
          }

          expect(loadingAttr).toBe('lazy')
        }
      }
    }

    // Log results for debugging
    console.log(
      `Below-fold content images with lazy loading: ${belowFoldContentImagesWithLazy}/${belowFoldContentImages}`
    )

    // Ensure we found and tested at least one below-fold content image (the demo GIF)
    expect(belowFoldContentImages).toBeGreaterThan(0)
  })

  test('demo GIF specifically uses lazy loading', async ({ page }) => {
    await page.goto('/', { waitUntil: 'domcontentloaded' })

    // The demo GIF should always have lazy loading as it's below the hero section
    const demoImage = page.locator('.demo__image')

    if ((await demoImage.count()) > 0) {
      const loadingAttr = await demoImage.getAttribute('loading')
      expect(loadingAttr).toBe('lazy')

      // Also verify it has width and height for CLS prevention
      const width = await demoImage.getAttribute('width')
      const height = await demoImage.getAttribute('height')

      expect(width).toBeTruthy()
      expect(height).toBeTruthy()
    }
  })

  test('above-fold images do not have lazy loading (optimization check)', async ({ page }) => {
    await page.goto('/', { waitUntil: 'domcontentloaded' })

    // The logo in the header is above the fold and should NOT have lazy loading
    // for optimal LCP performance
    const logoImage = page.locator('.header__logo')

    if ((await logoImage.count()) > 0) {
      const loadingAttr = await logoImage.getAttribute('loading')

      // Logo should either not have loading attribute or have loading="eager"
      // It should NOT have loading="lazy" as it's above the fold
      expect(loadingAttr).not.toBe('lazy')
    }
  })

  test('lazy-loaded images do not block initial page render', async ({ page }) => {
    // Track which images were loaded during initial render
    const loadedImages: string[] = []

    // Listen for image load events
    await page.route('**/*.gif', async (route) => {
      const url = route.request().url()
      loadedImages.push(url)
      await route.continue()
    })

    // Navigate but only wait for DOM content
    await page.goto('/', { waitUntil: 'domcontentloaded' })

    // Hero section should be visible immediately
    const heroSection = page.locator('#hero')
    await expect(heroSection).toBeVisible()

    // Primary CTA should be interactive
    const ctaButton = page.getByRole('button', { name: 'Get Started' })
    await expect(ctaButton).toBeEnabled()

    // The demo section image should not have blocked page interactivity
    // (it should load lazily in the background)
    console.log(`Images loaded during initial render: ${loadedImages.length}`)
  })
})
