const { test, expect } = require('@playwright/test');

/**
 * Error Handling - Missing Assets Test Suite
 * Verifies the page handles missing or failed assets gracefully
 */

test.describe('Error Handling - Missing Assets', () => {

  test.describe('Test Case 1: Block image loading', () => {
    test('Page displays properly with alt text shown for images when images are blocked', async ({ page }) => {
      // Block all image requests
      await page.route('**/*.{png,jpg,jpeg,gif,webp,svg}', route => route.abort());

      // Navigate to the page
      await page.goto('/');

      // Wait for page to be fully loaded
      await page.waitForLoadState('networkidle');

      // Verify the page still displays properly
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // Verify the hero title is still visible and readable
      const heroTitle = page.locator('[data-testid="product-name"]');
      await expect(heroTitle).toBeVisible();
      await expect(heroTitle).toHaveText('MirDB');

      // Verify the tagline is still visible
      const tagline = page.locator('[data-testid="hero-tagline"]');
      await expect(tagline).toBeVisible();

      // Verify feature cards are still displayed
      const featuresSection = page.locator('[data-testid="features-section"]');
      await expect(featuresSection).toBeVisible();

      // Verify all 8 feature cards are still present
      const featureCards = page.locator('.feature-card');
      await expect(featureCards).toHaveCount(8);

      // Verify SVG icons (inline SVGs should still work even with image blocking)
      const featureIcons = page.locator('[data-testid="feature-icon"]');
      await expect(featureIcons).toHaveCount(8);

      // Verify the navigation is still functional
      const navLinks = page.locator('.nav-links a');
      await expect(navLinks.first()).toBeVisible();

      // Verify footer is still visible
      const footer = page.locator('[data-testid="footer-section"]');
      await expect(footer).toBeVisible();

      // Check that the logo image has proper alt text
      const logoImage = page.locator('[data-testid="mirdb-logo"]');
      const altText = await logoImage.getAttribute('alt');
      expect(altText).toBe('MirDB Logo');
    });

    test('Page layout remains intact when images fail to load', async ({ page }) => {
      // Block image loading
      await page.route('**/*.{png,jpg,jpeg,gif,webp}', route => route.abort());

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Verify page structure is maintained
      const header = page.locator('.header');
      await expect(header).toBeVisible();

      const mainContent = page.locator('#main-content');
      await expect(mainContent).toBeVisible();

      // Verify CTA buttons are still functional
      const ctaButtons = page.locator('[data-testid="hero-cta"] a');
      await expect(ctaButtons).toHaveCount(2);

      // Verify code examples section is still readable
      const codeExampleSection = page.locator('[data-testid="code-example-section"]');
      await expect(codeExampleSection).toBeVisible();

      // Verify architecture section is still displayed
      const architectureSection = page.locator('[data-testid="architecture-section"]');
      await expect(architectureSection).toBeVisible();
    });
  });

  test.describe('Test Case 2: Block external CSS', () => {
    test('Page remains readable with fallback styles when external CSS is blocked', async ({ page }) => {
      // Block external CSS files (if any)
      await page.route('**/*.css', route => {
        // Only block external CSS, not the local styles.css
        const url = route.request().url();
        if (url.includes('localhost') || url.includes('127.0.0.1')) {
          return route.continue();
        }
        return route.abort();
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Verify the page is still readable - hero section visible
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // Verify text content is visible
      const heroTitle = page.locator('[data-testid="product-name"]');
      await expect(heroTitle).toBeVisible();
      await expect(heroTitle).toHaveText('MirDB');

      // Verify page is still navigable
      const navLinks = page.locator('.nav-links a');
      const firstNavLink = navLinks.first();
      await expect(firstNavLink).toBeVisible();

      // Verify features section is visible
      const featuresSection = page.locator('[data-testid="features-section"]');
      await expect(featuresSection).toBeVisible();
    });

    test('Page content is accessible when all CSS is disabled', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Disable all stylesheets
      await page.evaluate(() => {
        document.querySelectorAll('link[rel="stylesheet"], style').forEach(el => el.remove());
      });

      // Verify core content is still present and readable
      const heroTitle = page.locator('[data-testid="product-name"]');
      await expect(heroTitle).toBeVisible();

      // Verify semantic HTML structure keeps content organized
      const mainContent = page.locator('#main-content');
      await expect(mainContent).toBeVisible();

      // Verify links are still functional
      const footerGithubLink = page.locator('[data-testid="footer-github-link"]');
      await expect(footerGithubLink).toBeVisible();

      // Verify text content is readable
      const tagline = page.locator('[data-testid="hero-tagline"]');
      const taglineText = await tagline.textContent();
      expect(taglineText).toContain('Persistent Key-Value Store');
    });
  });

  test.describe('Test Case 3: Check for broken image indicators', () => {
    test('No broken image icons are visible under normal conditions', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Get all images on the page
      const images = page.locator('img');
      const imageCount = await images.count();

      // Check each image for broken state
      for (let i = 0; i < imageCount; i++) {
        const img = images.nth(i);

        // Check if image is properly loaded (naturalWidth > 0)
        const isLoaded = await img.evaluate((el) => {
          // For images that failed to load, naturalWidth is 0
          // Some external images (like CI badges) may have varying states
          return el.complete && (el.naturalWidth > 0 || el.naturalHeight > 0);
        });

        // If image is internal (not external badge), it should be properly loaded
        const src = await img.getAttribute('src');
        if (src && !src.includes('circleci.com')) {
          // Internal images should load properly
          expect(isLoaded).toBe(true);
        }
      }
    });

    test('All internal images have valid src attributes', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      const images = page.locator('img');
      const imageCount = await images.count();

      for (let i = 0; i < imageCount; i++) {
        const img = images.nth(i);
        const src = await img.getAttribute('src');

        // Verify src is not empty or null
        expect(src).toBeTruthy();
        expect(src.length).toBeGreaterThan(0);
      }
    });

    test('All images have meaningful alt text', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      const images = page.locator('img');
      const imageCount = await images.count();

      for (let i = 0; i < imageCount; i++) {
        const img = images.nth(i);
        const alt = await img.getAttribute('alt');

        // Every image should have alt text
        expect(alt).toBeTruthy();
        expect(alt.length).toBeGreaterThan(0);
      }
    });
  });

  test.describe('Test Case 4: Verify console for 404 errors', () => {
    test('No 404 errors for assets in browser console under normal conditions', async ({ page }) => {
      const errors = [];
      const notFoundRequests = [];

      // Listen for console errors
      page.on('console', msg => {
        if (msg.type() === 'error') {
          errors.push(msg.text());
        }
      });

      // Listen for failed requests
      page.on('requestfailed', request => {
        const failure = request.failure();
        if (failure) {
          errors.push(`Request failed: ${request.url()} - ${failure.errorText}`);
        }
      });

      // Listen for response status codes
      page.on('response', response => {
        if (response.status() === 404) {
          notFoundRequests.push(response.url());
        }
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Filter out external resources (like CI badges) which may have varying availability
      const internalNotFound = notFoundRequests.filter(url => {
        return url.includes('localhost') || url.includes('127.0.0.1');
      });

      // Verify no internal 404 errors
      expect(internalNotFound).toHaveLength(0);
    });

    test('All local asset paths are correctly configured', async ({ page }) => {
      const failedAssets = [];

      // Track all asset requests and their status
      page.on('response', response => {
        const url = response.url();
        const status = response.status();

        // Check local assets only
        if ((url.includes('localhost') || url.includes('127.0.0.1')) &&
            (url.includes('.css') || url.includes('.js') || url.includes('/assets/'))) {
          if (status >= 400) {
            failedAssets.push({ url, status });
          }
        }
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Verify no local assets failed to load
      expect(failedAssets).toHaveLength(0);
    });

    test('Stylesheet loads successfully', async ({ page }) => {
      let cssLoaded = false;

      page.on('response', response => {
        if (response.url().includes('styles.css') && response.status() === 200) {
          cssLoaded = true;
        }
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      expect(cssLoaded).toBe(true);
    });

    test('Page does not throw JavaScript errors on load', async ({ page }) => {
      const jsErrors = [];

      page.on('pageerror', error => {
        jsErrors.push(error.message);
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Verify no JS errors occurred
      expect(jsErrors).toHaveLength(0);
    });
  });

  test.describe('Graceful Degradation', () => {
    test('Page functionality is preserved when logo image fails to load', async ({ page }) => {
      // Block only the logo image
      await page.route('**/logo.gif', route => route.abort());

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Verify the hero section still displays
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // Verify the product name is still readable
      const productName = page.locator('[data-testid="product-name"]');
      await expect(productName).toBeVisible();
      await expect(productName).toHaveText('MirDB');

      // Verify CTA buttons are still functional
      const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
      await expect(getStartedBtn).toBeVisible();

      // Verify navigation still works
      const featuresLink = page.locator('.nav-links a[href="#features"]');
      await expect(featuresLink).toBeVisible();
    });

    test('All SVG icons remain visible when external resources fail', async ({ page }) => {
      // Block external resources
      await page.route('https://**/*', route => route.abort());

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Verify inline SVG icons in features section are still visible
      const featureIcons = page.locator('.feature-icon svg');
      const iconCount = await featureIcons.count();

      // Should have 8 feature icons
      expect(iconCount).toBe(8);

      // Verify each icon is visible
      for (let i = 0; i < iconCount; i++) {
        await expect(featureIcons.nth(i)).toBeVisible();
      }
    });

    test('Architecture diagram remains visible as inline SVG', async ({ page }) => {
      // Block external resources
      await page.route('https://**/*', route => route.abort());

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Verify the architecture SVG diagram is still visible
      const architectureSvg = page.locator('[data-testid="architecture-svg"]');
      await expect(architectureSvg).toBeVisible();

      // Verify key diagram elements are present
      const walBox = page.locator('[data-testid="wal-box"]');
      await expect(walBox).toBeVisible();

      const memtableBox = page.locator('[data-testid="memtable-box"]');
      await expect(memtableBox).toBeVisible();
    });
  });
});
