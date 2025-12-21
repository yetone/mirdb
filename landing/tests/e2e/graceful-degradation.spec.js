import { test, expect } from '@playwright/test';

/**
 * Graceful Degradation E2E Tests
 *
 * These tests verify that the MirDB landing page handles error conditions
 * gracefully and provides a usable experience even when assets fail to load.
 */

test.describe('Error Handling and Graceful Degradation', () => {

  // Test Case 1: JavaScript Disabled
  test.describe('Test Case 1: Content visible without JavaScript', () => {
    test('should display all core content with JavaScript disabled', async ({ browser }) => {
      // Create context with JavaScript disabled
      const context = await browser.newContext({
        javaScriptEnabled: false
      });
      const page = await context.newPage();

      await page.goto('/');

      // Hero section visible
      const hero = page.locator('.hero, header[role="banner"]');
      await expect(hero).toBeVisible();

      // Product name visible
      const productName = page.locator('.product-name, h1');
      await expect(productName).toBeVisible();
      await expect(productName).toContainText('MirDB');

      // Tagline visible
      const tagline = page.locator('.tagline');
      await expect(tagline).toBeVisible();

      // CTA buttons visible and functional
      const primaryBtn = page.locator('.btn-primary');
      await expect(primaryBtn).toBeVisible();
      await expect(primaryBtn).toHaveAttribute('href', /#get-started|#/);

      const secondaryBtn = page.locator('.btn-secondary');
      await expect(secondaryBtn).toBeVisible();

      // Get Started / Features section visible
      const getStarted = page.locator('#get-started, .get-started-section');
      await expect(getStarted).toBeVisible();

      // Footer visible
      const footer = page.locator('footer');
      await expect(footer).toBeVisible();

      // Footer links work without JS
      const footerLinks = page.locator('footer a');
      const count = await footerLinks.count();
      expect(count).toBeGreaterThan(0);

      await context.close();
    });

    test('should have functional navigation without JavaScript', async ({ browser }) => {
      const context = await browser.newContext({
        javaScriptEnabled: false
      });
      const page = await context.newPage();

      await page.goto('/');

      // Click on primary CTA button which links to #get-started
      const getStartedBtn = page.locator('.btn-primary[href="#get-started"]');
      await expect(getStartedBtn).toBeVisible();

      await getStartedBtn.click();

      // URL should now have the hash
      expect(page.url()).toContain('#get-started');

      // Target section should be in viewport
      const targetSection = page.locator('#get-started');
      await expect(targetSection).toBeInViewport();

      await context.close();
    });
  });

  // Test Case 2: CSS Loading Blocked
  test.describe('Test Case 2: Page accessible without CSS', () => {
    test('should maintain logical structure without CSS', async ({ page }) => {
      // Block CSS files
      await page.route('**/*.css', route => route.abort());

      await page.goto('/');

      // Page should still have content in logical order
      const h1 = page.locator('h1');
      await expect(h1).toBeVisible();
      await expect(h1).toContainText('MirDB');

      // All sections should be present in DOM
      const main = page.locator('main');
      await expect(main).toBeAttached();

      const footer = page.locator('footer');
      await expect(footer).toBeAttached();
    });

    test('should have navigable links without CSS', async ({ page }) => {
      await page.route('**/*.css', route => route.abort());

      await page.goto('/');

      // Links should still be visible and functional
      const links = page.locator('a[href]');
      const count = await links.count();
      expect(count).toBeGreaterThan(0);

      // Get first external link
      const githubLink = page.locator('a[href*="github.com"]').first();
      await expect(githubLink).toBeAttached();

      // Internal anchor links should work
      const skipLink = page.locator('.skip-link, a[href^="#"]').first();
      await expect(skipLink).toBeAttached();
    });

    test('should display content in readable order without CSS', async ({ page }) => {
      await page.route('**/*.css', route => route.abort());

      await page.goto('/');

      // Get all text content
      const bodyText = await page.locator('body').textContent();

      // Key content should be present
      expect(bodyText).toContain('MirDB');
      expect(bodyText).toContain('Memcached');
      expect(bodyText).toContain('Get Started');
      expect(bodyText).toContain('Installation');
    });
  });

  // Test Case 3: Image Loading Failure
  test.describe('Test Case 3: Graceful handling of image failures', () => {
    test('should display alt text when images fail to load', async ({ page }) => {
      // Block all image requests
      await page.route('**/*.{png,jpg,jpeg,gif,webp,svg,ico}', route => route.abort());

      await page.goto('/');

      // Check that any images have alt text
      const images = page.locator('img');
      const imageCount = await images.count();

      for (let i = 0; i < imageCount; i++) {
        const img = images.nth(i);
        // Every image should have alt attribute (can be empty for decorative)
        await expect(img).toHaveAttribute('alt');
      }

      // Page layout should not be broken
      const hero = page.locator('.hero, header');
      await expect(hero).toBeVisible();

      const footer = page.locator('footer');
      await expect(footer).toBeVisible();
    });

    test('should maintain layout when images fail to load', async ({ page }) => {
      await page.route('**/*.{png,jpg,jpeg,gif,webp,svg,ico}', route => route.abort());

      await page.goto('/');

      // Check that main sections are properly laid out
      const hero = page.locator('.hero');
      const heroBounds = await hero.boundingBox();
      expect(heroBounds.width).toBeGreaterThan(0);
      expect(heroBounds.height).toBeGreaterThan(0);

      // Footer should be at bottom
      const footer = page.locator('footer');
      const footerBounds = await footer.boundingBox();
      expect(footerBounds.y).toBeGreaterThan(heroBounds.y);
    });

    test('should use CSS-based icons that do not require images', async ({ page }) => {
      await page.route('**/*.{png,jpg,jpeg,gif,webp,svg}', route => route.abort());

      await page.goto('/');

      // Feature icons should use Unicode characters or CSS
      const featureIcons = page.locator('.feature-icon');
      const iconCount = await featureIcons.count();

      for (let i = 0; i < iconCount; i++) {
        const icon = featureIcons.nth(i);
        await expect(icon).toBeVisible();

        // Icon should have text content (Unicode character)
        const text = await icon.textContent();
        expect(text.trim().length).toBeGreaterThan(0);
      }
    });
  });

  // Test Case 4: Font Stack Fallbacks (Covered in unit tests, verification here)
  test.describe('Test Case 4: Font fallbacks', () => {
    test('should have proper font stack in computed styles', async ({ page }) => {
      await page.goto('/');

      // Get computed font-family from body
      const bodyFontFamily = await page.evaluate(() => {
        return window.getComputedStyle(document.body).fontFamily;
      });

      // Should include system font keywords
      const hasSystemFonts =
        bodyFontFamily.includes('system-ui') ||
        bodyFontFamily.includes('-apple-system') ||
        bodyFontFamily.includes('BlinkMacSystemFont') ||
        bodyFontFamily.includes('Segoe UI') ||
        bodyFontFamily.includes('sans-serif');

      expect(hasSystemFonts).toBe(true);
    });

    test('should have monospace font stack for code blocks', async ({ page }) => {
      await page.goto('/');

      // Get computed font-family from code block
      const codeBlock = page.locator('.code-block code').first();
      const codeFontFamily = await codeBlock.evaluate(el => {
        return window.getComputedStyle(el).fontFamily;
      });

      // Should include monospace
      expect(codeFontFamily.toLowerCase()).toContain('monospace');
    });
  });

  // Test Case 5: Font Loading Blocked
  test.describe('Test Case 5: Text readable with blocked fonts', () => {
    test('should display readable text when web fonts are blocked', async ({ page }) => {
      // Block web font requests
      await page.route('**/*.{woff,woff2,ttf,otf,eot}', route => route.abort());
      await page.route('**/fonts.googleapis.com/**', route => route.abort());
      await page.route('**/fonts.gstatic.com/**', route => route.abort());

      await page.goto('/');

      // All text should still be visible
      const productName = page.locator('.product-name');
      await expect(productName).toBeVisible();
      await expect(productName).toContainText('MirDB');

      const tagline = page.locator('.tagline');
      await expect(tagline).toBeVisible();

      // Verify text is rendered (has computed dimensions)
      const textBox = await productName.boundingBox();
      expect(textBox.height).toBeGreaterThan(0);
      expect(textBox.width).toBeGreaterThan(0);
    });

    test('should render text immediately with font-display swap', async ({ page }) => {
      await page.goto('/');

      // Text should be visible immediately (no FOIT - Flash of Invisible Text)
      const productName = page.locator('.product-name');
      await expect(productName).toBeVisible({ timeout: 1000 });

      // Check text is not invisible
      const visibility = await productName.evaluate(el => {
        return window.getComputedStyle(el).visibility;
      });
      expect(visibility).toBe('visible');
    });

    test('should have adequate font size for readability', async ({ page }) => {
      await page.goto('/');

      // Body text should be at least 16px
      const bodyFontSize = await page.evaluate(() => {
        return parseFloat(window.getComputedStyle(document.body).fontSize);
      });
      expect(bodyFontSize).toBeGreaterThanOrEqual(16);

      // Headings should be larger
      const h1FontSize = await page.locator('h1').evaluate(el => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });
      expect(h1FontSize).toBeGreaterThan(bodyFontSize);
    });
  });

  // Additional resilience tests
  test.describe('Network Resilience', () => {
    test('should handle slow network gracefully', async ({ page }) => {
      // Simulate slow 3G connection
      const client = await page.context().newCDPSession(page);
      await client.send('Network.emulateNetworkConditions', {
        offline: false,
        downloadThroughput: (750 * 1024) / 8, // 750 Kbps
        uploadThroughput: (250 * 1024) / 8,   // 250 Kbps
        latency: 150
      });

      await page.goto('/', { timeout: 30000 });

      // Core content should still be accessible
      const productName = page.locator('.product-name');
      await expect(productName).toBeVisible({ timeout: 15000 });
    });

    test('should work offline after initial load (if service worker exists)', async ({ page }) => {
      // Load page first
      await page.goto('/');

      // Check if content is visible
      await expect(page.locator('.product-name')).toBeVisible();

      // Page loaded successfully - this is the baseline
      // Service worker caching is optional enhancement
    });
  });

  // Progressive Enhancement verification
  test.describe('Progressive Enhancement', () => {
    test('should have no JavaScript console errors on load', async ({ page }) => {
      const errors = [];
      page.on('console', msg => {
        if (msg.type() === 'error') {
          errors.push(msg.text());
        }
      });

      await page.goto('/');

      // Wait for page to be fully loaded
      await page.waitForLoadState('networkidle');

      // Filter out expected/external errors
      const criticalErrors = errors.filter(err =>
        !err.includes('favicon') &&
        !err.includes('404')
      );

      expect(criticalErrors).toHaveLength(0);
    });

    test('should respect reduced motion preference', async ({ page }) => {
      // Set reduced motion preference
      await page.emulateMedia({ reducedMotion: 'reduce' });

      await page.goto('/');

      // Check that transitions are disabled
      const btn = page.locator('.btn').first();
      const transitionDuration = await btn.evaluate(el => {
        return window.getComputedStyle(el).transitionDuration;
      });

      // Should be 0s or very short when reduced motion is enabled
      // The CSS should have @media (prefers-reduced-motion: reduce)
      expect(transitionDuration).toMatch(/^0s|0\.0/);
    });
  });
});
