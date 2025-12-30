const { test, expect } = require('@playwright/test');

/**
 * Browser Compatibility - Chrome Tests
 * Scenario: Validate the homepage functions correctly in Google Chrome
 *
 * This test suite verifies:
 * 1. Page loads without errors and displays all sections correctly
 * 2. GIF animations play smoothly (load properly)
 */

test.describe('Browser Compatibility - Chrome', () => {
  test.describe('Test Case 1: Load homepage in Chrome - Page loads without errors and displays all sections correctly', () => {
    test('Page loads without JavaScript console errors', async ({ page }) => {
      // Collect console errors during page load
      const consoleErrors = [];
      page.on('console', (msg) => {
        if (msg.type() === 'error') {
          consoleErrors.push(msg.text());
        }
      });

      // Load the homepage
      await page.goto('/');

      // Wait for the page to fully load
      await page.waitForLoadState('networkidle');

      // Filter out expected/benign errors (like 404s for external resources that may not exist in test env)
      const criticalErrors = consoleErrors.filter(error => {
        // Filter out known benign errors
        const benignPatterns = [
          'favicon.ico',
          'Failed to load resource',  // External resources might not be available in test
        ];
        return !benignPatterns.some(pattern => error.includes(pattern));
      });

      // There should be no critical JavaScript errors
      expect(criticalErrors).toHaveLength(0);
    });

    test('Page returns successful HTTP status (200)', async ({ page }) => {
      const response = await page.goto('/');

      // Verify successful HTTP response
      expect(response.status()).toBe(200);
      expect(response.ok()).toBe(true);
    });

    test('Header section is visible and contains navigation', async ({ page }) => {
      await page.goto('/');

      // Verify header is visible
      const header = page.locator('header');
      await expect(header).toBeVisible();

      // Verify navigation exists
      const nav = page.locator('nav');
      await expect(nav).toBeVisible();

      // Verify logo text is present
      const logoText = page.locator('.logo-text');
      await expect(logoText).toBeVisible();
      await expect(logoText).toHaveText('MirDB');
    });

    test('Hero section displays correctly', async ({ page }) => {
      await page.goto('/');

      // Verify hero section is visible
      const heroSection = page.locator('.hero');
      await expect(heroSection).toBeVisible();

      // Verify headline is visible
      const headline = page.locator('.hero-headline');
      await expect(headline).toBeVisible();
      await expect(headline).toContainText('MirDB');

      // Verify subheadline is visible
      const subheadline = page.locator('.hero-subheadline');
      await expect(subheadline).toBeVisible();

      // Verify CTA buttons are visible
      const ctaButtons = page.locator('.hero-cta .btn');
      await expect(ctaButtons).toHaveCount(2);
    });

    test('Value Proposition section displays correctly', async ({ page }) => {
      await page.goto('/');

      // Verify value proposition section is visible
      const valuePropSection = page.locator('.value-prop');
      await expect(valuePropSection).toBeVisible();

      // Verify all three value proposition columns are visible
      const valuePropColumns = page.locator('.value-prop-column');
      await expect(valuePropColumns).toHaveCount(3);

      // Verify each column has expected content
      const column1 = valuePropColumns.nth(0);
      await expect(column1).toContainText('Memcached Protocol Support');

      const column2 = valuePropColumns.nth(1);
      await expect(column2).toContainText('Persistence');

      const column3 = valuePropColumns.nth(2);
      await expect(column3).toContainText('LSM Tree Architecture');
    });

    test('Features section displays correctly', async ({ page }) => {
      await page.goto('/');

      // Verify features section is visible
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();

      // Verify feature cards are present
      const featureCards = page.locator('.feature-card');
      const featureCount = await featureCards.count();
      expect(featureCount).toBeGreaterThanOrEqual(4);

      // Verify planned feature has distinctive styling
      const plannedFeature = page.locator('.feature-card.planned');
      await expect(plannedFeature).toBeVisible();
    });

    test('Commands section displays correctly', async ({ page }) => {
      await page.goto('/');

      // Verify commands section is visible
      const commandsSection = page.locator('#commands');
      await expect(commandsSection).toBeVisible();

      // Verify command categories are present
      const commandCategories = page.locator('.command-category');
      const categoryCount = await commandCategories.count();
      expect(categoryCount).toBe(4);
    });

    test('Demo section displays correctly', async ({ page }) => {
      await page.goto('/');

      // Verify demo section is visible
      const demoSection = page.locator('#demo');
      await expect(demoSection).toBeVisible();

      // Verify demo header is visible
      const demoHeader = page.locator('.demo-header');
      await expect(demoHeader).toBeVisible();
      await expect(demoHeader).toContainText('See MirDB in Action');
    });

    test('Quick Start section displays correctly', async ({ page }) => {
      await page.goto('/');

      // Verify quickstart section is visible
      const quickstartSection = page.locator('#quickstart');
      await expect(quickstartSection).toBeVisible();

      // Verify quickstart steps are present
      const quickstartSteps = page.locator('.quickstart-step');
      await expect(quickstartSteps).toHaveCount(3);

      // Verify code blocks are present
      const codeBlocks = page.locator('.code-block');
      const codeBlockCount = await codeBlocks.count();
      expect(codeBlockCount).toBeGreaterThanOrEqual(3);
    });

    test('Footer section displays correctly', async ({ page }) => {
      await page.goto('/');

      // Verify footer is visible
      const footer = page.locator('footer');
      await expect(footer).toBeVisible();

      // Verify footer contains MIT license reference
      const footerInfo = page.locator('.footer-info');
      await expect(footerInfo).toContainText('MIT License');

      // Verify footer links are present
      const footerLinks = page.locator('.footer-links a');
      const linkCount = await footerLinks.count();
      expect(linkCount).toBeGreaterThanOrEqual(3);
    });

    test('All internal navigation links function correctly', async ({ page }) => {
      await page.goto('/');

      // Test Features navigation link
      const featuresLink = page.locator('nav a[href="#features"]');
      await featuresLink.click();

      // Verify scroll to features section (URL should update)
      await expect(page).toHaveURL(/#features/);

      // Test Quick Start navigation link
      const quickstartLink = page.locator('nav a[href="#quickstart"]');
      await quickstartLink.click();

      // Verify scroll to quickstart section
      await expect(page).toHaveURL(/#quickstart/);
    });

    test('External links have proper security attributes', async ({ page }) => {
      await page.goto('/');

      // Check GitHub links have proper security attributes
      const externalLinks = page.locator('a[target="_blank"]');
      const linkCount = await externalLinks.count();

      for (let i = 0; i < linkCount; i++) {
        const link = externalLinks.nth(i);
        const rel = await link.getAttribute('rel');
        expect(rel).toContain('noopener');
      }
    });
  });

  test.describe('Test Case 2: Check animations in Chrome - GIF animations play smoothly', () => {
    test('Logo GIF loads successfully', async ({ page }) => {
      await page.goto('/');

      // Verify logo image is present
      const logoImage = page.locator('.hero-logo');
      await expect(logoImage).toBeVisible();

      // Verify logo src is correct
      const logoSrc = await logoImage.getAttribute('src');
      expect(logoSrc).toContain('logo.gif');

      // Verify the image has loaded (naturalWidth > 0 indicates successful load)
      const logoLoaded = await logoImage.evaluate((img) => {
        return img.complete && img.naturalWidth > 0;
      });
      expect(logoLoaded).toBe(true);
    });

    test('Usage demo GIF loads successfully', async ({ page }) => {
      await page.goto('/');

      // Scroll to demo section to trigger lazy loading
      const demoSection = page.locator('#demo');
      await demoSection.scrollIntoViewIfNeeded();

      // Wait for lazy loading to complete
      await page.waitForTimeout(1000);

      // Verify usage GIF is present
      const usageGif = page.locator('.demo-gif');
      await expect(usageGif).toBeVisible();

      // Verify src is correct
      const usageSrc = await usageGif.getAttribute('src');
      expect(usageSrc).toContain('usage.gif');

      // Verify the image has loaded successfully
      const usageLoaded = await usageGif.evaluate((img) => {
        return img.complete && img.naturalWidth > 0;
      });
      expect(usageLoaded).toBe(true);
    });

    test('GIF images have proper alt text for accessibility', async ({ page }) => {
      await page.goto('/');

      // Verify logo has alt text
      const logoImage = page.locator('.hero-logo');
      const logoAlt = await logoImage.getAttribute('alt');
      expect(logoAlt).toBeTruthy();
      expect(logoAlt.length).toBeGreaterThan(0);

      // Verify usage GIF has alt text
      const usageGif = page.locator('.demo-gif');
      const usageAlt = await usageGif.getAttribute('alt');
      expect(usageAlt).toBeTruthy();
      expect(usageAlt.length).toBeGreaterThan(0);
    });

    test('GIF images have lazy loading attribute for performance', async ({ page }) => {
      await page.goto('/');

      // Verify logo has lazy loading
      const logoImage = page.locator('.hero-logo');
      const logoLoading = await logoImage.getAttribute('loading');
      expect(logoLoading).toBe('lazy');

      // Verify usage GIF has lazy loading
      const usageGif = page.locator('.demo-gif');
      const usageLoading = await usageGif.getAttribute('loading');
      expect(usageLoading).toBe('lazy');
    });

    test('GIF images render at proper dimensions', async ({ page }) => {
      await page.goto('/');

      // Verify logo renders at reasonable dimensions
      const logoImage = page.locator('.hero-logo');
      const logoBoundingBox = await logoImage.boundingBox();
      expect(logoBoundingBox.width).toBeGreaterThan(100);
      expect(logoBoundingBox.height).toBeGreaterThan(50);

      // Scroll to demo section
      const demoSection = page.locator('#demo');
      await demoSection.scrollIntoViewIfNeeded();
      await page.waitForTimeout(500);

      // Verify usage GIF renders at reasonable dimensions
      const usageGif = page.locator('.demo-gif');
      const usageBoundingBox = await usageGif.boundingBox();
      expect(usageBoundingBox.width).toBeGreaterThan(200);
      expect(usageBoundingBox.height).toBeGreaterThan(100);
    });

    test('CSS animations and transitions work correctly in Chrome', async ({ page }) => {
      await page.goto('/');

      // Verify CSS transitions are applied to interactive elements
      const featureCard = page.locator('.feature-card').first();

      // Get initial transform value
      const initialTransform = await featureCard.evaluate((el) => {
        return window.getComputedStyle(el).transform;
      });

      // Hover over the feature card
      await featureCard.hover();

      // Wait for transition
      await page.waitForTimeout(300);

      // Verify hover effect CSS is applied (the transition property should be defined)
      const transition = await featureCard.evaluate((el) => {
        return window.getComputedStyle(el).transition;
      });
      expect(transition).toContain('transform');

      // Verify value proposition columns also have hover effects
      const valuePropColumn = page.locator('.value-prop-column').first();
      const valuePropTransition = await valuePropColumn.evaluate((el) => {
        return window.getComputedStyle(el).transition;
      });
      expect(valuePropTransition).toContain('transform');
    });
  });
});
