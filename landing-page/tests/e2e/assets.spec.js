/**
 * Asset Loading E2E Tests
 * Owner: Scenario 17 - Asset Loading - Logo and GIF
 *
 * Tests for verifying logo and usage GIF assets load correctly
 * and handle fallback behavior gracefully.
 */
const { test, expect } = require('@playwright/test');

test.describe('Asset Loading - Logo and GIF', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('TC1: Logo Loading in Hero Section', () => {
    test('Logo from /assets/images/logo.gif displays in hero section', async ({ page }) => {
      // Verify hero section exists
      const heroSection = page.locator('#hero');
      await expect(heroSection).toBeVisible();

      // Verify logo image in hero section
      const heroLogo = page.locator('#hero .hero-logo');
      await expect(heroLogo).toBeVisible();

      // Verify logo has correct source
      await expect(heroLogo).toHaveAttribute('src', 'assets/images/logo.gif');

      // Verify logo has alt text
      await expect(heroLogo).toHaveAttribute('alt', 'MirDB Logo');
    });

    test('Logo loads successfully with correct dimensions', async ({ page }) => {
      const heroLogo = page.locator('#hero .hero-logo');
      await expect(heroLogo).toBeVisible();

      // Verify image has loaded by checking natural dimensions
      const isLoaded = await heroLogo.evaluate((img) => {
        return img.complete && img.naturalWidth > 0 && img.naturalHeight > 0;
      });
      expect(isLoaded).toBe(true);
    });

    test('Logo is properly positioned within hero content', async ({ page }) => {
      const heroLogo = page.locator('#hero .hero-logo');
      const heroContent = page.locator('#hero .hero-content');

      await expect(heroLogo).toBeVisible();
      await expect(heroContent).toBeVisible();

      // Verify logo is within the viewport (visible without scrolling)
      await expect(heroLogo).toBeInViewport();
    });
  });

  test.describe('TC2: Usage GIF Loading in Usage Section', () => {
    test('Usage GIF from /assets/images/usage.gif displays in Usage section', async ({ page }) => {
      // Scroll to usage section
      const usageSection = page.locator('#usage');
      await usageSection.scrollIntoViewIfNeeded();
      await expect(usageSection).toBeVisible();

      // Verify usage GIF exists
      const usageGif = page.locator('#usage .usage__gif');
      await expect(usageGif).toBeVisible();

      // Verify GIF has correct source
      await expect(usageGif).toHaveAttribute('src', 'assets/images/usage.gif');
    });

    test('Usage GIF has meaningful alt text for accessibility', async ({ page }) => {
      const usageGif = page.locator('#usage .usage__gif');
      await usageGif.scrollIntoViewIfNeeded();

      // Verify alt text is present and descriptive
      const altText = await usageGif.getAttribute('alt');
      expect(altText).toBeTruthy();
      expect(altText.length).toBeGreaterThan(10);
      expect(altText.toLowerCase()).toMatch(/terminal|demonstration|usage|mirdb|command/);
    });

    test('Usage GIF loads successfully', async ({ page }) => {
      // Wait for the GIF to come into view (since it may be lazy loaded)
      const usageGif = page.locator('#usage .usage__gif');
      await usageGif.scrollIntoViewIfNeeded();

      // Wait for image to load
      await page.waitForFunction(
        (selector) => {
          const img = document.querySelector(selector);
          return img && img.complete && img.naturalWidth > 0;
        },
        '.usage__gif',
        { timeout: 10000 }
      );

      // Verify image is visible
      await expect(usageGif).toBeVisible();
    });

    test('Usage GIF is within usage demo container', async ({ page }) => {
      const demoContainer = page.locator('.usage__demo-container');
      await demoContainer.scrollIntoViewIfNeeded();
      await expect(demoContainer).toBeVisible();

      const usageGif = demoContainer.locator('.usage__gif');
      await expect(usageGif).toBeVisible();
    });
  });

  test.describe('TC4: Fallback Behavior - Image Loading Failure', () => {
    test('Page layout remains intact when logo fails to load', async ({ page }) => {
      // Block image requests before navigating
      await page.route('**/logo.gif', (route) => route.abort());

      await page.goto('/');

      // Verify page structure is intact
      const heroSection = page.locator('#hero');
      await expect(heroSection).toBeVisible();

      const heroTitle = page.locator('.hero-title');
      await expect(heroTitle).toBeVisible();
      await expect(heroTitle).toHaveText('MirDB');

      // Verify CTA buttons are still accessible
      const ctaButtons = page.locator('.hero-cta .btn');
      await expect(ctaButtons).toHaveCount(2);
    });

    test('Page layout remains intact when usage GIF fails to load', async ({ page }) => {
      // Block usage GIF requests before navigating
      await page.route('**/usage.gif', (route) => route.abort());

      await page.goto('/');

      // Scroll to usage section
      const usageSection = page.locator('#usage');
      await usageSection.scrollIntoViewIfNeeded();
      await expect(usageSection).toBeVisible();

      // Verify section title is visible
      const usageTitle = page.locator('.usage__title');
      await expect(usageTitle).toBeVisible();

      // Verify code blocks are still functional
      const codeBlocks = page.locator('#usage .code-block');
      await expect(codeBlocks.first()).toBeVisible();
    });

    test('Alt text is displayed when logo image fails to load', async ({ page }) => {
      // Block logo image
      await page.route('**/logo.gif', (route) => route.abort());

      await page.goto('/');

      // The img element should still be present with alt attribute
      const heroLogo = page.locator('#hero .hero-logo');
      await expect(heroLogo).toHaveAttribute('alt', 'MirDB Logo');

      // Verify the element exists even if image didn't load
      const imgElement = await heroLogo.evaluate((img) => ({
        exists: !!img,
        alt: img.alt,
        hasAlt: img.hasAttribute('alt')
      }));

      expect(imgElement.exists).toBe(true);
      expect(imgElement.hasAlt).toBe(true);
      expect(imgElement.alt).toBe('MirDB Logo');
    });

    test('Alt text is available for usage GIF when image fails to load', async ({ page }) => {
      // Block usage GIF
      await page.route('**/usage.gif', (route) => route.abort());

      await page.goto('/');

      // Scroll to usage section
      const usageSection = page.locator('#usage');
      await usageSection.scrollIntoViewIfNeeded();

      // The img element should still have alt attribute
      const usageGif = page.locator('#usage .usage__gif');
      const altText = await usageGif.getAttribute('alt');
      expect(altText).toBeTruthy();
      expect(altText.length).toBeGreaterThan(10);
    });

    test('Navigation remains functional when images fail to load', async ({ page }) => {
      // Block all images
      await page.route('**/*.gif', (route) => route.abort());

      await page.goto('/');

      // Verify navigation is functional
      const navLinks = page.locator('.nav-links a');
      await expect(navLinks.first()).toBeVisible();

      // Click a nav link to verify navigation works
      await navLinks.first().click();

      // Verify smooth scroll happened (URL should contain hash)
      await page.waitForTimeout(500);
      const url = page.url();
      expect(url).toContain('#');
    });

    test('Footer layout remains intact when images fail', async ({ page }) => {
      // Block all images
      await page.route('**/*.gif', (route) => route.abort());

      await page.goto('/');

      // Scroll to footer
      const footer = page.locator('footer');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();

      // Verify footer content is visible
      const footerLinks = page.locator('.footer__link');
      await expect(footerLinks.first()).toBeVisible();

      const githubLink = page.locator('.footer__github-link');
      await expect(githubLink).toBeVisible();
    });
  });

  test.describe('Additional Logo Locations', () => {
    test('Logo in navigation bar loads correctly', async ({ page }) => {
      const navLogo = page.locator('.nav-logo img');
      await expect(navLogo).toBeVisible();
      await expect(navLogo).toHaveAttribute('src', 'assets/images/logo.gif');
      await expect(navLogo).toHaveAttribute('alt', 'MirDB Logo');
    });

    test('Logo in footer loads correctly', async ({ page }) => {
      const footer = page.locator('footer');
      await footer.scrollIntoViewIfNeeded();

      const footerLogo = page.locator('.footer__logo img');
      await expect(footerLogo).toBeVisible();
      await expect(footerLogo).toHaveAttribute('src', 'assets/images/logo.gif');
      await expect(footerLogo).toHaveAttribute('alt', 'MirDB Logo');
    });
  });

  test.describe('Image Loading Performance', () => {
    test('Logo loads within acceptable time', async ({ page }) => {
      const startTime = Date.now();
      await page.goto('/');

      // Wait for hero logo to be visible
      const heroLogo = page.locator('#hero .hero-logo');
      await expect(heroLogo).toBeVisible();

      const loadTime = Date.now() - startTime;
      // Should load within 5 seconds
      expect(loadTime).toBeLessThan(5000);
    });

    test('Usage GIF has lazy loading attribute', async ({ page }) => {
      await page.goto('/');

      const usageGif = page.locator('.usage__gif');
      await expect(usageGif).toHaveAttribute('loading', 'lazy');
    });
  });
});
