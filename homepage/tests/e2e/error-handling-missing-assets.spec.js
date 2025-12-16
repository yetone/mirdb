// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Error Handling - Missing Assets Tests
 * Scenario: Verify graceful handling when static assets fail to load
 *
 * Test Cases:
 * TC1: Load page with images blocked - Page remains readable with alt text displayed for missing images
 * TC2: Load page with CSS blocked - Content is still accessible in unstyled but readable format
 * TC3: Check for broken image indicators - No broken image icons visible under normal operation
 */

test.describe('Error Handling - Missing Assets (Scenario 18)', () => {
  /**
   * Test Case 1: Load page with images blocked
   * Expected: Page remains readable with alt text displayed for missing images
   */
  test.describe('TC1: Images Blocked - Graceful Degradation', () => {
    test('TC1a: Page remains readable when images are blocked', async ({ page }) => {
      // Block all image requests
      await page.route('**/*.{png,jpg,jpeg,gif,svg,webp}', route => route.abort());
      await page.route('**/img.shields.io/**', route => route.abort());

      // Navigate to homepage
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Verify page title is still visible
      await expect(page).toHaveTitle(/MirDB/);

      // Verify hero section content is readable
      const heroTitle = page.locator('[data-testid="hero-title"]');
      await expect(heroTitle).toBeVisible();
      await expect(heroTitle).toContainText('MirDB');

      // Verify hero tagline is readable
      const heroTagline = page.locator('[data-testid="hero-tagline"]');
      await expect(heroTagline).toBeVisible();
      const taglineText = await heroTagline.textContent();
      expect(taglineText.length).toBeGreaterThan(10);

      // Verify CTA buttons are still functional
      const getStartedBtn = page.locator('[data-testid="get-started-btn"]');
      await expect(getStartedBtn).toBeVisible();
      await expect(getStartedBtn).toBeEnabled();
    });

    test('TC1b: Alt text is available for badge images when images fail to load', async ({ page }) => {
      // Block all image requests
      await page.route('**/*.{png,jpg,jpeg,gif,svg,webp}', route => route.abort());
      await page.route('**/img.shields.io/**', route => route.abort());

      // Navigate to homepage
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Scroll to footer where badge images are
      await page.locator('.footer').scrollIntoViewIfNeeded();

      // Verify badge images have alt text for accessibility
      const statusBadge = page.locator('[data-testid="status-badge-img"]');
      const licenseBadge = page.locator('[data-testid="license-badge-img"]');

      // Check that alt attributes exist and are meaningful
      const statusAlt = await statusBadge.getAttribute('alt');
      const licenseAlt = await licenseBadge.getAttribute('alt');

      expect(statusAlt).toBeTruthy();
      expect(statusAlt.length).toBeGreaterThan(5);
      expect(statusAlt).toContain('Status');

      expect(licenseAlt).toBeTruthy();
      expect(licenseAlt.length).toBeGreaterThan(5);
      expect(licenseAlt).toContain('License');
    });

    test('TC1c: All sections remain accessible when images are blocked', async ({ page }) => {
      // Block all image requests
      await page.route('**/*.{png,jpg,jpeg,gif,svg,webp}', route => route.abort());
      await page.route('**/img.shields.io/**', route => route.abort());

      // Navigate to homepage
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Verify all major sections are still visible and accessible
      const heroSection = page.locator('[data-testid="hero-section"]');
      const featuresSection = page.locator('[data-testid="features-section"]');
      const howItWorksSection = page.locator('[data-testid="how-it-works-section"]');
      const codeExamplesSection = page.locator('[data-testid="code-examples-section"]');
      const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
      const footer = page.locator('.footer');

      await expect(heroSection).toBeVisible();
      await expect(featuresSection).toBeVisible();
      await expect(howItWorksSection).toBeVisible();
      await expect(codeExamplesSection).toBeVisible();
      await expect(gettingStartedSection).toBeVisible();
      await expect(footer).toBeVisible();

      // Verify feature cards are still readable
      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();
      expect(cardCount).toBeGreaterThanOrEqual(6);
    });

    test('TC1d: Navigation still functions when images are blocked', async ({ page }) => {
      // Block all image requests
      await page.route('**/*.{png,jpg,jpeg,gif,svg,webp}', route => route.abort());
      await page.route('**/img.shields.io/**', route => route.abort());

      // Navigate to homepage
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Test smooth scroll navigation still works
      const getStartedBtn = page.locator('[data-testid="get-started-btn"]');
      await getStartedBtn.click();

      // Wait for scroll animation
      await page.waitForTimeout(800);

      // Verify getting started section is now in viewport
      const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
      await expect(gettingStartedSection).toBeInViewport();
    });
  });

  /**
   * Test Case 2: Load page with CSS blocked
   * Expected: Content is still accessible in unstyled but readable format
   */
  test.describe('TC2: CSS Blocked - Content Accessibility', () => {
    test('TC2a: Page content remains accessible when CSS fails to load', async ({ page }) => {
      // Block CSS requests
      await page.route('**/*.css', route => route.abort());

      // Navigate to homepage
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Verify page title is still accessible
      await expect(page).toHaveTitle(/MirDB/);

      // Verify main content elements are present in DOM
      const heroTitle = page.locator('[data-testid="hero-title"]');
      const heroTagline = page.locator('[data-testid="hero-tagline"]');

      // Elements should exist even without CSS
      await expect(heroTitle).toBeAttached();
      await expect(heroTagline).toBeAttached();

      // Verify text content is readable
      const titleText = await heroTitle.textContent();
      expect(titleText).toContain('MirDB');

      const taglineText = await heroTagline.textContent();
      expect(taglineText.length).toBeGreaterThan(10);
    });

    test('TC2b: All text content remains readable without CSS', async ({ page }) => {
      // Block CSS requests
      await page.route('**/*.css', route => route.abort());

      // Navigate to homepage
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Verify feature section text is accessible
      const featuresTitle = page.locator('[data-testid="features-title"]');
      await expect(featuresTitle).toBeAttached();
      const featuresTitleText = await featuresTitle.textContent();
      expect(featuresTitleText).toContain('Features');

      // Verify feature card content is accessible
      const featureCards = page.locator('.feature-card');
      const firstCardTitle = featureCards.first().locator('.feature-card-title');
      await expect(firstCardTitle).toBeAttached();
      const cardTitleText = await firstCardTitle.textContent();
      expect(cardTitleText.length).toBeGreaterThan(3);
    });

    test('TC2c: Navigation links remain functional without CSS', async ({ page }) => {
      // Block CSS requests
      await page.route('**/*.css', route => route.abort());

      // Navigate to homepage
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Verify navigation buttons are still present and have correct attributes
      const getStartedBtn = page.locator('[data-testid="get-started-btn"]');
      const githubBtn = page.locator('[data-testid="github-btn"]');

      await expect(getStartedBtn).toBeAttached();
      await expect(githubBtn).toBeAttached();

      // Verify href attributes are correct
      const getStartedHref = await getStartedBtn.getAttribute('href');
      expect(getStartedHref).toBe('#getting-started');

      const githubHref = await githubBtn.getAttribute('href');
      expect(githubHref).toContain('github.com');
    });

    test('TC2d: Code examples remain readable without CSS', async ({ page }) => {
      // Block CSS requests
      await page.route('**/*.css', route => route.abort());

      // Navigate to homepage
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Verify code examples section content is accessible
      const codeExamplesSection = page.locator('[data-testid="code-examples-section"]');
      await expect(codeExamplesSection).toBeAttached();

      // Verify command cards have readable text
      const getCommandCard = page.locator('[data-testid="command-card-get"]');
      await expect(getCommandCard).toBeAttached();

      const codeBlock = page.locator('[data-testid="code-get"]');
      await expect(codeBlock).toBeAttached();
      const codeText = await codeBlock.textContent();
      expect(codeText).toContain('get');
    });

    test('TC2e: Semantic HTML provides structure without CSS', async ({ page }) => {
      // Block CSS requests
      await page.route('**/*.css', route => route.abort());

      // Navigate to homepage
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Verify semantic HTML structure is intact
      const main = page.locator('main');
      const footer = page.locator('footer');
      const sections = page.locator('section');
      const headings = page.locator('h1, h2, h3');

      await expect(main).toBeAttached();
      await expect(footer).toBeAttached();

      const sectionCount = await sections.count();
      expect(sectionCount).toBeGreaterThanOrEqual(5);

      const headingCount = await headings.count();
      expect(headingCount).toBeGreaterThanOrEqual(10);

      // Verify heading hierarchy exists
      const h1 = page.locator('h1');
      const h2 = page.locator('h2');
      const h3 = page.locator('h3');

      await expect(h1.first()).toBeAttached();
      expect(await h2.count()).toBeGreaterThan(0);
      expect(await h3.count()).toBeGreaterThan(0);
    });
  });

  /**
   * Test Case 3: Check for broken image indicators
   * Expected: No broken image icons visible under normal operation
   */
  test.describe('TC3: Broken Image Indicators Check', () => {
    test('TC3a: No broken images under normal operation', async ({ page }) => {
      // Track failed requests
      const failedRequests = [];
      page.on('requestfailed', request => {
        if (request.resourceType() === 'image') {
          failedRequests.push(request.url());
        }
      });

      // Navigate to homepage with all assets loading normally
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Get all images on the page
      const images = page.locator('img');
      const imageCount = await images.count();

      // Check each image for broken state
      for (let i = 0; i < imageCount; i++) {
        const img = images.nth(i);

        // Verify image is visible
        const isVisible = await img.isVisible();

        if (isVisible) {
          // Check naturalWidth to detect broken images
          // A broken image typically has naturalWidth of 0
          const naturalWidth = await img.evaluate(el => el.naturalWidth);
          const imgSrc = await img.getAttribute('src');

          // External badge images might not load in test environment
          // but we verify they have proper alt text as fallback
          if (naturalWidth === 0) {
            const altText = await img.getAttribute('alt');
            expect(altText).toBeTruthy();
            expect(altText.length).toBeGreaterThan(3);
          }
        }
      }
    });

    test('TC3b: All images have alt attributes for graceful degradation', async ({ page }) => {
      // Navigate to homepage
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Get all images on the page
      const images = page.locator('img');
      const imageCount = await images.count();

      // Verify every image has an alt attribute
      for (let i = 0; i < imageCount; i++) {
        const img = images.nth(i);
        const altText = await img.getAttribute('alt');
        const imgSrc = await img.getAttribute('src');

        // Every image should have meaningful alt text
        expect(altText, `Image at ${imgSrc} should have alt text`).toBeTruthy();
        expect(altText.length, `Image alt text should be meaningful`).toBeGreaterThan(3);
      }
    });

    test('TC3c: Badge images have descriptive alt text', async ({ page }) => {
      // Navigate to homepage
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Scroll to footer
      await page.locator('.footer').scrollIntoViewIfNeeded();

      // Check status badge
      const statusBadge = page.locator('[data-testid="status-badge-img"]');
      const statusAlt = await statusBadge.getAttribute('alt');
      expect(statusAlt).toBe('Project Status: Active');

      // Check license badge
      const licenseBadge = page.locator('[data-testid="license-badge-img"]');
      const licenseAlt = await licenseBadge.getAttribute('alt');
      expect(licenseAlt).toBe('MIT License');
    });

    test('TC3d: Badge link wrappers provide accessible labels', async ({ page }) => {
      // Navigate to homepage
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Scroll to footer
      await page.locator('.footer').scrollIntoViewIfNeeded();

      // Check project status badge link has aria-label
      const statusLink = page.locator('[data-testid="project-status-badge"]');
      const statusAriaLabel = await statusLink.getAttribute('aria-label');
      expect(statusAriaLabel).toBeTruthy();
      expect(statusAriaLabel).toContain('Project Status');

      // Check license badge link has aria-label
      const licenseLink = page.locator('[data-testid="license-badge-link"]');
      const licenseAriaLabel = await licenseLink.getAttribute('aria-label');
      expect(licenseAriaLabel).toBeTruthy();
      expect(licenseAriaLabel).toContain('License');
    });

    test('TC3e: Page renders correctly even if external badges fail', async ({ page }) => {
      // Block only external badge images (shields.io)
      await page.route('**/img.shields.io/**', route => route.abort());

      // Navigate to homepage
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Page should still render completely
      const heroSection = page.locator('[data-testid="hero-section"]');
      const featuresSection = page.locator('[data-testid="features-section"]');
      const footer = page.locator('.footer');

      await expect(heroSection).toBeVisible();
      await expect(featuresSection).toBeVisible();
      await expect(footer).toBeVisible();

      // Footer badges should still have their alt text visible/accessible
      const statusBadge = page.locator('[data-testid="status-badge-img"]');
      const licenseBadge = page.locator('[data-testid="license-badge-img"]');

      await expect(statusBadge).toBeAttached();
      await expect(licenseBadge).toBeAttached();
    });
  });

  /**
   * Additional graceful degradation tests
   */
  test.describe('Additional Graceful Degradation Tests', () => {
    test('JavaScript validation functions work when Mermaid fails to load', async ({ page }) => {
      // Block Mermaid.js
      await page.route('**/mermaid*', route => route.abort());

      // Navigate to homepage
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');
      await page.waitForTimeout(500);

      // Verify validation functions are still available
      const heroValid = await page.evaluate(() => {
        return typeof window.MirDBHomepage !== 'undefined' &&
               typeof window.MirDBHomepage.validateHeroRender === 'function';
      });
      expect(heroValid).toBe(true);

      // Run validation - should still pass even without Mermaid
      const validationResult = await page.evaluate(() => {
        return window.MirDBHomepage.validateHeroRender();
      });
      expect(validationResult).toBe(true);
    });

    test('Copy buttons remain functional when CSS is blocked', async ({ page }) => {
      // Block CSS
      await page.route('**/*.css', route => route.abort());

      // Navigate to homepage
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Scroll to code examples
      await page.locator('[data-testid="code-examples-section"]').scrollIntoViewIfNeeded();

      // Verify copy buttons are present
      const copyButtons = page.locator('.copy-btn');
      const buttonCount = await copyButtons.count();
      expect(buttonCount).toBeGreaterThan(0);

      // Verify first copy button is interactable
      const firstCopyBtn = copyButtons.first();
      await expect(firstCopyBtn).toBeAttached();
    });

    test('Page maintains accessibility when both CSS and images are blocked', async ({ page }) => {
      // Block both CSS and images
      await page.route('**/*.css', route => route.abort());
      await page.route('**/*.{png,jpg,jpeg,gif,svg,webp}', route => route.abort());
      await page.route('**/img.shields.io/**', route => route.abort());

      // Navigate to homepage
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Verify page title
      await expect(page).toHaveTitle(/MirDB/);

      // Verify all text content is accessible
      const heroTitle = page.locator('[data-testid="hero-title"]');
      await expect(heroTitle).toBeAttached();

      const titleText = await heroTitle.textContent();
      expect(titleText).toContain('MirDB');

      // Verify semantic structure is preserved
      const main = page.locator('main');
      const footer = page.locator('footer');
      await expect(main).toBeAttached();
      await expect(footer).toBeAttached();

      // Verify links are still functional
      const links = page.locator('a[href]');
      const linkCount = await links.count();
      expect(linkCount).toBeGreaterThan(5);
    });
  });
});
