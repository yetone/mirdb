// @ts-check
const { test, expect, chromium } = require('@playwright/test');

/**
 * Test Suite: Browser Compatibility - Chrome
 * Scenario: Verify page renders correctly in Google Chrome (last 2 versions per NFR-3)
 *
 * NFR-3: Must support modern browsers (Chrome, Firefox, Safari, Edge - last 2 versions)
 * This test suite focuses specifically on Chrome browser compatibility.
 */

test.describe('Browser Compatibility - Chrome', () => {
  /**
   * Test Case 1: Page renders correctly without console errors
   * Input: Load page in Chrome latest version
   * Expected: Page renders correctly without console errors
   */
  test('TC1: Page renders correctly in Chrome without console errors', async ({ page }) => {
    // Collect console errors during page load
    const consoleErrors = [];
    const consoleWarnings = [];

    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        consoleErrors.push(msg.text());
      } else if (msg.type() === 'warning') {
        consoleWarnings.push(msg.text());
      }
    });

    // Collect page errors (uncaught exceptions)
    const pageErrors = [];
    page.on('pageerror', (error) => {
      pageErrors.push(error.message);
    });

    // Navigate to the homepage
    await page.goto('/', { waitUntil: 'networkidle' });

    // Wait for page to be fully loaded
    await page.waitForLoadState('domcontentloaded');

    // Log any warnings (for informational purposes)
    if (consoleWarnings.length > 0) {
      console.log('Console warnings detected (informational):');
      consoleWarnings.forEach((warning) => console.log(`  - ${warning}`));
    }

    // Verify no JavaScript errors in console
    if (consoleErrors.length > 0) {
      console.log('Console errors detected:');
      consoleErrors.forEach((error) => console.log(`  - ${error}`));
    }
    expect(consoleErrors.length, 'Should have no console errors').toBe(0);

    // Verify no uncaught page errors
    if (pageErrors.length > 0) {
      console.log('Page errors detected:');
      pageErrors.forEach((error) => console.log(`  - ${error}`));
    }
    expect(pageErrors.length, 'Should have no page errors').toBe(0);

    // Verify the page title is correct
    await expect(page).toHaveTitle(/MirDB/);

    // Verify critical page elements render correctly
    await expect(page.locator('header')).toBeVisible();
    await expect(page.locator('#hero')).toBeVisible();
    await expect(page.locator('#product-name')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();
    await expect(page.locator('#quick-start')).toBeVisible();
    await expect(page.locator('footer')).toBeVisible();

    // Verify CSS is loaded and applied (check a styled element)
    const heroSection = page.locator('.hero-section');
    await expect(heroSection).toBeVisible();

    // Verify the hero section has proper styling applied
    const heroStyles = await heroSection.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        display: styles.display,
        visibility: styles.visibility,
      };
    });
    expect(heroStyles.visibility).toBe('visible');

    // Verify fonts are loaded (text should not be invisible)
    const productName = page.locator('#product-name');
    const textContent = await productName.textContent();
    expect(textContent).toBe('MirDB');

    // Verify the product name is actually visible (not hidden by font loading issues)
    const isVisible = await productName.isVisible();
    expect(isVisible).toBe(true);
  });

  /**
   * Test Case 2: All interactive elements work correctly in Chrome
   * Input: Test all interactive elements in Chrome
   * Expected: All buttons, links, and navigation work correctly
   */
  test('TC2: All interactive elements work correctly in Chrome', async ({ page, context }) => {
    await page.goto('/', { waitUntil: 'networkidle' });

    // Test 1: Navigation links work
    const navLinks = page.locator('header nav .nav-links a');
    const navCount = await navLinks.count();
    expect(navCount).toBeGreaterThanOrEqual(3);

    // Test Features link
    const featuresLink = page.locator('header nav a[href="#features"]');
    await expect(featuresLink).toBeVisible();
    await featuresLink.click();
    await page.waitForTimeout(500);
    const featuresSection = page.locator('#features');
    const featuresInView = await featuresSection.evaluate((el) => {
      const rect = el.getBoundingClientRect();
      return rect.top >= -100 && rect.top <= window.innerHeight;
    });
    expect(featuresInView).toBe(true);

    // Test Docs link
    const docsLink = page.locator('header nav a:has-text("Docs")');
    await expect(docsLink).toBeVisible();
    await docsLink.click();
    await page.waitForTimeout(500);
    const docsHref = await docsLink.getAttribute('href');
    const docsTarget = page.locator(docsHref);
    const docsInView = await docsTarget.evaluate((el) => {
      const rect = el.getBoundingClientRect();
      return rect.top >= -100 && rect.top <= window.innerHeight;
    });
    expect(docsInView).toBe(true);

    // Reset scroll position
    await page.evaluate(() => window.scrollTo(0, 0));
    await page.waitForTimeout(300);

    // Test 2: CTA buttons work
    const getStartedBtn = page.locator('#get-started-btn');
    await expect(getStartedBtn).toBeVisible();
    await expect(getStartedBtn).toBeEnabled();

    // Click Get Started and verify scroll
    await getStartedBtn.click();
    await page.waitForTimeout(500);
    const quickStartSection = page.locator('#quick-start');
    const quickStartInView = await quickStartSection.evaluate((el) => {
      const rect = el.getBoundingClientRect();
      return rect.top >= -100 && rect.top <= window.innerHeight;
    });
    expect(quickStartInView).toBe(true);

    // Test 3: GitHub button has correct attributes for new tab
    const githubBtn = page.locator('#github-btn');
    await expect(githubBtn).toBeVisible();
    const githubHref = await githubBtn.getAttribute('href');
    const githubTarget = await githubBtn.getAttribute('target');
    const githubRel = await githubBtn.getAttribute('rel');

    expect(githubHref).toContain('github.com');
    expect(githubTarget).toBe('_blank');
    expect(githubRel).toContain('noopener');

    // Test 4: Footer links work
    const footerGithubLink = page.locator('[data-testid="footer-github-link"]');
    await expect(footerGithubLink).toBeVisible();
    const footerGithubHref = await footerGithubLink.getAttribute('href');
    expect(footerGithubHref).toContain('github.com');

    // Test 5: Mobile menu toggle is present (for responsive design)
    const mobileMenuToggle = page.locator('[data-testid="mobile-menu-toggle"]');
    await expect(mobileMenuToggle).toBeAttached();

    // Test 6: All anchor links are functional
    const allLinks = await page.locator('a[href^="#"]').all();
    for (const link of allLinks) {
      const href = await link.getAttribute('href');
      if (href && href.length > 1) {
        const targetId = href.substring(1);
        const target = page.locator(`#${targetId}`);
        const targetExists = (await target.count()) > 0;
        expect(targetExists, `Target ${href} should exist`).toBe(true);
      }
    }
  });

  /**
   * Additional Chrome-specific tests
   */

  test('Chrome renders all CSS correctly', async ({ page }) => {
    await page.goto('/', { waitUntil: 'networkidle' });

    // Verify CSS is loaded
    const stylesheets = await page.evaluate(() => {
      return Array.from(document.styleSheets).map((sheet) => ({
        href: sheet.href,
        rules: sheet.cssRules ? sheet.cssRules.length : 0,
      }));
    });

    // Should have at least one stylesheet loaded (styles.css)
    expect(stylesheets.length).toBeGreaterThan(0);

    // Check specific CSS properties are applied correctly
    const heroSection = page.locator('.hero-section');
    const heroComputedStyles = await heroSection.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        textAlign: styles.textAlign,
        padding: styles.padding,
      };
    });

    // Hero section should have center text alignment
    expect(heroComputedStyles.textAlign).toBe('center');

    // Verify value prop cards have correct layout
    const valuePropsGrid = page.locator('[data-testid="value-props-grid"]');
    const gridDisplay = await valuePropsGrid.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    // Should be grid or flex for card layout
    expect(['grid', 'flex']).toContain(gridDisplay);

    // Verify feature cards exist and are visible
    const featureCards = page.locator('.feature-card');
    const featureCardCount = await featureCards.count();
    expect(featureCardCount).toBeGreaterThanOrEqual(4);

    // Each feature card should be visible
    for (let i = 0; i < featureCardCount; i++) {
      await expect(featureCards.nth(i)).toBeVisible();
    }
  });

  test('Chrome handles images correctly', async ({ page }) => {
    // Track image loading
    const imageLoadStatus = [];

    page.on('response', async (response) => {
      const url = response.url();
      if (/\.(gif|png|jpg|jpeg|webp|svg)$/i.test(url)) {
        imageLoadStatus.push({
          url: url,
          status: response.status(),
          ok: response.ok(),
        });
      }
    });

    await page.goto('/', { waitUntil: 'networkidle' });

    // Verify all images loaded successfully
    for (const img of imageLoadStatus) {
      expect(img.ok, `Image ${img.url} should load successfully`).toBe(true);
    }

    // Verify logo images are visible
    const heroLogo = page.locator('#hero-logo');
    await expect(heroLogo).toBeVisible();

    const navLogo = page.locator('.nav-logo');
    await expect(navLogo).toBeVisible();

    // Verify images have natural dimensions (not broken)
    const heroLogoNaturalWidth = await heroLogo.evaluate(
      (img) => img.naturalWidth
    );
    expect(heroLogoNaturalWidth).toBeGreaterThan(0);
  });

  test('Chrome JavaScript functionality works correctly', async ({ page }) => {
    // Test mobile menu toggle functionality - resize to mobile viewport first
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/', { waitUntil: 'networkidle' });

    const mobileMenuToggle = page.locator('[data-testid="mobile-menu-toggle"]');
    const navLinks = page.locator('.nav-links');

    // Wait for the mobile menu toggle to be visible on mobile
    await expect(mobileMenuToggle).toBeVisible();

    // Get initial aria-expanded state
    const initialAriaExpanded =
      await mobileMenuToggle.getAttribute('aria-expanded');
    expect(initialAriaExpanded).toBe('false');

    // Click the toggle
    await mobileMenuToggle.click();

    // Verify aria-expanded changes
    const afterClickAriaExpanded =
      await mobileMenuToggle.getAttribute('aria-expanded');
    expect(afterClickAriaExpanded).toBe('true');

    // Verify nav-open class is added
    const hasNavOpen = await navLinks.evaluate((el) =>
      el.classList.contains('nav-open')
    );
    expect(hasNavOpen).toBe(true);

    // Click again to close
    await mobileMenuToggle.click();

    // Verify it closes
    const afterSecondClick =
      await mobileMenuToggle.getAttribute('aria-expanded');
    expect(afterSecondClick).toBe('false');
  });

  test('Chrome renders text content correctly', async ({ page }) => {
    await page.goto('/', { waitUntil: 'networkidle' });

    // Verify all major text content is rendered
    const productName = page.locator('#product-name');
    await expect(productName).toHaveText('MirDB');

    const tagline = page.locator('#tagline');
    const taglineText = await tagline.textContent();
    expect(taglineText).toContain('persistent');
    expect(taglineText).toContain('memcached');

    // Verify value proposition cards have content
    const persistentTitle = page.locator(
      '[data-testid="value-prop-persistent-storage-title"]'
    );
    await expect(persistentTitle).toHaveText('Persistent Storage');

    const memcachedTitle = page.locator(
      '[data-testid="value-prop-memcached-compatible-title"]'
    );
    await expect(memcachedTitle).toHaveText('Memcached Compatible');

    const rustTitle = page.locator('[data-testid="value-prop-rust-title"]');
    await expect(rustTitle).toHaveText('Written in Rust');

    // Verify features section content
    const featuresHeading = page.locator('#features h2');
    await expect(featuresHeading).toHaveText('Key Features');

    // Verify quick start section content
    const quickStartHeading = page.locator('#quick-start h2');
    await expect(quickStartHeading).toHaveText('Quick Start');

    // Verify footer content
    const footerVersion = page.locator('[data-testid="footer-version"]');
    await expect(footerVersion).toContainText('Version');

    const footerLicense = page.locator('[data-testid="footer-license"]');
    await expect(footerLicense).toContainText('MIT');
  });

  test('Chrome SVG icons render correctly', async ({ page }) => {
    await page.goto('/', { waitUntil: 'networkidle' });

    // Verify SVG icons in value prop cards are visible
    const valuePropsSection = page.locator(
      '[data-testid="value-propositions-section"]'
    );
    const svgIcons = valuePropsSection.locator('svg');
    const svgCount = await svgIcons.count();

    // Should have 3 SVG icons (one for each value prop)
    expect(svgCount).toBeGreaterThanOrEqual(3);

    // Verify each SVG is visible and has dimensions
    for (let i = 0; i < svgCount; i++) {
      const svg = svgIcons.nth(i);
      await expect(svg).toBeVisible();

      const svgBox = await svg.boundingBox();
      expect(svgBox).toBeTruthy();
      expect(svgBox.width).toBeGreaterThan(0);
      expect(svgBox.height).toBeGreaterThan(0);
    }
  });
});
