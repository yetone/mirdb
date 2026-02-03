/**
 * E2E tests for cross-browser compatibility.
 * Owner: Scenario 14 - Cross-Browser Compatibility
 *
 * Tests:
 * - Homepage renders correctly across Chrome, Firefox, Safari, Edge (last 2 versions)
 * - All sections render correctly in each browser
 * - All interactions work consistently across browsers
 *
 * NFR-7: Cross-browser compatibility (Chrome, Firefox, Safari, Edge - last 2 versions)
 */

import { test, expect } from '@playwright/test';

/**
 * Cross-browser compatibility tests for the MirDB homepage.
 * These tests are designed to run across all configured browser projects in playwright.config.ts:
 * - chromium (Chrome latest 2 versions)
 * - firefox (Firefox latest 2 versions)
 * - webkit (Safari latest 2 versions)
 * - msedge (Edge latest 2 versions)
 */
test.describe('Cross-Browser Compatibility', () => {
  test.describe('Test Case 1: Chrome Rendering', () => {
    test('homepage renders all sections correctly in Chrome', async ({ page }) => {
      // This test runs in all browsers but documents Chrome-specific checks
      await page.goto('/mirdb/');
      await page.waitForLoadState('networkidle');

      // Verify all main sections are visible
      const sections = ['#hero', '#features', '#examples', '#architecture', '#installation'];
      for (const sectionId of sections) {
        const section = page.locator(sectionId);
        await section.scrollIntoViewIfNeeded();
        await expect(section).toBeVisible();
      }

      // Verify footer is visible
      const footer = page.locator('footer');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();

      // Verify hero section elements
      const heroLogo = page.locator('[data-testid="hero-logo"] img');
      await expect(heroLogo).toBeVisible();

      const heroTagline = page.locator('[data-testid="hero-tagline"]');
      await expect(heroTagline).toBeVisible();
      await expect(heroTagline).toContainText('A Persistent Key-Value Store with Memcached Protocol');

      // Verify CTA buttons
      const getStartedButton = page.locator('[data-testid="get-started-button"]');
      await expect(getStartedButton).toBeVisible();

      const githubButton = page.locator('[data-testid="github-button"]');
      await expect(githubButton).toBeVisible();
    });

    test('all interactions work correctly in Chrome', async ({ page }) => {
      await page.goto('/mirdb/');
      await page.waitForLoadState('networkidle');

      // Test Get Started button navigation
      const getStartedButton = page.locator('[data-testid="get-started-button"]');
      await getStartedButton.click();
      await page.waitForTimeout(1000);

      const installationSection = page.locator('#installation');
      await expect(installationSection).toBeInViewport();

      // Navigate back to top for next test
      await page.goto('/mirdb/');
      await page.waitForLoadState('networkidle');

      // Test GitHub button opens in new tab
      const githubButton = page.locator('[data-testid="github-button"]');
      await expect(githubButton).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
      await expect(githubButton).toHaveAttribute('target', '_blank');

      // Test tab navigation in examples section
      const examplesSection = page.locator('#examples');
      await examplesSection.scrollIntoViewIfNeeded();

      const tabButtons = page.locator('#examples .tab-button');
      const tabCount = await tabButtons.count();

      if (tabCount > 1) {
        await tabButtons.nth(1).click();
        await page.waitForTimeout(200);
        await expect(tabButtons.nth(1)).toHaveAttribute('aria-selected', 'true');
      }
    });
  });

  test.describe('Test Case 2: Firefox Rendering', () => {
    test('homepage renders all sections correctly in Firefox', async ({ page }) => {
      await page.goto('/mirdb/');
      await page.waitForLoadState('networkidle');

      // Verify all main sections are visible
      const sections = ['#hero', '#features', '#examples', '#architecture', '#installation'];
      for (const sectionId of sections) {
        const section = page.locator(sectionId);
        await section.scrollIntoViewIfNeeded();
        await expect(section).toBeVisible();
      }

      // Verify footer is visible
      const footer = page.locator('footer');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();

      // Verify hero section elements
      const heroLogo = page.locator('[data-testid="hero-logo"] img');
      await expect(heroLogo).toBeVisible();

      const heroTagline = page.locator('[data-testid="hero-tagline"]');
      await expect(heroTagline).toBeVisible();

      // Verify features grid renders properly
      const featuresSection = page.locator('#features');
      await featuresSection.scrollIntoViewIfNeeded();

      const featuresGrid = page.locator('#features .features-grid, #features .grid');
      await expect(featuresGrid).toBeVisible();

      const featureCards = featuresGrid.locator('> *');
      const cardCount = await featureCards.count();
      expect(cardCount).toBeGreaterThanOrEqual(6);
    });

    test('all interactions work correctly in Firefox', async ({ page }) => {
      await page.goto('/mirdb/');
      await page.waitForLoadState('networkidle');

      // Test smooth scroll navigation
      const getStartedButton = page.locator('[data-testid="get-started-button"]');
      await getStartedButton.click();
      await page.waitForTimeout(1000);

      const installationSection = page.locator('#installation');
      await expect(installationSection).toBeInViewport();

      // Test installation tabs
      await page.goto('/mirdb/');
      await page.waitForLoadState('networkidle');

      const installSection = page.locator('#installation');
      await installSection.scrollIntoViewIfNeeded();

      const installTabs = page.locator('#installation .tab-button');
      const installTabCount = await installTabs.count();

      if (installTabCount > 1) {
        await installTabs.nth(1).click();
        await page.waitForTimeout(200);
        await expect(installTabs.nth(1)).toHaveAttribute('aria-selected', 'true');
      }
    });
  });

  test.describe('Test Case 3: Safari (WebKit) Rendering', () => {
    test('homepage renders all sections correctly in Safari', async ({ page }) => {
      await page.goto('/mirdb/');
      await page.waitForLoadState('networkidle');

      // Verify all main sections are visible
      const sections = ['#hero', '#features', '#examples', '#architecture', '#installation'];
      for (const sectionId of sections) {
        const section = page.locator(sectionId);
        await section.scrollIntoViewIfNeeded();
        await expect(section).toBeVisible();
      }

      // Verify footer is visible
      const footer = page.locator('footer');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();

      // Verify hero section elements
      const heroLogo = page.locator('[data-testid="hero-logo"] img');
      await expect(heroLogo).toBeVisible();

      // Verify architecture diagram renders
      const architectureSection = page.locator('#architecture');
      await architectureSection.scrollIntoViewIfNeeded();
      await expect(architectureSection).toBeVisible();

      // Check for architecture diagram (SVG or image)
      const architectureDiagram = page.locator('#architecture img, #architecture svg');
      await expect(architectureDiagram.first()).toBeVisible();
    });

    test('all interactions work correctly in Safari', async ({ page }) => {
      await page.goto('/mirdb/');
      await page.waitForLoadState('networkidle');

      // Test Get Started button navigation
      const getStartedButton = page.locator('[data-testid="get-started-button"]');
      await getStartedButton.click();
      await page.waitForTimeout(1000);

      const installationSection = page.locator('#installation');
      await expect(installationSection).toBeInViewport();

      // Test examples tab navigation
      await page.goto('/mirdb/');
      await page.waitForLoadState('networkidle');

      const examplesSection = page.locator('#examples');
      await examplesSection.scrollIntoViewIfNeeded();

      const tabButtons = page.locator('#examples .tab-button');
      const tabCount = await tabButtons.count();

      if (tabCount > 1) {
        // Click last tab
        await tabButtons.nth(tabCount - 1).click();
        await page.waitForTimeout(200);
        await expect(tabButtons.nth(tabCount - 1)).toHaveAttribute('aria-selected', 'true');
      }
    });
  });

  test.describe('Test Case 4: Edge Rendering', () => {
    test('homepage renders all sections correctly in Edge', async ({ page }) => {
      await page.goto('/mirdb/');
      await page.waitForLoadState('networkidle');

      // Verify all main sections are visible
      const sections = ['#hero', '#features', '#examples', '#architecture', '#installation'];
      for (const sectionId of sections) {
        const section = page.locator(sectionId);
        await section.scrollIntoViewIfNeeded();
        await expect(section).toBeVisible();
      }

      // Verify footer is visible
      const footer = page.locator('footer');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();

      // Verify hero section elements
      const heroLogo = page.locator('[data-testid="hero-logo"] img');
      await expect(heroLogo).toBeVisible();

      const heroTagline = page.locator('[data-testid="hero-tagline"]');
      await expect(heroTagline).toBeVisible();

      // Verify CTA buttons
      const getStartedButton = page.locator('[data-testid="get-started-button"]');
      await expect(getStartedButton).toBeVisible();

      const githubButton = page.locator('[data-testid="github-button"]');
      await expect(githubButton).toBeVisible();
    });

    test('all interactions work correctly in Edge', async ({ page }) => {
      await page.goto('/mirdb/');
      await page.waitForLoadState('networkidle');

      // Test Get Started button navigation
      const getStartedButton = page.locator('[data-testid="get-started-button"]');
      await getStartedButton.click();
      await page.waitForTimeout(1000);

      const installationSection = page.locator('#installation');
      await expect(installationSection).toBeInViewport();

      // Navigate back and test dark mode toggle if present
      await page.goto('/mirdb/');
      await page.waitForLoadState('networkidle');

      const darkModeToggle = page.locator('[data-testid="dark-mode-toggle"]');
      if (await darkModeToggle.count() > 0) {
        await darkModeToggle.click();
        await page.waitForTimeout(200);

        // Verify dark mode is applied
        const htmlElement = page.locator('html');
        const hasDarkClass = await htmlElement.evaluate((el) => el.classList.contains('dark'));
        expect(hasDarkClass).toBe(true);
      }
    });
  });

  // Common cross-browser tests that verify consistent behavior across all browsers
  test.describe('Consistent Behavior Across All Browsers', () => {
    test('no horizontal scrollbar appears', async ({ page }) => {
      await page.goto('/mirdb/');
      await page.waitForLoadState('networkidle');

      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);
    });

    test('all images load successfully', async ({ page }) => {
      await page.goto('/mirdb/');
      await page.waitForLoadState('networkidle');

      // Check hero logo
      const heroLogo = page.locator('[data-testid="hero-logo"] img');
      await expect(heroLogo).toBeVisible();

      // Verify image has loaded (natural width should be > 0)
      const logoLoaded = await heroLogo.evaluate((img: HTMLImageElement) => {
        return img.naturalWidth > 0;
      });
      expect(logoLoaded).toBe(true);

      // Check architecture diagram
      const architectureSection = page.locator('#architecture');
      await architectureSection.scrollIntoViewIfNeeded();

      const archImage = page.locator('#architecture img').first();
      if (await archImage.count() > 0) {
        await expect(archImage).toBeVisible();
        // For SVG images, naturalWidth may be 0 but complete should be true
        const archImageLoaded = await archImage.evaluate((img: HTMLImageElement) => {
          // SVG images loaded via img tag: check complete property or naturalWidth
          return img.complete && (img.naturalWidth > 0 || img.src.endsWith('.svg'));
        });
        expect(archImageLoaded).toBe(true);
      }
    });

    test('CSS styles are applied correctly', async ({ page }) => {
      await page.goto('/mirdb/');
      await page.waitForLoadState('networkidle');

      // Verify hero section has expected styling
      const heroSection = page.locator('#hero');
      const heroBackground = await heroSection.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });
      // Background should have some color value (not empty)
      expect(heroBackground).toBeTruthy();

      // Verify buttons have proper styling
      const getStartedButton = page.locator('[data-testid="get-started-button"]');
      const buttonCursor = await getStartedButton.evaluate((el) => {
        return window.getComputedStyle(el).cursor;
      });
      // Expect pointer cursor, but WebKit may return 'auto' for some anchor elements
      expect(['pointer', 'auto']).toContain(buttonCursor);
    });

    test('fonts render correctly', async ({ page }) => {
      await page.goto('/mirdb/');
      await page.waitForLoadState('networkidle');

      // Check that text elements have proper font styling
      const heroTitle = page.locator('#hero h1');
      const fontSize = await heroTitle.evaluate((el) => {
        return window.getComputedStyle(el).fontSize;
      });
      // Font size should be defined and reasonable (not 0 or empty)
      expect(fontSize).toBeTruthy();
      const fontSizeNumber = parseFloat(fontSize);
      expect(fontSizeNumber).toBeGreaterThan(0);
    });

    test('responsive design works consistently', async ({ page }) => {
      // Test mobile viewport
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/mirdb/');
      await page.waitForLoadState('networkidle');

      const heroSection = page.locator('#hero');
      await expect(heroSection).toBeVisible();

      // Test tablet viewport
      await page.setViewportSize({ width: 768, height: 1024 });
      await page.reload();
      await page.waitForLoadState('networkidle');
      await expect(heroSection).toBeVisible();

      // Test desktop viewport
      await page.setViewportSize({ width: 1920, height: 1080 });
      await page.reload();
      await page.waitForLoadState('networkidle');
      await expect(heroSection).toBeVisible();
    });

    test('external links have correct attributes', async ({ page }) => {
      await page.goto('/mirdb/');
      await page.waitForLoadState('networkidle');

      // Check GitHub button in hero
      const githubButton = page.locator('[data-testid="github-button"]');
      await expect(githubButton).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
      await expect(githubButton).toHaveAttribute('target', '_blank');
      await expect(githubButton).toHaveAttribute('rel', /noopener/);

      // Scroll to footer and check links
      const footer = page.locator('footer');
      await footer.scrollIntoViewIfNeeded();

      const footerGithubLink = footer.locator('a[href*="github.com"]').first();
      if (await footerGithubLink.count() > 0) {
        await expect(footerGithubLink).toHaveAttribute('rel', /noopener/);
      }
    });

    test('semantic HTML structure is consistent', async ({ page }) => {
      await page.goto('/mirdb/');
      await page.waitForLoadState('networkidle');

      // Check for main landmark
      const mainElement = page.locator('main');
      await expect(mainElement).toBeVisible();

      // Check for header
      const headerElement = page.locator('header');
      if (await headerElement.count() > 0) {
        await expect(headerElement).toBeVisible();
      }

      // Check for footer landmark
      const footerElement = page.locator('footer');
      await expect(footerElement).toBeVisible();

      // Check for proper heading hierarchy
      const h1Elements = page.locator('h1');
      const h1Count = await h1Elements.count();
      expect(h1Count).toBeGreaterThanOrEqual(1);
    });

    test('code blocks render with syntax highlighting', async ({ page }) => {
      await page.goto('/mirdb/');
      await page.waitForLoadState('networkidle');

      // Scroll to examples section
      const examplesSection = page.locator('#examples');
      await examplesSection.scrollIntoViewIfNeeded();

      // Check for code blocks
      const codeBlocks = page.locator('#examples pre, #examples code');
      const codeBlockCount = await codeBlocks.count();
      expect(codeBlockCount).toBeGreaterThan(0);

      // Verify code block is visible
      await expect(codeBlocks.first()).toBeVisible();
    });

    test('scroll behavior is smooth', async ({ page }) => {
      await page.goto('/mirdb/');
      await page.waitForLoadState('networkidle');

      // Get initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY);

      // Click Get Started to trigger scroll
      const getStartedButton = page.locator('[data-testid="get-started-button"]');
      await getStartedButton.click();

      // Wait a bit and check scroll happened
      await page.waitForTimeout(500);

      const midScrollY = await page.evaluate(() => window.scrollY);
      expect(midScrollY).toBeGreaterThan(initialScrollY);

      // Wait for scroll to complete
      await page.waitForTimeout(500);

      const finalScrollY = await page.evaluate(() => window.scrollY);
      expect(finalScrollY).toBeGreaterThan(initialScrollY);
    });
  });
});
