/**
 * Cross-Browser Compatibility E2E Tests
 * Owner: Scenario 17 - Cross-Browser Compatibility
 *
 * Tests for validating the site works correctly across all supported browsers:
 * - Chrome latest
 * - Firefox latest
 * - Safari (WebKit) latest
 * - Edge latest
 *
 * Tests cover:
 * - Page load and rendering
 * - All major sections visible and functional
 * - No visual issues or layout breaks
 * - Clipboard functionality across browsers
 */

import { test, expect } from '@playwright/test';

/**
 * Test Case 1: Load page in Chrome latest
 * Test Case 2: Load page in Firefox latest
 * Test Case 3: Load page in Safari latest
 * Test Case 4: Load page in Edge latest
 *
 * These tests run automatically across all browser projects defined in playwright.config.ts
 */
test.describe('Cross-Browser Compatibility - All Features Work Correctly', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Page loads without errors', async ({ page, browserName }) => {
    // Verify the page loaded successfully
    await expect(page).toHaveTitle(/MirDB/i);

    // Wait for page to be fully loaded
    await page.waitForLoadState('networkidle');

    // Log the browser being tested for reference
    console.log(`Testing on browser: ${browserName}`);
  });

  test('Hero section renders correctly', async ({ page, browserName }) => {
    // Verify hero title is visible
    const heroTitle = page.getByTestId('hero-title');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toHaveText('MirDB');

    // Verify tagline is visible
    const tagline = page.getByTestId('hero-tagline');
    await expect(tagline).toBeVisible();

    // Verify CTA button is visible and clickable
    const ctaButton = page.getByTestId('cta-button');
    await expect(ctaButton).toBeVisible();
    await expect(ctaButton).toHaveText('Get Started');

    // Verify font size is at least 48px on desktop
    await page.setViewportSize({ width: 1280, height: 720 });
    const fontSize = await heroTitle.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    expect(fontSize).toBeGreaterThanOrEqual(48);

    console.log(`Hero section verified on: ${browserName}`);
  });

  test('Features section renders correctly', async ({ page, browserName }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify all 4 feature cards are present
    const featureCards = page.locator('[data-testid^="feature-card-"]');
    await expect(featureCards).toHaveCount(4);

    // Verify grid layout on desktop
    await page.setViewportSize({ width: 1280, height: 720 });
    const featuresGrid = page.getByTestId('features-grid');
    const gridStyle = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(gridStyle).toBe('grid');

    // Verify LSM-tree feature card
    const lsmTreeCard = page.getByTestId('feature-card-lsm-tree');
    await expect(lsmTreeCard).toBeVisible();

    // Verify Rust implementation card
    const rustCard = page.getByTestId('feature-card-rust-implementation');
    await expect(rustCard).toBeVisible();

    // Verify crash recovery card
    const crashRecoveryCard = page.getByTestId('feature-card-crash-recovery');
    await expect(crashRecoveryCard).toBeVisible();

    // Verify Memcached compatibility card
    const memcachedCard = page.getByTestId('feature-card-memcached-compatibility');
    await expect(memcachedCard).toBeVisible();

    console.log(`Features section verified on: ${browserName}`);
  });

  test('Quick Start section renders correctly', async ({ page, browserName }) => {
    const quickstartSection = page.getByTestId('quickstart-section');
    await expect(quickstartSection).toBeVisible();

    // Verify heading
    const heading = page.getByTestId('quickstart-heading');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Quick Start');

    // Verify install command
    const installCommand = page.getByTestId('quickstart-install-command');
    await expect(installCommand).toBeVisible();

    // Verify usage command
    const usageCommand = page.getByTestId('quickstart-usage-command');
    await expect(usageCommand).toBeVisible();

    // Verify steps
    const steps = page.locator('[data-testid^="quickstart-step-"]');
    await expect(steps).toHaveCount(3);

    console.log(`Quick Start section verified on: ${browserName}`);
  });

  test('Performance section renders correctly', async ({ page, browserName }) => {
    const performanceSection = page.getByTestId('performance-section');
    await expect(performanceSection).toBeVisible();

    // Verify metric cards
    const throughputCard = page.getByTestId('metric-card-throughput');
    await expect(throughputCard).toBeVisible();

    const latencyCard = page.getByTestId('metric-card-latency');
    await expect(latencyCard).toBeVisible();

    const memoryCard = page.getByTestId('metric-card-memory');
    await expect(memoryCard).toBeVisible();

    // Verify comparison table
    const comparisonTable = page.getByTestId('comparison-table');
    await expect(comparisonTable).toBeVisible();

    console.log(`Performance section verified on: ${browserName}`);
  });

  test('Documentation section renders correctly', async ({ page, browserName }) => {
    const docsSection = page.locator('#documentation');
    await expect(docsSection).toBeVisible();

    // Verify documentation links are present
    const docsLinks = docsSection.locator('a');
    const linkCount = await docsLinks.count();
    expect(linkCount).toBeGreaterThan(0);

    console.log(`Documentation section verified on: ${browserName}`);
  });

  test('Contributing section renders correctly', async ({ page, browserName }) => {
    const contributingSection = page.locator('#contributing');
    await expect(contributingSection).toBeVisible();

    // Verify GitHub link (uses contributing-link-github test ID)
    const githubLink = page.getByTestId('contributing-link-github');
    await expect(githubLink).toBeVisible();

    console.log(`Contributing section verified on: ${browserName}`);
  });

  test('Footer renders correctly', async ({ page, browserName }) => {
    const footer = page.getByTestId('footer-section');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Verify copyright
    const copyright = page.getByTestId('footer-copyright');
    await expect(copyright).toBeVisible();

    // Verify license link
    const licenseLink = page.getByTestId('footer-license-link');
    await expect(licenseLink).toBeVisible();

    // Verify social links
    const socialLinks = page.getByTestId('footer-social-links');
    await expect(socialLinks).toBeVisible();

    console.log(`Footer verified on: ${browserName}`);
  });

  test('CSS styles render consistently', async ({ page, browserName }) => {
    await page.setViewportSize({ width: 1280, height: 720 });

    // Verify CSS custom properties are applied
    const rootStyles = await page.evaluate(() => {
      const root = document.documentElement;
      const styles = getComputedStyle(root);
      return {
        primaryColor: styles.getPropertyValue('--color-primary').trim(),
        backgroundColor: styles.getPropertyValue('--color-background').trim(),
        fontFamily: styles.getPropertyValue('--font-family').trim(),
      };
    });

    // Verify CSS variables are defined
    expect(rootStyles.primaryColor).toBeTruthy();
    expect(rootStyles.backgroundColor).toBeTruthy();
    expect(rootStyles.fontFamily).toBeTruthy();

    console.log(`CSS styles verified on: ${browserName}`);
  });

  test('Responsive layout works correctly', async ({ page, browserName }) => {
    // Test mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });
    await page.waitForTimeout(100);

    const featuresGrid = page.getByTestId('features-grid');
    const mobileGridColumns = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns.split(' ').length;
    });
    expect(mobileGridColumns).toBe(1);

    // Test tablet viewport
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.waitForTimeout(100);

    // Test desktop viewport
    await page.setViewportSize({ width: 1280, height: 720 });
    await page.waitForTimeout(100);

    const desktopGridColumns = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns.split(' ').length;
    });
    expect(desktopGridColumns).toBeGreaterThanOrEqual(2);

    console.log(`Responsive layout verified on: ${browserName}`);
  });

  test('Hover states work correctly', async ({ page, browserName }) => {
    // Test CTA button hover
    const ctaButton = page.getByTestId('cta-button');
    await expect(ctaButton).toBeVisible();

    const initialBgColor = await ctaButton.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    await ctaButton.hover();
    await page.waitForTimeout(300);

    const hoverBgColor = await ctaButton.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Verify hover state changed the appearance
    expect(initialBgColor !== hoverBgColor).toBe(true);

    console.log(`Hover states verified on: ${browserName}`);
  });

  test('Keyboard navigation works correctly', async ({ page, browserName }) => {
    // Start from the page
    await page.keyboard.press('Tab');

    // Find focused element
    const focusedElement = await page.evaluate(() => {
      return document.activeElement?.tagName.toLowerCase();
    });

    // Verify something is focused
    expect(focusedElement).toBeTruthy();
    expect(['a', 'button', 'input']).toContain(focusedElement);

    // Tab through a few elements
    await page.keyboard.press('Tab');
    await page.keyboard.press('Tab');

    console.log(`Keyboard navigation verified on: ${browserName}`);
  });

  test('Links have correct attributes', async ({ page, browserName }) => {
    // Check external links have proper attributes
    const footer = page.getByTestId('footer-section');
    await footer.scrollIntoViewIfNeeded();

    const githubLink = page.getByTestId('footer-social-github');
    await expect(githubLink).toHaveAttribute('target', '_blank');
    const rel = await githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');

    console.log(`Link attributes verified on: ${browserName}`);
  });

  test('No visual layout issues', async ({ page, browserName }) => {
    await page.setViewportSize({ width: 1280, height: 720 });

    // Check that major sections are not overlapping
    const hero = page.getByTestId('hero-section');
    const features = page.locator('#features');

    const heroBox = await hero.boundingBox();
    const featuresBox = await features.boundingBox();

    expect(heroBox).toBeTruthy();
    expect(featuresBox).toBeTruthy();

    if (heroBox && featuresBox) {
      // Hero should end before features start (no overlap)
      expect(heroBox.y + heroBox.height).toBeLessThanOrEqual(featuresBox.y + 10);
    }

    // Check that page width doesn't exceed viewport (no horizontal scroll)
    const pageWidth = await page.evaluate(() => {
      return document.body.scrollWidth;
    });
    expect(pageWidth).toBeLessThanOrEqual(1280);

    console.log(`No visual layout issues on: ${browserName}`);
  });
});

/**
 * Test Case 5: Test copy-to-clipboard on all browsers
 * Clipboard functionality works on all supported browsers
 *
 * Note: Clipboard permissions work differently across browsers.
 * We test the button interaction and visual feedback rather than
 * actual clipboard content to ensure cross-browser compatibility.
 */
test.describe('Cross-Browser Clipboard Functionality', () => {
  test.beforeEach(async ({ page, context, browserName }) => {
    // Grant clipboard permissions only for Chromium-based browsers
    // Firefox and WebKit handle clipboard differently
    if (browserName === 'chromium') {
      try {
        await context.grantPermissions(['clipboard-read', 'clipboard-write']);
      } catch {
        // Ignore if permissions can't be granted
      }
    }
    await page.goto('/');
  });

  test('Copy button in Quick Start section works', async ({ page, browserName }) => {
    // Navigate to Quick Start section
    const quickstartSection = page.getByTestId('quickstart-section');
    await quickstartSection.scrollIntoViewIfNeeded();

    // Find and click copy button for install command
    const installCopyButton = page.getByTestId('quickstart-install-command-copy-button');
    await expect(installCopyButton).toBeVisible();

    // Click the copy button
    await installCopyButton.click();

    // Wait for state change
    await page.waitForTimeout(200);

    // Verify button shows copied state (visual feedback via aria-label "Copied!")
    await expect(installCopyButton).toHaveAttribute('aria-label', /Copied/i);

    // Verify the copied class is added (quickstart__copy-button--copied)
    await expect(installCopyButton).toHaveClass(/quickstart__copy-button--copied/);

    console.log(`Copy button in Quick Start works on: ${browserName}`);
  });

  test('Copy button visual feedback after copy', async ({ page, browserName }) => {
    const quickstartSection = page.getByTestId('quickstart-section');
    await quickstartSection.scrollIntoViewIfNeeded();

    const copyButton = page.getByTestId('quickstart-install-command-copy-button');
    await expect(copyButton).toBeVisible();

    // Click copy
    await copyButton.click();
    await page.waitForTimeout(200);

    // Verify aria-label changed to indicate copied
    const copiedAriaLabel = await copyButton.getAttribute('aria-label');
    expect(copiedAriaLabel?.toLowerCase()).toContain('copied');

    console.log(`Copy button visual feedback works on: ${browserName}`);
  });

  test('Multiple copy buttons work independently', async ({ page, browserName }) => {
    const quickstartSection = page.getByTestId('quickstart-section');
    await quickstartSection.scrollIntoViewIfNeeded();

    // Get all copy buttons
    const installCopyButton = page.getByTestId('quickstart-install-command-copy-button');
    const usageCopyButton = page.getByTestId('quickstart-usage-command-copy-button');
    const runCopyButton = page.getByTestId('quickstart-run-command-copy-button');

    await expect(installCopyButton).toBeVisible();
    await expect(usageCopyButton).toBeVisible();
    await expect(runCopyButton).toBeVisible();

    // Click install copy button
    await installCopyButton.click();
    await page.waitForTimeout(200);

    // Verify install button shows copied state
    await expect(installCopyButton).toHaveClass(/quickstart__copy-button--copied/);

    // Click usage copy button
    await usageCopyButton.click();
    await page.waitForTimeout(200);

    // Verify usage button shows copied state
    await expect(usageCopyButton).toHaveClass(/quickstart__copy-button--copied/);

    console.log(`Multiple copy buttons work independently on: ${browserName}`);
  });

  test('Copy button resets after timeout', async ({ page, browserName }) => {
    const quickstartSection = page.getByTestId('quickstart-section');
    await quickstartSection.scrollIntoViewIfNeeded();

    const copyButton = page.getByTestId('quickstart-install-command-copy-button');
    await expect(copyButton).toBeVisible();

    // Click copy
    await copyButton.click();
    await page.waitForTimeout(200);

    // Verify copied state
    await expect(copyButton).toHaveClass(/quickstart__copy-button--copied/);

    // Wait for reset (typically 2-3 seconds)
    await page.waitForTimeout(3000);

    // Verify button reset to original state
    const ariaLabel = await copyButton.getAttribute('aria-label');
    expect(ariaLabel?.toLowerCase()).toContain('copy');
    expect(ariaLabel?.toLowerCase()).not.toMatch(/^copied$/);

    console.log(`Copy button resets after timeout on: ${browserName}`);
  });

  test('Clipboard API integration works', async ({ page, browserName }) => {
    // This test verifies that the Clipboard API visual feedback works
    const quickstartSection = page.getByTestId('quickstart-section');
    await quickstartSection.scrollIntoViewIfNeeded();

    // Click the copy button
    const copyButton = page.getByTestId('quickstart-install-command-copy-button');
    await copyButton.click();
    await page.waitForTimeout(200);

    // Verify button shows success state
    await expect(copyButton).toHaveClass(/quickstart__copy-button--copied/);

    console.log(`Clipboard API integration verified on: ${browserName}`);
  });

  test('Copy button has proper accessibility', async ({ page, browserName }) => {
    const quickstartSection = page.getByTestId('quickstart-section');
    await quickstartSection.scrollIntoViewIfNeeded();

    const copyButtons = [
      page.getByTestId('quickstart-install-command-copy-button'),
      page.getByTestId('quickstart-usage-command-copy-button'),
      page.getByTestId('quickstart-run-command-copy-button'),
    ];

    for (const button of copyButtons) {
      await expect(button).toBeVisible();

      // Verify button has aria-label
      const ariaLabel = await button.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel?.toLowerCase()).toMatch(/copy/);

      // Verify button is a button element
      const tagName = await button.evaluate((el) => el.tagName.toLowerCase());
      expect(tagName).toBe('button');

      // Verify button is focusable
      await button.focus();
      const isFocused = await button.evaluate((el) => document.activeElement === el);
      expect(isFocused).toBe(true);
    }

    console.log(`Copy button accessibility verified on: ${browserName}`);
  });
});

/**
 * Additional cross-browser compatibility tests
 */
test.describe('Cross-Browser Animations and Transitions', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('CSS transitions work correctly', async ({ page, browserName }) => {
    const ctaButton = page.getByTestId('cta-button');
    await expect(ctaButton).toBeVisible();

    // Verify transition property is set
    const transitionProperty = await ctaButton.evaluate((el) => {
      return window.getComputedStyle(el).transitionProperty;
    });

    // Should have some transition defined
    expect(transitionProperty).toBeTruthy();
    expect(transitionProperty).not.toBe('none');

    console.log(`CSS transitions work on: ${browserName}`);
  });

  test('Transform animations work', async ({ page, browserName }) => {
    const featureCard = page.getByTestId('feature-card-lsm-tree');
    await expect(featureCard).toBeVisible();

    // Get initial transform
    const initialTransform = await featureCard.evaluate((el) => {
      return window.getComputedStyle(el).transform;
    });

    // Hover over the card
    await featureCard.hover();
    await page.waitForTimeout(300);

    // Get hover transform
    const hoverTransform = await featureCard.evaluate((el) => {
      return window.getComputedStyle(el).transform;
    });

    // Verify transform changed on hover
    expect(initialTransform !== hoverTransform).toBe(true);

    console.log(`Transform animations work on: ${browserName}`);
  });
});

/**
 * Cross-browser font rendering tests
 */
test.describe('Cross-Browser Font Rendering', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Fonts render correctly', async ({ page, browserName }) => {
    // Verify font family is applied
    const heroTitle = page.getByTestId('hero-title');
    const fontFamily = await heroTitle.evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });

    // Font family should be defined
    expect(fontFamily).toBeTruthy();

    // Verify monospace font for code
    const commandCode = page.getByTestId('quickstart-install-command-code');
    const codeFontFamily = await commandCode.evaluate((el) => {
      return window.getComputedStyle(el).fontFamily.toLowerCase();
    });

    // Should use a monospace font
    expect(codeFontFamily).toMatch(/mono|consolas|courier|fira|menlo|monaco/);

    console.log(`Fonts render correctly on: ${browserName}`);
  });
});

/**
 * Cross-browser scroll behavior tests
 */
test.describe('Cross-Browser Scroll Behavior', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Page scrolls correctly to all sections', async ({ page, browserName }) => {
    // Scroll to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Scroll to performance section
    const performanceSection = page.getByTestId('performance-section');
    await performanceSection.scrollIntoViewIfNeeded();
    await expect(performanceSection).toBeVisible();

    // Scroll to footer
    const footer = page.getByTestId('footer-section');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    console.log(`Page scrolls correctly on: ${browserName}`);
  });
});
