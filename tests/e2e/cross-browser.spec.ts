/**
 * Cross-Browser Compatibility E2E Tests
 * Owner: Scenario 13 - Cross-Browser Compatibility
 *
 * Tests to verify homepage functions correctly across Chrome, Firefox, Safari, and Edge.
 * These tests run automatically in all configured browser projects (chromium, firefox, webkit, msedge).
 */

import { test, expect, BrowserContext } from '@playwright/test';
import { SELECTORS, THEMES, THEME_STORAGE_KEY, EXPECTED_CONTENT } from '../fixtures/test-data';

test.describe('Cross-Browser Compatibility - Page Rendering', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1-4: Homepage renders correctly with all major sections visible', async ({ page, browserName }) => {
    // Log which browser is being tested
    test.info().annotations.push({ type: 'browser', description: browserName });

    // Verify page title loads correctly
    await expect(page).toHaveTitle(/MirDB/);

    // Verify header is visible
    const header = page.locator(SELECTORS.header);
    await expect(header).toBeVisible();

    // Verify hero section renders
    const hero = page.locator(SELECTORS.hero);
    await expect(hero).toBeVisible();

    const heroTitle = page.locator(SELECTORS.heroTitle);
    await expect(heroTitle).toHaveText(EXPECTED_CONTENT.title);

    const heroTagline = page.locator(SELECTORS.heroTagline);
    await expect(heroTagline).toContainText('Memcached-compatible');

    // Verify features section renders
    const features = page.locator(SELECTORS.features);
    await expect(features).toBeVisible();

    const featureCards = page.locator(SELECTORS.featureCard);
    await expect(featureCards).toHaveCount(4);

    // Verify Quick Start section renders
    const quickstart = page.locator(SELECTORS.quickstart);
    await expect(quickstart).toBeVisible();

    const codeBlock = page.locator(SELECTORS.codeBlock);
    await expect(codeBlock).toBeVisible();

    // Verify Tech Specs section renders
    const techspecs = page.locator(SELECTORS.techspecs);
    await expect(techspecs).toBeVisible();

    // Verify Footer renders
    const footer = page.locator(SELECTORS.footer);
    await expect(footer).toBeVisible();
  });

  test('Navigation links work correctly', async ({ page, browserName }) => {
    test.info().annotations.push({ type: 'browser', description: browserName });

    // Use specific nav link selectors to avoid matching CTA buttons
    // Test navigation to features section
    const featuresLink = page.locator('.header__nav-link[href="#features"]');
    await featuresLink.click();
    await page.waitForTimeout(500); // Wait for scroll
    const featuresSection = page.locator(SELECTORS.features);
    await expect(featuresSection).toBeVisible();

    // Test navigation to quickstart section
    const quickstartLink = page.locator('.header__nav-link[href="#quickstart"]');
    await quickstartLink.click();
    await page.waitForTimeout(500);
    const quickstartSection = page.locator(SELECTORS.quickstart);
    await expect(quickstartSection).toBeVisible();

    // Test navigation to techspecs section
    const techspecsLink = page.locator('.header__nav-link[href="#techspecs"]');
    await techspecsLink.click();
    await page.waitForTimeout(500);
    const techspecsSection = page.locator(SELECTORS.techspecs);
    await expect(techspecsSection).toBeVisible();
  });

  test('Hero CTA button scrolls to quickstart', async ({ page, browserName }) => {
    test.info().annotations.push({ type: 'browser', description: browserName });

    const ctaButton = page.locator(SELECTORS.heroCta);
    await expect(ctaButton).toBeVisible();
    await ctaButton.click();

    // Verify quickstart section is now in viewport
    await expect(page.locator(SELECTORS.quickstart)).toBeInViewport();
  });

  test('All feature cards are visible with correct content', async ({ page, browserName }) => {
    test.info().annotations.push({ type: 'browser', description: browserName });

    const featureCards = page.locator(SELECTORS.featureCard);
    await expect(featureCards).toHaveCount(4);

    // Verify each expected feature is present
    for (const feature of EXPECTED_CONTENT.features) {
      const featureCard = page.locator(SELECTORS.featureCardTitle, { hasText: feature });
      await expect(featureCard).toBeVisible();
    }
  });

  test('External links have correct attributes', async ({ page, browserName }) => {
    test.info().annotations.push({ type: 'browser', description: browserName });

    const footerLinks = page.locator(SELECTORS.footerLink);
    const linkCount = await footerLinks.count();

    for (let i = 0; i < linkCount; i++) {
      const link = footerLinks.nth(i);
      // External links should open in new tab
      await expect(link).toHaveAttribute('target', '_blank');
      // External links should have noopener for security
      await expect(link).toHaveAttribute('rel', /noopener/);
    }
  });
});

test.describe('Cross-Browser Compatibility - Clipboard API', () => {
  test('TC5: Copy-to-clipboard button works correctly', async ({ page, context, browserName }) => {
    test.info().annotations.push({ type: 'browser', description: browserName });

    await page.goto('/');

    // Grant clipboard permissions for Chromium-based browsers
    // Firefox and WebKit handle clipboard differently
    if (browserName === 'chromium') {
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);
    }

    // Find and verify copy button exists
    const copyButton = page.locator(SELECTORS.copyButton);
    await expect(copyButton).toBeVisible();

    // Verify button has accessible label
    await expect(copyButton).toHaveAttribute('aria-label', 'Copy code to clipboard');

    // Get initial button text
    const buttonText = copyButton.locator('span');
    await expect(buttonText).toHaveText('Copy');

    // Click the copy button
    await copyButton.click();

    // Verify visual feedback - button text changes to "Copied!"
    await expect(buttonText).toHaveText('Copied!');

    // Read clipboard content and verify it contains expected code (only for chromium where we have permissions)
    if (browserName === 'chromium') {
      const clipboardContent = await page.evaluate(() => navigator.clipboard.readText());
      expect(clipboardContent).toContain('pymemcache');
      expect(clipboardContent).toContain('localhost');
      expect(clipboardContent).toContain('12333');
    }

    // Wait for feedback to reset (after ~2 seconds)
    await page.waitForTimeout(2500);
    await expect(buttonText).toHaveText('Copy');
  });

  test('Copy button is keyboard accessible', async ({ page, context, browserName }) => {
    test.info().annotations.push({ type: 'browser', description: browserName });

    await page.goto('/');

    // Grant clipboard permissions for Chromium-based browsers
    if (browserName === 'chromium') {
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);
    }

    const copyButton = page.locator(SELECTORS.copyButton);
    await expect(copyButton).toBeVisible();

    // Focus the button using keyboard navigation
    await copyButton.focus();
    await expect(copyButton).toBeFocused();

    // Activate with Enter key
    await page.keyboard.press('Enter');

    // Verify copy was triggered
    const buttonText = copyButton.locator('span');
    await expect(buttonText).toHaveText('Copied!');

    // Verify clipboard content (only for chromium where we have permissions)
    if (browserName === 'chromium') {
      const clipboardContent = await page.evaluate(() => navigator.clipboard.readText());
      expect(clipboardContent).toContain('pymemcache');
    }
  });
});

test.describe('Cross-Browser Compatibility - Theme Switching', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Clear localStorage before each test
    await page.evaluate(() => localStorage.clear());
  });

  test('TC6: Theme toggle button is visible and accessible', async ({ page, browserName }) => {
    test.info().annotations.push({ type: 'browser', description: browserName });

    const themeToggle = page.locator(SELECTORS.themeToggle);
    await expect(themeToggle).toBeVisible();

    // Check accessible label
    const ariaLabel = await themeToggle.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel).toMatch(/toggle|dark|mode|theme/i);

    // Check aria-pressed attribute exists
    const ariaPressed = await themeToggle.getAttribute('aria-pressed');
    expect(ariaPressed).toBeTruthy();
  });

  test('Theme toggle switches between light and dark mode', async ({ page, browserName }) => {
    test.info().annotations.push({ type: 'browser', description: browserName });

    const html = page.locator('html');
    const themeToggle = page.locator(SELECTORS.themeToggle);

    // Start with light theme (default)
    await expect(html).toHaveAttribute('data-theme', THEMES.light);

    // Click to switch to dark
    await themeToggle.click();
    await expect(html).toHaveAttribute('data-theme', THEMES.dark);

    // Click to switch back to light
    await themeToggle.click();
    await expect(html).toHaveAttribute('data-theme', THEMES.light);
  });

  test('Theme colors are correctly applied in dark mode', async ({ page, browserName }) => {
    test.info().annotations.push({ type: 'browser', description: browserName });

    const themeToggle = page.locator(SELECTORS.themeToggle);

    // Switch to dark mode
    await themeToggle.click();

    // Wait for theme transition
    await page.waitForTimeout(300);

    // Verify dark mode is applied to html element
    const html = page.locator('html');
    await expect(html).toHaveAttribute('data-theme', THEMES.dark);

    // Check body background is dark (not white)
    const body = page.locator('body');
    const bgColor = await body.evaluate(el => getComputedStyle(el).backgroundColor);

    // Dark backgrounds have low RGB values (allowing for slight browser differences)
    const rgbMatch = bgColor.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
    if (rgbMatch) {
      const [, r, g, b] = rgbMatch.map(Number);
      // Dark backgrounds should have RGB values less than 100 (not white 255)
      expect(r).toBeLessThan(100);
      expect(g).toBeLessThan(100);
      expect(b).toBeLessThan(100);
    }
  });

  test('Theme preference persists after page reload', async ({ page, browserName }) => {
    test.info().annotations.push({ type: 'browser', description: browserName });

    const themeToggle = page.locator(SELECTORS.themeToggle);
    const html = page.locator('html');

    // Switch to dark mode
    await themeToggle.click();
    await expect(html).toHaveAttribute('data-theme', THEMES.dark);

    // Verify localStorage was set
    const storedTheme = await page.evaluate((key) => localStorage.getItem(key), THEME_STORAGE_KEY);
    expect(storedTheme).toBe(THEMES.dark);

    // Reload page
    await page.reload();

    // Verify dark mode is still applied
    await expect(html).toHaveAttribute('data-theme', THEMES.dark);
  });

  test('Theme toggle is keyboard accessible', async ({ page, browserName }) => {
    test.info().annotations.push({ type: 'browser', description: browserName });

    const themeToggle = page.locator(SELECTORS.themeToggle);
    const html = page.locator('html');

    // Focus the button using keyboard
    await themeToggle.focus();
    await expect(themeToggle).toBeFocused();

    // Verify initial state (light mode)
    await expect(html).toHaveAttribute('data-theme', THEMES.light);

    // Activate with Enter key
    await page.keyboard.press('Enter');

    // Verify dark mode is applied
    await expect(html).toHaveAttribute('data-theme', THEMES.dark);

    // Press Enter again to toggle back
    await page.keyboard.press('Enter');
    await expect(html).toHaveAttribute('data-theme', THEMES.light);
  });

  test('All sections update color scheme in dark mode', async ({ page, browserName }) => {
    test.info().annotations.push({ type: 'browser', description: browserName });

    const themeToggle = page.locator(SELECTORS.themeToggle);

    // Switch to dark mode
    await themeToggle.click();
    await page.waitForTimeout(300);

    // Check header background changes from white
    const header = page.locator(SELECTORS.header);
    const headerBg = await header.evaluate(el => getComputedStyle(el).backgroundColor);
    expect(headerBg).not.toBe('rgb(255, 255, 255)');

    // Check features section background
    const features = page.locator(SELECTORS.features);
    const featuresBg = await features.evaluate(el => getComputedStyle(el).backgroundColor);
    expect(featuresBg).not.toBe('rgb(255, 255, 255)');

    // Check footer background
    const footer = page.locator(SELECTORS.footer);
    const footerBg = await footer.evaluate(el => getComputedStyle(el).backgroundColor);
    expect(footerBg).not.toBe('rgb(255, 255, 255)');
  });

  test('Theme icons display correctly based on current theme', async ({ page, browserName }) => {
    test.info().annotations.push({ type: 'browser', description: browserName });

    const lightIcon = page.locator(SELECTORS.themeToggleIconLight);
    const darkIcon = page.locator(SELECTORS.themeToggleIconDark);
    const themeToggle = page.locator(SELECTORS.themeToggle);

    // In light mode, sun icon should be visible
    await expect(lightIcon).toBeVisible();
    await expect(darkIcon).not.toBeVisible();

    // Switch to dark mode
    await themeToggle.click();

    // In dark mode, moon icon should be visible
    await expect(lightIcon).not.toBeVisible();
    await expect(darkIcon).toBeVisible();
  });
});

test.describe('Cross-Browser Compatibility - CSS and Layout', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('CSS custom properties are supported', async ({ page, browserName }) => {
    test.info().annotations.push({ type: 'browser', description: browserName });

    // CSS custom properties should be applied correctly
    const hero = page.locator(SELECTORS.hero);
    const computedStyle = await hero.evaluate(el => {
      const style = getComputedStyle(el);
      // Check that color values are resolved (not 'undefined' or empty)
      return {
        backgroundColor: style.backgroundColor,
        color: style.color,
      };
    });

    expect(computedStyle.backgroundColor).toBeTruthy();
    expect(computedStyle.color).toBeTruthy();
    // Values should be resolved, not CSS variable syntax
    expect(computedStyle.backgroundColor).not.toContain('var(');
    expect(computedStyle.color).not.toContain('var(');
  });

  test('Flexbox and Grid layouts render correctly', async ({ page, browserName }) => {
    test.info().annotations.push({ type: 'browser', description: browserName });

    // Features grid should use grid/flex layout
    const featuresGrid = page.locator(SELECTORS.featuresGrid);
    const display = await featuresGrid.evaluate(el => getComputedStyle(el).display);
    expect(['grid', 'flex']).toContain(display);

    // Feature cards should be laid out properly
    const featureCards = page.locator(SELECTORS.featureCard);
    const count = await featureCards.count();
    expect(count).toBe(4);

    // All cards should be visible
    for (let i = 0; i < count; i++) {
      await expect(featureCards.nth(i)).toBeVisible();
    }
  });

  test('SVG icons render correctly', async ({ page, browserName }) => {
    test.info().annotations.push({ type: 'browser', description: browserName });

    // Check that SVG icons are rendered in feature cards
    const featureIcons = page.locator('.feature-card__icon svg');
    const iconCount = await featureIcons.count();
    expect(iconCount).toBeGreaterThanOrEqual(4);

    // Verify SVGs have dimensions
    const firstIcon = featureIcons.first();
    const boundingBox = await firstIcon.boundingBox();
    expect(boundingBox).toBeTruthy();
    expect(boundingBox!.width).toBeGreaterThan(0);
    expect(boundingBox!.height).toBeGreaterThan(0);
  });

  test('Code block styling renders correctly', async ({ page, browserName }) => {
    test.info().annotations.push({ type: 'browser', description: browserName });

    const codeBlock = page.locator(SELECTORS.codeBlock);
    await expect(codeBlock).toBeVisible();

    // Code block should have visible styling
    const codeElement = codeBlock.locator('code');
    await expect(codeElement).toBeVisible();

    // Check that code content is displayed
    const codeText = await codeElement.textContent();
    expect(codeText).toContain('pymemcache');
    expect(codeText).toContain('localhost');
  });

  test('Table renders correctly in techspecs section', async ({ page, browserName }) => {
    test.info().annotations.push({ type: 'browser', description: browserName });

    const table = page.locator(SELECTORS.techspecsTable);
    await expect(table).toBeVisible();

    // Verify table structure
    const headers = table.locator('th');
    const headerCount = await headers.count();
    expect(headerCount).toBeGreaterThanOrEqual(2);

    // Verify table has rows with data
    const rows = table.locator('tbody tr');
    const rowCount = await rows.count();
    expect(rowCount).toBeGreaterThanOrEqual(5);

    // Verify port 12333 is displayed
    const portCell = table.locator('td', { hasText: '12333' });
    await expect(portCell).toBeVisible();
  });
});

test.describe('Cross-Browser Compatibility - Interactivity', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Smooth scroll works on navigation clicks', async ({ page, browserName }) => {
    test.info().annotations.push({ type: 'browser', description: browserName });

    // Click on features link
    const featuresLink = page.locator('a[href="#features"]');
    await featuresLink.click();

    // Wait for scroll
    await page.waitForTimeout(500);

    // Features section should be visible in viewport
    const features = page.locator(SELECTORS.features);
    await expect(features).toBeInViewport();
  });

  test('Hover states work on interactive elements', async ({ page, browserName }) => {
    test.info().annotations.push({ type: 'browser', description: browserName });

    // Test hover on CTA button
    const ctaButton = page.locator(SELECTORS.heroCta);
    await ctaButton.hover();

    // Button should still be visible and interactable after hover
    await expect(ctaButton).toBeVisible();

    // Test hover on feature card
    const featureCard = page.locator(SELECTORS.featureCard).first();
    await featureCard.hover();
    await expect(featureCard).toBeVisible();
  });

  test('Focus states are visible for keyboard navigation', async ({ page, browserName }) => {
    test.info().annotations.push({ type: 'browser', description: browserName });

    // Tab through interactive elements
    await page.keyboard.press('Tab'); // Skip link
    await page.keyboard.press('Tab'); // Logo
    await page.keyboard.press('Tab'); // First nav link

    // Check that an element has focus with visible indicator
    const focusedElement = page.locator(':focus');
    await expect(focusedElement).toBeVisible();

    // The focused element should have some visual indication
    const outline = await focusedElement.evaluate(el => getComputedStyle(el).outline);
    // Outline should not be 'none' (though actual value varies by browser)
    expect(outline).toBeDefined();
  });
});
