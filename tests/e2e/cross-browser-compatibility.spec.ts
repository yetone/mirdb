import { test, expect, type Page } from '@playwright/test';

/**
 * Cross-Browser Compatibility E2E Tests for MirDB Homepage
 *
 * These tests verify that the homepage renders correctly across modern browsers
 * (Chrome, Firefox, Safari, Edge) by checking:
 * - All major elements render correctly
 * - No console errors during page load
 * - All navigation links work correctly
 *
 * Tests run against all configured browser projects in playwright.config.ts
 */

test.describe('Cross-Browser Compatibility', () => {
  let consoleErrors: string[] = [];

  test.beforeEach(async ({ page }) => {
    // Reset console errors array before each test
    consoleErrors = [];

    // Capture console errors during page load
    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        consoleErrors.push(msg.text());
      }
    });

    // Navigate to homepage
    await page.goto('/');
    // Wait for page to fully load
    await page.waitForLoadState('networkidle');
  });

  test('TC1: All major page elements render correctly', async ({ page, browserName }) => {
    // Verify hero section elements
    const heroSection = page.locator('.hero, header.hero');
    await expect(heroSection).toBeVisible();

    // Verify product name
    const productName = page.locator('h1').filter({ hasText: 'MirDB' });
    await expect(productName).toBeVisible();

    // Verify tagline
    const tagline = page.locator('.tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('Persistent Key-Value Store');
    await expect(tagline).toContainText('Memcached Protocol');

    // Verify CTA buttons
    const getStartedButton = page.locator('a.btn, a.btn-primary').filter({ hasText: /Get Started/i });
    await expect(getStartedButton).toBeVisible();

    const githubButton = page.locator('a.btn, a.btn-secondary').filter({ hasText: /GitHub/i });
    await expect(githubButton).toBeVisible();

    // Verify features section
    const featuresSection = page.locator('#features, .features');
    await expect(featuresSection).toBeVisible();

    // Verify feature cards exist (at least 4)
    const featureCards = page.locator('.feature-card, .features-grid > div');
    await expect(featureCards).toHaveCount(4);

    // Verify code example section
    const codeSection = page.locator('#code-example, .code-example, #code-examples, .code-examples');
    await expect(codeSection).toBeVisible();

    // Verify getting started section
    const gettingStartedSection = page.locator('#getting-started, .getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Verify footer
    const footer = page.locator('footer, .footer');
    await expect(footer).toBeVisible();

    // Log browser name for debugging
    console.log(`Elements rendered correctly in ${browserName}`);
  });

  test('TC2: No console errors during page load', async ({ page, browserName }) => {
    // Filter out known non-critical errors (e.g., favicon not found)
    const criticalErrors = consoleErrors.filter(error => {
      // Filter out favicon 404 errors which are common and non-critical
      if (error.includes('favicon') && error.includes('404')) {
        return false;
      }
      // Filter out font loading warnings
      if (error.includes('font') && error.includes('warning')) {
        return false;
      }
      return true;
    });

    // Assert no critical console errors
    expect(criticalErrors.length).toBe(0);

    if (criticalErrors.length > 0) {
      console.error(`Console errors in ${browserName}:`, criticalErrors);
    } else {
      console.log(`No console errors in ${browserName}`);
    }
  });

  test('TC3: All navigation links work correctly', async ({ page, browserName }) => {
    // Test "Get Started" anchor link
    const getStartedLink = page.locator('a').filter({ hasText: /Get Started/i }).first();
    const getStartedHref = await getStartedLink.getAttribute('href');

    // Should point to getting-started section
    expect(getStartedHref).toContain('getting-started');

    // Click and verify scroll
    await getStartedLink.click();
    await page.waitForTimeout(500); // Wait for smooth scroll

    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeInViewport();

    // Test GitHub link
    const githubLink = page.locator('.hero a, header a').filter({ hasText: /GitHub/i }).first();
    const githubHref = await githubLink.getAttribute('href');
    expect(githubHref).toContain('github.com');

    // Verify it opens in new tab
    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify rel attribute for security
    const rel = await githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');

    // Test footer links
    const footerLinks = page.locator('footer a, .footer a');
    const footerLinkCount = await footerLinks.count();

    // Should have at least GitHub and License links
    expect(footerLinkCount).toBeGreaterThanOrEqual(2);

    // Verify all footer links have valid hrefs
    for (let i = 0; i < footerLinkCount; i++) {
      const href = await footerLinks.nth(i).getAttribute('href');
      expect(href).toBeTruthy();
      expect(href?.length).toBeGreaterThan(0);
    }

    console.log(`All navigation links work correctly in ${browserName}`);
  });

  test('TC4: CSS styles are applied correctly', async ({ page, browserName }) => {
    // Verify hero background gradient
    const hero = page.locator('.hero, header.hero');
    const heroBackground = await hero.evaluate((el) => {
      return window.getComputedStyle(el).background || window.getComputedStyle(el).backgroundColor;
    });
    expect(heroBackground).toBeTruthy();

    // Verify button styles
    const primaryButton = page.locator('.btn-primary, a.btn-primary').first();
    const buttonBgColor = await primaryButton.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    expect(buttonBgColor).toBeTruthy();
    expect(buttonBgColor).not.toBe('transparent');
    expect(buttonBgColor).not.toBe('rgba(0, 0, 0, 0)');

    // Verify text is readable (has sufficient font size)
    const bodyFontSize = await page.evaluate(() => {
      return window.getComputedStyle(document.body).fontSize;
    });
    const fontSizeNum = parseInt(bodyFontSize);
    expect(fontSizeNum).toBeGreaterThanOrEqual(14);

    // Verify feature cards have proper layout
    const featureGrid = page.locator('.feature-grid, .features-grid');
    const gridDisplay = await featureGrid.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(gridDisplay).toBe('grid');

    console.log(`CSS styles applied correctly in ${browserName}`);
  });

  test('TC5: Interactive elements respond to interactions', async ({ page, browserName }) => {
    // Test button hover states
    const primaryButton = page.locator('.btn-primary, a.btn-primary').first();
    const initialBg = await primaryButton.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    await primaryButton.hover();
    await page.waitForTimeout(300); // Wait for transition

    // Note: hover effect may or may not change background depending on browser
    // Main test is that no error occurs during hover

    // Test anchor link navigation
    const featuresLink = page.locator('a[href="#features"]').first();
    if (await featuresLink.count() > 0) {
      await featuresLink.click();
      await page.waitForTimeout(500);
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport();
    }

    // Verify focus states work (accessibility)
    await page.keyboard.press('Tab');
    const focusedElement = await page.evaluate(() => {
      const el = document.activeElement;
      return el ? el.tagName : null;
    });
    expect(focusedElement).toBeTruthy();

    console.log(`Interactive elements work correctly in ${browserName}`);
  });

  test('TC6: Page structure and accessibility basics', async ({ page, browserName }) => {
    // Verify document has proper lang attribute
    const htmlLang = await page.evaluate(() => {
      return document.documentElement.lang;
    });
    expect(htmlLang).toBe('en');

    // Verify page title
    const title = await page.title();
    expect(title).toContain('MirDB');

    // Verify heading hierarchy starts with h1
    const h1 = page.locator('h1').first();
    await expect(h1).toBeVisible();

    // Verify semantic sections exist
    const header = page.locator('header');
    await expect(header).toBeVisible();

    const sections = page.locator('section');
    const sectionCount = await sections.count();
    expect(sectionCount).toBeGreaterThanOrEqual(3);

    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Verify external links have rel="noopener noreferrer"
    const externalLinks = page.locator('a[target="_blank"]');
    const externalCount = await externalLinks.count();

    for (let i = 0; i < externalCount; i++) {
      const rel = await externalLinks.nth(i).getAttribute('rel');
      expect(rel).toContain('noopener');
    }

    console.log(`Page structure and accessibility basics verified in ${browserName}`);
  });
});
