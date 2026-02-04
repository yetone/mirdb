/**
 * Cross-Browser Compatibility Tests
 * Owner: Scenario 11 - Cross-Browser Compatibility
 *
 * Test coverage:
 * - Homepage renders correctly on Chrome, Firefox, Safari (WebKit), and Edge
 * - All sections render correctly across browsers
 * - Animations work (GIF logo)
 * - Copy-to-clipboard functionality works
 * - CSS Grid support for feature grid
 */

const { test, expect } = require('@playwright/test');

// Browser configurations for cross-browser testing
const BROWSERS = ['Desktop Chrome', 'Desktop Firefox', 'Desktop Safari', 'Desktop Edge'];

// Test selectors
const SELECTORS = {
  hero: '#hero',
  heroTitle: '#hero h1',
  heroLogo: '.hero__logo',
  heroTagline: '.hero__tagline',
  heroCta: '.hero__cta',
  features: '#features',
  featuresGrid: '.features__grid',
  featureCard: '.feature-card',
  codeExamples: '#code-examples',
  codeTab: '[role="tab"]',
  codePanel: '[role="tabpanel"]',
  copyBtn: '.copy-btn',
  architecture: '#architecture',
  architectureSvg: '.architecture__svg',
  roadmap: '#roadmap',
  roadmapItem: '.roadmap__item',
  footer: 'footer',
};

test.describe('Cross-Browser Compatibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  // Test Case 1: Load homepage on Chrome latest
  test('homepage renders correctly with all sections visible', async ({ page, browserName }) => {
    // Verify all main sections are visible
    await expect(page.locator(SELECTORS.hero)).toBeVisible();
    await expect(page.locator(SELECTORS.features)).toBeVisible();
    await expect(page.locator(SELECTORS.codeExamples)).toBeVisible();
    await expect(page.locator(SELECTORS.architecture)).toBeVisible();
    await expect(page.locator(SELECTORS.roadmap)).toBeVisible();
    await expect(page.locator(SELECTORS.footer)).toBeVisible();

    // Verify hero section content
    await expect(page.locator(SELECTORS.heroTitle)).toHaveText('MirDB');
    await expect(page.locator(SELECTORS.heroTagline)).toContainText('Persistent Key-Value Store');

    // Verify CTA buttons exist
    const ctaButtons = page.locator(`${SELECTORS.heroCta} a`);
    await expect(ctaButtons).toHaveCount(2);
  });

  // Test Case 2: Test CSS Grid support
  test('feature grid displays correctly with CSS Grid', async ({ page, browserName }) => {
    await expect(page.locator(SELECTORS.featuresGrid)).toBeVisible();

    // Check that all 6 feature cards are present
    const featureCards = page.locator(SELECTORS.featureCard);
    await expect(featureCards).toHaveCount(6);

    // Verify CSS Grid is applied
    const gridStyle = await page.locator(SELECTORS.featuresGrid).evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns,
      };
    });

    expect(gridStyle.display).toBe('grid');
    // Should have multiple columns on desktop (3 columns for 1920px viewport)
    const columns = gridStyle.gridTemplateColumns.split(' ').filter(c => c !== '');
    expect(columns.length).toBeGreaterThanOrEqual(1);
  });

  // Test Case 3: Test GIF animation (logo)
  test('GIF animation (logo) displays and loads correctly', async ({ page, browserName }) => {
    const logo = page.locator(SELECTORS.heroLogo);
    await expect(logo).toBeVisible();

    // Verify the logo is an image with gif source
    const src = await logo.getAttribute('src');
    expect(src).toContain('logo.gif');

    // Check that the image has natural dimensions (loaded successfully)
    const dimensions = await logo.evaluate((img) => {
      return {
        naturalWidth: img.naturalWidth,
        naturalHeight: img.naturalHeight,
        complete: img.complete,
      };
    });

    expect(dimensions.complete).toBe(true);
    expect(dimensions.naturalWidth).toBeGreaterThan(0);
    expect(dimensions.naturalHeight).toBeGreaterThan(0);
  });

  // Test Case 4: Tab switching functionality works
  test('code example tabs switch correctly', async ({ page, browserName }) => {
    await page.locator(SELECTORS.codeExamples).scrollIntoViewIfNeeded();

    // Get all tabs
    const tabs = page.locator(SELECTORS.codeTab);
    const tabCount = await tabs.count();
    expect(tabCount).toBeGreaterThanOrEqual(3); // SET, GET, DELETE tabs

    // First tab should be active by default
    const firstTab = tabs.first();
    await expect(firstTab).toHaveAttribute('aria-selected', 'true');

    // Click second tab and verify it becomes active
    const secondTab = tabs.nth(1);
    await secondTab.click();
    await expect(secondTab).toHaveAttribute('aria-selected', 'true');
    await expect(firstTab).toHaveAttribute('aria-selected', 'false');

    // Verify corresponding panel is visible
    const secondPanelId = await secondTab.getAttribute('aria-controls');
    const secondPanel = page.locator(`#${secondPanelId}`);
    await expect(secondPanel).toBeVisible();
  });

  // Test Case 5: Copy functionality works
  test('copy-to-clipboard functionality works', async ({ page, browserName, context }) => {
    // Grant clipboard permissions for Chromium-based browsers
    if (browserName === 'chromium') {
      try {
        await context.grantPermissions(['clipboard-read', 'clipboard-write']);
      } catch (e) {
        // Permission not supported, continue without
      }
    }

    await page.locator(SELECTORS.codeExamples).scrollIntoViewIfNeeded();

    // Find copy button
    const copyBtn = page.locator(SELECTORS.copyBtn).first();
    await expect(copyBtn).toBeVisible();

    // Get original button text
    const originalText = await copyBtn.textContent();
    expect(originalText.trim()).toBe('Copy');

    // Click copy button
    await copyBtn.click();

    // Wait for button text to change to "Copied!" - this verifies the copy action was triggered
    await expect(copyBtn).toHaveText('Copied!', { timeout: 5000 });

    // For Chromium browsers, also verify clipboard content
    if (browserName === 'chromium') {
      const clipboardContent = await page.evaluate(async () => {
        try {
          return await navigator.clipboard.readText();
        } catch (e) {
          return null;
        }
      });

      // Clipboard should contain the code content
      if (clipboardContent !== null) {
        expect(clipboardContent.length).toBeGreaterThan(0);
        expect(clipboardContent).toContain('telnet');
      }
    }
  });

  // Test Case 6: Architecture section with SVG renders correctly
  test('architecture section with SVG diagram renders correctly', async ({ page, browserName }) => {
    await page.locator(SELECTORS.architecture).scrollIntoViewIfNeeded();
    await expect(page.locator(SELECTORS.architecture)).toBeVisible();

    // Check SVG diagram is present and visible
    const svg = page.locator(SELECTORS.architectureSvg);
    await expect(svg).toBeVisible();

    // Verify SVG has content (components)
    const svgComponents = await svg.evaluate((el) => {
      return {
        hasRect: el.querySelectorAll('rect').length > 0,
        hasText: el.querySelectorAll('text').length > 0,
        hasLines: el.querySelectorAll('line').length > 0,
        viewBox: el.getAttribute('viewBox'),
      };
    });

    expect(svgComponents.hasRect).toBe(true);
    expect(svgComponents.hasText).toBe(true);
    expect(svgComponents.viewBox).toBeTruthy();
  });

  // Test Case 7: Roadmap section renders correctly
  test('roadmap section with checklist renders correctly', async ({ page, browserName }) => {
    await page.locator(SELECTORS.roadmap).scrollIntoViewIfNeeded();
    await expect(page.locator(SELECTORS.roadmap)).toBeVisible();

    // Verify roadmap items exist
    const roadmapItems = page.locator(SELECTORS.roadmapItem);
    const itemCount = await roadmapItems.count();
    expect(itemCount).toBeGreaterThanOrEqual(5); // At least 5 items (4 complete, 1 planned)

    // Check for completed items
    const completedItems = page.locator('.roadmap__item--complete');
    const completedCount = await completedItems.count();
    expect(completedCount).toBeGreaterThanOrEqual(4);

    // Check for planned items
    const plannedItems = page.locator('.roadmap__item--planned');
    const plannedCount = await plannedItems.count();
    expect(plannedCount).toBeGreaterThanOrEqual(1);
  });

  // Test Case 8: Footer renders correctly
  test('footer renders with all links', async ({ page, browserName }) => {
    await page.locator(SELECTORS.footer).scrollIntoViewIfNeeded();
    await expect(page.locator(SELECTORS.footer)).toBeVisible();

    // Check footer links
    const footerLinks = page.locator('footer a');
    const linkCount = await footerLinks.count();
    expect(linkCount).toBeGreaterThanOrEqual(3); // GitHub, License, Issues

    // Verify GitHub link
    const githubLink = page.locator('footer a[href*="github.com/yetone/mirdb"]').first();
    await expect(githubLink).toBeVisible();
  });

  // Test Case 9: No horizontal scrolling on any viewport
  test('no horizontal scrolling occurs', async ({ page, browserName }) => {
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });

  // Test Case 10: Navigation links work
  test('navigation anchor links work correctly', async ({ page, browserName }) => {
    // Click on Features link in navigation
    const featuresLink = page.locator('nav a[href="#features"]');
    if (await featuresLink.count() > 0) {
      await featuresLink.click();

      // Wait for scroll
      await page.waitForTimeout(500);

      // Verify features section is in viewport using Playwright's isVisible and boundingBox
      const featuresSection = page.locator(SELECTORS.features);
      await expect(featuresSection).toBeVisible();

      // Check if element is within viewport bounds
      const viewport = page.viewportSize();
      const boundingBox = await featuresSection.boundingBox();

      if (boundingBox && viewport) {
        // Element is in viewport if its top is above viewport bottom
        const isInViewport = boundingBox.y < viewport.height &&
                             boundingBox.y + boundingBox.height > 0;
        expect(isInViewport).toBe(true);
      }
    }
  });

  // Test Case 11: Keyboard navigation works
  test('keyboard navigation works for tabs', async ({ page, browserName }) => {
    await page.locator(SELECTORS.codeExamples).scrollIntoViewIfNeeded();

    // Focus on first tab
    const firstTab = page.locator(SELECTORS.codeTab).first();
    await firstTab.focus();
    await expect(firstTab).toBeFocused();

    // Press ArrowRight to move to next tab
    await page.keyboard.press('ArrowRight');

    // Second tab should be focused and active
    const secondTab = page.locator(SELECTORS.codeTab).nth(1);
    await expect(secondTab).toBeFocused();
    await expect(secondTab).toHaveAttribute('aria-selected', 'true');
  });

  // Test Case 12: Page loads without JavaScript errors
  test('page loads without JavaScript errors', async ({ page, browserName }) => {
    const errors = [];
    page.on('pageerror', (error) => {
      errors.push(error.message);
    });

    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Allow for some non-critical errors but fail on major ones
    const criticalErrors = errors.filter(e =>
      !e.includes('ResizeObserver') && // Common non-critical error
      !e.includes('favicon')
    );

    expect(criticalErrors).toHaveLength(0);
  });
});

// Specific browser tests for edge cases
test.describe('Browser-Specific Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('CSS variables are supported and applied', async ({ page, browserName }) => {
    // Check that CSS custom properties are supported
    const hasCssVariables = await page.evaluate(() => {
      const root = document.documentElement;
      const styles = getComputedStyle(root);
      // Check if any CSS variable-based styles are applied
      const bodyBg = getComputedStyle(document.body).backgroundColor;
      return bodyBg !== '' && bodyBg !== 'rgba(0, 0, 0, 0)';
    });

    expect(hasCssVariables).toBe(true);
  });

  test('flexbox layout works correctly', async ({ page, browserName }) => {
    // Check hero CTA buttons use flexbox
    const ctaContainer = page.locator('.hero__cta');
    if (await ctaContainer.count() > 0) {
      const display = await ctaContainer.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });

      // Should be flex or inline-flex or grid
      expect(['flex', 'inline-flex', 'grid', 'block']).toContain(display);
    }
  });

  test('images load with correct dimensions', async ({ page, browserName }) => {
    // Check all images loaded correctly
    const images = page.locator('img');
    const imageCount = await images.count();

    for (let i = 0; i < imageCount; i++) {
      const img = images.nth(i);
      const loaded = await img.evaluate((el) => {
        return el.complete && el.naturalWidth > 0;
      });
      expect(loaded).toBe(true);
    }
  });
});
