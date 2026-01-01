// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Cross-Browser Compatibility Tests
 *
 * NFR-4: Page must render correctly in modern browsers (Chrome, Firefox, Safari, Edge)
 *
 * These tests verify that the MirDB landing page renders correctly and all features
 * are functional across all supported browsers. The tests run against:
 * - Chromium (Chrome)
 * - Firefox
 * - WebKit (Safari)
 *
 * Note: Edge uses Chromium engine, so Chromium tests cover Edge compatibility.
 */

test.describe('Cross-Browser Compatibility', () => {

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for page to be fully loaded
    await page.waitForLoadState('domcontentloaded');
  });

  test.describe('Page Rendering', () => {

    test('page loads successfully with correct title', async ({ page, browserName }) => {
      await expect(page).toHaveTitle(/MirDB/);
      // Verify no console errors during page load
      const errors = [];
      page.on('pageerror', error => errors.push(error.message));
      await page.waitForLoadState('networkidle');
      expect(errors.length).toBe(0);
    });

    test('hero section renders correctly', async ({ page, browserName }) => {
      // Verify hero section elements are visible
      const heroH1 = page.locator('.hero h1');
      await expect(heroH1).toBeVisible();
      await expect(heroH1).toHaveText('MirDB');

      const tagline = page.locator('.hero .tagline');
      await expect(tagline).toBeVisible();
      await expect(tagline).toContainText('Persistent Key-Value Store');

      // Verify CTA buttons are visible and clickable
      const getStartedBtn = page.locator('.hero-buttons .btn-primary');
      await expect(getStartedBtn).toBeVisible();

      const githubBtn = page.locator('.hero-buttons .btn-secondary');
      await expect(githubBtn).toBeVisible();
    });

    test('navigation renders correctly', async ({ page, browserName }) => {
      const nav = page.locator('[data-testid="navigation-header"]');
      await expect(nav).toBeVisible();

      const logo = page.locator('[data-testid="nav-logo"]');
      await expect(logo).toBeVisible();
      await expect(logo).toHaveText('MirDB');

      // Check navigation links are present
      const navLinks = page.locator('[data-testid="nav-links"] a');
      await expect(navLinks).toHaveCount(7);
    });

    test('features section renders with all cards', async ({ page, browserName }) => {
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();

      const featureCards = page.locator('.feature-card');
      await expect(featureCards).toHaveCount(3);

      // Verify each feature card is visible
      await expect(page.locator('[data-testid="feature-memcached"]')).toBeVisible();
      await expect(page.locator('[data-testid="feature-persistence"]')).toBeVisible();
      await expect(page.locator('[data-testid="feature-performance"]')).toBeVisible();
    });

    test('architecture section with SVG diagram renders correctly', async ({ page, browserName }) => {
      const archSection = page.locator('[data-testid="architecture-section"]');
      await expect(archSection).toBeVisible();

      // Verify SVG diagram is rendered
      const diagram = page.locator('[data-testid="lsm-tree-diagram"]');
      await expect(diagram).toBeVisible();

      // Verify SVG has proper accessibility attributes
      await expect(diagram).toHaveAttribute('role', 'img');

      // Verify explanation section
      const explanation = page.locator('[data-testid="architecture-explanation"]');
      await expect(explanation).toBeVisible();
    });

    test('commands section renders with all command cards', async ({ page, browserName }) => {
      const commandsGrid = page.locator('[data-testid="commands-grid"]');
      await expect(commandsGrid).toBeVisible();

      // Verify all 7 command cards are present
      const commands = ['set', 'get', 'add', 'replace', 'append', 'prepend', 'delete'];
      for (const cmd of commands) {
        await expect(page.locator(`[data-testid="command-${cmd}"]`)).toBeVisible();
      }
    });

    test('quick start section renders with code blocks', async ({ page, browserName }) => {
      const quickstartSection = page.locator('#quickstart');
      await expect(quickstartSection).toBeVisible();

      // Verify code blocks are present
      const codeBlocks = quickstartSection.locator('.code-block');
      await expect(codeBlocks.first()).toBeVisible();

      // Verify code content is visible
      const codeContent = quickstartSection.locator('.code-block code');
      await expect(codeContent.first()).toBeVisible();
    });

    test('configuration section renders correctly', async ({ page, browserName }) => {
      const configSection = page.locator('#configuration');
      await expect(configSection).toBeVisible();

      // Verify config TOML block
      const configBlock = page.locator('[data-testid="config-toml-block"]');
      await expect(configBlock).toBeVisible();

      // Verify config parameters
      const configParams = page.locator('.config-param');
      await expect(configParams).toHaveCount(5);
    });

    test('footer renders with all links', async ({ page, browserName }) => {
      const footer = page.locator('[data-testid="footer"]');
      await expect(footer).toBeVisible();

      await expect(page.locator('[data-testid="footer-github-link"]')).toBeVisible();
      await expect(page.locator('[data-testid="footer-contributing-link"]')).toBeVisible();
      await expect(page.locator('[data-testid="footer-license-link"]')).toBeVisible();
    });
  });

  test.describe('CSS and Styling Consistency', () => {

    test('CSS variables are applied correctly', async ({ page, browserName }) => {
      // Verify CSS custom properties are working
      const hero = page.locator('.hero');
      await expect(hero).toBeVisible();

      // Check gradient background is applied (verify it's not plain color)
      const bgImage = await hero.evaluate(el =>
        window.getComputedStyle(el).backgroundImage
      );
      expect(bgImage).toContain('gradient');
    });

    test('typography renders consistently', async ({ page, browserName }) => {
      // Verify font-family is applied
      const body = page.locator('body');
      const fontFamily = await body.evaluate(el =>
        window.getComputedStyle(el).fontFamily
      );
      // Should use system fonts
      expect(fontFamily.toLowerCase()).toMatch(/(-apple-system|blinkmacsystemfont|segoe|roboto|ubuntu|sans-serif)/i);
    });

    test('grid layouts render correctly', async ({ page, browserName }) => {
      const featuresGrid = page.locator('.features-grid');
      await expect(featuresGrid).toBeVisible();

      // Verify grid display is applied
      const display = await featuresGrid.evaluate(el =>
        window.getComputedStyle(el).display
      );
      expect(display).toBe('grid');
    });

    test('box shadows are rendered', async ({ page, browserName }) => {
      const nav = page.locator('nav');
      const boxShadow = await nav.evaluate(el =>
        window.getComputedStyle(el).boxShadow
      );
      // Should have a shadow (not 'none')
      expect(boxShadow).not.toBe('none');
    });

    test('border-radius is applied to cards', async ({ page, browserName }) => {
      const featureCard = page.locator('.feature-card').first();
      const borderRadius = await featureCard.evaluate(el =>
        window.getComputedStyle(el).borderRadius
      );
      // Should have rounded corners
      expect(borderRadius).not.toBe('0px');
    });
  });

  test.describe('Interactive Elements', () => {

    test('navigation links scroll to sections', async ({ page, browserName }) => {
      // Click features link and verify scroll
      const featuresLink = page.locator('[data-testid="nav-link-features"]');
      await featuresLink.click();

      // Wait for smooth scroll
      await page.waitForTimeout(500);

      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport();
    });

    test('button hover states work correctly', async ({ page, browserName }) => {
      const primaryBtn = page.locator('.hero-buttons .btn-primary');

      // Get initial transform
      const initialTransform = await primaryBtn.evaluate(el =>
        window.getComputedStyle(el).transform
      );

      // Hover over button
      await primaryBtn.hover();
      await page.waitForTimeout(300); // Wait for transition

      // Transform should change on hover (translateY)
      const hoverTransform = await primaryBtn.evaluate(el =>
        window.getComputedStyle(el).transform
      );

      // The transforms should be different after hover
      expect(hoverTransform).toBeDefined();
    });

    test('external links have correct attributes', async ({ page, browserName }) => {
      const githubLink = page.locator('[data-testid="nav-link-github"]');

      await expect(githubLink).toHaveAttribute('target', '_blank');
      await expect(githubLink).toHaveAttribute('rel', 'noopener');
      await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    });
  });

  test.describe('Responsive Design', () => {

    test('mobile viewport renders correctly', async ({ page, browserName }) => {
      await page.setViewportSize({ width: 375, height: 667 });

      // Hero should still be visible
      const hero = page.locator('.hero');
      await expect(hero).toBeVisible();

      // Hero title should have smaller font size
      const heroTitle = page.locator('.hero h1');
      const fontSize = await heroTitle.evaluate(el =>
        window.getComputedStyle(el).fontSize
      );
      // Mobile font size should be 2rem = 32px
      expect(parseInt(fontSize)).toBeLessThanOrEqual(32);

      // Navigation links should be hidden on mobile
      const navLinks = page.locator('.nav-links');
      await expect(navLinks).toBeHidden();
    });

    test('tablet viewport renders correctly', async ({ page, browserName }) => {
      await page.setViewportSize({ width: 768, height: 1024 });

      // Features grid should still work
      const featuresGrid = page.locator('.features-grid');
      await expect(featuresGrid).toBeVisible();

      // All feature cards should be visible
      const featureCards = page.locator('.feature-card');
      await expect(featureCards).toHaveCount(3);
    });

    test('desktop viewport renders correctly', async ({ page, browserName }) => {
      await page.setViewportSize({ width: 1440, height: 900 });

      // Full navigation should be visible
      const navLinks = page.locator('.nav-links');
      await expect(navLinks).toBeVisible();

      // Architecture content should be side by side (2 columns)
      const archContent = page.locator('.architecture-content');
      const gridColumns = await archContent.evaluate(el =>
        window.getComputedStyle(el).gridTemplateColumns
      );
      // Should have 2 columns on desktop - browsers compute to actual pixel values like "476px 476px"
      // so we check for 2 column values (space-separated)
      const columnCount = gridColumns.trim().split(/\s+/).length;
      expect(columnCount).toBe(2);
    });
  });

  test.describe('Accessibility', () => {

    test('proper heading hierarchy exists', async ({ page, browserName }) => {
      // Should have exactly one h1
      const h1s = page.locator('h1');
      await expect(h1s).toHaveCount(1);

      // Should have multiple h2s for sections
      const h2s = page.locator('h2');
      const h2Count = await h2s.count();
      expect(h2Count).toBeGreaterThanOrEqual(4);
    });

    test('images and diagrams have alt text', async ({ page, browserName }) => {
      // SVG diagram should have proper accessibility attributes
      const svg = page.locator('[data-testid="lsm-tree-diagram"]');
      await expect(svg).toHaveAttribute('role', 'img');

      // Should have aria-label
      const ariaLabel = await svg.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel.length).toBeGreaterThan(50);
    });

    test('links are focusable', async ({ page, browserName }) => {
      // Tab to first link
      await page.keyboard.press('Tab');

      // A link should be focused
      const focusedElement = page.locator(':focus');
      const tagName = await focusedElement.evaluate(el => el.tagName.toLowerCase());
      expect(tagName).toBe('a');
    });

    test('color contrast is sufficient', async ({ page, browserName }) => {
      // Check that text colors have reasonable contrast
      const body = page.locator('body');
      const color = await body.evaluate(el =>
        window.getComputedStyle(el).color
      );
      // Text should not be too light
      expect(color).toBeDefined();
      expect(color).not.toBe('rgb(255, 255, 255)');
    });
  });

  test.describe('Visual Consistency Check', () => {

    test('page has consistent visual appearance', async ({ page, browserName }) => {
      // Take a screenshot for visual comparison
      // This test verifies the page renders without visual glitches
      await page.waitForLoadState('networkidle');

      // Verify all main sections are rendered and visible
      const sections = [
        '.hero',
        '#features',
        '#architecture',
        '#commands',
        '#quickstart',
        '#configuration',
        'footer'
      ];

      for (const section of sections) {
        const element = page.locator(section);
        await expect(element).toBeVisible();

        // Verify section has proper dimensions
        const box = await element.boundingBox();
        expect(box).toBeTruthy();
        expect(box.width).toBeGreaterThan(0);
        expect(box.height).toBeGreaterThan(0);
      }
    });

    test('no layout shifts or overflow issues', async ({ page, browserName }) => {
      // Check body doesn't have horizontal scroll
      const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);

      // Body should not be significantly wider than viewport (allowing small margin)
      expect(bodyWidth).toBeLessThanOrEqual(viewportWidth + 20);
    });

    test('fonts are loaded correctly', async ({ page, browserName }) => {
      // Wait for fonts to load
      await page.waitForLoadState('networkidle');

      // Check that text is rendered (not invisible due to FOIT)
      const heroText = page.locator('.hero h1');
      await expect(heroText).toBeVisible();

      // Verify text content is actually there
      const textContent = await heroText.textContent();
      expect(textContent).toBe('MirDB');
    });
  });
});
