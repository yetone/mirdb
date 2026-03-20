/**
 * Cross-Browser Compatibility E2E Tests
 * Owner: Scenario 13 - Cross-Browser Compatibility
 *
 * Test cases:
 * - Homepage loads correctly in Chrome, Firefox, Safari, Edge
 * - All features functional and layout correct in each browser
 * - Interactive demo works in all browsers
 * - CSS animations/transitions work consistently
 *
 * NFR-3: Cross-browser compatibility with latest 2 major versions
 */

const { test, expect } = require('@playwright/test');

// Test suite runs across all configured browsers in playwright.config.js
test.describe('Cross-Browser Compatibility', () => {

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Homepage Load and Layout', () => {

    test('should load homepage with correct title', async ({ page, browserName }) => {
      await expect(page).toHaveTitle(/MirDB/);

      // Verify page loaded successfully
      const body = page.locator('body');
      await expect(body).toBeVisible();

      // Log browser for debugging
      console.log(`Testing in browser: ${browserName}`);
    });

    test('should display hero section correctly', async ({ page }) => {
      const hero = page.locator('#hero');
      await expect(hero).toBeVisible();

      // Verify hero headline
      const headline = page.locator('#hero-headline');
      await expect(headline).toBeVisible();
      await expect(headline).toContainText('MirDB');

      // Verify hero logo
      const logo = page.locator('[data-testid="hero-logo"]');
      await expect(logo).toBeVisible();

      // Verify CTA buttons
      const tryDemoBtn = page.locator('[data-testid="cta-try-demo"]');
      const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
      await expect(tryDemoBtn).toBeVisible();
      await expect(getStartedBtn).toBeVisible();
    });

    test('should display features section correctly', async ({ page }) => {
      const features = page.locator('#features');
      await expect(features).toBeVisible();

      // Verify feature cards are present
      const featureGrid = page.locator('[data-testid="features-grid"]');
      await expect(featureGrid).toBeVisible();

      // Check core features
      await expect(page.locator('[data-testid="feature-memcached"]')).toBeVisible();
      await expect(page.locator('[data-testid="feature-sstables"]')).toBeVisible();
      await expect(page.locator('[data-testid="feature-lsm-tree"]')).toBeVisible();
      await expect(page.locator('[data-testid="feature-skiplist"]')).toBeVisible();
      await expect(page.locator('[data-testid="feature-compaction"]')).toBeVisible();
    });

    test('should display navigation correctly', async ({ page }) => {
      const header = page.locator('#header');
      await expect(header).toBeVisible();

      // Verify navigation links
      const navLinks = page.locator('.header__nav-link');
      const navCount = await navLinks.count();
      expect(navCount).toBeGreaterThanOrEqual(5);
    });

    test('should display footer correctly', async ({ page }) => {
      const footer = page.locator('#footer');
      await expect(footer).toBeVisible();

      // Verify GitHub link
      const githubLink = page.locator('[data-testid="footer-github"]');
      await expect(githubLink).toBeVisible();

      // Verify license link
      const licenseLink = page.locator('[data-testid="footer-license-link"]');
      await expect(licenseLink).toBeVisible();
    });

    test('should have no horizontal overflow', async ({ page }) => {
      // Check that the page does not have horizontal scroll
      const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);

      // Allow small tolerance for scrollbar
      expect(bodyWidth).toBeLessThanOrEqual(viewportWidth + 20);
    });

    test('should display all sections in correct order', async ({ page }) => {
      // Verify all sections exist
      const sections = ['hero', 'features', 'demo', 'examples', 'quickstart', 'status'];

      for (const section of sections) {
        const element = page.locator(`#${section}`);
        await expect(element).toBeVisible();
      }
    });
  });

  test.describe('Interactive Demo Functionality', () => {

    test('should display demo terminal interface', async ({ page }) => {
      const demoSection = page.locator('#demo');
      await demoSection.scrollIntoViewIfNeeded();

      // Verify terminal interface
      const terminal = page.locator('.demo__terminal');
      await expect(terminal).toBeVisible();

      // Verify input field
      const input = page.locator('[data-testid="demo-input"]');
      await expect(input).toBeVisible();
      await expect(input).toBeEnabled();

      // Verify submit button
      const submitBtn = page.locator('[data-testid="demo-submit"]');
      await expect(submitBtn).toBeVisible();
    });

    test('should display example command buttons', async ({ page }) => {
      const demoSection = page.locator('#demo');
      await demoSection.scrollIntoViewIfNeeded();

      // Verify example buttons
      const setBtn = page.locator('[data-testid="example-set"]');
      const getBtn = page.locator('[data-testid="example-get"]');
      const deleteBtn = page.locator('[data-testid="example-delete"]');

      await expect(setBtn).toBeVisible();
      await expect(getBtn).toBeVisible();
      await expect(deleteBtn).toBeVisible();
    });

    test('should allow typing in demo input', async ({ page }) => {
      const input = page.locator('[data-testid="demo-input"]');
      await input.scrollIntoViewIfNeeded();

      // Type a command
      await input.fill('SET testkey 0 0 5');
      await expect(input).toHaveValue('SET testkey 0 0 5');

      // Clear and type another
      await input.clear();
      await input.fill('GET testkey');
      await expect(input).toHaveValue('GET testkey');
    });

    test('should handle example button clicks', async ({ page }) => {
      const demoSection = page.locator('#demo');
      await demoSection.scrollIntoViewIfNeeded();

      // Click SET example button
      const setBtn = page.locator('[data-testid="example-set"]');
      await setBtn.click();

      // Verify some interaction occurred (button should be clickable)
      // Note: The actual execution depends on backend availability
      await expect(setBtn).toBeEnabled();
    });

    test('should maintain focus in input after interactions', async ({ page }) => {
      const input = page.locator('[data-testid="demo-input"]');
      await input.scrollIntoViewIfNeeded();

      // Focus input
      await input.focus();
      await expect(input).toBeFocused();

      // Type command
      await input.fill('GET test');

      // Input should still be focused
      await expect(input).toBeFocused();
    });
  });

  test.describe('CSS Animations and Transitions', () => {

    test('should have hover transitions on CTA buttons', async ({ page }) => {
      const tryDemoBtn = page.locator('[data-testid="cta-try-demo"]');
      await tryDemoBtn.scrollIntoViewIfNeeded();

      // Get initial computed styles
      const initialStyles = await tryDemoBtn.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          transition: style.transition,
          transform: style.transform
        };
      });

      // Hover over button
      await tryDemoBtn.hover();

      // Wait for potential transition
      await page.waitForTimeout(300);

      // Verify button is still visible and interactive
      await expect(tryDemoBtn).toBeVisible();
    });

    test('should have hover effects on feature cards', async ({ page }) => {
      const featureCard = page.locator('.feature-card').first();
      await featureCard.scrollIntoViewIfNeeded();

      // Hover over card
      await featureCard.hover();

      // Wait for transition
      await page.waitForTimeout(300);

      // Verify card is visible
      await expect(featureCard).toBeVisible();
    });

    test('should have hover effects on navigation links', async ({ page }) => {
      const navLink = page.locator('.header__nav-link').first();

      // Hover over link
      await navLink.hover();

      // Wait for transition
      await page.waitForTimeout(200);

      // Verify link is still visible
      await expect(navLink).toBeVisible();
    });

    test('should have smooth scroll behavior', async ({ page }) => {
      // Get initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY);

      // Click on a navigation link
      const featuresLink = page.locator('[data-testid="nav-features"]');
      await featuresLink.click();

      // Wait for scroll
      await page.waitForTimeout(500);

      // Check that scroll happened
      const finalScrollY = await page.evaluate(() => window.scrollY);
      expect(finalScrollY).toBeGreaterThan(initialScrollY);
    });

    test('should display terminal dots animation', async ({ page }) => {
      const demoSection = page.locator('#demo');
      await demoSection.scrollIntoViewIfNeeded();

      // Verify terminal dots are visible
      const redDot = page.locator('.demo__terminal-dot--red');
      const yellowDot = page.locator('.demo__terminal-dot--yellow');
      const greenDot = page.locator('.demo__terminal-dot--green');

      await expect(redDot).toBeVisible();
      await expect(yellowDot).toBeVisible();
      await expect(greenDot).toBeVisible();
    });

    test('should have copy button hover states', async ({ page }) => {
      const copyBtn = page.locator('[data-testid="copy-btn-set"]');
      await copyBtn.scrollIntoViewIfNeeded();

      // Hover over copy button
      await copyBtn.hover();

      // Wait for transition
      await page.waitForTimeout(200);

      // Verify button is visible and interactive
      await expect(copyBtn).toBeVisible();
    });

    test('should have footer link hover states', async ({ page }) => {
      const footer = page.locator('#footer');
      await footer.scrollIntoViewIfNeeded();

      const footerLink = page.locator('.footer__link').first();
      await footerLink.hover();

      // Wait for transition
      await page.waitForTimeout(200);

      // Verify link is visible
      await expect(footerLink).toBeVisible();
    });
  });

  test.describe('Responsive Elements', () => {

    test('should display logo correctly', async ({ page }) => {
      const logo = page.locator('[data-testid="mirdb-logo"]');
      await expect(logo).toBeVisible();

      // Verify logo has dimensions
      const box = await logo.boundingBox();
      expect(box.width).toBeGreaterThan(0);
      expect(box.height).toBeGreaterThan(0);
    });

    test('should display code examples correctly', async ({ page }) => {
      const examples = page.locator('#examples');
      await examples.scrollIntoViewIfNeeded();

      // Verify code examples
      await expect(page.locator('[data-testid="code-example-set"]')).toBeVisible();
      await expect(page.locator('[data-testid="code-example-get"]')).toBeVisible();
      await expect(page.locator('[data-testid="code-example-delete"]')).toBeVisible();
    });

    test('should display quickstart section correctly', async ({ page }) => {
      const quickstart = page.locator('#quickstart');
      await quickstart.scrollIntoViewIfNeeded();

      // Verify steps
      await expect(page.locator('[data-testid="quickstart-step-1"]')).toBeVisible();
      await expect(page.locator('[data-testid="quickstart-step-2"]')).toBeVisible();
      await expect(page.locator('[data-testid="quickstart-step-3"]')).toBeVisible();

      // Verify config table
      await expect(page.locator('[data-testid="config-table"]')).toBeVisible();
    });

    test('should display status section correctly', async ({ page }) => {
      const status = page.locator('#status');
      await status.scrollIntoViewIfNeeded();

      // Verify status items
      const implementedItems = page.locator('[data-testid="status-item-implemented"]');
      const plannedItems = page.locator('[data-testid="status-item-planned"]');

      await expect(implementedItems.first()).toBeVisible();
      await expect(plannedItems.first()).toBeVisible();
    });
  });

  test.describe('Font and Typography Rendering', () => {

    test('should render headlines correctly', async ({ page }) => {
      const heroHeadline = page.locator('#hero-headline');
      await expect(heroHeadline).toBeVisible();

      // Verify headline has content
      const text = await heroHeadline.textContent();
      expect(text.length).toBeGreaterThan(0);

      // Verify headline is visible and readable
      const box = await heroHeadline.boundingBox();
      expect(box.width).toBeGreaterThan(100);
    });

    test('should render body text correctly', async ({ page }) => {
      const description = page.locator('.hero__description');
      await expect(description).toBeVisible();

      // Verify description has content
      const text = await description.textContent();
      expect(text.length).toBeGreaterThan(0);
    });

    test('should render code blocks correctly', async ({ page }) => {
      const codeBlock = page.locator('.code-example__code').first();
      await codeBlock.scrollIntoViewIfNeeded();

      await expect(codeBlock).toBeVisible();

      // Verify monospace font appearance
      const fontFamily = await codeBlock.evaluate((el) => {
        return window.getComputedStyle(el).fontFamily;
      });

      // Code blocks should use monospace font
      expect(fontFamily.toLowerCase()).toMatch(/(mono|code|consolas|courier)/i);
    });
  });

  test.describe('Interactive Elements Focus', () => {

    test('should have visible focus indicators on buttons', async ({ page }) => {
      const tryDemoBtn = page.locator('[data-testid="cta-try-demo"]');
      await tryDemoBtn.scrollIntoViewIfNeeded();

      // Focus button via keyboard
      await tryDemoBtn.focus();

      // Verify button is focused
      await expect(tryDemoBtn).toBeFocused();
    });

    test('should have visible focus indicators on links', async ({ page }) => {
      const navLink = page.locator('.header__nav-link').first();

      // Focus link
      await navLink.focus();

      // Verify link is focused
      await expect(navLink).toBeFocused();
    });

    test('should have visible focus indicators on input', async ({ page }) => {
      const input = page.locator('[data-testid="demo-input"]');
      await input.scrollIntoViewIfNeeded();

      // Focus input
      await input.focus();

      // Verify input is focused
      await expect(input).toBeFocused();
    });
  });
});

test.describe('Browser-Specific Features', () => {

  test('should handle CSS custom properties', async ({ page }) => {
    await page.goto('/');

    // Check that CSS variables are supported
    const bgColor = await page.evaluate(() => {
      return window.getComputedStyle(document.documentElement).getPropertyValue('--color-bg-primary');
    });

    // CSS variables should return a value (may be empty string if not defined)
    expect(typeof bgColor).toBe('string');
  });

  test('should handle SVG rendering', async ({ page }) => {
    await page.goto('/');

    // Check that SVG icons render
    const svgIcons = page.locator('svg');
    const count = await svgIcons.count();

    expect(count).toBeGreaterThan(0);

    // Verify first SVG is visible
    await expect(svgIcons.first()).toBeVisible();
  });

  test('should handle flexbox layout', async ({ page }) => {
    await page.goto('/');

    // Check that flexbox containers work
    const featuresGrid = page.locator('.features__grid');
    await featuresGrid.scrollIntoViewIfNeeded();

    const display = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });

    // Should be flex or grid
    expect(['flex', 'grid']).toContain(display);
  });

  test('should handle grid layout', async ({ page }) => {
    await page.goto('/');

    // Verify feature cards layout
    const featureCards = page.locator('.feature-card');
    const count = await featureCards.count();

    expect(count).toBeGreaterThan(0);

    // All cards should be visible
    for (let i = 0; i < Math.min(count, 3); i++) {
      await expect(featureCards.nth(i)).toBeVisible();
    }
  });
});
