/**
 * E2E tests for Cross-Browser Compatibility
 * Owner: Scenario 11
 *
 * Test cases:
 * - Chrome rendering and functionality
 * - Firefox rendering and functionality
 * - Safari rendering and functionality
 * - Edge rendering and functionality
 * - CSS Grid/Flexbox consistency
 * - localStorage theme persistence
 *
 * These tests run across all browser projects defined in playwright.config.js
 * (chromium, firefox, webkit) to validate cross-browser compatibility.
 */

const { test, expect } = require('@playwright/test');

/**
 * Helper function to validate page renders correctly:
 * - Page loads without horizontal scroll
 * - All main sections are visible
 */
async function validatePageRendering(page) {
  // Check no horizontal scroll
  const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
  const viewportWidth = await page.evaluate(() => window.innerWidth);
  expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth);

  // Verify main sections are visible
  const header = page.locator('header');
  await expect(header).toBeVisible();

  const hero = page.locator('#hero');
  await expect(hero).toBeVisible();

  const features = page.locator('#features');
  await expect(features).toBeVisible();

  const architecture = page.locator('#architecture');
  await expect(architecture).toBeVisible();

  const gettingStarted = page.locator('#getting-started');
  await expect(gettingStarted).toBeVisible();

  const status = page.locator('#status');
  await expect(status).toBeVisible();

  const footer = page.locator('footer');
  await expect(footer).toBeVisible();
}

/**
 * Helper function to validate all features work:
 * - Theme toggle works
 * - Navigation links work
 * - Interactive elements respond
 */
async function validateFeaturesWork(page) {
  // Test theme toggle
  const themeToggle = page.locator('#theme-toggle');
  await expect(themeToggle).toBeVisible();

  // Get initial dark state
  const html = page.locator('html');
  const initialHasDark = await html.evaluate(el => el.classList.contains('dark'));

  // Click toggle and verify state change
  await themeToggle.click();
  const afterClickHasDark = await html.evaluate(el => el.classList.contains('dark'));
  expect(afterClickHasDark).not.toBe(initialHasDark);

  // Toggle back
  await themeToggle.click();
  const finalHasDark = await html.evaluate(el => el.classList.contains('dark'));
  expect(finalHasDark).toBe(initialHasDark);

  // Test navigation - scroll to top first
  await page.evaluate(() => window.scrollTo(0, 0));
  await page.waitForTimeout(100);

  const nav = page.locator('nav[aria-label="Main navigation"]');
  await expect(nav).toBeVisible();
}

test.describe('Cross-Browser Compatibility', () => {
  // Test Case 1, 2, 3, 4: Load homepage in various browsers
  // These tests run across all browser projects defined in playwright.config.js
  test('Page renders correctly across browsers with all features working', async ({ page, browserName }) => {
    const consoleErrors = [];
    page.on('console', msg => {
      if (msg.type() === 'error') {
        consoleErrors.push(msg.text());
      }
    });
    page.on('pageerror', err => {
      consoleErrors.push(err.message);
    });

    await page.goto('');
    await page.waitForLoadState('networkidle');

    // Validate rendering
    await validatePageRendering(page);

    // Validate features work
    await validateFeaturesWork(page);

    // No critical console errors
    const criticalErrors = consoleErrors.filter(err =>
      !err.includes('favicon') &&
      !err.includes('Failed to load resource')
    );
    expect(criticalErrors).toHaveLength(0);
  });

  test('All images load correctly', async ({ page, browserName }) => {
    await page.goto('');
    await page.waitForLoadState('networkidle');

    // Check logo
    const logo = page.locator('header img[alt="MirDB Logo"]');
    await expect(logo).toBeVisible();

    const logoSrc = await logo.getAttribute('src');
    expect(logoSrc).toContain('logo.gif');

    // Wait for image to fully load
    await logo.evaluate(img => {
      return new Promise((resolve) => {
        if (img.complete) resolve();
        else img.onload = () => resolve();
      });
    });

    // Verify image loaded successfully - for GIFs naturalWidth may be 0 in some browsers
    // Just check the image is visible and has some dimensions
    const logoBox = await logo.boundingBox();
    expect(logoBox).not.toBeNull();
    expect(logoBox.width).toBeGreaterThan(0);
    expect(logoBox.height).toBeGreaterThan(0);
  });

  // Test Case 5: CSS Grid/Flexbox consistency
  test('Test Case 5: CSS Grid/Flexbox renders consistently', async ({ page, browserName }) => {
    await page.goto('');
    await page.waitForLoadState('networkidle');

    // Check features grid uses CSS grid
    const featuresGrid = page.locator('#features .grid');
    await expect(featuresGrid).toBeVisible();

    const gridDisplay = await featuresGrid.evaluate(el => {
      return window.getComputedStyle(el).display;
    });
    expect(gridDisplay).toBe('grid');

    // Verify grid has expected number of cards
    const featureCards = page.locator('#features .feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(3);

    // Check header nav wrapper uses flexbox
    const headerContent = page.locator('header nav > div').first();
    const headerDisplay = await headerContent.evaluate(el => {
      return window.getComputedStyle(el).display;
    });
    expect(headerDisplay).toBe('flex');

    // Verify flexbox alignment in header
    const headerJustify = await headerContent.evaluate(el => {
      return window.getComputedStyle(el).justifyContent;
    });
    expect(headerJustify).toBe('space-between');
  });

  // Test Case 6: localStorage theme persistence
  test('Test Case 6: localStorage theme persistence works', async ({ page, browserName }) => {
    await page.goto('');

    // Clear localStorage and ensure light mode
    await page.evaluate(() => {
      localStorage.clear();
      document.documentElement.classList.remove('dark');
    });
    await page.reload();

    const html = page.locator('html');
    const themeToggle = page.locator('#theme-toggle');

    // Toggle to dark mode
    await themeToggle.click();
    await expect(html).toHaveClass(/dark/);

    // Verify localStorage was set (the app uses 'theme' as the key)
    const storedTheme = await page.evaluate(() => localStorage.getItem('theme'));
    expect(storedTheme).toBe('dark');

    // Reload and verify persistence
    await page.reload();
    await expect(html).toHaveClass(/dark/);

    // Toggle back to light mode
    await themeToggle.click();
    await expect(html).not.toHaveClass(/dark/);

    // Verify localStorage updated
    const updatedTheme = await page.evaluate(() => localStorage.getItem('theme'));
    expect(updatedTheme).toBe('light');

    // Reload and verify light mode persists
    await page.reload();
    await expect(html).not.toHaveClass(/dark/);
  });

  test('All sections have correct HTML structure', async ({ page, browserName }) => {
    await page.goto('');
    await page.waitForLoadState('networkidle');

    // Hero section has h1
    const h1 = page.locator('#hero h1');
    await expect(h1).toBeVisible();

    // Features section has h2
    const featuresH2 = page.locator('#features h2');
    await expect(featuresH2).toBeVisible();
    await expect(featuresH2).toContainText('Features');

    // Architecture section has h2
    const archH2 = page.locator('#architecture h2');
    await expect(archH2).toBeVisible();

    // Getting Started section has h2
    const gsH2 = page.locator('#getting-started h2');
    await expect(gsH2).toBeVisible();

    // Status section has h2
    const statusH2 = page.locator('#status h2');
    await expect(statusH2).toBeVisible();
  });

  test('Keyboard navigation works correctly', async ({ page, browserName }) => {
    await page.goto('');

    // Press Tab to start navigation
    await page.keyboard.press('Tab');

    // First focusable should be skip-to-content link
    const skipLink = page.locator('.skip-to-content');
    await expect(skipLink).toBeFocused();

    // Tab through navigation
    await page.keyboard.press('Tab'); // Logo link
    await page.keyboard.press('Tab'); // First nav link

    const featuresLink = page.locator('nav[aria-label="Main navigation"] .hidden.md\\:flex a[href="#features"]');
    await expect(featuresLink).toBeFocused();

    // Verify focus indicator is visible
    const focusOutline = await featuresLink.evaluate(el => {
      const styles = window.getComputedStyle(el);
      return styles.outline !== 'none' || styles.outlineWidth !== '0px';
    });
    expect(focusOutline).toBe(true);
  });

  test('Smooth scrolling navigation works', async ({ page, browserName }) => {
    await page.goto('');
    await page.waitForLoadState('networkidle');

    // Get initial scroll position
    const initialScroll = await page.evaluate(() => window.scrollY);
    expect(initialScroll).toBe(0);

    // Click on Getting Started link
    const gettingStartedLink = page.locator('nav[aria-label="Main navigation"] .hidden.md\\:flex a[href="#getting-started"]');
    await gettingStartedLink.click();

    // Wait for smooth scroll to complete
    await page.waitForTimeout(1000);

    // Scroll position should have changed
    const finalScroll = await page.evaluate(() => window.scrollY);
    expect(finalScroll).toBeGreaterThan(0);

    // Getting Started section should be visible in viewport (allow more tolerance)
    const gettingStarted = page.locator('#getting-started');
    const isVisible = await gettingStarted.isVisible();
    expect(isVisible).toBe(true);

    // Check it's reasonably near the top (within the visible viewport)
    const inView = await gettingStarted.evaluate(el => {
      const rect = el.getBoundingClientRect();
      return rect.top < window.innerHeight;
    });
    expect(inView).toBe(true);
  });

  test('Theme toggle visual styles apply correctly', async ({ page, browserName }) => {
    await page.goto('');
    await page.waitForLoadState('networkidle');

    const html = page.locator('html');
    const themeToggle = page.locator('#theme-toggle');
    const body = page.locator('body');

    // Clear localStorage and set to light mode
    await page.evaluate(() => {
      localStorage.clear();
      document.documentElement.classList.remove('dark');
    });
    await page.reload();
    await page.waitForLoadState('networkidle');

    // Get light mode background color
    const lightBgColor = await body.evaluate(el => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Toggle to dark mode
    await themeToggle.click();
    await expect(html).toHaveClass(/dark/);

    // Get dark mode background color
    const darkBgColor = await body.evaluate(el => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Background colors should be different
    expect(darkBgColor).not.toBe(lightBgColor);
  });

  test('External links have correct security attributes', async ({ page, browserName }) => {
    await page.goto('');
    await page.waitForLoadState('networkidle');

    // Check GitHub link in navigation
    const githubLink = page.locator('nav[aria-label="Main navigation"] a[href="https://github.com/yetone/mirdb"]').first();
    await expect(githubLink).toBeVisible();
    await expect(githubLink).toHaveAttribute('target', '_blank');
    await expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');
  });

  test('Skip link functionality works', async ({ page, browserName }) => {
    await page.goto('');

    const skipLink = page.locator('.skip-to-content');

    // Press Tab to focus skip link
    await page.keyboard.press('Tab');
    await expect(skipLink).toBeFocused();

    // Verify link points to main content
    await expect(skipLink).toHaveAttribute('href', '#main-content');
  });

  test('Header remains sticky during scroll', async ({ page, browserName }) => {
    await page.goto('');
    await page.waitForLoadState('networkidle');

    const header = page.locator('header.sticky');
    await expect(header).toBeVisible();

    // Check header has position sticky
    const position = await header.evaluate(el => {
      return window.getComputedStyle(el).position;
    });
    expect(position).toBe('sticky');

    // Scroll down
    await page.evaluate(() => window.scrollTo(0, 500));
    await page.waitForTimeout(100);

    // Header should still be at top
    const boundingBox = await header.boundingBox();
    expect(boundingBox.y).toBe(0);
  });

  test('Container maintains max-width and centering', async ({ page, browserName }) => {
    await page.goto('');
    await page.waitForLoadState('networkidle');

    const container = page.locator('.container-custom').first();
    const containerBox = await container.boundingBox();
    const viewportWidth = await page.evaluate(() => window.innerWidth);

    // Container should have max-width (not full viewport width for wide screens)
    if (viewportWidth > 1280) {
      expect(containerBox.width).toBeLessThanOrEqual(1280);

      // Container should be centered
      const leftMargin = containerBox.x;
      const rightMargin = viewportWidth - (containerBox.x + containerBox.width);
      expect(Math.abs(leftMargin - rightMargin)).toBeLessThan(10);
    }
  });

  test('Navigation links are all present and accessible', async ({ page, browserName }) => {
    await page.goto('');
    await page.waitForLoadState('networkidle');

    const nav = page.locator('nav[aria-label="Main navigation"]');
    await expect(nav).toBeVisible();

    // Check all required nav links in desktop view
    const desktopNav = page.locator('nav[aria-label="Main navigation"] .hidden.md\\:flex');
    const requiredLinks = [
      { href: '#features', text: 'Features' },
      { href: '#architecture', text: 'Architecture' },
      { href: '#getting-started', text: 'Getting Started' },
      { href: '#status', text: 'Status' },
      { href: 'https://github.com/yetone/mirdb', text: 'GitHub' }
    ];

    for (const link of requiredLinks) {
      const navLink = desktopNav.locator(`a[href="${link.href}"]`);
      await expect(navLink).toBeVisible();
      await expect(navLink).toContainText(link.text);
    }
  });

  test('Theme toggle button has proper accessibility attributes', async ({ page, browserName }) => {
    await page.goto('');
    await page.waitForLoadState('networkidle');

    const themeToggle = page.locator('#theme-toggle');
    await expect(themeToggle).toBeVisible();
    await expect(themeToggle).toHaveAttribute('aria-label', 'Toggle dark mode');

    // Verify it's a button element
    const tagName = await themeToggle.evaluate(el => el.tagName.toLowerCase());
    expect(tagName).toBe('button');
  });
});
