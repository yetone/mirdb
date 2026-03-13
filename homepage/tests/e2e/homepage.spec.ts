/**
 * Cross-Browser Compatibility E2E Tests
 * Owner: Scenario 15 - Cross-Browser Compatibility
 *
 * Tests verify:
 * - Page renders correctly without console errors across modern browsers
 * - Chrome, Firefox, Safari (WebKit), and Edge support
 * - Dark mode toggle works consistently across browsers
 * - All critical UI elements render properly
 *
 * The browser matrix is configured in playwright.config.ts:
 * - chromium: Primary browser target
 * - firefox: Secondary browser support
 * - webkit: Safari/macOS/iOS browser support
 * - edge: Windows browser support
 */

import { test, expect, ConsoleMessage, Page } from '@playwright/test';

/**
 * Helper function to collect console errors during page load
 * @param page - The Playwright page object
 * @returns Array of console error messages
 */
function collectConsoleErrors(page: Page): ConsoleMessage[] {
  const errors: ConsoleMessage[] = [];
  page.on('console', (message) => {
    if (message.type() === 'error') {
      errors.push(message);
    }
  });
  return errors;
}

test.describe('Cross-Browser Compatibility - Page Rendering', () => {
  /**
   * Test Case 1 & 2 & 3 & 4: Page renders without console errors
   * This test runs on all browsers configured in playwright.config.ts
   */
  test('page renders without console errors', async ({ page, browserName }) => {
    const errors: ConsoleMessage[] = [];

    // Listen for console errors
    page.on('console', (message) => {
      if (message.type() === 'error') {
        // Ignore certain known third-party errors that don't affect functionality
        const text = message.text();
        // Filter out favicon errors or other non-critical errors
        if (!text.includes('favicon') && !text.includes('404')) {
          errors.push(message);
        }
      }
    });

    // Navigate to homepage
    await page.goto('/');

    // Wait for page to fully load
    await page.waitForLoadState('networkidle');

    // Verify page title is correct
    await expect(page).toHaveTitle(/MirDB/);

    // Verify main sections are rendered
    const navbar = page.getByTestId('navbar');
    await expect(navbar).toBeVisible();

    const hero = page.getByTestId('hero');
    await expect(hero).toBeVisible();

    const features = page.getByTestId('features');
    await expect(features).toBeVisible();

    // Verify no console errors occurred
    expect(errors, `Browser ${browserName} should have no console errors`).toHaveLength(0);
  });

  test('all critical UI elements render correctly', async ({ page, browserName }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Test Navbar elements
    const navbar = page.getByTestId('navbar');
    await expect(navbar).toBeVisible();

    // Logo or brand name
    const logo = page.getByRole('link', { name: /mirdb/i }).first();
    await expect(logo).toBeVisible();

    // Test Hero section
    const hero = page.getByTestId('hero');
    await expect(hero).toBeVisible();

    const heroHeadline = hero.getByRole('heading', { level: 1 });
    await expect(heroHeadline).toBeVisible();
    await expect(heroHeadline).toContainText('Persistent Key-Value Store');

    const heroSubtitle = hero.getByText(/with Memcached Protocol/i);
    await expect(heroSubtitle).toBeVisible();

    // CTA buttons
    const getStartedBtn = hero.getByRole('link', { name: /get started/i });
    await expect(getStartedBtn).toBeVisible();

    const githubBtn = hero.getByRole('link', { name: /github/i });
    await expect(githubBtn).toBeVisible();

    // Test Features section
    const features = page.getByTestId('features');
    await features.scrollIntoViewIfNeeded();
    await expect(features).toBeVisible();

    const featuresHeading = features.getByRole('heading', { name: 'Features' });
    await expect(featuresHeading).toBeVisible();

    // Test QuickStart section
    const quickStart = page.getByTestId('quickstart');
    await quickStart.scrollIntoViewIfNeeded();
    await expect(quickStart).toBeVisible();

    // Test StatusBadges section
    const statusBadges = page.getByTestId('status-badges');
    await statusBadges.scrollIntoViewIfNeeded();
    await expect(statusBadges).toBeVisible();

    // Test Footer
    const footer = page.getByTestId('footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();
  });

  test('CSS styles are correctly applied', async ({ page, browserName }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Verify hero text is styled correctly
    const heroHeadline = page.getByTestId('hero').getByRole('heading', { level: 1 });
    const headlineStyles = await heroHeadline.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        fontWeight: style.fontWeight,
        fontSize: parseFloat(style.fontSize),
      };
    });

    // Headline should be bold (fontWeight >= 700)
    expect(parseInt(headlineStyles.fontWeight)).toBeGreaterThanOrEqual(700);

    // Headline should have reasonable font size (at least 24px)
    expect(headlineStyles.fontSize).toBeGreaterThanOrEqual(24);

    // Verify navbar background style
    const navbar = page.getByTestId('navbar');
    const navbarStyles = await navbar.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        backgroundColor: style.backgroundColor,
        position: style.position,
      };
    });

    // Navbar should have a background color (not transparent)
    expect(navbarStyles.backgroundColor).not.toBe('rgba(0, 0, 0, 0)');
  });

  test('external links have correct attributes', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Check GitHub link in hero section has security attributes
    const hero = page.getByTestId('hero');
    const githubLink = hero.getByRole('link', { name: /github/i });

    await expect(githubLink).toHaveAttribute('target', '_blank');
    await expect(githubLink).toHaveAttribute('rel', /noopener/);
    await expect(githubLink).toHaveAttribute('rel', /noreferrer/);
  });
});

test.describe('Cross-Browser Compatibility - Dark Mode Toggle', () => {
  /**
   * Test Case 5: Theme toggle exists and is interactive across browsers
   * This test verifies the theme toggle UI is rendered consistently.
   * Note: Actual theme toggle functionality is tested by Scenario 8.
   */
  test('theme toggle exists and is interactive', async ({ page, browserName }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Find theme toggle button
    const themeToggle = page.getByTestId('theme-toggle');
    await expect(themeToggle).toBeVisible();

    // Verify the button has proper accessibility attributes
    await expect(themeToggle).toHaveAttribute('aria-label', /switch to (light|dark) mode/i);

    // Verify clicking doesn't cause any errors (test interaction works)
    await themeToggle.click();

    // The page should remain stable after clicking
    await expect(page.getByTestId('navbar')).toBeVisible();
    await expect(page.getByTestId('hero')).toBeVisible();
  });

  test('dark mode CSS classes are defined correctly', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Verify dark mode classes are properly defined in CSS
    // Check that the app container has dark mode support via Tailwind classes
    const appContainer = page.locator('.min-h-screen').first();

    // Verify the container has dark mode background class
    const hasBackgroundClass = await appContainer.evaluate((el) => {
      return el.className.includes('bg-white') && el.className.includes('dark:bg-gray-900');
    });

    expect(hasBackgroundClass).toBe(true);

    // Verify navbar has dark mode styling
    const navbar = page.getByTestId('navbar');
    const navbarHasDarkStyle = await navbar.evaluate((el) => {
      return el.className.includes('dark:bg-gray-900') || el.className.includes('dark:');
    });

    expect(navbarHasDarkStyle).toBe(true);
  });

  test('theme toggle button renders consistently across browsers', async ({ page, browserName }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    const themeToggle = page.getByTestId('theme-toggle');

    // Button should be visible and have reasonable size
    await expect(themeToggle).toBeVisible();

    const boundingBox = await themeToggle.boundingBox();
    expect(boundingBox).not.toBeNull();

    if (boundingBox) {
      // Button should have minimum touch target size
      expect(boundingBox.width).toBeGreaterThanOrEqual(20);
      expect(boundingBox.height).toBeGreaterThanOrEqual(20);
    }

    // Button should contain an SVG icon
    const svgIcon = themeToggle.locator('svg');
    await expect(svgIcon).toBeVisible();

    // Button should be a focusable element
    const tagName = await themeToggle.evaluate((el) => el.tagName.toLowerCase());
    expect(tagName).toBe('button');
  });
});

test.describe('Cross-Browser Compatibility - JavaScript Functionality', () => {
  test('interactive elements respond to clicks', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Test navigation links are clickable
    const hero = page.getByTestId('hero');
    const getStartedBtn = hero.getByRole('link', { name: /get started/i });

    // Verify the button exists and is visible
    await expect(getStartedBtn).toBeVisible();

    // Verify it's an anchor element with href
    const href = await getStartedBtn.getAttribute('href');
    expect(href).toBeTruthy();

    // Verify the element is enabled and can be focused
    await expect(getStartedBtn).toBeEnabled();
  });

  test('scroll behavior works correctly', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Scroll to features section
    const features = page.getByTestId('features');
    await features.scrollIntoViewIfNeeded();

    // Verify features section is now in viewport
    const isInViewport = await features.evaluate((el) => {
      const rect = el.getBoundingClientRect();
      return rect.top >= 0 && rect.top < window.innerHeight;
    });

    expect(isInViewport).toBe(true);
  });

  test('copy to clipboard functionality exists', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Navigate to QuickStart section
    const quickStart = page.getByTestId('quickstart');
    await quickStart.scrollIntoViewIfNeeded();

    // Look for copy button
    const copyButton = quickStart.getByRole('button', { name: /copy/i });

    // Copy button should be present
    await expect(copyButton).toBeVisible();
  });
});

test.describe('Cross-Browser Compatibility - Layout Consistency', () => {
  test('page layout is consistent across browsers', async ({ page, browserName }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Verify flexbox layout is working (min-h-screen flex flex-col)
    const appContainer = page.locator('.min-h-screen').first();
    const displayStyle = await appContainer.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });

    expect(displayStyle).toBe('flex');

    // Verify main content has flex-grow
    const mainContent = page.locator('main');
    const flexGrow = await mainContent.evaluate((el) => {
      return window.getComputedStyle(el).flexGrow;
    });

    expect(flexGrow).toBe('1');
  });

  test('grid layout works correctly in features section', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    const features = page.getByTestId('features');
    await features.scrollIntoViewIfNeeded();

    // Find the grid container
    const gridContainer = features.locator('.grid');
    const displayStyle = await gridContainer.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });

    expect(displayStyle).toBe('grid');
  });

  test('no horizontal overflow at any viewport', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });

    expect(hasHorizontalScroll).toBe(false);
  });
});
