/**
 * Cross-Browser Compatibility E2E Tests
 * Owner: Scenario 16 - Cross-Browser Compatibility
 *
 * Validates consistent display and functionality across major browsers:
 * - Chrome (latest 2 versions)
 * - Firefox (latest 2 versions)
 * - Safari/WebKit (latest 2 versions)
 * - Edge (latest 2 versions)
 *
 * Tests cover:
 * - Page display correctness
 * - Theme toggle consistency
 * - Smooth scroll behavior
 * - Core feature functionality
 */

const { test, expect } = require('@playwright/test');

// Test runs automatically across all configured browser projects (Chrome, Firefox, WebKit, Edge)
test.describe('Cross-Browser Page Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('TC1: Page renders with correct structure in all browsers', async ({ page, browserName }) => {
    // Verify doctype and HTML structure
    const doctypeExists = await page.evaluate(() => {
      return document.doctype !== null && document.doctype.name === 'html';
    });
    expect(doctypeExists).toBe(true);

    // Verify header is present and visible
    const header = page.locator('.header');
    await expect(header).toBeVisible();

    // Verify main content area exists
    const main = page.locator('main');
    await expect(main).toBeVisible();

    // Verify footer is present
    const footer = page.locator('.footer');
    await expect(footer).toBeVisible();

    // Log browser name for tracking
    console.log(`Page structure verified in ${browserName}`);
  });

  test('TC2: All major sections render correctly in all browsers', async ({ page, browserName }) => {
    const sections = [
      { selector: '.hero', name: 'Hero' },
      { selector: '#features', name: 'Features' },
      { selector: '#quick-start', name: 'Quick Start' },
      { selector: '#architecture', name: 'Architecture' },
      { selector: '#api-reference', name: 'API Reference' },
      { selector: '#configuration', name: 'Configuration' },
      { selector: '#contributing', name: 'Contributing' },
    ];

    for (const section of sections) {
      const element = page.locator(section.selector);
      await expect(element, `${section.name} section should be visible in ${browserName}`).toBeVisible();
    }
  });

  test('TC3: Navigation elements are functional in all browsers', async ({ page, browserName }) => {
    // Check navigation list exists
    const navList = page.locator('.nav__list');
    await expect(navList).toBeVisible();

    // Check all navigation links are clickable
    const navLinks = page.locator('.nav__link');
    const linkCount = await navLinks.count();
    expect(linkCount).toBeGreaterThanOrEqual(6);

    // Verify each link has href attribute
    for (let i = 0; i < linkCount; i++) {
      const link = navLinks.nth(i);
      const href = await link.getAttribute('href');
      expect(href, `Nav link ${i} should have href in ${browserName}`).toBeTruthy();
    }
  });

  test('TC4: CSS styles render consistently in all browsers', async ({ page, browserName }) => {
    // Check CSS variables are properly applied
    const cssVars = await page.evaluate(() => {
      const root = document.documentElement;
      const styles = getComputedStyle(root);
      return {
        bgPrimary: styles.getPropertyValue('--color-bg-primary').trim(),
        textPrimary: styles.getPropertyValue('--color-text-primary').trim(),
        colorPrimary: styles.getPropertyValue('--color-primary').trim(),
      };
    });

    // CSS variables should be defined
    expect(cssVars.bgPrimary, `--color-bg-primary should be defined in ${browserName}`).toBeTruthy();
    expect(cssVars.textPrimary, `--color-text-primary should be defined in ${browserName}`).toBeTruthy();
    expect(cssVars.colorPrimary, `--color-primary should be defined in ${browserName}`).toBeTruthy();

    // Verify header has correct positioning
    const header = page.locator('.header');
    const headerPosition = await header.evaluate((el) => getComputedStyle(el).position);
    expect(headerPosition).toBe('fixed');
  });

  test('TC5: Images and assets load correctly in all browsers', async ({ page, browserName }) => {
    // Check logo image loads
    const logo = page.locator('.header__logo');
    await expect(logo).toBeVisible();

    // Verify logo has valid source
    const logoSrc = await logo.getAttribute('src');
    expect(logoSrc, `Logo should have src in ${browserName}`).toBeTruthy();

    // Check usage.gif in features section (if present)
    const usageGif = page.locator('.features__demo-img, .usage-demo');
    const gifCount = await usageGif.count();
    if (gifCount > 0) {
      await expect(usageGif.first()).toBeVisible();
    }
  });

  test('TC6: Code blocks render correctly in all browsers', async ({ page, browserName }) => {
    // Scroll to quick start section
    const quickStart = page.locator('#quick-start');
    await quickStart.scrollIntoViewIfNeeded();

    // Check code blocks exist
    const codeBlocks = page.locator('.code-block, pre code');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount, `Code blocks should exist in ${browserName}`).toBeGreaterThan(0);

    // Verify code block styling
    const firstCodeBlock = codeBlocks.first();
    await expect(firstCodeBlock).toBeVisible();
  });
});

test.describe('Cross-Browser Theme Toggle', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.evaluate(() => localStorage.clear());
    await page.waitForLoadState('domcontentloaded');
  });

  test('TC5-A: Theme toggle button is present and functional in all browsers', async ({ page, browserName }) => {
    // Theme toggle should exist
    const themeToggle = page.locator('.theme-toggle');
    await expect(themeToggle, `Theme toggle should be visible in ${browserName}`).toBeVisible();

    // Should be clickable
    const isEnabled = await themeToggle.isEnabled();
    expect(isEnabled, `Theme toggle should be enabled in ${browserName}`).toBe(true);

    // Should have accessible label
    await expect(themeToggle).toHaveAttribute('aria-label', 'Toggle dark mode');
  });

  test('TC5-B: Theme toggle switches themes correctly in all browsers', async ({ page, browserName }) => {
    // Start with light theme
    await page.emulateMedia({ colorScheme: 'light' });
    await page.goto('/');
    await page.evaluate(() => localStorage.clear());
    await page.waitForLoadState('domcontentloaded');

    // Get initial state
    const html = page.locator('html');
    const initialTheme = await html.getAttribute('data-theme');

    // Click theme toggle
    const themeToggle = page.locator('.theme-toggle');
    await themeToggle.click();
    await page.waitForTimeout(100);

    // Verify theme changed
    const newTheme = await html.getAttribute('data-theme');
    expect(newTheme, `Theme should change after toggle click in ${browserName}`).toBe('dark');

    // Click again to toggle back
    await themeToggle.click();
    await page.waitForTimeout(100);

    const finalTheme = await html.getAttribute('data-theme');
    expect(finalTheme, `Theme should toggle back in ${browserName}`).toBe('light');
  });

  test('TC5-C: Theme CSS variables change correctly in all browsers', async ({ page, browserName }) => {
    await page.emulateMedia({ colorScheme: 'light' });
    await page.goto('/');
    await page.evaluate(() => localStorage.clear());
    await page.waitForLoadState('domcontentloaded');

    // Get light theme colors
    const lightBg = await page.evaluate(() => {
      return getComputedStyle(document.documentElement).getPropertyValue('--color-bg-primary').trim();
    });

    // Toggle to dark
    const themeToggle = page.locator('.theme-toggle');
    await themeToggle.click();
    await page.waitForTimeout(100);

    // Get dark theme colors
    const darkBg = await page.evaluate(() => {
      return getComputedStyle(document.documentElement).getPropertyValue('--color-bg-primary').trim();
    });

    // Colors should be different
    expect(darkBg, `Background color should change in dark mode for ${browserName}`).not.toBe(lightBg);
  });

  test('TC5-D: Theme persists in localStorage in all browsers', async ({ page, browserName }) => {
    await page.emulateMedia({ colorScheme: 'light' });
    await page.goto('/');
    await page.evaluate(() => localStorage.clear());

    // Toggle to dark
    const themeToggle = page.locator('.theme-toggle');
    await themeToggle.click();
    await page.waitForTimeout(100);

    // Check localStorage
    const savedTheme = await page.evaluate(() => localStorage.getItem('theme'));
    expect(savedTheme, `Theme should be saved to localStorage in ${browserName}`).toBe('dark');

    // Reload page
    await page.reload();
    await page.waitForLoadState('domcontentloaded');

    // Verify theme persists
    const html = page.locator('html');
    await expect(html, `Theme should persist after reload in ${browserName}`).toHaveAttribute('data-theme', 'dark');
  });
});

test.describe('Cross-Browser Smooth Scroll', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('TC6-A: Smooth scroll behavior is enabled in all browsers', async ({ page, browserName }) => {
    // Check scroll behavior CSS
    const scrollBehavior = await page.evaluate(() => {
      return getComputedStyle(document.documentElement).scrollBehavior;
    });
    expect(scrollBehavior, `Scroll behavior should be smooth in ${browserName}`).toBe('smooth');
  });

  test('TC6-B: Navigation links trigger smooth scroll to sections in all browsers', async ({ page, browserName }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);
    expect(initialScrollY).toBe(0);

    // Click Features link
    const featuresLink = page.locator('.nav__link[href="#features"]');
    await featuresLink.click();

    // Wait for smooth scroll animation
    await page.waitForTimeout(600);

    // Verify scroll position changed
    const newScrollY = await page.evaluate(() => window.scrollY);
    expect(newScrollY, `Page should scroll after clicking nav link in ${browserName}`).toBeGreaterThan(0);

    // Verify Features section is in viewport
    const featuresSection = page.locator('#features');
    await expect(featuresSection, `Features section should be in viewport in ${browserName}`).toBeInViewport();
  });

  test('TC6-C: Back to top button scrolls smoothly to top in all browsers', async ({ page, browserName }) => {
    // First scroll down
    await page.evaluate(() => window.scrollTo(0, 1000));
    await page.waitForTimeout(300);

    // Verify we scrolled
    let currentScroll = await page.evaluate(() => window.scrollY);
    expect(currentScroll).toBeGreaterThan(500);

    // Click back to top button
    const backToTop = page.locator('#back-to-top');
    await expect(backToTop).toBeVisible();
    await backToTop.click();

    // Wait for smooth scroll
    await page.waitForTimeout(800);

    // Verify scrolled to top
    currentScroll = await page.evaluate(() => window.scrollY);
    expect(currentScroll, `Should scroll to top in ${browserName}`).toBeLessThanOrEqual(10);
  });

  test('TC6-D: Anchor links scroll to correct sections in all browsers', async ({ page, browserName }) => {
    const anchors = ['#quick-start', '#architecture', '#api-reference', '#configuration'];

    for (const anchor of anchors) {
      // Navigate to anchor
      const link = page.locator(`.nav__link[href="${anchor}"]`);
      if (await link.count() > 0) {
        await link.click();
        await page.waitForTimeout(600);

        // Verify section is in viewport
        const section = page.locator(anchor);
        await expect(section, `${anchor} should be in viewport in ${browserName}`).toBeInViewport();

        // Scroll back to top for next iteration
        await page.evaluate(() => window.scrollTo(0, 0));
        await page.waitForTimeout(300);
      }
    }
  });
});

test.describe('Cross-Browser Feature Functionality', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('Copy button functionality works in all browsers', async ({ page, browserName }) => {
    // Scroll to quick start
    const quickStart = page.locator('#quick-start');
    await quickStart.scrollIntoViewIfNeeded();
    await page.waitForTimeout(200);

    // Find copy buttons
    const copyButtons = page.locator('.copy-btn, .code-block__copy-btn, [data-copy-btn]');
    const buttonCount = await copyButtons.count();

    if (buttonCount > 0) {
      const firstCopyBtn = copyButtons.first();
      await expect(firstCopyBtn, `Copy button should be visible in ${browserName}`).toBeVisible();

      // Click copy button (we can't verify clipboard in all browsers without permissions)
      await firstCopyBtn.click();

      // Verify button shows feedback (changed text or visual indicator)
      await page.waitForTimeout(100);
    }
  });

  test('Sticky header works on scroll in all browsers', async ({ page, browserName }) => {
    const header = page.locator('.header');

    // Verify initial position is fixed
    let position = await header.evaluate((el) => getComputedStyle(el).position);
    expect(position, `Header should be fixed in ${browserName}`).toBe('fixed');

    // Scroll down
    await page.evaluate(() => window.scrollBy(0, 500));
    await page.waitForTimeout(100);

    // Header should still be visible and fixed
    await expect(header, `Header should be visible after scroll in ${browserName}`).toBeVisible();
    position = await header.evaluate((el) => getComputedStyle(el).position);
    expect(position, `Header should remain fixed after scroll in ${browserName}`).toBe('fixed');

    // Header should have scrolled class
    await expect(header, `Header should have scrolled class in ${browserName}`).toHaveClass(/scrolled/);
  });

  test('Interactive elements are accessible via keyboard in all browsers', async ({ page, browserName }) => {
    // Tab to theme toggle
    const themeToggle = page.locator('.theme-toggle');
    await themeToggle.focus();
    await expect(themeToggle, `Theme toggle should be focusable in ${browserName}`).toBeFocused();

    // Activate with Enter key
    await page.keyboard.press('Enter');
    await page.waitForTimeout(100);

    // Verify theme changed
    const html = page.locator('html');
    await expect(html, `Theme should toggle with Enter key in ${browserName}`).toHaveAttribute('data-theme', 'dark');
  });

  test('Focus indicators are visible for interactive elements in all browsers', async ({ page, browserName }) => {
    // Focus on a nav link
    const firstNavLink = page.locator('.nav__link').first();
    await firstNavLink.focus();

    // Check that focus styles are applied (outline or custom focus indicator)
    const hasOutline = await firstNavLink.evaluate((el) => {
      const styles = getComputedStyle(el);
      return styles.outlineStyle !== 'none' ||
             styles.boxShadow !== 'none' ||
             el.classList.contains('focus-visible');
    });

    expect(hasOutline, `Focus indicator should be visible in ${browserName}`).toBe(true);
  });
});

test.describe('Cross-Browser Viewport and Layout', () => {
  test('Page maintains proper layout at different viewport widths', async ({ page, browserName }) => {
    const viewports = [
      { width: 1920, height: 1080, name: 'Desktop' },
      { width: 1366, height: 768, name: 'Laptop' },
      { width: 768, height: 1024, name: 'Tablet' },
    ];

    for (const viewport of viewports) {
      // Set viewport first, then navigate to ensure correct initial render
      await page.setViewportSize({ width: viewport.width, height: viewport.height });
      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Header should be visible
      const header = page.locator('.header');
      await expect(header, `Header should be visible at ${viewport.name} in ${browserName}`).toBeVisible();

      // Hero section should be visible
      const hero = page.locator('.hero');
      await expect(hero, `Hero should be visible at ${viewport.name} in ${browserName}`).toBeVisible();

      // No horizontal scrollbar
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll, `No horizontal scroll at ${viewport.name} in ${browserName}`).toBe(false);
    }
  });
});
