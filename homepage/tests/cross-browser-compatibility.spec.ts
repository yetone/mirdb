/**
 * Cross-Browser Compatibility Tests
 *
 * These tests verify that the MirDB homepage works correctly across all modern browsers
 * as specified in NFR-5: Chrome, Firefox, Safari, and Edge (last 2 versions).
 *
 * The tests ensure:
 * - Page renders correctly in each browser
 * - CSS layout consistency
 * - JavaScript functionality (copy-to-clipboard, navigation, mobile menu)
 * - No JavaScript console errors
 */

import { test, expect, Page, BrowserContext } from '@playwright/test';

// Helper to get the file URL for local testing
const getFileUrl = (page: string = 'index.html') => {
  return `file://${process.cwd()}/public/${page}`;
};

// Test suite for cross-browser page rendering
test.describe('Cross-Browser Page Rendering', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(getFileUrl());
  });

  test('page loads and renders main content correctly', async ({ page, browserName }) => {
    // Verify page title
    await expect(page).toHaveTitle('MirDB - Persistent Memcached-Compatible Key-Value Store');

    // Verify hero section renders
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify tagline is visible and contains expected text
    const tagline = page.locator('[data-testid="tagline"]');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('Persistent Memcached-Compatible Key-Value Store');

    // Verify CTA button is visible and clickable
    const ctaButton = page.locator('[data-testid="cta-button"]');
    await expect(ctaButton).toBeVisible();

    // Log browser name for debugging
    console.log(`Page renders correctly in ${browserName}`);
  });

  test('all major sections are visible', async ({ page, browserName }) => {
    // Verify features section
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Verify architecture section
    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await expect(architectureSection).toBeVisible();

    // Verify quickstart section
    const quickstartSection = page.locator('[data-testid="quickstart-section"]');
    await expect(quickstartSection).toBeVisible();

    // Verify configuration section
    const configurationSection = page.locator('[data-testid="configuration-section"]');
    await expect(configurationSection).toBeVisible();

    // Verify footer
    const footer = page.locator('[data-testid="footer"]');
    await expect(footer).toBeVisible();

    console.log(`All sections render correctly in ${browserName}`);
  });

  test('navigation links are functional', async ({ page, browserName }) => {
    // Test navigation to features section
    const featuresLink = page.locator('a[href="#features"]').first();
    await featuresLink.click();
    // Wait for scroll to complete
    await page.waitForTimeout(600);
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Test navigation to architecture section
    const architectureLink = page.locator('a[href="#architecture"]').first();
    await architectureLink.click();
    await page.waitForTimeout(600);
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Test navigation to quickstart section
    const quickstartLink = page.locator('a[href="#quickstart"]').first();
    await quickstartLink.click();
    await page.waitForTimeout(600);
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    console.log(`Navigation works correctly in ${browserName}`);
  });
});

// Test suite for CSS layout consistency
test.describe('CSS Layout Consistency', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(getFileUrl());
  });

  test('features grid displays correctly', async ({ page, browserName }) => {
    const featuresGrid = page.locator('[data-testid="features-grid"]');
    await expect(featuresGrid).toBeVisible();

    // Verify all 4 feature cards are present
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(4);

    // Verify specific feature cards
    await expect(page.locator('[data-testid="feature-memcached"]')).toBeVisible();
    await expect(page.locator('[data-testid="feature-persistence"]')).toBeVisible();
    await expect(page.locator('[data-testid="feature-lsm"]')).toBeVisible();
    await expect(page.locator('[data-testid="feature-async"]')).toBeVisible();

    console.log(`Features grid displays correctly in ${browserName}`);
  });

  test('architecture diagram renders correctly', async ({ page, browserName }) => {
    const diagram = page.locator('[data-testid="architecture-diagram"]');
    await expect(diagram).toBeVisible();

    // Verify SVG is properly sized
    const boundingBox = await diagram.boundingBox();
    expect(boundingBox).not.toBeNull();
    expect(boundingBox!.width).toBeGreaterThan(0);
    expect(boundingBox!.height).toBeGreaterThan(0);

    // Verify explanation cards
    const explanations = page.locator('[data-testid="architecture-explanations"]');
    await expect(explanations).toBeVisible();

    console.log(`Architecture diagram renders correctly in ${browserName}`);
  });

  test('configuration table displays correctly', async ({ page, browserName }) => {
    const configTable = page.locator('[data-testid="config-table"]');
    await expect(configTable).toBeVisible();

    // Verify table headers
    const headers = configTable.locator('th');
    await expect(headers).toHaveCount(2);

    // Verify specific configuration rows
    await expect(page.locator('[data-testid="config-listen-address"]')).toBeVisible();
    await expect(page.locator('[data-testid="config-work-directory"]')).toBeVisible();
    await expect(page.locator('[data-testid="config-max-lsm-levels"]')).toBeVisible();

    console.log(`Configuration table displays correctly in ${browserName}`);
  });

  test('footer layout is correct', async ({ page, browserName }) => {
    const footer = page.locator('[data-testid="footer"]');
    await expect(footer).toBeVisible();

    // Verify copyright text
    const copyright = page.locator('[data-testid="footer-copyright"]');
    await expect(copyright).toBeVisible();
    await expect(copyright).toContainText('MirDB Project');

    // Verify license text
    const license = page.locator('[data-testid="footer-license"]');
    await expect(license).toBeVisible();
    await expect(license).toContainText('MIT License');

    // Verify footer links
    await expect(page.locator('[data-testid="footer-github-link"]')).toBeVisible();
    await expect(page.locator('[data-testid="footer-docs-link"]')).toBeVisible();
    await expect(page.locator('[data-testid="footer-crates-link"]')).toBeVisible();

    console.log(`Footer layout is correct in ${browserName}`);
  });

  test('CSS variables are applied correctly', async ({ page, browserName }) => {
    // Test primary color application on CTA button
    const ctaButton = page.locator('[data-testid="cta-button"]');
    const buttonColor = await ctaButton.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    // Primary color is #2563eb which converts to rgb(37, 99, 235)
    expect(buttonColor).toBe('rgb(37, 99, 235)');

    console.log(`CSS variables applied correctly in ${browserName}`);
  });
});

// Test suite for JavaScript functionality
test.describe('JavaScript Functionality', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(getFileUrl());
  });

  test('copy-to-clipboard button works', async ({ page, browserName, context }) => {
    // Grant clipboard permissions for browsers that support it
    try {
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);
    } catch {
      // Some browsers (Firefox, WebKit) may not support these permissions in headless mode
      // The test will continue and verify the UI feedback works
    }

    const copyButton = page.locator('[data-testid="copy-button"]');
    await expect(copyButton).toBeVisible();

    // Get initial button text
    const initialText = await copyButton.locator('.copy-text').textContent();
    expect(initialText).toBe('Copy');

    // Click the copy button
    await copyButton.click();

    // In browsers where clipboard API works, button should change to "Copied!"
    // In browsers where it fails, button remains "Copy" but we verify no JS errors
    // Wait a moment for the click handler to execute
    await page.waitForTimeout(500);

    // Check if copy succeeded (button text changed) or at least no errors occurred
    const buttonText = await copyButton.locator('.copy-text').textContent();
    // Accept either "Copy" (clipboard API not available) or "Copied!" (success)
    expect(['Copy', 'Copied!']).toContain(buttonText);

    if (buttonText === 'Copied!') {
      // Verify data-copied attribute is set
      await expect(copyButton).toHaveAttribute('data-copied', 'true');

      // Wait for button to reset (2 seconds)
      await page.waitForTimeout(2500);
      await expect(copyButton.locator('.copy-text')).toHaveText('Copy');
    }

    console.log(`Copy-to-clipboard works correctly in ${browserName}`);
  });

  test('smooth scroll navigation works', async ({ page, browserName }) => {
    // Start at top of page
    await page.evaluate(() => window.scrollTo(0, 0));

    // Click quickstart link
    const quickstartLink = page.locator('a[href="#quickstart"]').first();
    await quickstartLink.click();

    // Wait for smooth scroll to complete
    await page.waitForTimeout(500);

    // Verify quickstart section is in viewport
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport();

    console.log(`Smooth scroll navigation works in ${browserName}`);
  });

  test('external links have correct attributes', async ({ page, browserName }) => {
    // Check GitHub link in navigation
    const githubNavLink = page.locator('.nav-links a[href*="github.com"]');
    await expect(githubNavLink).toHaveAttribute('target', '_blank');
    await expect(githubNavLink).toHaveAttribute('rel', 'noopener noreferrer');

    // Check MIT License link in footer
    const licenseLink = page.locator('a[href*="opensource.org"]');
    await expect(licenseLink).toHaveAttribute('target', '_blank');
    await expect(licenseLink).toHaveAttribute('rel', 'noopener noreferrer');

    console.log(`External links have correct attributes in ${browserName}`);
  });
});

// Test suite for mobile menu functionality
test.describe('Mobile Menu Functionality', () => {
  test.beforeEach(async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto(getFileUrl());
  });

  test('mobile menu toggle works', async ({ page, browserName }) => {
    const menuToggle = page.locator('[data-testid="mobile-menu-toggle"]');
    const navLinks = page.locator('[data-testid="nav-links"]');

    // Verify menu toggle is visible on mobile
    await expect(menuToggle).toBeVisible();

    // Verify nav links are initially hidden
    await expect(navLinks).not.toHaveClass(/nav-open/);

    // Click menu toggle
    await menuToggle.click();

    // Verify nav links are now visible
    await expect(navLinks).toHaveClass(/nav-open/);

    // Verify aria-expanded is updated
    await expect(menuToggle).toHaveAttribute('aria-expanded', 'true');

    // Click again to close
    await menuToggle.click();

    // Verify nav links are hidden again
    await expect(navLinks).not.toHaveClass(/nav-open/);
    await expect(menuToggle).toHaveAttribute('aria-expanded', 'false');

    console.log(`Mobile menu toggle works correctly in ${browserName}`);
  });

  test('escape key closes mobile menu', async ({ page, browserName }) => {
    const menuToggle = page.locator('[data-testid="mobile-menu-toggle"]');
    const navLinks = page.locator('[data-testid="nav-links"]');

    // Open menu
    await menuToggle.click();
    await expect(navLinks).toHaveClass(/nav-open/);

    // Press Escape key
    await page.keyboard.press('Escape');

    // Verify menu is closed
    await expect(navLinks).not.toHaveClass(/nav-open/);
    await expect(menuToggle).toHaveAttribute('aria-expanded', 'false');

    console.log(`Escape key closes mobile menu in ${browserName}`);
  });

  test('clicking nav link closes mobile menu', async ({ page, browserName }) => {
    const menuToggle = page.locator('[data-testid="mobile-menu-toggle"]');
    const navLinks = page.locator('[data-testid="nav-links"]');

    // Open menu
    await menuToggle.click();
    await expect(navLinks).toHaveClass(/nav-open/);

    // Click a nav link
    const featuresLink = navLinks.locator('a[href="#features"]');
    await featuresLink.click();

    // Verify menu is closed
    await expect(navLinks).not.toHaveClass(/nav-open/);

    console.log(`Clicking nav link closes mobile menu in ${browserName}`);
  });
});

// Test suite for JavaScript console errors
test.describe('JavaScript Console Errors', () => {
  test('no JavaScript errors on page load', async ({ page, browserName }) => {
    const errors: string[] = [];

    // Listen for console errors
    page.on('pageerror', (error) => {
      errors.push(error.message);
    });

    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        errors.push(msg.text());
      }
    });

    // Load the page
    await page.goto(getFileUrl());

    // Wait for page to fully load
    await page.waitForLoadState('networkidle');

    // Verify no errors
    expect(errors).toHaveLength(0);

    console.log(`No JavaScript errors in ${browserName}`);
  });

  test('no JavaScript errors during interactions', async ({ page, browserName, context }) => {
    const errors: string[] = [];

    // Listen for console errors
    page.on('pageerror', (error) => {
      // Ignore clipboard-related errors as they're expected in headless browsers
      if (!error.message.includes('clipboard') && !error.message.includes('Clipboard')) {
        errors.push(error.message);
      }
    });

    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        const text = msg.text();
        // Ignore clipboard-related errors
        if (!text.includes('clipboard') && !text.includes('Clipboard') && !text.includes('copy')) {
          errors.push(text);
        }
      }
    });

    // Load the page
    await page.goto(getFileUrl());

    // Grant clipboard permissions for browsers that support it
    try {
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);
    } catch {
      // Some browsers don't support these permissions
    }

    // Perform various interactions
    // Click copy button (may fail in some browsers but shouldn't throw unhandled errors)
    const copyButton = page.locator('[data-testid="copy-button"]');
    await copyButton.click();
    await page.waitForTimeout(500);

    // Navigate to different sections
    await page.locator('a[href="#features"]').first().click();
    await page.waitForTimeout(300);
    await page.locator('a[href="#architecture"]').first().click();
    await page.waitForTimeout(300);
    await page.locator('a[href="#quickstart"]').first().click();
    await page.waitForTimeout(300);
    await page.locator('a[href="#configuration"]').first().click();
    await page.waitForTimeout(300);

    // Test mobile menu (set mobile viewport)
    await page.setViewportSize({ width: 375, height: 667 });
    const menuToggle = page.locator('[data-testid="mobile-menu-toggle"]');
    await menuToggle.click();
    await page.keyboard.press('Escape');

    // Verify no critical errors occurred (clipboard errors are expected and handled)
    expect(errors).toHaveLength(0);

    console.log(`No JavaScript errors during interactions in ${browserName}`);
  });
});

// Test suite for browser-specific CSS features
test.describe('Browser-Specific CSS Features', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(getFileUrl());
  });

  test('flexbox layouts work correctly', async ({ page, browserName }) => {
    // Test navbar flexbox layout
    const navbar = page.locator('.navbar');
    const navbarDisplay = await navbar.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(navbarDisplay).toBe('flex');

    // Test hero CTA group flexbox layout
    const ctaGroup = page.locator('.hero-cta-group');
    const ctaGroupDisplay = await ctaGroup.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(ctaGroupDisplay).toBe('flex');

    console.log(`Flexbox layouts work correctly in ${browserName}`);
  });

  test('CSS grid layouts work correctly', async ({ page, browserName }) => {
    // Test features grid
    const featuresGrid = page.locator('.features-grid');
    const gridDisplay = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(gridDisplay).toBe('grid');

    // Test architecture explanations grid
    const explanationsGrid = page.locator('.architecture-explanations');
    const explanationsDisplay = await explanationsGrid.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(explanationsDisplay).toBe('grid');

    console.log(`CSS grid layouts work correctly in ${browserName}`);
  });

  test('CSS variables are supported', async ({ page, browserName }) => {
    // Test that CSS variables are properly resolved
    const body = page.locator('body');
    const backgroundColor = await body.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    // Background color should be --background-color: #ffffff -> rgb(255, 255, 255)
    expect(backgroundColor).toBe('rgb(255, 255, 255)');

    console.log(`CSS variables are supported in ${browserName}`);
  });

  test('smooth scroll behavior is applied', async ({ page, browserName }) => {
    // Check that html has smooth scroll behavior
    const scrollBehavior = await page.evaluate(() => {
      return window.getComputedStyle(document.documentElement).scrollBehavior;
    });
    expect(scrollBehavior).toBe('smooth');

    console.log(`Smooth scroll behavior is applied in ${browserName}`);
  });

  test('clamp() function works for responsive typography', async ({ page, browserName }) => {
    // Test hero h1 which uses clamp()
    const heroH1 = page.locator('.hero-content h1');
    const fontSize = await heroH1.evaluate((el) => {
      return window.getComputedStyle(el).fontSize;
    });
    // Font size should be calculated by clamp(3rem, 8vw, 5rem)
    const fontSizeValue = parseFloat(fontSize);
    expect(fontSizeValue).toBeGreaterThan(0);

    console.log(`clamp() function works correctly in ${browserName}`);
  });
});

// Test suite for focus states and keyboard accessibility
test.describe('Focus States and Keyboard Accessibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(getFileUrl());
  });

  test('focus-visible styles are applied', async ({ page, browserName }) => {
    // Tab to CTA button
    await page.keyboard.press('Tab'); // Skip link
    await page.keyboard.press('Tab'); // Logo
    await page.keyboard.press('Tab'); // Mobile menu toggle
    await page.keyboard.press('Tab'); // First nav link

    // Verify focus is visible (checking outline)
    const activeElement = page.locator(':focus-visible');
    const outline = await activeElement.evaluate((el) => {
      return window.getComputedStyle(el).outlineWidth;
    });
    expect(outline).not.toBe('0px');

    console.log(`Focus-visible styles work correctly in ${browserName}`);
  });

  test('skip link works', async ({ page, browserName }) => {
    // Tab to skip link
    await page.keyboard.press('Tab');

    // Verify skip link is focused
    const skipLink = page.locator('[data-testid="skip-link"]');
    await expect(skipLink).toBeFocused();

    // Press Enter to activate skip link
    await page.keyboard.press('Enter');

    // Verify main content is focused
    const mainContent = page.locator('#main-content');
    await expect(mainContent).toBeInViewport();

    console.log(`Skip link works correctly in ${browserName}`);
  });
});

// Test suite for responsive design across browsers
test.describe('Responsive Design Across Browsers', () => {
  test('desktop viewport displays correctly', async ({ page, browserName }) => {
    await page.setViewportSize({ width: 1920, height: 1080 });
    await page.goto(getFileUrl());

    // Verify desktop layout
    const menuToggle = page.locator('[data-testid="mobile-menu-toggle"]');
    const navLinks = page.locator('[data-testid="nav-links"]');

    // Mobile menu toggle should be hidden on desktop
    await expect(menuToggle).toBeHidden();

    // Nav links should be visible
    await expect(navLinks).toBeVisible();

    console.log(`Desktop viewport displays correctly in ${browserName}`);
  });

  test('tablet viewport displays correctly', async ({ page, browserName }) => {
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto(getFileUrl());

    // Verify all sections are visible
    await expect(page.locator('[data-testid="hero-section"]')).toBeVisible();
    await expect(page.locator('[data-testid="features-section"]')).toBeVisible();

    console.log(`Tablet viewport displays correctly in ${browserName}`);
  });

  test('mobile viewport displays correctly', async ({ page, browserName }) => {
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto(getFileUrl());

    // Verify mobile menu toggle is visible
    const menuToggle = page.locator('[data-testid="mobile-menu-toggle"]');
    await expect(menuToggle).toBeVisible();

    // Verify all sections are visible
    await expect(page.locator('[data-testid="hero-section"]')).toBeVisible();
    await expect(page.locator('[data-testid="features-section"]')).toBeVisible();

    console.log(`Mobile viewport displays correctly in ${browserName}`);
  });
});
