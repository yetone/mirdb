/**
 * JavaScript Interactivity E2E Tests
 * Owner: Scenario 17 - JavaScript Interactivity
 *
 * Tests:
 * - Copy button functionality (clipboard + visual feedback)
 * - Mobile hamburger menu toggle
 * - Smooth scroll for anchor navigation
 * - JS file size under 10KB
 * - No console errors on page load
 */

const { test, expect } = require('@playwright/test');
const { loadPage } = require('../utils/test-helpers');
const fs = require('fs');
const path = require('path');

// Mobile viewport dimensions
const MOBILE_VIEWPORT = { width: 375, height: 667 };

// Desktop viewport dimensions
const DESKTOP_VIEWPORT = { width: 1280, height: 800 };

test.describe('Scenario 17: JavaScript Interactivity', () => {
  test.describe('Test Case 1: Copy-to-clipboard functionality', () => {
    test('clicking copy button shows visual feedback', async ({ page }) => {
      await loadPage(page, DESKTOP_VIEWPORT);

      // Scroll to quick start section
      await page.locator('#quickstart').scrollIntoViewIfNeeded();

      // Find copy buttons
      const copyButtons = page.locator('.copy-btn');
      const count = await copyButtons.count();
      expect(count).toBeGreaterThan(0);

      // Get the first copy button
      const firstCopyBtn = copyButtons.first();
      await expect(firstCopyBtn).toBeVisible();

      // Get original text
      const originalText = await firstCopyBtn.textContent();
      expect(originalText).toBe('Copy');

      // Click the copy button
      await firstCopyBtn.click();

      // Should show "Copied!" feedback
      await expect(firstCopyBtn).toHaveText('Copied!');

      // Should have copy-success class and data-copied attribute
      await expect(firstCopyBtn).toHaveClass(/copy-success/);
      await expect(firstCopyBtn).toHaveAttribute('data-copied', 'true');

      // After timeout, should revert (wait up to 2.5s)
      await expect(firstCopyBtn).toHaveText('Copy', { timeout: 3000 });
    });

    test('copy button copies correct code content', async ({ page, context }) => {
      // Grant clipboard permissions
      await context.grantPermissions(['clipboard-read', 'clipboard-write']);

      await loadPage(page, DESKTOP_VIEWPORT);
      await page.locator('#quickstart').scrollIntoViewIfNeeded();

      // Get the first code block's content
      const codeContainer = page.locator('.code-container').first();
      const codeContent = await codeContainer.locator('pre code').textContent();

      // Click copy
      const copyBtn = codeContainer.locator('.copy-btn');
      await copyBtn.click();

      // Wait for copy to complete
      await expect(copyBtn).toHaveText('Copied!');

      // Read clipboard content
      const clipboardContent = await page.evaluate(async () => {
        return await navigator.clipboard.readText();
      });

      // Verify clipboard has the code content
      expect(clipboardContent).toBe(codeContent);
    });
  });

  test.describe('Test Case 2: Mobile hamburger menu toggle', () => {
    test('hamburger menu toggles navigation visibility', async ({ page }) => {
      await loadPage(page, MOBILE_VIEWPORT);

      const hamburger = page.locator('.hamburger');
      const navLinks = page.locator('.nav-links');

      // Hamburger should be visible on mobile
      await expect(hamburger).toBeVisible();

      // Nav links should be hidden initially
      await expect(navLinks).not.toBeVisible();

      // Click hamburger to open menu
      await hamburger.click();

      // Nav links should now be visible
      await expect(navLinks).toBeVisible();
      await expect(navLinks).toHaveClass(/nav-open/);
      await expect(hamburger).toHaveAttribute('aria-expanded', 'true');

      // Click hamburger again to close
      await hamburger.click();

      // Nav links should be hidden again
      await expect(navLinks).not.toBeVisible();
      await expect(hamburger).toHaveAttribute('aria-expanded', 'false');
    });

    test('clicking nav link closes mobile menu', async ({ page }) => {
      await loadPage(page, MOBILE_VIEWPORT);

      const hamburger = page.locator('.hamburger');
      const navLinks = page.locator('.nav-links');

      // Open the menu
      await hamburger.click();
      await expect(navLinks).toBeVisible();

      // Click a nav link
      const featuresLink = navLinks.locator('a[href="#features"]');
      await featuresLink.click();

      // Menu should close
      await expect(navLinks).not.toBeVisible();
      await expect(hamburger).toHaveAttribute('aria-expanded', 'false');
    });

    test('clicking outside menu closes it', async ({ page }) => {
      await loadPage(page, MOBILE_VIEWPORT);

      const hamburger = page.locator('.hamburger');
      const navLinks = page.locator('.nav-links');

      // Open the menu
      await hamburger.click();
      await expect(navLinks).toBeVisible();

      // Click outside the menu on the main content area
      // Use page.mouse.click at a specific coordinate that's outside the menu
      // The menu is typically positioned at the top, so click lower on the page
      await page.mouse.click(187, 500); // Middle of screen, below nav area

      // Menu should close
      await expect(navLinks).not.toBeVisible();
    });
  });

  test.describe('Test Case 3: Smooth scroll behavior', () => {
    test('nav link scrolls to target section', async ({ page }) => {
      await loadPage(page, DESKTOP_VIEWPORT);

      // Get initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY);

      // Click on features link
      const featuresLink = page.locator('.nav-links a[href="#features"]');
      await featuresLink.click();

      // Wait for scroll to happen
      await page.waitForTimeout(500);

      // Check that page scrolled
      const afterScrollY = await page.evaluate(() => window.scrollY);
      expect(afterScrollY).toBeGreaterThan(initialScrollY);

      // Features section should be near top of viewport
      const featuresSection = page.locator('#features');
      const box = await featuresSection.boundingBox();
      expect(box.y).toBeLessThan(100); // Should be near top
    });

    test('smooth scroll respects prefers-reduced-motion', async ({ page }) => {
      // Emulate reduced motion preference
      await page.emulateMedia({ reducedMotion: 'reduce' });
      await loadPage(page, DESKTOP_VIEWPORT);

      // Click on architecture link
      const archLink = page.locator('.nav-links a[href="#architecture"]');

      // Get scroll position before click
      const beforeY = await page.evaluate(() => window.scrollY);

      await archLink.click();

      // Wait a tiny bit for instant scroll
      await page.waitForTimeout(50);

      // With reduced motion, scroll should be instant (already at destination)
      const afterY = await page.evaluate(() => window.scrollY);
      expect(afterY).toBeGreaterThan(beforeY);

      // Architecture section should be visible
      const archSection = page.locator('#architecture');
      await expect(archSection).toBeInViewport();
    });

    test('URL hash updates after scroll', async ({ page }) => {
      await loadPage(page, DESKTOP_VIEWPORT);

      // Click on quickstart link
      const quickstartLink = page.locator('.nav-links a[href="#quickstart"]');
      await quickstartLink.click();

      // Wait for scroll
      await page.waitForTimeout(600);

      // URL should contain the hash
      expect(page.url()).toContain('#quickstart');
    });
  });

  test.describe('Test Case 4: JavaScript file size', () => {
    test('main.js file is under 10KB', async () => {
      const jsFilePath = path.resolve(__dirname, '../../docs/js/main.js');
      const stats = fs.statSync(jsFilePath);
      const sizeInKB = stats.size / 1024;

      // File should be under 10KB
      expect(sizeInKB).toBeLessThan(10);

      // Also verify it's a reasonable size (not empty)
      expect(stats.size).toBeGreaterThan(500); // At least 500 bytes
    });

    test('main.js has no external dependencies', async () => {
      const jsFilePath = path.resolve(__dirname, '../../docs/js/main.js');
      const content = fs.readFileSync(jsFilePath, 'utf-8');

      // Should not have import or require statements for external modules
      expect(content).not.toMatch(/import\s+.*from\s+['"][^.]/);
      expect(content).not.toMatch(/require\s*\(\s*['"][^.]/);

      // Should not reference CDN or external URLs
      expect(content).not.toMatch(/https?:\/\//);
    });
  });

  test.describe('Test Case 5: No console errors on load', () => {
    test('page loads without JavaScript console errors', async ({ page }) => {
      const consoleErrors = [];
      const consoleWarnings = [];

      // Listen for console events
      page.on('console', (msg) => {
        if (msg.type() === 'error') {
          consoleErrors.push(msg.text());
        } else if (msg.type() === 'warning') {
          consoleWarnings.push(msg.text());
        }
      });

      // Listen for page errors (uncaught exceptions)
      const pageErrors = [];
      page.on('pageerror', (error) => {
        pageErrors.push(error.message);
      });

      // Load the page
      await loadPage(page, DESKTOP_VIEWPORT);

      // Wait for any async operations to complete
      await page.waitForTimeout(1000);

      // Should have no JavaScript errors
      expect(consoleErrors).toHaveLength(0);
      expect(pageErrors).toHaveLength(0);
    });

    test('all interactive elements work without errors', async ({ page }) => {
      const errors = [];

      page.on('console', (msg) => {
        if (msg.type() === 'error') {
          errors.push(msg.text());
        }
      });

      page.on('pageerror', (error) => {
        errors.push(error.message);
      });

      await loadPage(page, DESKTOP_VIEWPORT);

      // Test copy button interaction
      await page.locator('#quickstart').scrollIntoViewIfNeeded();
      const copyBtn = page.locator('.copy-btn').first();
      await copyBtn.click();

      // Test navigation scroll
      const navLink = page.locator('.nav-links a[href="#features"]');
      await navLink.click();
      await page.waitForTimeout(500);

      // Should still have no errors
      expect(errors).toHaveLength(0);
    });

    test('mobile interactions work without errors', async ({ page }) => {
      const errors = [];

      page.on('console', (msg) => {
        if (msg.type() === 'error') {
          errors.push(msg.text());
        }
      });

      page.on('pageerror', (error) => {
        errors.push(error.message);
      });

      await loadPage(page, MOBILE_VIEWPORT);

      // Test hamburger menu
      const hamburger = page.locator('.hamburger');
      await hamburger.click();
      await page.waitForTimeout(100);
      await hamburger.click();

      // Should have no errors
      expect(errors).toHaveLength(0);
    });
  });
});
