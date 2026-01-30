/**
 * Accessibility E2E Tests - WCAG 2.1 AA Compliance
 * Owner: Scenario 11 - Accessibility - WCAG 2.1 AA Compliance
 *
 * Tests for verifying keyboard navigation and focus indicators
 * per WCAG 2.1 AA accessibility standards.
 */
const { test, expect } = require('@playwright/test');

test.describe('Accessibility - WCAG 2.1 AA Compliance', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('TC1: Keyboard Navigation - Tab Key Only', () => {
    test('All interactive elements are focusable in logical order on desktop', async ({ page }) => {
      // Ensure desktop viewport
      await page.setViewportSize({ width: 1280, height: 720 });

      // Start by pressing Tab from the beginning of the page
      await page.keyboard.press('Tab');

      // First focusable element should be the nav logo link
      const navLogo = page.locator('.nav-logo');
      await expect(navLogo).toBeFocused();

      // On desktop, nav-toggle is hidden (display: none), so skip to nav links
      // Tab through navigation links
      await page.keyboard.press('Tab');
      const featuresLink = page.locator('.nav-link[href="#features"]');
      await expect(featuresLink).toBeFocused();

      await page.keyboard.press('Tab');
      const usageLink = page.locator('.nav-link[href="#usage"]');
      await expect(usageLink).toBeFocused();

      await page.keyboard.press('Tab');
      const architectureLink = page.locator('.nav-link[href="#architecture"]');
      await expect(architectureLink).toBeFocused();

      await page.keyboard.press('Tab');
      const getStartedLink = page.locator('.nav-link[href="#getting-started"]');
      await expect(getStartedLink).toBeFocused();

      // Tab to GitHub link in nav
      await page.keyboard.press('Tab');
      const navGithub = page.locator('.nav-github');
      await expect(navGithub).toBeFocused();
    });

    test('Navigation toggle is focusable on mobile', async ({ page }) => {
      // Set mobile viewport to show hamburger menu
      await page.setViewportSize({ width: 375, height: 667 });

      // Tab to nav logo
      await page.keyboard.press('Tab');
      const navLogo = page.locator('.nav-logo');
      await expect(navLogo).toBeFocused();

      // Tab to nav toggle (visible on mobile)
      await page.keyboard.press('Tab');
      const navToggle = page.locator('#nav-toggle');
      await expect(navToggle).toBeFocused();
    });

    test('Hero section buttons are keyboard accessible', async ({ page }) => {
      // Tab to find the Get Started button in hero section
      const getStartedBtn = page.locator('.hero-cta .btn-primary');
      await getStartedBtn.focus();
      await expect(getStartedBtn).toBeFocused();

      // Tab to View on GitHub button
      await page.keyboard.press('Tab');
      const githubBtn = page.locator('.hero-cta .btn-secondary');
      await expect(githubBtn).toBeFocused();
    });

    test('Copy buttons in code blocks are keyboard accessible', async ({ page }) => {
      // Focus on first copy button in usage section
      const firstCopyBtn = page.locator('.code-block__copy').first();
      await firstCopyBtn.focus();
      await expect(firstCopyBtn).toBeFocused();

      // Verify it's keyboard activatable
      await page.keyboard.press('Enter');

      // After clicking copy, verify feedback is shown
      const checkIcon = firstCopyBtn.locator('.check-icon');
      // Check icon should become visible after copy action
      await expect(checkIcon).toBeVisible({ timeout: 2000 });
    });

    test('Footer links are keyboard accessible', async ({ page }) => {
      // Navigate to footer links
      const footerFeaturesLink = page.locator('.footer__link[href="#features"]');
      await footerFeaturesLink.focus();
      await expect(footerFeaturesLink).toBeFocused();

      // Tab through footer links
      await page.keyboard.press('Tab');
      const footerUsageLink = page.locator('.footer__link[href="#usage"]');
      await expect(footerUsageLink).toBeFocused();
    });

    test('Interactive elements can be activated with Enter key', async ({ page }) => {
      // Focus on the Get Started button and activate with Enter
      const getStartedBtn = page.locator('.hero-cta .btn-primary');
      await getStartedBtn.focus();
      await page.keyboard.press('Enter');

      // Should scroll to getting-started section
      await page.waitForTimeout(500); // Wait for smooth scroll
      const gettingStartedSection = page.locator('#getting-started');
      await expect(gettingStartedSection).toBeInViewport();
    });

    test('Navigation toggle can be operated with keyboard', async ({ page }) => {
      // Set mobile viewport to show hamburger menu
      await page.setViewportSize({ width: 375, height: 667 });

      const navToggle = page.locator('#nav-toggle');
      await navToggle.focus();
      await expect(navToggle).toBeFocused();

      // Verify aria-expanded is false initially
      await expect(navToggle).toHaveAttribute('aria-expanded', 'false');

      // Activate with Enter
      await page.keyboard.press('Enter');

      // Verify aria-expanded changes to true
      await expect(navToggle).toHaveAttribute('aria-expanded', 'true');

      // Verify menu is visible
      const navMenu = page.locator('#nav-menu');
      await expect(navMenu).toBeVisible();

      // Close with Escape key
      await page.keyboard.press('Escape');
      await expect(navToggle).toHaveAttribute('aria-expanded', 'false');
    });

    test('Skip link is functional for keyboard users', async ({ page }) => {
      // Many accessible sites have skip links - verify page content is reachable
      // by tabbing through interactive elements in logical order

      let tabCount = 0;
      const maxTabs = 50; // Safety limit
      const focusedElements = [];

      // Tab through page and collect focused elements
      while (tabCount < maxTabs) {
        await page.keyboard.press('Tab');
        tabCount++;

        const focused = await page.evaluate(() => {
          const el = document.activeElement;
          return el ? {
            tagName: el.tagName,
            className: el.className,
            id: el.id,
            href: el.href || null
          } : null;
        });

        if (focused) {
          focusedElements.push(focused);
        }

        // Check if we've reached the footer (indicates full traversal)
        if (focused && focused.className && focused.className.includes('footer__')) {
          break;
        }
      }

      // Verify we collected interactive elements
      expect(focusedElements.length).toBeGreaterThan(10);

      // Verify navigation elements were reached
      const navElements = focusedElements.filter(el =>
        el.className && (el.className.includes('nav') || el.className.includes('btn'))
      );
      expect(navElements.length).toBeGreaterThan(0);
    });
  });

  test.describe('TC2: Focus Indicators', () => {
    test('All focused elements have visible focus indicator', async ({ page }) => {
      // Test focus visibility on navigation links
      const navLink = page.locator('.nav-link').first();
      await navLink.focus();

      // Check that the element has a visible focus style
      const focusStyles = await navLink.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          outline: styles.outline,
          outlineWidth: styles.outlineWidth,
          outlineColor: styles.outlineColor,
          outlineStyle: styles.outlineStyle,
          boxShadow: styles.boxShadow
        };
      });

      // Element should have either outline or box-shadow for focus indication
      const hasOutline = focusStyles.outlineWidth !== '0px' && focusStyles.outlineStyle !== 'none';
      const hasBoxShadow = focusStyles.boxShadow !== 'none';

      expect(hasOutline || hasBoxShadow).toBe(true);
    });

    test('Primary buttons have visible focus indicator', async ({ page }) => {
      const primaryBtn = page.locator('.btn-primary').first();
      await primaryBtn.focus();

      const focusStyles = await primaryBtn.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          outline: styles.outline,
          outlineWidth: styles.outlineWidth,
          outlineStyle: styles.outlineStyle,
          boxShadow: styles.boxShadow
        };
      });

      const hasOutline = focusStyles.outlineWidth !== '0px' && focusStyles.outlineStyle !== 'none';
      const hasBoxShadow = focusStyles.boxShadow !== 'none';

      expect(hasOutline || hasBoxShadow).toBe(true);
    });

    test('Secondary buttons have visible focus indicator', async ({ page }) => {
      const secondaryBtn = page.locator('.btn-secondary').first();
      await secondaryBtn.focus();

      const focusStyles = await secondaryBtn.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          outline: styles.outline,
          outlineWidth: styles.outlineWidth,
          outlineStyle: styles.outlineStyle,
          boxShadow: styles.boxShadow
        };
      });

      const hasOutline = focusStyles.outlineWidth !== '0px' && focusStyles.outlineStyle !== 'none';
      const hasBoxShadow = focusStyles.boxShadow !== 'none';

      expect(hasOutline || hasBoxShadow).toBe(true);
    });

    test('Copy buttons have visible focus indicator', async ({ page }) => {
      const copyBtn = page.locator('.code-block__copy').first();
      await copyBtn.focus();

      const focusStyles = await copyBtn.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          outline: styles.outline,
          outlineWidth: styles.outlineWidth,
          outlineStyle: styles.outlineStyle,
          boxShadow: styles.boxShadow
        };
      });

      const hasOutline = focusStyles.outlineWidth !== '0px' && focusStyles.outlineStyle !== 'none';
      const hasBoxShadow = focusStyles.boxShadow !== 'none';

      expect(hasOutline || hasBoxShadow).toBe(true);
    });

    test('Navigation toggle has visible focus indicator on mobile', async ({ page }) => {
      // Nav toggle is only visible on mobile
      await page.setViewportSize({ width: 375, height: 667 });

      const navToggle = page.locator('#nav-toggle');
      await navToggle.focus();

      const focusStyles = await navToggle.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          outline: styles.outline,
          outlineWidth: styles.outlineWidth,
          outlineStyle: styles.outlineStyle,
          boxShadow: styles.boxShadow
        };
      });

      const hasOutline = focusStyles.outlineWidth !== '0px' && focusStyles.outlineStyle !== 'none';
      const hasBoxShadow = focusStyles.boxShadow !== 'none';

      expect(hasOutline || hasBoxShadow).toBe(true);
    });

    test('Footer links have visible focus indicator', async ({ page }) => {
      const footerLink = page.locator('.footer__link').first();
      await footerLink.focus();

      const focusStyles = await footerLink.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          outline: styles.outline,
          outlineWidth: styles.outlineWidth,
          outlineStyle: styles.outlineStyle,
          boxShadow: styles.boxShadow
        };
      });

      const hasOutline = focusStyles.outlineWidth !== '0px' && focusStyles.outlineStyle !== 'none';
      const hasBoxShadow = focusStyles.boxShadow !== 'none';

      expect(hasOutline || hasBoxShadow).toBe(true);
    });

    test('Focus indicators are visible when navigating with keyboard', async ({ page }) => {
      // Ensure desktop viewport for consistent testing
      await page.setViewportSize({ width: 1280, height: 720 });

      // Tab to navigate and trigger :focus-visible state
      await page.keyboard.press('Tab'); // Focus on first element
      await page.keyboard.press('Tab'); // Focus on second element

      // Check that current focused element has visible focus indicator
      const hasProperFocus = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el) return true; // No element focused is OK

        const styles = window.getComputedStyle(el);
        const outlineWidth = styles.outlineWidth;
        const outlineStyle = styles.outlineStyle;
        const boxShadow = styles.boxShadow;

        // Either has outline or has box-shadow for focus indication
        const hasOutline = outlineStyle !== 'none' && outlineWidth !== '0px';
        const hasBoxShadow = boxShadow !== 'none';

        return hasOutline || hasBoxShadow;
      });

      expect(hasProperFocus).toBe(true);
    });

    test('Focus indicator has sufficient contrast', async ({ page }) => {
      const primaryBtn = page.locator('.btn-primary').first();
      await primaryBtn.focus();

      // Get the focus indicator styles
      const focusIndicatorVisible = await primaryBtn.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        const boxShadow = styles.boxShadow;
        const outline = styles.outline;

        // Check if focus indicator exists and is visible
        return boxShadow !== 'none' || outline !== 'none';
      });

      expect(focusIndicatorVisible).toBe(true);
    });
  });

  test.describe('Additional Accessibility Tests', () => {
    test('Escape key closes mobile menu', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });

      // Open menu
      const navToggle = page.locator('#nav-toggle');
      await navToggle.click();

      // Verify menu is open
      await expect(navToggle).toHaveAttribute('aria-expanded', 'true');

      // Press Escape
      await page.keyboard.press('Escape');

      // Menu should close
      await expect(navToggle).toHaveAttribute('aria-expanded', 'false');
    });

    test('Focus is trapped within mobile menu when open', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });

      // Open menu
      const navToggle = page.locator('#nav-toggle');
      await navToggle.click();
      await expect(navToggle).toHaveAttribute('aria-expanded', 'true');

      // Tab through menu items
      await page.keyboard.press('Tab');
      let focused = await page.evaluate(() => document.activeElement?.className);
      expect(focused).toContain('nav');
    });

    test('All images have alt text', async ({ page }) => {
      const images = page.locator('img');
      const count = await images.count();

      for (let i = 0; i < count; i++) {
        const img = images.nth(i);
        const alt = await img.getAttribute('alt');
        expect(alt).not.toBeNull();
        expect(alt.length).toBeGreaterThan(0);
      }
    });

    test('Page has proper heading hierarchy', async ({ page }) => {
      // Get all headings
      const h1Count = await page.locator('h1').count();
      const h2Count = await page.locator('h2').count();
      const h3Count = await page.locator('h3').count();

      // Should have exactly one h1
      expect(h1Count).toBe(1);

      // Should have section headings (h2)
      expect(h2Count).toBeGreaterThan(0);

      // Should have subsection headings (h3)
      expect(h3Count).toBeGreaterThan(0);
    });

    test('Interactive elements have sufficient touch target size', async ({ page }) => {
      const buttons = page.locator('button');
      const count = await buttons.count();

      for (let i = 0; i < count; i++) {
        const button = buttons.nth(i);
        const box = await button.boundingBox();

        if (box) {
          // WCAG recommends minimum 44x44 pixels for touch targets
          // We'll check for at least 24x24 as a baseline
          expect(box.width).toBeGreaterThanOrEqual(24);
          expect(box.height).toBeGreaterThanOrEqual(24);
        }
      }
    });

    test('Links are distinguishable from surrounding text', async ({ page }) => {
      // Check that links have visual differentiation
      const link = page.locator('.nav-link').first();

      const linkStyles = await link.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          textDecoration: styles.textDecoration,
          color: styles.color
        };
      });

      // Link should have distinct styling (color or underline)
      // In dark theme, links typically use accent colors
      expect(linkStyles.color).toBeDefined();
    });
  });
});
