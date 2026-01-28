/**
 * Navigation and Documentation Links Tests
 * Owner: Scenario 14 - Navigation and Documentation Links
 *
 * Tests for:
 * - Navigation element presence
 * - Anchor link functionality
 * - Smooth scroll behavior
 * - Documentation CTA functionality
 */

const { test, expect } = require('@playwright/test');

test.describe('Navigation and Documentation Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('TC1: Check for navigation element or links', () => {
    test('Navigation links to different sections or documentation exist', async ({ page }) => {
      // Check for internal navigation links (anchor links starting with #)
      const internalLinks = page.locator('a[href^="#"]:not([href="#"])');
      const internalLinksCount = await internalLinks.count();

      // Should have at least one internal navigation link
      expect(internalLinksCount).toBeGreaterThan(0);

      // Specifically check for the documentation CTA
      const docsCTA = page.locator('#cta-docs');
      await expect(docsCTA).toBeAttached();

      // Verify it has a proper href
      const href = await docsCTA.getAttribute('href');
      expect(href).toBeTruthy();
      expect(href.startsWith('#')).toBe(true);
    });

    test('Page has navigable section targets with proper IDs', async ({ page }) => {
      // Verify main sections have IDs that can be navigation targets
      const sections = ['#hero', '#features', '#code-example', '#quick-start'];

      for (const sectionId of sections) {
        const section = page.locator(sectionId);
        await expect(section).toBeAttached();
      }
    });

    test('Documentation link exists in hero section CTA area', async ({ page }) => {
      const heroCTAs = page.locator('.hero-ctas');
      await expect(heroCTAs).toBeVisible();

      // Check for a documentation-related link
      const docsLink = heroCTAs.locator('a[href*="quick-start"], a[href*="docs"], a[id*="docs"]');
      await expect(docsLink.first()).toBeAttached();

      // Verify it has visible text
      const linkText = await docsLink.first().textContent();
      expect(linkText).toBeTruthy();
    });

    test('Navigation links have accessible attributes', async ({ page }) => {
      const docsCTA = page.locator('#cta-docs');
      await expect(docsCTA).toBeAttached();

      // Check for aria-label for accessibility
      const ariaLabel = await docsCTA.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
    });
  });

  test.describe('TC2: Anchor links within page', () => {
    test('Clicking section links scrolls to appropriate section smoothly', async ({ page }) => {
      // Get the Read Documentation CTA button which links to #quick-start
      const docsLink = page.locator('#cta-docs');
      await expect(docsLink).toBeVisible();

      // Get initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY);

      // Click the documentation link
      await docsLink.click();

      // Wait for smooth scroll to complete
      await page.waitForTimeout(500);

      // Get new scroll position
      const newScrollY = await page.evaluate(() => window.scrollY);

      // Verify that the page has scrolled (scroll position changed)
      expect(newScrollY).toBeGreaterThan(initialScrollY);

      // Verify the quick-start section is now in viewport
      const quickStartSection = page.locator('#quick-start');
      await expect(quickStartSection).toBeInViewport();
    });

    test('Anchor links use smooth scroll behavior', async ({ page }) => {
      // Verify that html has smooth scroll behavior via CSS
      const htmlScrollBehavior = await page.evaluate(() => {
        return window.getComputedStyle(document.documentElement).scrollBehavior;
      });
      expect(htmlScrollBehavior).toBe('smooth');
    });

    test('All internal anchor links navigate to existing sections', async ({ page }) => {
      // Get all internal anchor links (starting with #)
      const anchorLinks = page.locator('a[href^="#"]');
      const count = await anchorLinks.count();

      // There should be at least one internal anchor link
      expect(count).toBeGreaterThan(0);

      // Check each anchor link points to an existing element
      for (let i = 0; i < count; i++) {
        const link = anchorLinks.nth(i);
        const href = await link.getAttribute('href');

        // Skip empty hash links
        if (href === '#') continue;

        // Verify the target element exists
        const targetId = href.substring(1); // Remove the # prefix
        const targetElement = page.locator(`#${targetId}`);
        await expect(targetElement).toBeAttached();
      }
    });
  });

  test.describe('TC3: Documentation CTA functionality', () => {
    test('Read Documentation button navigates to docs or appropriate section', async ({ page }) => {
      // Find the Read Documentation CTA
      const docsCTA = page.locator('#cta-docs');
      await expect(docsCTA).toBeVisible();

      // Verify button text
      await expect(docsCTA).toHaveText('Read Documentation');

      // Verify it links to the quick-start/docs section
      const href = await docsCTA.getAttribute('href');
      expect(href).toBe('#quick-start');

      // Click the button
      await docsCTA.click();

      // Wait for navigation/scroll
      await page.waitForTimeout(500);

      // Verify we're now viewing the quick-start section
      const quickStartSection = page.locator('#quick-start');
      await expect(quickStartSection).toBeInViewport();

      // Verify the section has the expected content
      const sectionTitle = page.locator('#quick-start-title');
      await expect(sectionTitle).toBeVisible();
      await expect(sectionTitle).toContainText('Quick Start');
    });

    test('Documentation CTA has proper styling', async ({ page }) => {
      const docsCTA = page.locator('#cta-docs');
      await expect(docsCTA).toBeVisible();

      // Verify it has the secondary button class
      await expect(docsCTA).toHaveClass(/btn/);
      await expect(docsCTA).toHaveClass(/btn-secondary/);
    });

    test('Documentation CTA has accessible aria-label', async ({ page }) => {
      const docsCTA = page.locator('#cta-docs');
      await expect(docsCTA).toBeVisible();

      // Verify it has an aria-label for accessibility
      const ariaLabel = await docsCTA.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel.toLowerCase()).toMatch(/documentation|docs/);
    });

    test('Documentation CTA is keyboard accessible', async ({ page }) => {
      const docsCTA = page.locator('#cta-docs');

      // Focus the element using keyboard navigation
      await docsCTA.focus();
      await expect(docsCTA).toBeFocused();

      // Get initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY);

      // Press Enter to activate the link
      await page.keyboard.press('Enter');

      // Wait for scroll
      await page.waitForTimeout(500);

      // Verify scroll happened
      const newScrollY = await page.evaluate(() => window.scrollY);
      expect(newScrollY).toBeGreaterThan(initialScrollY);
    });
  });

  test.describe('Navigation Links General Tests', () => {
    test('Page has navigation links to documentation sections', async ({ page }) => {
      // Look for links that navigate to different sections
      const sectionLinks = page.locator('a[href^="#"]:not([href="#"])');
      const count = await sectionLinks.count();

      // Should have at least one internal navigation link
      expect(count).toBeGreaterThan(0);
    });

    test('Hero section contains documentation navigation CTA', async ({ page }) => {
      const heroSection = page.locator('#hero');
      await expect(heroSection).toBeVisible();

      // Find documentation link within hero
      const docsLink = heroSection.locator('a[href*="quick-start"], a[href*="docs"], a[href*="documentation"]');
      await expect(docsLink.first()).toBeVisible();
    });

    test('Navigation links are visually distinct and clickable', async ({ page }) => {
      const docsCTA = page.locator('#cta-docs');
      await expect(docsCTA).toBeVisible();

      // Check that the button is clickable (not disabled)
      await expect(docsCTA).toBeEnabled();

      // Check cursor style on hover
      const cursorStyle = await docsCTA.evaluate((el) => {
        return window.getComputedStyle(el).cursor;
      });
      expect(cursorStyle).toBe('pointer');
    });

    test('Page sections have proper IDs for navigation targets', async ({ page }) => {
      // Verify key sections have IDs that can be linked to
      const heroSection = page.locator('#hero');
      const featuresSection = page.locator('#features');
      const codeExampleSection = page.locator('#code-example');
      const quickStartSection = page.locator('#quick-start');

      await expect(heroSection).toBeAttached();
      await expect(featuresSection).toBeAttached();
      await expect(codeExampleSection).toBeAttached();
      await expect(quickStartSection).toBeAttached();
    });
  });
});
