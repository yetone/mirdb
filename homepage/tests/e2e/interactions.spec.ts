/**
 * User Interaction Patterns E2E Tests
 * Owner: Scenario 13 - User Interaction Patterns
 *
 * Tests:
 * - Smooth scroll behavior for anchor links
 * - Hover states on interactive elements
 * - Copy button visibility on code block hover
 */

import { test, expect } from '@playwright/test';

test.describe('User Interaction Patterns', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for page to be fully loaded
    await page.waitForLoadState('networkidle');
  });

  test.describe('Smooth Scroll', () => {
    test('clicking Get Started button smooth scrolls to quickstart section', async ({ page }) => {
      // Get the initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY);
      expect(initialScrollY).toBe(0);

      // Find and click the Get Started button
      const getStartedButton = page.locator('a.btn-primary:has-text("Get Started")');
      await expect(getStartedButton).toBeVisible();

      // Click the button
      await getStartedButton.click();

      // Wait for scroll animation to complete
      await page.waitForTimeout(1000);

      // Verify the quickstart section is now in view
      const quickstartSection = page.locator('#quickstart');
      await expect(quickstartSection).toBeInViewport();

      // Verify we've scrolled down
      const newScrollY = await page.evaluate(() => window.scrollY);
      expect(newScrollY).toBeGreaterThan(initialScrollY);
    });

    test('anchor links scroll smoothly to their targets', async ({ page }) => {
      // Test clicking the About link in navigation
      const aboutLink = page.locator('.header__nav-link[href="#about"]');

      // If the about link exists, test it
      const aboutLinkCount = await aboutLink.count();
      if (aboutLinkCount > 0) {
        await aboutLink.click();
        await page.waitForTimeout(1000);

        const aboutSection = page.locator('#about');
        await expect(aboutSection).toBeInViewport();
      }
    });

    test('smooth scroll respects sticky header offset', async ({ page }) => {
      // Click Get Started to scroll to quickstart
      const getStartedButton = page.locator('a.btn-primary:has-text("Get Started")');
      await getStartedButton.click();
      await page.waitForTimeout(1000);

      // The quickstart section should be visible and not hidden under the sticky header
      const quickstartSection = page.locator('#quickstart');
      const header = page.locator('.header');

      const headerBox = await header.boundingBox();
      const sectionBox = await quickstartSection.boundingBox();

      if (headerBox && sectionBox) {
        // The section should start below the header
        expect(sectionBox.y).toBeGreaterThanOrEqual(headerBox.height);
      }
    });
  });

  test.describe('Hover States - Navigation Links', () => {
    test('navigation links show visual hover state', async ({ page }) => {
      // Get a navigation link
      const navLink = page.locator('.header__nav-link').first();
      await expect(navLink).toBeVisible();

      // Get initial styles
      const initialColor = await navLink.evaluate(el =>
        window.getComputedStyle(el).color
      );
      const initialBgColor = await navLink.evaluate(el =>
        window.getComputedStyle(el).backgroundColor
      );

      // Hover over the link
      await navLink.hover();

      // Wait for transition
      await page.waitForTimeout(300);

      // Get hover styles
      const hoverColor = await navLink.evaluate(el =>
        window.getComputedStyle(el).color
      );
      const hoverBgColor = await navLink.evaluate(el =>
        window.getComputedStyle(el).backgroundColor
      );

      // At least one style should change on hover
      const hasStyleChange = initialColor !== hoverColor || initialBgColor !== hoverBgColor;
      expect(hasStyleChange).toBeTruthy();
    });
  });

  test.describe('Hover States - CTA Buttons', () => {
    test('primary CTA button shows visual hover state', async ({ page }) => {
      const primaryBtn = page.locator('a.btn-primary:has-text("Get Started")');
      await expect(primaryBtn).toBeVisible();

      // Get initial styles
      const initialBgColor = await primaryBtn.evaluate(el =>
        window.getComputedStyle(el).backgroundColor
      );
      const initialTransform = await primaryBtn.evaluate(el =>
        window.getComputedStyle(el).transform
      );
      const initialBoxShadow = await primaryBtn.evaluate(el =>
        window.getComputedStyle(el).boxShadow
      );

      // Hover over the button
      await primaryBtn.hover();

      // Wait for transition
      await page.waitForTimeout(300);

      // Get hover styles
      const hoverBgColor = await primaryBtn.evaluate(el =>
        window.getComputedStyle(el).backgroundColor
      );
      const hoverTransform = await primaryBtn.evaluate(el =>
        window.getComputedStyle(el).transform
      );
      const hoverBoxShadow = await primaryBtn.evaluate(el =>
        window.getComputedStyle(el).boxShadow
      );

      // At least one style should change on hover (background color, transform, or shadow)
      const hasStyleChange =
        initialBgColor !== hoverBgColor ||
        initialTransform !== hoverTransform ||
        initialBoxShadow !== hoverBoxShadow;
      expect(hasStyleChange).toBeTruthy();
    });

    test('secondary CTA button shows visual hover state', async ({ page }) => {
      const secondaryBtn = page.locator('a.btn-secondary:has-text("View on GitHub")');
      await expect(secondaryBtn).toBeVisible();

      // Get initial styles
      const initialBgColor = await secondaryBtn.evaluate(el =>
        window.getComputedStyle(el).backgroundColor
      );
      const initialBorderColor = await secondaryBtn.evaluate(el =>
        window.getComputedStyle(el).borderColor
      );

      // Hover over the button
      await secondaryBtn.hover();

      // Wait for transition
      await page.waitForTimeout(300);

      // Get hover styles
      const hoverBgColor = await secondaryBtn.evaluate(el =>
        window.getComputedStyle(el).backgroundColor
      );
      const hoverBorderColor = await secondaryBtn.evaluate(el =>
        window.getComputedStyle(el).borderColor
      );

      // At least one style should change on hover
      const hasStyleChange =
        initialBgColor !== hoverBgColor ||
        initialBorderColor !== hoverBorderColor;
      expect(hasStyleChange).toBeTruthy();
    });
  });

  test.describe('Hover States - Code Blocks', () => {
    test('copy button appears on code block hover', async ({ page }) => {
      // Scroll to the quickstart section where code blocks are located
      const quickstartSection = page.locator('#quickstart');
      await quickstartSection.scrollIntoViewIfNeeded();
      await page.waitForTimeout(500);

      // Find a code block
      const codeBlock = page.locator('.code-block').first();
      await expect(codeBlock).toBeVisible();

      // Find the copy button within the code block
      const copyBtn = codeBlock.locator('.copy-btn');

      // Check initial opacity (should be 0 or very low)
      const initialOpacity = await copyBtn.evaluate(el =>
        window.getComputedStyle(el).opacity
      );
      expect(parseFloat(initialOpacity)).toBeLessThanOrEqual(0.1);

      // Hover over the code block
      await codeBlock.hover();

      // Wait for transition
      await page.waitForTimeout(300);

      // Check that copy button is now visible (opacity 1 or close to it)
      const hoverOpacity = await copyBtn.evaluate(el =>
        window.getComputedStyle(el).opacity
      );
      expect(parseFloat(hoverOpacity)).toBeGreaterThan(0.5);
    });

    test('copy button remains visible after hover and interaction', async ({ page }) => {
      // Navigate to quickstart section
      const quickstartSection = page.locator('#quickstart');
      await quickstartSection.scrollIntoViewIfNeeded();
      await page.waitForTimeout(500);

      // Find a code block and its copy button
      const codeBlock = page.locator('.code-block').first();
      const copyBtn = codeBlock.locator('.copy-btn');

      // Hover over the code block to show the button
      await codeBlock.hover();
      await page.waitForTimeout(300);

      // Move hover to the copy button
      await copyBtn.hover();
      await page.waitForTimeout(200);

      // Button should still be visible when hovered directly
      const opacity = await copyBtn.evaluate(el =>
        window.getComputedStyle(el).opacity
      );
      expect(parseFloat(opacity)).toBeGreaterThan(0.5);
    });
  });

  test.describe('Accessibility - Focus States', () => {
    test('interactive elements have visible focus indicators', async ({ page }) => {
      // Tab through the page to check focus states
      const getStartedButton = page.locator('a.btn-primary:has-text("Get Started")');

      // Focus the button using keyboard navigation
      await getStartedButton.focus();

      // Check for focus outline
      const outlineStyle = await getStartedButton.evaluate(el =>
        window.getComputedStyle(el).outline
      );
      const outlineOffset = await getStartedButton.evaluate(el =>
        window.getComputedStyle(el).outlineOffset
      );

      // Should have some outline on focus
      expect(outlineStyle).not.toBe('none');
    });

    test('navigation links have focus indicators', async ({ page }) => {
      const navLink = page.locator('.header__nav-link').first();
      await navLink.focus();

      // Check for focus-visible styles
      const outline = await navLink.evaluate(el => {
        const style = window.getComputedStyle(el);
        return style.outline || style.outlineStyle;
      });

      // Should have visible focus indication
      expect(outline).toBeTruthy();
    });
  });
});
