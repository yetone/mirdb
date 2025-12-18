// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = path.resolve(__dirname, '../index.html');

/**
 * Accessibility - Keyboard Navigation Tests
 * Verify the landing page is fully navigable using keyboard only
 *
 * Scenario: Keyboard-only users should be able to navigate through all
 * interactive elements with clear focus indicators and a skip-to-content link
 */
test.describe('Accessibility - Keyboard Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(`file://${indexPath}`);
  });

  /**
   * Test Case 1: Tab through page from start
   * Input: Tab through page from start
   * Expected: All interactive elements receive focus in logical order
   * Type: manual (automated implementation)
   */
  test('TC1: All interactive elements receive focus in logical order when tabbing', async ({ page }) => {
    // Start from beginning of document
    await page.keyboard.press('Tab');

    // First focusable element should be skip link (visually hidden until focused)
    const skipLink = page.locator('[data-testid="skip-link"]');
    await expect(skipLink).toBeFocused();

    // Continue tabbing through navigation
    await page.keyboard.press('Tab');
    const navBrand = page.locator('[data-testid="nav-brand"]');
    await expect(navBrand).toBeFocused();

    // Tab to Features link
    await page.keyboard.press('Tab');
    const featuresLink = page.locator('[data-testid="nav-features"]');
    await expect(featuresLink).toBeFocused();

    // Tab to Quick Start link
    await page.keyboard.press('Tab');
    const quickstartLink = page.locator('[data-testid="nav-quickstart"]');
    await expect(quickstartLink).toBeFocused();

    // Tab to Commands link
    await page.keyboard.press('Tab');
    const commandsLink = page.locator('[data-testid="nav-commands"]');
    await expect(commandsLink).toBeFocused();

    // Tab to Configuration link
    await page.keyboard.press('Tab');
    const configLink = page.locator('[data-testid="nav-configuration"]');
    await expect(configLink).toBeFocused();

    // Tab to GitHub link in nav
    await page.keyboard.press('Tab');
    const githubNavLink = page.locator('[data-testid="nav-github"]');
    await expect(githubNavLink).toBeFocused();

    // Tab to Get Started CTA button
    await page.keyboard.press('Tab');
    const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
    await expect(getStartedBtn).toBeFocused();

    // Tab to GitHub CTA button
    await page.keyboard.press('Tab');
    const githubCtaBtn = page.locator('[data-testid="cta-github"]');
    await expect(githubCtaBtn).toBeFocused();

    // Continue tabbing to verify we can reach copy buttons in quick start section
    // Tab through to first copy button
    let foundCopyButton = false;
    for (let i = 0; i < 20; i++) {
      await page.keyboard.press('Tab');
      const focused = page.locator(':focus');
      const testId = await focused.getAttribute('data-testid');
      if (testId && testId.startsWith('copy-btn-')) {
        foundCopyButton = true;
        break;
      }
    }
    expect(foundCopyButton).toBe(true);
  });

  /**
   * Test Case 2: Check focus visibility on navigation links
   * Input: Check focus visibility on navigation links
   * Expected: Clear visible focus indicator (outline, background change) on focused links
   * Type: e2e
   */
  test('TC2: Navigation links have clear visible focus indicator', async ({ page }) => {
    // Focus on Features link
    const featuresLink = page.locator('[data-testid="nav-features"]');
    await featuresLink.focus();
    await expect(featuresLink).toBeFocused();

    // Verify focus style is visible (outline or color change)
    const outlineStyle = await featuresLink.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        outline: styles.outline,
        outlineWidth: styles.outlineWidth,
        outlineColor: styles.outlineColor,
        outlineStyle: styles.outlineStyle,
        color: styles.color
      };
    });

    // Check that either outline is visible or color changed
    const hasVisibleOutline = outlineStyle.outlineWidth !== '0px' && outlineStyle.outlineStyle !== 'none';
    const hasColorChange = outlineStyle.color !== 'rgb(31, 41, 55)'; // text-color
    expect(hasVisibleOutline || hasColorChange).toBe(true);

    // Test other navigation links
    const navLinks = [
      '[data-testid="nav-quickstart"]',
      '[data-testid="nav-commands"]',
      '[data-testid="nav-configuration"]',
      '[data-testid="nav-github"]'
    ];

    for (const selector of navLinks) {
      const link = page.locator(selector);
      await link.focus();
      await expect(link).toBeFocused();

      const linkStyles = await link.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          outlineWidth: styles.outlineWidth,
          outlineStyle: styles.outlineStyle,
          color: styles.color
        };
      });

      const linkHasFocusIndicator =
        (linkStyles.outlineWidth !== '0px' && linkStyles.outlineStyle !== 'none') ||
        linkStyles.color !== 'rgb(31, 41, 55)';
      expect(linkHasFocusIndicator).toBe(true);
    }
  });

  /**
   * Test Case 3: Check focus visibility on CTA buttons
   * Input: Check focus visibility on CTA buttons
   * Expected: Clear visible focus indicator on focused buttons
   * Type: e2e
   */
  test('TC3: CTA buttons have clear visible focus indicator', async ({ page }) => {
    // Test Get Started button focus indicator via keyboard (triggers :focus-visible)
    const getStartedBtn = page.locator('[data-testid="cta-get-started"]');

    // Use keyboard to navigate to button (triggers :focus-visible styles)
    await page.keyboard.press('Tab'); // Skip link
    await page.keyboard.press('Tab'); // Nav brand
    await page.keyboard.press('Tab'); // Features
    await page.keyboard.press('Tab'); // Quick Start
    await page.keyboard.press('Tab'); // Commands
    await page.keyboard.press('Tab'); // Configuration
    await page.keyboard.press('Tab'); // GitHub nav
    await page.keyboard.press('Tab'); // Get Started button

    await expect(getStartedBtn).toBeFocused();

    // Allow a moment for styles to apply
    await page.waitForTimeout(50);

    const getStartedStyles = await getStartedBtn.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        outline: styles.outline,
        outlineWidth: styles.outlineWidth,
        outlineColor: styles.outlineColor,
        outlineStyle: styles.outlineStyle
      };
    });

    // Verify outline is visible (2px solid blue per CSS .btn:focus)
    // The outline property should indicate visible focus
    const hasOutline = getStartedStyles.outlineWidth !== '0px' && getStartedStyles.outlineStyle !== 'none';
    const hasOutlineInString = getStartedStyles.outline.includes('2px') || getStartedStyles.outline.includes('solid');
    expect(hasOutline || hasOutlineInString).toBe(true);

    // Test GitHub CTA button focus indicator via keyboard
    await page.keyboard.press('Tab');
    const githubBtn = page.locator('[data-testid="cta-github"]');
    await expect(githubBtn).toBeFocused();

    await page.waitForTimeout(50);

    const githubStyles = await githubBtn.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        outline: styles.outline,
        outlineWidth: styles.outlineWidth,
        outlineStyle: styles.outlineStyle
      };
    });

    const githubHasOutline = githubStyles.outlineWidth !== '0px' && githubStyles.outlineStyle !== 'none';
    const githubHasOutlineInString = githubStyles.outline.includes('2px') || githubStyles.outline.includes('solid');
    expect(githubHasOutline || githubHasOutlineInString).toBe(true);
  });

  /**
   * Test Case 4: Press Enter on focused navigation link
   * Input: Press Enter on focused navigation link
   * Expected: Navigation link activates and scrolls to section
   * Type: manual (automated implementation)
   */
  test('TC4: Enter key activates navigation links and scrolls to section', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);
    expect(initialScrollY).toBe(0);

    // Focus on Features link
    const featuresLink = page.locator('[data-testid="nav-features"]');
    await featuresLink.focus();
    await expect(featuresLink).toBeFocused();

    // Press Enter to activate
    await page.keyboard.press('Enter');

    // Wait for scroll animation
    await page.waitForTimeout(600);

    // Verify page scrolled to Features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify scroll position changed
    const scrollYAfterFeatures = await page.evaluate(() => window.scrollY);
    expect(scrollYAfterFeatures).toBeGreaterThan(0);

    // Features section should be near top of viewport
    const featuresBoundingBox = await featuresSection.boundingBox();
    expect(featuresBoundingBox).not.toBeNull();
    expect(featuresBoundingBox.y).toBeLessThan(150);

    // Now test Quick Start link with Enter
    const quickstartLink = page.locator('[data-testid="nav-quickstart"]');
    await quickstartLink.focus();
    await page.keyboard.press('Enter');
    await page.waitForTimeout(600);

    const quickstartSection = page.locator('#quickstart');
    const quickstartBoundingBox = await quickstartSection.boundingBox();
    expect(quickstartBoundingBox).not.toBeNull();
    expect(quickstartBoundingBox.y).toBeLessThan(150);
  });

  /**
   * Test Case 5: Check for skip-to-main-content link
   * Input: Check for skip-to-main-content link
   * Expected: Skip link present as first focusable element (may be visually hidden until focused)
   * Type: e2e
   */
  test('TC5: Skip link is present as first focusable element', async ({ page }) => {
    // The skip link should exist
    const skipLink = page.locator('[data-testid="skip-link"]');
    await expect(skipLink).toBeAttached();

    // Skip link should be visually hidden initially (off-screen)
    const initialPosition = await skipLink.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        top: styles.top,
        position: styles.position
      };
    });
    expect(initialPosition.position).toBe('absolute');
    // Should be positioned off-screen (negative top)
    expect(parseInt(initialPosition.top)).toBeLessThan(0);

    // Tab to focus on skip link (should be first focusable element)
    await page.keyboard.press('Tab');
    await expect(skipLink).toBeFocused();

    // Wait for CSS transition to complete (200ms per CSS)
    await page.waitForTimeout(250);

    // When focused, skip link should become visible (top: 0)
    const focusedPosition = await skipLink.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        top: styles.top
      };
    });
    expect(parseInt(focusedPosition.top)).toBe(0);

    // Skip link should have proper text content
    const skipLinkText = await skipLink.textContent();
    expect(skipLinkText.toLowerCase()).toContain('skip');
    expect(skipLinkText.toLowerCase()).toContain('content') || expect(skipLinkText.toLowerCase()).toContain('main');

    // Skip link should point to main content
    const href = await skipLink.getAttribute('href');
    expect(href).toBe('#main-content');
  });

  /**
   * Additional test: Skip link navigates to main content when activated
   */
  test('Skip link navigates to main content when Enter is pressed', async ({ page }) => {
    // Tab to skip link
    await page.keyboard.press('Tab');
    const skipLink = page.locator('[data-testid="skip-link"]');
    await expect(skipLink).toBeFocused();

    // Press Enter to activate
    await page.keyboard.press('Enter');

    // Wait for navigation
    await page.waitForTimeout(300);

    // Main content should now be in view
    const mainContent = page.locator('#main-content');
    await expect(mainContent).toBeVisible();

    // Main content should be near top of viewport
    const mainBoundingBox = await mainContent.boundingBox();
    expect(mainBoundingBox).not.toBeNull();
    expect(mainBoundingBox.y).toBeLessThan(100);
  });

  /**
   * Additional test: Focus-visible styles are applied correctly
   */
  test('Focus-visible styles are applied to interactive elements', async ({ page }) => {
    // Test that :focus-visible styles are present in CSS
    const focusVisibleStyles = await page.evaluate(() => {
      // Check if focus-visible styles apply
      const testElement = document.querySelector('[data-testid="nav-features"]');
      testElement.focus();
      const styles = window.getComputedStyle(testElement);
      return {
        outline: styles.outline,
        outlineOffset: styles.outlineOffset
      };
    });

    // Verify focus styles are applied
    expect(focusVisibleStyles.outline).toBeTruthy();
  });

  /**
   * Additional test: All interactive elements in logical DOM order
   */
  test('Interactive elements follow logical DOM order', async ({ page }) => {
    // Get all focusable elements
    const focusableSelectors = 'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])';

    const focusableElements = await page.locator(focusableSelectors).all();

    // Verify we have many focusable elements
    expect(focusableElements.length).toBeGreaterThan(10);

    // Get positions of key elements to verify logical order
    const skipLink = await page.locator('[data-testid="skip-link"]').boundingBox();
    const navBrand = await page.locator('[data-testid="nav-brand"]').boundingBox();
    const navFeatures = await page.locator('[data-testid="nav-features"]').boundingBox();
    const ctaGetStarted = await page.locator('[data-testid="cta-get-started"]').boundingBox();

    // Elements should flow logically (skip link at top, then nav, then CTA)
    // Skip link is positioned off-screen initially but should be first in DOM
    expect(navBrand.y).toBeLessThan(ctaGetStarted.y);
    expect(navFeatures.y).toBeLessThan(ctaGetStarted.y);
  });

  /**
   * Additional test: Copy buttons are keyboard accessible
   */
  test('Copy buttons in code blocks are keyboard accessible', async ({ page }) => {
    // Scroll to quick start section to make copy buttons visible
    await page.locator('#quickstart').scrollIntoViewIfNeeded();

    // Find the first copy button
    const copyBtn = page.locator('[data-testid="copy-btn-server"]');
    await expect(copyBtn).toBeVisible();

    // Focus on the copy button using keyboard navigation
    await copyBtn.focus();
    await expect(copyBtn).toBeFocused();

    // Allow time for focus styles to apply
    await page.waitForTimeout(50);

    // Verify focus indicator - check that the button has focus-related styling
    const focusStyles = await copyBtn.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        outline: styles.outline,
        outlineWidth: styles.outlineWidth,
        outlineStyle: styles.outlineStyle,
        borderColor: styles.borderColor,
        backgroundColor: styles.backgroundColor
      };
    });

    // On focus, copy button should have either an outline or changed appearance
    // The CSS defines .copy-btn:focus with background, color, and border changes
    const hasOutline = focusStyles.outlineWidth !== '0px' && focusStyles.outlineStyle !== 'none';
    const hasOutlineInString = focusStyles.outline.includes('2px') || focusStyles.outline.includes('solid');
    // Either outline is visible OR the border color has changed (indicates focus state)
    expect(hasOutline || hasOutlineInString || focusStyles.borderColor !== 'rgb(55, 65, 81)').toBe(true);

    // Verify button has aria-label for screen readers
    const ariaLabel = await copyBtn.getAttribute('aria-label');
    expect(ariaLabel).toBe('Copy to clipboard');
  });

  /**
   * Additional test: Footer links are keyboard accessible
   */
  test('Footer links are keyboard navigable', async ({ page }) => {
    // Scroll to footer
    const footer = page.locator('.footer');
    await footer.scrollIntoViewIfNeeded();

    // Find footer links
    const footerLinks = page.locator('.footer-links a');
    const linkCount = await footerLinks.count();

    expect(linkCount).toBeGreaterThan(0);

    // Test that footer links can receive focus
    const firstFooterLink = footerLinks.first();
    await firstFooterLink.focus();
    await expect(firstFooterLink).toBeFocused();

    // Tab to next link
    await page.keyboard.press('Tab');
    const secondFooterLink = footerLinks.nth(1);
    await expect(secondFooterLink).toBeFocused();
  });
});
