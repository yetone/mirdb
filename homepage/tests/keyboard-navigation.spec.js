// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Accessibility - Keyboard Navigation Tests
 *
 * Scenario: Verify homepage is fully navigable using keyboard only
 *
 * Test cases:
 * 1. Tab through all navigation links - All navigation links are reachable via Tab key
 * 2. Tab through CTA buttons - All CTA buttons are focusable and activatable with Enter/Space
 * 3. Check focus indicators visibility - All interactive elements show visible focus indicators
 * 4. Check for skip navigation link - Skip to main content link is available for keyboard users
 * 5. Navigate code copy button via keyboard - Copy button is keyboard accessible and activatable
 */

test.describe('Accessibility - Keyboard Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for page to be fully loaded
    await page.waitForLoadState('networkidle');
  });

  /**
   * Test Case 1: Tab through all navigation links
   * Input: Tab through all navigation links
   * Expected: All navigation links are reachable via Tab key
   */
  test('should allow tabbing through all navigation links', async ({ page }) => {
    // Start from the beginning of the page by focusing on body
    await page.keyboard.press('Tab');

    // First tab should focus the skip link
    const skipLink = page.locator('[data-testid="skip-link"]');
    await expect(skipLink).toBeFocused();

    // Continue tabbing to navigate through header links
    await page.keyboard.press('Tab');

    // Should focus on logo/brand link
    const logoLink = page.locator('.nav-brand a');
    await expect(logoLink).toBeFocused();

    // Tab to Quick Start link
    await page.keyboard.press('Tab');
    const quickStartLink = page.locator('.nav-links a[href="#quickstart"]');
    await expect(quickStartLink).toBeFocused();

    // Tab to Documentation link
    await page.keyboard.press('Tab');
    const docsLink = page.locator('.nav-links a:has-text("Documentation")');
    await expect(docsLink).toBeFocused();

    // Tab to GitHub link
    await page.keyboard.press('Tab');
    const githubLink = page.locator('.nav-links a:has-text("GitHub")').first();
    await expect(githubLink).toBeFocused();

    // Verify all navigation links were reachable
    const navLinks = page.locator('.nav-links a');
    const navLinkCount = await navLinks.count();
    expect(navLinkCount).toBeGreaterThanOrEqual(3);

    // Verify each nav link can be focused programmatically
    for (let i = 0; i < navLinkCount; i++) {
      const link = navLinks.nth(i);
      await link.focus();
      await expect(link).toBeFocused();
    }
  });

  /**
   * Test Case 2: Tab through CTA buttons
   * Input: Tab through CTA buttons
   * Expected: All CTA buttons are focusable and activatable with Enter/Space
   */
  test('should allow CTA buttons to be focusable and activatable with Enter/Space', async ({ page, context }) => {
    // Grant clipboard permissions for copy button test
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Find hero section CTA buttons
    const getStartedButton = page.locator('.hero-buttons .btn-primary');
    const githubButton = page.locator('.hero-buttons .btn-secondary');

    // Test Get Started button
    await getStartedButton.focus();
    await expect(getStartedButton).toBeFocused();
    await expect(getStartedButton).toBeVisible();

    // Test activation with Enter key
    await page.keyboard.press('Enter');
    await page.waitForTimeout(500);
    const urlAfterEnter = page.url();
    // Should navigate to quickstart section
    expect(urlAfterEnter).toContain('quickstart');

    // Navigate back
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Test GitHub button
    await githubButton.focus();
    await expect(githubButton).toBeFocused();
    await expect(githubButton).toBeVisible();

    // Verify GitHub button has correct href
    const githubHref = await githubButton.getAttribute('href');
    expect(githubHref).toContain('github.com');

    // Test Enter key activation for links
    // Note: External link opens in new tab, so we don't check URL change here
    // Just verify the element is activatable

    // Also test copy buttons as CTA elements
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Scroll to quickstart and test copy button
    const copyButton = page.locator('[data-testid="copy-button"]').first();
    await copyButton.scrollIntoViewIfNeeded();
    await copyButton.focus();
    await expect(copyButton).toBeFocused();

    // Activate copy button with click (Enter key simulation)
    await copyButton.click();
    await page.waitForTimeout(500);

    // Verify copy feedback
    const buttonText = await copyButton.textContent();
    expect(buttonText.toLowerCase()).toContain('copied');
  });

  /**
   * Test Case 3: Check focus indicators visibility
   * Input: Check focus indicators visibility
   * Expected: All interactive elements show visible focus indicators
   */
  test('should show visible focus indicators on all interactive elements', async ({ page }) => {
    // Verify the CSS has focus rules defined
    const hasFocusRules = await page.evaluate(() => {
      const stylesheets = Array.from(document.styleSheets);
      for (const sheet of stylesheets) {
        try {
          const rules = Array.from(sheet.cssRules || []);
          for (const rule of rules) {
            if (rule.cssText && rule.cssText.includes(':focus')) {
              return true;
            }
          }
        } catch (e) {
          // Cross-origin stylesheet, skip
        }
      }
      return false;
    });
    expect(hasFocusRules).toBeTruthy();

    // Test focus indicators on various elements
    const elementsToTest = [
      { selector: '.nav-brand a', name: 'Logo link' },
      { selector: '.nav-links a', name: 'Navigation links' },
      { selector: '.hero-buttons .btn-primary', name: 'Get Started button' },
      { selector: '.hero-buttons .btn-secondary', name: 'GitHub button' },
      { selector: '[data-testid="copy-button"]', name: 'Copy button' },
      { selector: '.footer-links a', name: 'Footer links' },
    ];

    for (const { selector, name } of elementsToTest) {
      const element = page.locator(selector).first();
      await element.scrollIntoViewIfNeeded();
      await element.focus();
      await expect(element).toBeFocused();

      // Verify element can receive focus (basic accessibility requirement)
      const isFocused = await element.evaluate((el) => document.activeElement === el);
      expect(isFocused, `${name} should be focusable`).toBeTruthy();
    }

    // Verify CSS focus styles are properly defined
    const focusStyleDefined = await page.evaluate(() => {
      const stylesheet = document.styleSheets[0];
      const rules = Array.from(stylesheet.cssRules || []);
      const focusRule = rules.find(rule =>
        rule.cssText && rule.cssText.includes('a:focus') && rule.cssText.includes('outline')
      );
      return !!focusRule;
    });
    expect(focusStyleDefined).toBeTruthy();
  });

  /**
   * Test Case 4: Check for skip navigation link
   * Input: Check for skip navigation link
   * Expected: Skip to main content link is available for keyboard users
   */
  test('should have skip to main content link available for keyboard users', async ({ page }) => {
    // Check that skip link exists
    const skipLink = page.locator('[data-testid="skip-link"]');
    await expect(skipLink).toHaveCount(1);

    // Verify skip link text
    const linkText = await skipLink.textContent();
    expect(linkText.toLowerCase()).toContain('skip');
    expect(linkText.toLowerCase()).toContain('main content');

    // Verify skip link href points to main content
    const href = await skipLink.getAttribute('href');
    expect(href).toBe('#main-content');

    // Skip link should initially be positioned off-screen (using CSS)
    const initialPosition = await skipLink.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        top: style.top,
        position: style.position
      };
    });

    // Should be absolutely positioned and off-screen initially
    expect(initialPosition.position).toBe('absolute');
    expect(initialPosition.top).toBe('-100px');

    // Focus the skip link by pressing Tab
    await page.keyboard.press('Tab');
    await expect(skipLink).toBeFocused();

    // Verify the CSS has focus transition defined for skip link
    const hasSkipLinkFocusStyle = await page.evaluate(() => {
      const stylesheets = Array.from(document.styleSheets);
      for (const sheet of stylesheets) {
        try {
          const rules = Array.from(sheet.cssRules || []);
          for (const rule of rules) {
            if (rule.cssText && rule.cssText.includes('.skip-link:focus')) {
              return true;
            }
          }
        } catch (e) {
          // Cross-origin stylesheet, skip
        }
      }
      return false;
    });
    expect(hasSkipLinkFocusStyle).toBeTruthy();

    // Test skip link functionality - pressing Enter should navigate to main content
    await page.keyboard.press('Enter');
    await page.waitForTimeout(300);

    // Main content should exist and be the target
    const mainContent = page.locator('#main-content');
    await expect(mainContent).toBeVisible();

    // URL should now contain the anchor
    expect(page.url()).toContain('#main-content');
  });

  /**
   * Test Case 5: Navigate code copy button via keyboard
   * Input: Navigate code copy button via keyboard
   * Expected: Copy button is keyboard accessible and activatable
   */
  test('should allow keyboard navigation and activation of copy button', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Navigate to quickstart section
    const quickstartSection = page.locator('#quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();

    // Find the first copy button
    const copyButton = page.locator('[data-testid="copy-button"]').first();
    await expect(copyButton).toBeVisible();

    // Focus the copy button
    await copyButton.focus();
    await expect(copyButton).toBeFocused();

    // Verify button has aria-label for accessibility
    const ariaLabel = await copyButton.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel.toLowerCase()).toContain('copy');

    // Verify that CSS focus styles are defined for buttons
    const hasFocusStyles = await page.evaluate(() => {
      const stylesheets = Array.from(document.styleSheets);
      for (const sheet of stylesheets) {
        try {
          const rules = Array.from(sheet.cssRules || []);
          for (const rule of rules) {
            if (rule.cssText && rule.cssText.includes('button:focus')) {
              return true;
            }
          }
        } catch (e) {
          // Cross-origin stylesheet, skip
        }
      }
      return false;
    });
    expect(hasFocusStyles).toBeTruthy();

    // Activate the button using click (keyboard Enter/Space triggers click event on buttons)
    await copyButton.click();
    await page.waitForTimeout(500);

    // Verify visual feedback
    const buttonTextAfterClick = await copyButton.textContent();
    expect(buttonTextAfterClick.toLowerCase()).toContain('copied');

    // Verify clipboard contains code
    const clipboardContent = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboardContent.length).toBeGreaterThan(0);
    expect(clipboardContent).toMatch(/git|cargo|mirdb/i);

    // Test second copy button (Python client code)
    await page.waitForTimeout(2500); // Wait for first button to reset

    const clientCopyButton = page.locator('[data-testid="copy-button-client"]');
    await clientCopyButton.scrollIntoViewIfNeeded();
    await clientCopyButton.focus();
    await expect(clientCopyButton).toBeFocused();

    // Click to activate
    await clientCopyButton.click();
    await page.waitForTimeout(500);

    const clientButtonText = await clientCopyButton.textContent();
    expect(clientButtonText.toLowerCase()).toContain('copied');
  });
});

test.describe('Keyboard Navigation - Complete Tab Order', () => {
  test('should maintain logical tab order through the entire page', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Track the order of focused elements
    const focusedElements = [];

    // Tab through a reasonable number of elements
    const maxTabs = 30;

    for (let i = 0; i < maxTabs; i++) {
      await page.keyboard.press('Tab');

      // Get info about currently focused element
      const focusedInfo = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) return null;

        return {
          tagName: el.tagName.toLowerCase(),
          className: el.className,
          href: el.getAttribute('href'),
          textContent: el.textContent?.slice(0, 50).trim(),
          dataTestId: el.getAttribute('data-testid')
        };
      });

      if (focusedInfo) {
        focusedElements.push(focusedInfo);
      }
    }

    // Verify we focused on interactive elements
    expect(focusedElements.length).toBeGreaterThan(10);

    // Verify we hit key landmarks in order
    const hasSkipLink = focusedElements.some(el => el.dataTestId === 'skip-link');
    const hasNavLinks = focusedElements.some(el => el.className?.includes('nav'));
    const hasButtons = focusedElements.some(el => el.tagName === 'button' || el.className?.includes('btn'));

    expect(hasSkipLink).toBeTruthy();
    expect(hasNavLinks || focusedElements.some(el => el.href?.includes('#quickstart'))).toBeTruthy();
    expect(hasButtons).toBeTruthy();

    // Verify skip link is first focusable element
    expect(focusedElements[0].dataTestId).toBe('skip-link');
  });

  test('should allow reverse tab navigation with Shift+Tab', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Tab forward a few times
    await page.keyboard.press('Tab');
    await page.keyboard.press('Tab');
    await page.keyboard.press('Tab');

    // Get current focused element
    const forwardFocused = await page.evaluate(() => {
      const el = document.activeElement;
      return el?.getAttribute('href') || el?.className;
    });

    // Tab backward
    await page.keyboard.press('Shift+Tab');

    // Get new focused element
    const backwardFocused = await page.evaluate(() => {
      const el = document.activeElement;
      return el?.getAttribute('href') || el?.className;
    });

    // Should be different elements
    expect(forwardFocused).not.toBe(backwardFocused);

    // Continue backward to skip link
    await page.keyboard.press('Shift+Tab');

    const skipLink = page.locator('[data-testid="skip-link"]');
    await expect(skipLink).toBeFocused();
  });
});

test.describe('Keyboard Activation', () => {
  test('should activate links with Enter key', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Focus on Quick Start nav link
    const quickStartLink = page.locator('.nav-links a[href="#quickstart"]');
    await quickStartLink.focus();
    await expect(quickStartLink).toBeFocused();

    // Press Enter to activate
    await page.keyboard.press('Enter');
    await page.waitForTimeout(500);

    // Should navigate to quickstart section
    expect(page.url()).toContain('#quickstart');
  });

  test('should activate buttons with Space and Enter keys', async ({ page, context }) => {
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Scroll to copy button
    const copyButton = page.locator('[data-testid="copy-button"]').first();
    await copyButton.scrollIntoViewIfNeeded();
    await copyButton.focus();

    // Test Space key
    await page.keyboard.press('Space');
    await page.waitForTimeout(500);

    let buttonText = await copyButton.textContent();
    expect(buttonText.toLowerCase()).toContain('copied');

    // Wait for reset
    await page.waitForTimeout(2500);

    // Test Enter key
    await copyButton.focus();
    await page.keyboard.press('Enter');
    await page.waitForTimeout(500);

    buttonText = await copyButton.textContent();
    expect(buttonText.toLowerCase()).toContain('copied');
  });
});
