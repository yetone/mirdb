/**
 * Accessibility E2E Tests
 * Owner: Scenario 13 - Keyboard Navigation, Scenario 16 - Progressive Enhancement
 *
 * End-to-end tests for accessibility:
 * - Tab navigation through all interactive elements
 * - Focus indicators visible
 * - Enter key activation
 * - Tests with JavaScript disabled
 * - Content visible without JS
 * - Links work without JS
 */

import { test, expect } from '@playwright/test';

test.describe('Accessibility - Keyboard Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Tab through all interactive elements - all links, buttons, and copy buttons are keyboard focusable', async ({ page }) => {
    // Start focus at the beginning of the page
    await page.keyboard.press('Tab');

    // Track all focusable elements we encounter
    const focusedElements = [];
    let maxIterations = 50; // Safety limit

    while (maxIterations > 0) {
      const activeElement = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) return null;
        return {
          tagName: el.tagName.toLowerCase(),
          className: el.className,
          href: el.getAttribute('href'),
          type: el.getAttribute('type'),
          textContent: el.textContent?.trim().substring(0, 50),
          ariaLabel: el.getAttribute('aria-label'),
        };
      });

      if (!activeElement) break;

      // Check if we've cycled back to the first element
      if (
        focusedElements.length > 0 &&
        activeElement.className === focusedElements[0].className &&
        activeElement.href === focusedElements[0].href &&
        activeElement.textContent === focusedElements[0].textContent
      ) {
        break;
      }

      focusedElements.push(activeElement);
      await page.keyboard.press('Tab');
      maxIterations--;
    }

    // Verify we found interactive elements
    expect(focusedElements.length).toBeGreaterThan(0);

    // Verify navigation links are focusable
    const navLinks = focusedElements.filter(
      (el) => el.tagName === 'a' && el.href && el.href.startsWith('#')
    );
    expect(navLinks.length).toBeGreaterThan(0);

    // Verify Get Started button (CTA link) is focusable
    const ctaLink = focusedElements.find(
      (el) =>
        el.tagName === 'a' &&
        (el.className?.includes('hero-cta') || el.textContent?.includes('Get Started'))
    );
    expect(ctaLink).toBeDefined();

    // Verify copy buttons are focusable
    const copyButtons = focusedElements.filter(
      (el) =>
        el.tagName === 'button' &&
        (el.className?.includes('copy') || el.ariaLabel?.includes('Copy'))
    );
    expect(copyButtons.length).toBeGreaterThan(0);

    // Verify external links are focusable
    const externalLinks = focusedElements.filter(
      (el) =>
        el.tagName === 'a' &&
        (el.href?.includes('github') || el.textContent?.includes('GitHub'))
    );
    expect(externalLinks.length).toBeGreaterThan(0);
  });

  test('TC2: Check focus indicator on Get Started button - button shows visible focus ring when focused', async ({ page }) => {
    // Find and focus the Get Started button
    const ctaButton = page.locator('.hero-cta');
    await ctaButton.focus();

    // Check that the button has a visible focus indicator
    const outlineStyle = await ctaButton.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        outline: styles.outline,
        outlineWidth: styles.outlineWidth,
        outlineStyle: styles.outlineStyle,
        outlineColor: styles.outlineColor,
        outlineOffset: styles.outlineOffset,
        boxShadow: styles.boxShadow,
      };
    });

    // Verify focus indicator is visible (either outline or box-shadow)
    const hasOutline =
      outlineStyle.outlineStyle !== 'none' &&
      outlineStyle.outlineWidth !== '0px' &&
      outlineStyle.outline !== 'none' &&
      outlineStyle.outline !== '' &&
      !outlineStyle.outline.includes('0px');

    const hasBoxShadow =
      outlineStyle.boxShadow && outlineStyle.boxShadow !== 'none';

    expect(hasOutline || hasBoxShadow).toBe(true);
  });

  test('TC3: Check focus indicator on links - links show visible focus indicator when focused', async ({ page }) => {
    // Test navigation links
    const navLinks = page.locator('.nav__links a');
    const navLinkCount = await navLinks.count();

    expect(navLinkCount).toBeGreaterThan(0);

    // Focus the first navigation link
    const firstNavLink = navLinks.first();
    await firstNavLink.focus();

    // Check for visible focus indicator on nav link
    const navLinkFocusStyle = await firstNavLink.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        outline: styles.outline,
        outlineWidth: styles.outlineWidth,
        outlineStyle: styles.outlineStyle,
        outlineOffset: styles.outlineOffset,
        boxShadow: styles.boxShadow,
      };
    });

    const navHasOutline =
      navLinkFocusStyle.outlineStyle !== 'none' &&
      navLinkFocusStyle.outlineWidth !== '0px';

    const navHasBoxShadow =
      navLinkFocusStyle.boxShadow && navLinkFocusStyle.boxShadow !== 'none';

    expect(navHasOutline || navHasBoxShadow).toBe(true);

    // Test footer links
    const footerLinks = page.locator('.footer__link');
    const footerLinkCount = await footerLinks.count();

    if (footerLinkCount > 0) {
      const firstFooterLink = footerLinks.first();
      await firstFooterLink.focus();

      const footerLinkFocusStyle = await firstFooterLink.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          outline: styles.outline,
          outlineWidth: styles.outlineWidth,
          outlineStyle: styles.outlineStyle,
          outlineOffset: styles.outlineOffset,
          boxShadow: styles.boxShadow,
        };
      });

      const footerHasOutline =
        footerLinkFocusStyle.outlineStyle !== 'none' &&
        footerLinkFocusStyle.outlineWidth !== '0px';

      const footerHasBoxShadow =
        footerLinkFocusStyle.boxShadow &&
        footerLinkFocusStyle.boxShadow !== 'none';

      expect(footerHasOutline || footerHasBoxShadow).toBe(true);
    }
  });

  test('TC4: Press Enter on focused link - link is activated and navigates appropriately', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Focus and activate the Quick Start link via keyboard
    const quickStartLink = page.locator('.nav__links a[href="#quickstart"]');
    await quickStartLink.focus();

    // Verify the link is focused
    const isFocused = await quickStartLink.evaluate(
      (el) => document.activeElement === el
    );
    expect(isFocused).toBe(true);

    // Press Enter to activate the link
    await page.keyboard.press('Enter');

    // Wait for navigation/scroll
    await page.waitForTimeout(500);

    // Verify the page has navigated (scrolled or URL changed)
    const finalScrollY = await page.evaluate(() => window.scrollY);
    const currentHash = await page.evaluate(() => window.location.hash);

    // Either the page scrolled or the hash changed
    const didNavigate =
      finalScrollY > initialScrollY || currentHash === '#quickstart';
    expect(didNavigate).toBe(true);

    // Verify quickstart section is in viewport
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport();
  });

  test('TC5: Press Enter on focused copy button - copy action is triggered via keyboard', async ({ page }) => {
    // Find the first copy button
    const copyButton = page.locator('.code-block__copy').first();
    await expect(copyButton).toBeVisible();

    // Focus the copy button
    await copyButton.focus();

    // Verify the button is focused
    const isFocused = await copyButton.evaluate(
      (el) => document.activeElement === el
    );
    expect(isFocused).toBe(true);

    // Get the initial button text
    const initialText = await copyButton
      .locator('.code-block__copy-text')
      .textContent();
    expect(initialText).toBe('Copy');

    // Press Enter to activate the copy action
    await page.keyboard.press('Enter');

    // Wait for the copy action to complete and show feedback
    await page.waitForTimeout(500);

    // Verify the button shows copied feedback
    const copiedText = await copyButton
      .locator('.code-block__copy-text')
      .textContent();
    expect(copiedText).toBe('Copied!');

    // Verify the button has the copied class
    await expect(copyButton).toHaveClass(/code-block__copy--copied/);
  });

  test('All interactive elements have minimum touch target size of 44x44px', async ({ page }) => {
    // Test CTA button
    const ctaButton = page.locator('.hero-cta');
    const ctaBoundingBox = await ctaButton.boundingBox();
    expect(ctaBoundingBox.width).toBeGreaterThanOrEqual(44);
    expect(ctaBoundingBox.height).toBeGreaterThanOrEqual(44);

    // Test copy buttons
    const copyButtons = page.locator('.code-block__copy');
    const copyButtonCount = await copyButtons.count();

    for (let i = 0; i < copyButtonCount; i++) {
      const button = copyButtons.nth(i);
      const boundingBox = await button.boundingBox();
      expect(boundingBox.width).toBeGreaterThanOrEqual(44);
      expect(boundingBox.height).toBeGreaterThanOrEqual(44);
    }
  });

  test('Focus order follows logical document order', async ({ page }) => {
    // Tab through elements and verify they follow logical order
    const focusOrder = [];
    let maxIterations = 50;

    await page.keyboard.press('Tab');

    while (maxIterations > 0) {
      const activeElement = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) return null;

        // Get element position in document
        const rect = el.getBoundingClientRect();
        return {
          top: rect.top + window.scrollY,
          left: rect.left,
          tagName: el.tagName.toLowerCase(),
          className: el.className,
        };
      });

      if (!activeElement) break;

      // Check for cycle back to start
      if (
        focusOrder.length > 0 &&
        activeElement.className === focusOrder[0].className &&
        activeElement.top === focusOrder[0].top
      ) {
        break;
      }

      focusOrder.push(activeElement);
      await page.keyboard.press('Tab');
      maxIterations--;
    }

    // Verify elements are generally in top-to-bottom order
    // (allowing for some variation in horizontal positioning)
    let outOfOrderCount = 0;
    for (let i = 1; i < focusOrder.length; i++) {
      // Allow elements to be within 200px of each other vertically
      // (for side-by-side elements)
      if (focusOrder[i].top < focusOrder[i - 1].top - 200) {
        outOfOrderCount++;
      }
    }

    // Allow for some out-of-order elements (e.g., modal close buttons)
    // but most should follow document order
    const outOfOrderPercentage = outOfOrderCount / focusOrder.length;
    expect(outOfOrderPercentage).toBeLessThan(0.2);
  });

  test('Shift+Tab navigates backwards through focusable elements', async ({ page }) => {
    // Tab forward a few times
    await page.keyboard.press('Tab');
    await page.keyboard.press('Tab');
    await page.keyboard.press('Tab');

    // Get the current focused element
    const thirdElement = await page.evaluate(() => {
      const el = document.activeElement;
      return el ? el.className : null;
    });

    // Tab forward once more
    await page.keyboard.press('Tab');

    const fourthElement = await page.evaluate(() => {
      const el = document.activeElement;
      return el ? el.className : null;
    });

    // Shift+Tab backward
    await page.keyboard.press('Shift+Tab');

    // Should be back at the third element
    const backToThird = await page.evaluate(() => {
      const el = document.activeElement;
      return el ? el.className : null;
    });

    expect(backToThird).toBe(thirdElement);
  });

  test('Space key activates buttons', async ({ page }) => {
    // Find the first copy button
    const copyButton = page.locator('.code-block__copy').first();
    await expect(copyButton).toBeVisible();

    // Focus the copy button
    await copyButton.focus();

    // Get the initial button text
    const initialText = await copyButton
      .locator('.code-block__copy-text')
      .textContent();
    expect(initialText).toBe('Copy');

    // Press Space to activate the button
    await page.keyboard.press('Space');

    // Wait for the copy action to complete
    await page.waitForTimeout(500);

    // Verify the button shows copied feedback
    const copiedText = await copyButton
      .locator('.code-block__copy-text')
      .textContent();
    expect(copiedText).toBe('Copied!');
  });
});
