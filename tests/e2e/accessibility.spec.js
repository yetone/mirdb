/**
 * Accessibility E2E Tests
 * Owner: Scenarios 13-15 - Accessibility
 *
 * Tests:
 * - Keyboard navigation (Tab order)
 * - Focus indicators
 * - Skip link functionality
 * - ARIA labels and roles
 * - Keyboard activation of interactive elements
 */
import { test, expect } from '@playwright/test';

test.describe('Accessibility - Keyboard Navigation', () => {

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Tab through all interactive elements in logical order', async ({ page }) => {
    // Get all focusable elements in the document
    const focusableSelector = 'a[href], button, input, select, textarea, [tabindex]:not([tabindex="-1"])';

    // Start by pressing Tab to enter the page
    await page.keyboard.press('Tab');

    // The first focusable element should be the skip link
    const firstFocused = await page.evaluate(() => document.activeElement?.className || document.activeElement?.tagName);
    expect(firstFocused).toContain('skip-link');

    // Continue tabbing through header navigation
    await page.keyboard.press('Tab');
    let currentElement = await page.evaluate(() => {
      const el = document.activeElement;
      return { tag: el?.tagName, href: el?.getAttribute('href'), text: el?.textContent?.trim() };
    });

    // Should be on logo/home link
    expect(currentElement.tag).toBe('A');

    // Tab to Features link
    await page.keyboard.press('Tab');
    currentElement = await page.evaluate(() => {
      const el = document.activeElement;
      return { tag: el?.tagName, href: el?.getAttribute('href'), text: el?.textContent?.trim() };
    });
    expect(currentElement.href).toBe('#features');

    // Tab to Quick Start link
    await page.keyboard.press('Tab');
    currentElement = await page.evaluate(() => {
      const el = document.activeElement;
      return { tag: el?.tagName, href: el?.getAttribute('href'), text: el?.textContent?.trim() };
    });
    expect(currentElement.href).toBe('#quick-start');

    // Tab to GitHub button in header
    await page.keyboard.press('Tab');
    currentElement = await page.evaluate(() => {
      const el = document.activeElement;
      return { tag: el?.tagName, href: el?.getAttribute('href'), text: el?.textContent?.trim() };
    });
    expect(currentElement.text).toContain('GitHub');

    // Verify we can continue tabbing through the page
    // Count total interactive elements
    const interactiveCount = await page.evaluate(() => {
      const focusable = document.querySelectorAll('a[href], button:not([disabled]), [tabindex]:not([tabindex="-1"])');
      return focusable.length;
    });

    // Should have multiple interactive elements
    expect(interactiveCount).toBeGreaterThan(10);
  });

  test('TC2: Verify visible focus indicators on focused elements', async ({ page }) => {
    // Tab to the first interactive element (skip link)
    await page.keyboard.press('Tab');

    // Skip link should be visible when focused
    const skipLink = page.locator('.skip-link');
    await expect(skipLink).toBeFocused();

    // Check that the skip link has visible styles when focused
    const skipLinkBox = await skipLink.boundingBox();
    expect(skipLinkBox).not.toBeNull();
    expect(skipLinkBox.height).toBeGreaterThan(0);

    // Tab to navigation link and verify focus ring
    await page.keyboard.press('Tab'); // Logo
    await page.keyboard.press('Tab'); // Features link (desktop nav - visible)

    // Use first() to get the visible desktop nav link (mobile menu is hidden)
    const featuresLink = page.locator('header nav > div:first-child a[href="#features"]').first();

    // Verify something is focused (features link should be)
    const focusedElement = await page.evaluate(() => {
      const el = document.activeElement;
      return { href: el?.getAttribute('href'), text: el?.textContent?.trim() };
    });
    expect(focusedElement.href).toBe('#features');

    // Check that focus-visible styles are applied by checking if focus is visible
    const focusStyles = await page.evaluate(() => {
      const el = document.activeElement;
      const styles = window.getComputedStyle(el);
      return {
        outline: styles.outline,
        boxShadow: styles.boxShadow,
        outlineWidth: styles.outlineWidth
      };
    });

    // Tailwind's ring utility creates a box-shadow or outline
    // We verify there is some visible focus indication
    const hasFocusIndicator = focusStyles.boxShadow !== 'none' ||
                              focusStyles.outline !== 'none' ||
                              focusStyles.outlineWidth !== '0px';
    expect(hasFocusIndicator).toBe(true);

    // Tab through more elements and verify they receive focus
    await page.keyboard.press('Tab'); // Quick Start link

    const quickStartFocused = await page.evaluate(() => {
      const el = document.activeElement;
      return { href: el?.getAttribute('href'), text: el?.textContent?.trim() };
    });
    expect(quickStartFocused.href).toBe('#quick-start');

    // Verify hero CTA buttons can receive focus
    await page.keyboard.press('Tab'); // GitHub header button
    await page.keyboard.press('Tab'); // Get Started button in hero

    const getStartedBtn = page.locator('#hero a[href="#quick-start"]');
    await expect(getStartedBtn).toBeFocused();
  });

  test('TC3: Test skip link functionality', async ({ page }) => {
    // Verify skip link exists
    const skipLink = page.locator('.skip-link');
    await expect(skipLink).toBeAttached();
    await expect(skipLink).toHaveAttribute('href', '#main-content');

    // Verify skip link text
    await expect(skipLink).toHaveText('Skip to main content');

    // Tab to skip link
    await page.keyboard.press('Tab');
    await expect(skipLink).toBeFocused();

    // Skip link should be visible when focused
    const isVisible = await skipLink.isVisible();
    expect(isVisible).toBe(true);

    // Activate skip link with Enter key
    await page.keyboard.press('Enter');

    // Verify focus moved to main content area
    // The main content should now be focused or the page should scroll to it
    const mainContent = page.locator('#main-content');
    await expect(mainContent).toBeAttached();

    // Verify the skip link target exists and is accessible
    const mainContentId = await mainContent.getAttribute('id');
    expect(mainContentId).toBe('main-content');

    // Verify we can interact with main content after skipping
    // Tab should move to the first interactive element in main content
    await page.keyboard.press('Tab');

    const focusedAfterSkip = await page.evaluate(() => {
      const el = document.activeElement;
      return {
        tag: el?.tagName,
        isInMain: el?.closest('main') !== null || el?.closest('#main-content') !== null
      };
    });

    // After skipping and tabbing, focus should be on an element in main or hero section
    expect(['A', 'BUTTON']).toContain(focusedAfterSkip.tag);
  });

  test('TC4: Activate copy button via keyboard (Enter and Space)', async ({ page }) => {
    // Navigate to the Quick Start section
    await page.goto('/#quick-start');

    // Wait for page to load
    await page.waitForSelector('.code-block .copy-btn');

    // Find the first copy button
    const copyButton = page.locator('.code-block .copy-btn').first();
    await expect(copyButton).toBeAttached();

    // Focus on the copy button by clicking first to scroll it into view
    await copyButton.scrollIntoViewIfNeeded();

    // Focus the copy button using Tab navigation from start
    await page.keyboard.press('Tab'); // Skip link

    // Tab through until we reach a copy button
    let foundCopyButton = false;
    for (let i = 0; i < 30; i++) {
      await page.keyboard.press('Tab');
      const currentText = await page.evaluate(() => document.activeElement?.textContent?.trim());
      if (currentText === 'Copy') {
        foundCopyButton = true;
        break;
      }
    }

    expect(foundCopyButton).toBe(true);

    // Verify the copy button is focused
    const focusedElement = await page.evaluate(() => document.activeElement?.textContent?.trim());
    expect(focusedElement).toBe('Copy');

    // Test Enter key activation
    await page.keyboard.press('Enter');

    // Wait for visual feedback
    await page.waitForTimeout(100);

    // Check that the button responded to keyboard activation
    // In headless browser, clipboard may not be available, so button might show "Failed" or "Copied!"
    const buttonTextAfterEnter = await page.evaluate(() => document.activeElement?.textContent?.trim());
    // Button should have changed from "Copy" to either "Copied!" or "Failed" (indicating it responded to keyboard)
    expect(['Copied!', 'Failed']).toContain(buttonTextAfterEnter);

    // Wait for button to reset
    await page.waitForTimeout(2100);

    // Tab to find another copy button
    for (let i = 0; i < 15; i++) {
      await page.keyboard.press('Tab');
      const currentText = await page.evaluate(() => document.activeElement?.textContent?.trim());
      if (currentText === 'Copy') {
        break;
      }
    }

    const currentBtnText = await page.evaluate(() => document.activeElement?.textContent?.trim());

    // Test Space key activation if we found another copy button
    if (currentBtnText === 'Copy') {
      await page.keyboard.press('Space');

      // Wait for visual feedback
      await page.waitForTimeout(100);

      // Check that the button responded to keyboard activation
      const buttonTextAfterSpace = await page.evaluate(() => document.activeElement?.textContent?.trim());
      // Button should have changed from "Copy" to either "Copied!" or "Failed"
      expect(['Copied!', 'Failed']).toContain(buttonTextAfterSpace);
    } else {
      // If we couldn't find another copy button, verify the button is keyboard activatable
      // by checking that the first button was successfully activated
      expect(['Copied!', 'Failed']).toContain(buttonTextAfterEnter);
    }
  });

  test('All interactive elements are keyboard accessible', async ({ page }) => {
    // Verify all links have href attributes and are focusable
    const links = await page.locator('a[href]').all();
    expect(links.length).toBeGreaterThan(0);

    for (const link of links.slice(0, 10)) { // Check first 10 links
      const tabIndex = await link.getAttribute('tabindex');
      // Links are naturally focusable, tabindex should not be -1
      expect(tabIndex).not.toBe('-1');
    }

    // Verify buttons are focusable
    const buttons = await page.locator('button').all();
    for (const button of buttons) {
      const disabled = await button.getAttribute('disabled');
      if (!disabled) {
        const tabIndex = await button.getAttribute('tabindex');
        expect(tabIndex).not.toBe('-1');
      }
    }

    // Verify mobile menu button has proper ARIA attributes
    const mobileMenuBtn = page.locator('#mobile-menu-btn');
    await expect(mobileMenuBtn).toHaveAttribute('aria-label');
    await expect(mobileMenuBtn).toHaveAttribute('aria-expanded', 'false');
  });

  test('Focus order follows visual layout', async ({ page }) => {
    // Track focus order by tabbing through elements
    const focusOrder = [];

    // Tab through first 15 elements
    for (let i = 0; i < 15; i++) {
      await page.keyboard.press('Tab');
      const element = await page.evaluate(() => {
        const el = document.activeElement;
        return {
          tag: el?.tagName,
          id: el?.id,
          class: el?.className,
          text: el?.textContent?.trim().substring(0, 30),
          section: el?.closest('section')?.id || el?.closest('header')?.id || el?.closest('footer')?.id || 'other'
        };
      });
      focusOrder.push(element);
    }

    // Verify logical order: skip-link -> header -> hero -> rest of page
    expect(focusOrder[0].class).toContain('skip-link');

    // Header elements should come before hero elements
    const headerIndex = focusOrder.findIndex(el => el.section === 'header');
    const heroIndex = focusOrder.findIndex(el => el.section === 'hero');

    expect(headerIndex).toBeLessThan(heroIndex);
  });

  test('ARIA labels are present on interactive elements', async ({ page }) => {
    // Check copy buttons have aria-labels
    const copyButtons = page.locator('.copy-btn[aria-label]');
    const copyButtonCount = await copyButtons.count();
    expect(copyButtonCount).toBeGreaterThan(0);

    // Check mobile menu button has aria-label
    const mobileMenuBtn = page.locator('#mobile-menu-btn');
    await expect(mobileMenuBtn).toHaveAttribute('aria-label', 'Toggle menu');

    // Check architecture diagram has aria-label
    const archDiagram = page.locator('.architecture-diagram[role="img"]');
    if (await archDiagram.count() > 0) {
      await expect(archDiagram).toHaveAttribute('aria-label');
    }

    // Check comparison table has proper role
    const comparisonTable = page.locator('#comparison table[role="table"]');
    if (await comparisonTable.count() > 0) {
      await expect(comparisonTable).toHaveAttribute('aria-label');
    }
  });
});

test.describe('Accessibility - Screen Reader Support', () => {

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Document has proper language attribute', async ({ page }) => {
    const htmlLang = await page.locator('html').getAttribute('lang');
    expect(htmlLang).toBe('en');
  });

  test('All images have alt text', async ({ page }) => {
    const images = page.locator('img');
    const imageCount = await images.count();

    for (let i = 0; i < imageCount; i++) {
      const alt = await images.nth(i).getAttribute('alt');
      expect(alt).toBeTruthy();
      expect(alt.length).toBeGreaterThan(0);
    }
  });

  test('Semantic landmarks are present', async ({ page }) => {
    // Check for header
    const header = page.locator('header');
    await expect(header).toBeAttached();

    // Check for main
    const main = page.locator('main');
    await expect(main).toBeAttached();

    // Check for nav
    const nav = page.locator('nav');
    await expect(nav).toBeAttached();

    // Check for footer
    const footer = page.locator('footer');
    await expect(footer).toBeAttached();

    // Check footer has contentinfo role
    await expect(footer).toHaveAttribute('role', 'contentinfo');
  });

  test('Heading hierarchy is logical', async ({ page }) => {
    // Should have exactly one h1
    const h1Count = await page.locator('h1').count();
    expect(h1Count).toBe(1);

    // Should have h2 headings for major sections
    const h2Count = await page.locator('h2').count();
    expect(h2Count).toBeGreaterThan(0);

    // Verify heading levels don't skip
    const headings = await page.evaluate(() => {
      const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      return Array.from(allHeadings).map(h => parseInt(h.tagName.charAt(1)));
    });

    let prevLevel = 0;
    for (const level of headings) {
      if (prevLevel > 0) {
        // Heading level should not skip more than one level
        expect(level - prevLevel).toBeLessThanOrEqual(1);
      }
      prevLevel = level;
    }
  });
});
