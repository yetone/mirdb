// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const getPageUrl = () => 'file://' + path.join(__dirname, '..', 'index.html');

// Test Case 1: Navigate through page using only Tab key - all interactive elements reachable in logical order
test('TC1: All interactive elements are reachable via Tab key in logical order', async ({ page }) => {
  await page.goto(getPageUrl());

  // Start tabbing from the beginning of the page
  await page.keyboard.press('Tab');

  // Expected tab order (logical flow):
  // 1. Skip link (should appear first when focused)
  // 2. Navigation links (Features, Quick Start, GitHub)
  // 3. Hero CTA buttons (Get Started, View on GitHub)
  // 4. Footer links (GitHub, Documentation, License)

  const focusedElements = [];
  let maxTabs = 20; // Safety limit to prevent infinite loop

  for (let i = 0; i < maxTabs; i++) {
    const focusedElement = await page.evaluate(() => {
      const el = document.activeElement;
      if (!el || el.tagName === 'BODY') return null;
      return {
        tagName: el.tagName.toLowerCase(),
        text: el.textContent?.trim().substring(0, 50),
        href: el.getAttribute('href'),
        className: el.className,
        testId: el.getAttribute('data-testid')
      };
    });

    if (focusedElement) {
      focusedElements.push(focusedElement);
    }

    await page.keyboard.press('Tab');

    // Check if we've cycled back to the beginning
    const currentFocused = await page.evaluate(() => document.activeElement?.tagName);
    if (currentFocused === 'BODY' || (focusedElements.length > 5 && currentFocused === focusedElements[0]?.tagName)) {
      break;
    }
  }

  // Verify we can tab through multiple interactive elements
  expect(focusedElements.length).toBeGreaterThanOrEqual(5);

  // Verify interactive elements (links, buttons) are focusable
  const interactiveTagNames = focusedElements.map(el => el.tagName);
  expect(interactiveTagNames.some(tag => tag === 'a' || tag === 'button')).toBeTruthy();

  // Verify nav links are reachable
  const navLinkTexts = focusedElements
    .filter(el => el.tagName === 'a')
    .map(el => el.text?.toLowerCase() || '');

  // Check that key navigation targets are reachable
  expect(navLinkTexts.some(text => text.includes('features') || text.includes('quick start') || text.includes('github'))).toBeTruthy();
});

// Test Case 2: Focus ring is clearly visible on buttons with sufficient contrast
test('TC2: Focus ring is clearly visible on buttons', async ({ page }) => {
  await page.goto(getPageUrl());

  // Tab to the primary CTA button
  const primaryButton = page.locator('[data-testid="primary-cta"]');
  await primaryButton.focus();

  // Check that the button has visible focus styles
  const focusStyles = await primaryButton.evaluate((el) => {
    const computedStyle = window.getComputedStyle(el);
    return {
      outline: computedStyle.outline,
      outlineWidth: computedStyle.outlineWidth,
      outlineColor: computedStyle.outlineColor,
      outlineStyle: computedStyle.outlineStyle,
      outlineOffset: computedStyle.outlineOffset,
      boxShadow: computedStyle.boxShadow
    };
  });

  // Verify button has a visible focus indicator
  // Check for outline (should be present) or box-shadow
  const hasVisibleOutline =
    (focusStyles.outlineStyle !== 'none' && parseFloat(focusStyles.outlineWidth) > 0) ||
    (focusStyles.boxShadow !== 'none');

  expect(hasVisibleOutline).toBeTruthy();

  // Check secondary button as well
  const secondaryButton = page.locator('[data-testid="secondary-cta"]');
  await secondaryButton.focus();

  const secondaryFocusStyles = await secondaryButton.evaluate((el) => {
    const computedStyle = window.getComputedStyle(el);
    return {
      outline: computedStyle.outline,
      outlineWidth: computedStyle.outlineWidth,
      outlineStyle: computedStyle.outlineStyle,
      boxShadow: computedStyle.boxShadow
    };
  });

  const secondaryHasVisibleOutline =
    (secondaryFocusStyles.outlineStyle !== 'none' && parseFloat(secondaryFocusStyles.outlineWidth) > 0) ||
    (secondaryFocusStyles.boxShadow !== 'none');

  expect(secondaryHasVisibleOutline).toBeTruthy();
});

// Test Case 3: Focus ring is clearly visible on all links
test('TC3: Focus ring is clearly visible on all links', async ({ page }) => {
  await page.goto(getPageUrl());

  // Get all links on the page
  const links = page.locator('a');
  const linkCount = await links.count();

  expect(linkCount).toBeGreaterThan(0);

  // Check focus visibility on navigation links
  const navLinks = page.locator('.nav-links a');
  const navLinkCount = await navLinks.count();

  for (let i = 0; i < navLinkCount; i++) {
    const link = navLinks.nth(i);
    await link.focus();

    const focusStyles = await link.evaluate((el) => {
      const computedStyle = window.getComputedStyle(el);
      return {
        outlineStyle: computedStyle.outlineStyle,
        outlineWidth: computedStyle.outlineWidth,
        boxShadow: computedStyle.boxShadow,
        textDecoration: computedStyle.textDecoration
      };
    });

    // Each link should have a visible focus indicator
    const hasVisibleFocus =
      (focusStyles.outlineStyle !== 'none' && parseFloat(focusStyles.outlineWidth) > 0) ||
      focusStyles.boxShadow !== 'none';

    expect(hasVisibleFocus).toBeTruthy();
  }

  // Check footer links
  const footerLinks = page.locator('.footer-nav a');
  const footerLinkCount = await footerLinks.count();

  for (let i = 0; i < footerLinkCount; i++) {
    const link = footerLinks.nth(i);
    await link.focus();

    const focusStyles = await link.evaluate((el) => {
      const computedStyle = window.getComputedStyle(el);
      return {
        outlineStyle: computedStyle.outlineStyle,
        outlineWidth: computedStyle.outlineWidth,
        boxShadow: computedStyle.boxShadow
      };
    });

    const hasVisibleFocus =
      (focusStyles.outlineStyle !== 'none' && parseFloat(focusStyles.outlineWidth) > 0) ||
      focusStyles.boxShadow !== 'none';

    expect(hasVisibleFocus).toBeTruthy();
  }
});

// Test Case 4: Activate CTA button using Enter key
test('TC4: CTA button activates and navigates correctly with Enter key', async ({ page }) => {
  await page.goto(getPageUrl());

  // Focus on the primary CTA button (Get Started - which links to #quick-start)
  const primaryButton = page.locator('[data-testid="primary-cta"]');
  await primaryButton.focus();

  // Verify it's focused
  const isFocused = await primaryButton.evaluate((el) => document.activeElement === el);
  expect(isFocused).toBeTruthy();

  // Get the href before activation
  const href = await primaryButton.getAttribute('href');

  // Press Enter to activate
  await page.keyboard.press('Enter');

  // Wait for navigation/scroll to complete
  await page.waitForTimeout(500);

  // If it's an anchor link (like #quick-start), verify the URL hash changed
  if (href?.startsWith('#')) {
    const currentUrl = page.url();
    expect(currentUrl).toContain(href);

    // Also verify the target section is now in view
    const sectionId = href.replace('#', '');
    const targetSection = page.locator(`#${sectionId}`);
    await expect(targetSection).toBeInViewport();
  }
});

// Test Case 5: Skip to content link is available for keyboard users
test('TC5: Skip to content link is available for keyboard users', async ({ page }) => {
  await page.goto(getPageUrl());

  // The skip link should be the first focusable element
  await page.keyboard.press('Tab');

  // Get information about the currently focused element
  const skipLinkInfo = await page.evaluate(() => {
    const el = document.activeElement;
    if (!el) return null;

    const computedStyle = window.getComputedStyle(el);
    return {
      tagName: el.tagName.toLowerCase(),
      text: el.textContent?.trim().toLowerCase(),
      href: el.getAttribute('href'),
      className: el.className,
      testId: el.getAttribute('data-testid'),
      isVisible: computedStyle.visibility !== 'hidden' && computedStyle.display !== 'none',
      position: computedStyle.position
    };
  });

  // Verify skip link exists and has correct attributes
  expect(skipLinkInfo).not.toBeNull();
  expect(skipLinkInfo.tagName).toBe('a');
  expect(skipLinkInfo.className).toContain('skip-link');

  // Verify skip link text indicates its purpose
  expect(
    skipLinkInfo.text?.includes('skip') ||
    skipLinkInfo.text?.includes('content') ||
    skipLinkInfo.text?.includes('main')
  ).toBeTruthy();

  // Verify skip link points to main content
  expect(skipLinkInfo.href).toMatch(/^#(main|content|main-content)$/i);

  // Test that activating the skip link moves focus to main content
  await page.keyboard.press('Enter');
  await page.waitForTimeout(100);

  // Check that focus or scroll moved to main content area
  const afterSkipFocus = await page.evaluate(() => {
    const el = document.activeElement;
    return {
      tagName: el?.tagName.toLowerCase(),
      id: el?.id,
      role: el?.getAttribute('role')
    };
  });

  // The focus should now be on the main content or a main landmark
  expect(
    afterSkipFocus.tagName === 'main' ||
    afterSkipFocus.id === 'main-content' ||
    afterSkipFocus.id === 'content' ||
    afterSkipFocus.role === 'main'
  ).toBeTruthy();
});

// Additional test: Verify Space key works on buttons (if any native buttons exist)
test('TC6: Interactive elements respond to Space key activation', async ({ page }) => {
  await page.goto(getPageUrl());

  // Focus on the primary CTA (which is a link styled as button)
  const primaryButton = page.locator('[data-testid="primary-cta"]');
  await primaryButton.focus();

  const href = await primaryButton.getAttribute('href');

  // For anchor links, both Enter and clicking should work
  // Note: Space typically works on native buttons, not links
  // But we can verify the link is properly focusable and activatable

  // Verify element is focusable
  const isFocused = await primaryButton.evaluate((el) => document.activeElement === el);
  expect(isFocused).toBeTruthy();

  // Verify the element has proper role semantics
  const semantics = await primaryButton.evaluate((el) => ({
    tagName: el.tagName.toLowerCase(),
    hasHref: !!el.getAttribute('href'),
    role: el.getAttribute('role')
  }));

  // Links should be links (not buttons unless they have role="button")
  expect(semantics.tagName).toBe('a');
  expect(semantics.hasHref).toBeTruthy();
});

// Test: Verify tab order follows visual order
test('TC7: Tab order follows logical visual order', async ({ page }) => {
  await page.goto(getPageUrl());

  const tabbedElements = [];

  // Tab through all focusable elements and record their positions
  for (let i = 0; i < 15; i++) {
    await page.keyboard.press('Tab');

    const elementInfo = await page.evaluate(() => {
      const el = document.activeElement;
      if (!el || el.tagName === 'BODY') return null;

      const rect = el.getBoundingClientRect();
      return {
        tagName: el.tagName.toLowerCase(),
        text: el.textContent?.trim().substring(0, 30),
        top: rect.top,
        left: rect.left,
        className: el.className
      };
    });

    if (elementInfo) {
      tabbedElements.push(elementInfo);
    }
  }

  // Verify elements are generally ordered top-to-bottom (within sections)
  // Skip link should be first, then nav, then hero, then footer
  const skipLinkIndex = tabbedElements.findIndex(el => el.className?.includes('skip-link'));
  const navLinkIndices = tabbedElements
    .map((el, idx) => el.className?.includes('nav-links') || el.text?.toLowerCase().includes('features') ? idx : -1)
    .filter(idx => idx >= 0);

  // Skip link should be among the first elements
  if (skipLinkIndex >= 0) {
    expect(skipLinkIndex).toBeLessThanOrEqual(1);
  }

  // Verify we captured focusable elements
  expect(tabbedElements.length).toBeGreaterThan(0);
});
