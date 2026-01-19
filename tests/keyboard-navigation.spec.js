// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Accessibility - Keyboard Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Tab through page from start to end - All interactive elements receive focus in logical order', async ({ page }) => {
    // Define the expected order of interactive elements
    const expectedFocusOrder = [
      // Hero section buttons
      { selector: 'a.btn-primary, a.cta-primary', description: 'Get Started button' },
      { selector: 'a.btn-secondary, a.cta-secondary', description: 'Learn More button' },
      // Footer links
      { selector: 'footer a[href*="circleci"]', description: 'CircleCI badge link' },
      { selector: 'footer a.github-link', description: 'GitHub footer link' },
    ];

    // Start by focusing on the body to simulate user starting keyboard navigation
    await page.keyboard.press('Tab');

    // Track focused elements
    const focusedElements = [];

    // Tab through and verify each element receives focus
    for (let i = 0; i < expectedFocusOrder.length; i++) {
      const expected = expectedFocusOrder[i];

      // Get the currently focused element
      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el) return null;
        return {
          tagName: el.tagName.toLowerCase(),
          className: el.className,
          href: el.getAttribute('href'),
          text: el.textContent?.trim().substring(0, 50)
        };
      });

      // Verify the expected element is focused
      const expectedElement = page.locator(expected.selector).first();
      const isFocused = await expectedElement.evaluate(el => el === document.activeElement);

      expect(isFocused, `${expected.description} should be focused at position ${i + 1}`).toBe(true);

      focusedElements.push({ ...expected, focused: focusedElement });

      // Move to next element (except on the last one)
      if (i < expectedFocusOrder.length - 1) {
        await page.keyboard.press('Tab');
      }
    }

    // Verify we found all expected focusable elements
    expect(focusedElements.length).toBe(expectedFocusOrder.length);
  });

  test('TC2: Check focus visibility on buttons - Focused buttons have visible focus ring or indicator', async ({ page }) => {
    // Get all buttons (anchor elements styled as buttons)
    const buttons = page.locator('.btn, [class*="btn-"]');
    const buttonCount = await buttons.count();

    expect(buttonCount).toBeGreaterThan(0);

    for (let i = 0; i < buttonCount; i++) {
      const button = buttons.nth(i);

      // Focus the button
      await button.focus();

      // Give browser time to apply focus styles
      await page.waitForTimeout(100);

      // Check that the button is visible and focused
      await expect(button).toBeFocused();

      // Get computed styles to verify focus indicator
      const focusStyles = await button.evaluate(el => {
        const styles = window.getComputedStyle(el);
        return {
          outline: styles.outline,
          outlineWidth: styles.outlineWidth,
          outlineStyle: styles.outlineStyle,
          outlineColor: styles.outlineColor,
          boxShadow: styles.boxShadow,
          border: styles.border,
          borderColor: styles.borderColor
        };
      });

      // Check for visible focus indicator (outline, box-shadow, or border change)
      const hasOutline = focusStyles.outlineStyle !== 'none' && focusStyles.outlineWidth !== '0px';
      const hasBoxShadow = focusStyles.boxShadow !== 'none' && focusStyles.boxShadow !== '';

      // At minimum, there should be some visual focus indicator
      // Browsers provide default focus styles, so this should pass
      const hasFocusIndicator = hasOutline || hasBoxShadow;

      expect(hasFocusIndicator, `Button at index ${i} should have visible focus indicator`).toBe(true);
    }
  });

  test('TC3: Check focus visibility on links - Focused links have visible focus indicator', async ({ page }) => {
    // Get all links on the page
    const links = page.locator('a[href]');
    const linkCount = await links.count();

    expect(linkCount).toBeGreaterThan(0);

    for (let i = 0; i < linkCount; i++) {
      const link = links.nth(i);

      // Skip links that might not be visible
      const isVisible = await link.isVisible();
      if (!isVisible) continue;

      // Focus the link
      await link.focus();

      // Give browser time to apply focus styles
      await page.waitForTimeout(100);

      // Check that the link is focused
      await expect(link).toBeFocused();

      // Get computed styles to verify focus indicator
      const focusStyles = await link.evaluate(el => {
        const styles = window.getComputedStyle(el);
        return {
          outline: styles.outline,
          outlineWidth: styles.outlineWidth,
          outlineStyle: styles.outlineStyle,
          outlineColor: styles.outlineColor,
          boxShadow: styles.boxShadow,
          textDecoration: styles.textDecoration
        };
      });

      // Check for visible focus indicator
      const hasOutline = focusStyles.outlineStyle !== 'none' && focusStyles.outlineWidth !== '0px';
      const hasBoxShadow = focusStyles.boxShadow !== 'none' && focusStyles.boxShadow !== '';

      // Browser default focus styles should provide visible focus
      const hasFocusIndicator = hasOutline || hasBoxShadow;

      expect(hasFocusIndicator, `Link at index ${i} should have visible focus indicator`).toBe(true);
    }
  });

  test('TC4: Activate CTA button using Enter key - Button can be activated with keyboard', async ({ page }) => {
    // Get the primary CTA button
    const ctaButton = page.locator('.hero a.btn-primary, .hero .cta-primary').first();
    await expect(ctaButton).toBeVisible();

    // Get the href of the button to verify navigation would occur
    const href = await ctaButton.getAttribute('href');
    expect(href).toBeTruthy();

    // Focus the button
    await ctaButton.focus();
    await expect(ctaButton).toBeFocused();

    // Since the link opens in a new tab, we need to listen for the popup
    const [popup] = await Promise.all([
      page.waitForEvent('popup'),
      page.keyboard.press('Enter')
    ]);

    // Verify the popup was opened (Enter key activated the link)
    expect(popup).toBeTruthy();

    // Verify the popup URL matches the expected GitHub URL
    await popup.waitForLoadState();
    const popupUrl = popup.url();
    expect(popupUrl).toContain('github.com/yetone/mirdb');

    // Close the popup
    await popup.close();

    // Also test with Space key on a different button
    // Note: Native links don't respond to Space in most browsers,
    // but we should verify the button is still accessible
    const secondaryButton = page.locator('.hero a.btn-secondary, .hero .cta-secondary').first();
    await expect(secondaryButton).toBeVisible();

    // Focus and press Enter on secondary button (internal anchor link)
    await secondaryButton.focus();
    await expect(secondaryButton).toBeFocused();

    // Get the href for the internal link
    const secondaryHref = await secondaryButton.getAttribute('href');
    expect(secondaryHref).toBe('#features');

    // Press Enter to activate the internal link
    await page.keyboard.press('Enter');

    // Verify navigation to the features section occurred
    const currentUrl = page.url();
    expect(currentUrl).toContain('#features');

    // Verify the features section is now visible in viewport
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });
});
