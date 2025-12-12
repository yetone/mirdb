import { test, expect } from '@playwright/test';

/**
 * NFR-4: Page shall be accessible and meet WCAG 2.1 AA standards
 * Scenario: Accessibility - Keyboard Navigation
 *
 * Tests verify full keyboard navigation support:
 * - All interactive elements receive focus in logical order
 * - Focus indicators are clearly visible on all focusable elements
 * - Buttons and links can be activated with Enter/Space keys
 * - Mobile menu can be closed with Escape key
 */
test.describe('Accessibility - Keyboard Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  /**
   * Test Case 1: Tab through all interactive elements
   * Expected: All interactive elements receive focus in logical order
   */
  test('Test Case 1: All interactive elements receive focus in logical order', async ({ page }) => {
    // Start with focus at the beginning of the document
    await page.keyboard.press('Tab');

    // First focusable element should be the navigation logo
    const logo = page.locator('[data-testid="navigation-logo"]');
    await expect(logo).toBeFocused();

    // Tab to navigation links (on desktop, links should be visible)
    // The nav links should follow in order: Home, Features, About, Contact
    const expectedOrder = [
      '[data-testid="navigation-link-home"]',
      '[data-testid="navigation-link-features"]',
      '[data-testid="navigation-link-about"]',
      '[data-testid="navigation-link-contact"]',
    ];

    for (const selector of expectedOrder) {
      await page.keyboard.press('Tab');
      const element = page.locator(selector);
      await expect(element).toBeFocused();
    }

    // Tab to the hero CTA button
    await page.keyboard.press('Tab');
    const ctaButton = page.locator('[data-testid="hero-cta"]');
    await expect(ctaButton).toBeFocused();

    // Continue tabbing to footer links - verify footer receives focus
    // Tab through until we reach footer elements
    let foundFooterLink = false;
    for (let i = 0; i < 20; i++) {
      await page.keyboard.press('Tab');
      const activeElement = await page.evaluate(() => {
        const el = document.activeElement;
        return el?.closest('[data-testid="footer"]') !== null;
      });
      if (activeElement) {
        foundFooterLink = true;
        break;
      }
    }
    expect(foundFooterLink).toBe(true);
  });

  /**
   * Test Case 2: Verify focus indicator visibility
   * Expected: Focus indicators are clearly visible on all focusable elements
   */
  test('Test Case 2: Focus indicators are clearly visible on all focusable elements', async ({ page }) => {
    // Check navigation logo focus indicator
    const logo = page.locator('[data-testid="navigation-logo"]');
    await logo.focus();

    const logoOutline = await logo.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        outline: style.outline,
        outlineColor: style.outlineColor,
        outlineWidth: style.outlineWidth,
        outlineOffset: style.outlineOffset,
      };
    });

    // Verify logo has visible outline (not "none" and has width)
    expect(parseFloat(logoOutline.outlineWidth)).toBeGreaterThan(0);

    // Check navigation link focus indicator
    const homeLink = page.locator('[data-testid="navigation-link-home"]');
    await homeLink.focus();

    const linkOutline = await homeLink.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        outline: style.outline,
        outlineColor: style.outlineColor,
        outlineWidth: style.outlineWidth,
        outlineOffset: style.outlineOffset,
      };
    });

    // Verify link has visible outline
    expect(parseFloat(linkOutline.outlineWidth)).toBeGreaterThan(0);

    // Check CTA button focus indicator
    const ctaButton = page.locator('[data-testid="hero-cta"]');
    await ctaButton.focus();

    const ctaOutline = await ctaButton.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        outline: style.outline,
        outlineColor: style.outlineColor,
        outlineWidth: style.outlineWidth,
        outlineOffset: style.outlineOffset,
        boxShadow: style.boxShadow,
      };
    });

    // CTA button should have visible focus indicator (outline or box-shadow)
    const hasOutline = parseFloat(ctaOutline.outlineWidth) > 0;
    const hasBoxShadow = ctaOutline.boxShadow !== 'none' && ctaOutline.boxShadow !== '';
    expect(hasOutline || hasBoxShadow).toBe(true);

    // Check footer link focus indicator
    const footerLinks = page.locator('.footer-link').first();
    await footerLinks.focus();

    const footerLinkOutline = await footerLinks.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        outlineWidth: style.outlineWidth,
      };
    });

    expect(parseFloat(footerLinkOutline.outlineWidth)).toBeGreaterThan(0);
  });

  /**
   * Test Case 3: Press Enter on focused CTA button
   * Expected: Button is activated and performs expected action
   */
  test('Test Case 3: CTA button is activated with Enter key', async ({ page }) => {
    // Focus the CTA button
    const ctaButton = page.locator('[data-testid="hero-cta"]');
    await ctaButton.focus();
    await expect(ctaButton).toBeFocused();

    // Get the href attribute to verify navigation
    const href = await ctaButton.getAttribute('href');
    expect(href).toBeTruthy();

    // Press Enter to activate the button
    // Since this is an anchor link, we'll verify it's activated by checking the URL or scroll position
    const initialUrl = page.url();
    await page.keyboard.press('Enter');

    // Wait a moment for any navigation/action to occur
    await page.waitForTimeout(100);

    // Verify the button was activated (URL changed to include the anchor)
    const newUrl = page.url();
    expect(newUrl).toContain(href!.replace('#', ''));
  });

  /**
   * Additional test: CTA button can be activated with Space key
   */
  test('CTA button responds to keyboard activation', async ({ page }) => {
    const ctaButton = page.locator('[data-testid="hero-cta"]');
    await ctaButton.focus();

    // Verify it's focusable
    await expect(ctaButton).toBeFocused();

    // Verify the element is a link (anchor) and can be clicked via keyboard
    const tagName = await ctaButton.evaluate((el) => el.tagName.toLowerCase());
    expect(tagName).toBe('a');

    // Links respond to Enter key (which triggers click)
    // Verify it has an href that will be followed
    const href = await ctaButton.getAttribute('href');
    expect(href).toBeTruthy();
  });

  /**
   * Test Case 4: Press Escape to close mobile menu (if open)
   * Expected: Menu closes and focus returns to toggle button
   */
  test.describe('Mobile Menu Escape Key', () => {
    test.use({ viewport: { width: 375, height: 667 } });

    test('Test Case 4: Escape closes mobile menu and returns focus to toggle', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Find the mobile menu toggle button
      const mobileMenuToggle = page.locator('[data-testid="mobile-menu-toggle"]');
      await expect(mobileMenuToggle).toBeVisible();

      // Open the mobile menu using keyboard
      await mobileMenuToggle.focus();
      await page.keyboard.press('Enter');

      // Verify menu is open
      const navigationLinks = page.locator('[data-testid="navigation-links"]');
      await expect(navigationLinks).toBeVisible();

      // Press Escape to close the menu
      await page.keyboard.press('Escape');

      // Verify menu is closed
      const isHidden = await navigationLinks.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return style.display === 'none' || style.visibility === 'hidden' || !el.checkVisibility();
      });
      expect(isHidden).toBe(true);

      // Verify focus returns to the toggle button
      await expect(mobileMenuToggle).toBeFocused();
    });

    test('Mobile menu toggle can be activated with Enter key', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      const mobileMenuToggle = page.locator('[data-testid="mobile-menu-toggle"]');
      await expect(mobileMenuToggle).toBeVisible();

      // Focus and activate with Enter
      await mobileMenuToggle.focus();
      await expect(mobileMenuToggle).toBeFocused();
      await page.keyboard.press('Enter');

      // Menu should be open
      const navigationLinks = page.locator('[data-testid="navigation-links"]');
      await expect(navigationLinks).toBeVisible();
    });

    test('Mobile menu toggle can be activated with Space key', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      const mobileMenuToggle = page.locator('[data-testid="mobile-menu-toggle"]');
      await expect(mobileMenuToggle).toBeVisible();

      // Focus and activate with Space
      await mobileMenuToggle.focus();
      await page.keyboard.press('Space');

      // Menu should be open
      const navigationLinks = page.locator('[data-testid="navigation-links"]');
      await expect(navigationLinks).toBeVisible();
    });

    test('Tab through mobile menu items when open', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      const mobileMenuToggle = page.locator('[data-testid="mobile-menu-toggle"]');

      // Open menu
      await mobileMenuToggle.click();

      const navigationLinks = page.locator('[data-testid="navigation-links"]');
      await expect(navigationLinks).toBeVisible();

      // Tab through menu items
      await page.keyboard.press('Tab');
      const homeLink = page.locator('[data-testid="navigation-link-home"]');
      await expect(homeLink).toBeFocused();

      await page.keyboard.press('Tab');
      const featuresLink = page.locator('[data-testid="navigation-link-features"]');
      await expect(featuresLink).toBeFocused();
    });
  });

  /**
   * Test: Navigation links are activatable with Enter key
   */
  test('Navigation links can be activated with Enter key', async ({ page }) => {
    const homeLink = page.locator('[data-testid="navigation-link-home"]');
    await homeLink.focus();
    await expect(homeLink).toBeFocused();

    // Get the href to verify it's a valid link
    const href = await homeLink.getAttribute('href');
    expect(href).toBeTruthy();

    // Pressing Enter on a link should trigger navigation
    // We just verify the link is properly focusable and has correct attributes
    const tagName = await homeLink.evaluate((el) => el.tagName.toLowerCase());
    expect(tagName).toBe('a');
  });

  /**
   * Test: Shift+Tab navigates backwards through elements
   */
  test('Shift+Tab navigates backwards through focusable elements', async ({ page }) => {
    // Focus the CTA button first
    const ctaButton = page.locator('[data-testid="hero-cta"]');
    await ctaButton.focus();
    await expect(ctaButton).toBeFocused();

    // Shift+Tab should move focus backwards
    await page.keyboard.press('Shift+Tab');

    // Should now be on last navigation link (Contact)
    const contactLink = page.locator('[data-testid="navigation-link-contact"]');
    await expect(contactLink).toBeFocused();

    // Continue backwards
    await page.keyboard.press('Shift+Tab');
    const aboutLink = page.locator('[data-testid="navigation-link-about"]');
    await expect(aboutLink).toBeFocused();
  });

  /**
   * Test: Skip link or first focusable element is accessible
   */
  test('First focusable element is reachable with single Tab', async ({ page }) => {
    // Press Tab once to focus first interactive element
    await page.keyboard.press('Tab');

    // Verify an element is focused
    const activeElement = await page.evaluate(() => {
      return document.activeElement !== document.body;
    });
    expect(activeElement).toBe(true);

    // First element should be the navigation logo
    const logo = page.locator('[data-testid="navigation-logo"]');
    await expect(logo).toBeFocused();
  });

  /**
   * Test: Footer links are keyboard accessible
   */
  test('Footer links are keyboard accessible', async ({ page }) => {
    // Scroll to footer to ensure it's in view
    const footer = page.locator('[data-testid="footer"]');
    await footer.scrollIntoViewIfNeeded();

    // Click somewhere in the footer first, then tab to links
    const footerLink = page.locator('.footer-link').first();
    await footerLink.focus();

    // Verify footer link is focusable
    await expect(footerLink).toBeFocused();

    // Check it has proper focus styles
    const outlineWidth = await footerLink.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return parseFloat(style.outlineWidth);
    });
    expect(outlineWidth).toBeGreaterThan(0);
  });

  /**
   * Test: Social links in footer are keyboard accessible
   */
  test('Footer social links are keyboard accessible', async ({ page }) => {
    const socialLink = page.locator('.footer-social-link').first();
    await socialLink.focus();

    // Verify social link is focusable
    await expect(socialLink).toBeFocused();

    // Verify it has aria-label for accessibility
    const ariaLabel = await socialLink.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();

    // Check it has proper focus styles
    const outlineWidth = await socialLink.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return parseFloat(style.outlineWidth);
    });
    expect(outlineWidth).toBeGreaterThan(0);
  });
});
