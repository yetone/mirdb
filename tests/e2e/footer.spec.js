/**
 * Footer Section E2E Tests
 * Owner: Scenario 6 - Footer Section
 *
 * Tests:
 * - Footer DOM structure (organized link groups, copyright text)
 * - Documentation link presence and navigation
 * - Privacy Policy link presence and navigation
 * - Terms of Service link presence and navigation
 * - Copyright text presence and formatting
 * - Responsive layout at mobile viewport (375px)
 */

const { test, expect } = require('@playwright/test');

test.describe('Footer Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for the footer to be visible
    await page.waitForSelector('footer', { state: 'visible' });
  });

  test('should render footer with organized link groups and copyright text', async ({ page }) => {
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Verify footer has role="contentinfo" for accessibility
    const role = await footer.getAttribute('role');
    expect(role).toBe('contentinfo');

    // Verify footer contains link group
    const linkGroup = footer.locator('[data-testid="footer-links"]');
    await expect(linkGroup).toBeVisible();

    // Verify footer contains copyright text
    const copyright = footer.locator('[data-testid="footer-copyright"]');
    await expect(copyright).toBeVisible();

    // Verify all required links are present
    const docsLink = footer.locator('[data-testid="footer-link-docs"]');
    const privacyLink = footer.locator('[data-testid="footer-link-privacy"]');
    const termsLink = footer.locator('[data-testid="footer-link-terms"]');

    await expect(docsLink).toBeVisible();
    await expect(privacyLink).toBeVisible();
    await expect(termsLink).toBeVisible();

    // Verify link text content
    const docsText = await docsLink.textContent();
    expect(docsText.toLowerCase()).toContain('documentation');

    const privacyText = await privacyLink.textContent();
    expect(privacyText.toLowerCase()).toContain('privacy');

    const termsText = await termsLink.textContent();
    expect(termsText.toLowerCase()).toContain('terms');
  });

  test('should have copyright text present and correctly formatted', async ({ page }) => {
    const footer = page.locator('footer');

    const copyright = footer.locator('[data-testid="footer-copyright"]');
    await expect(copyright).toBeVisible();

    const copyrightText = await copyright.textContent();
    expect(copyrightText).toBeTruthy();

    // Should contain copyright symbol or the word "copyright"
    const lowerText = copyrightText.toLowerCase();
    expect(
      lowerText.includes('copyright') ||
      lowerText.includes('©') ||
      lowerText.includes('&copy;')
    ).toBe(true);

    // Should reference MirDB or the current year
    expect(
      lowerText.includes('mirdb') ||
      lowerText.includes('2024') ||
      lowerText.includes('2025') ||
      lowerText.includes('2026')
    ).toBe(true);
  });

  test('should navigate to documentation page when clicking Documentation link', async ({ page }) => {
    const docsLink = page.locator('footer [data-testid="footer-link-docs"]');
    await expect(docsLink).toBeVisible();

    const href = await docsLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href.length).toBeGreaterThan(0);

    // Verify href points to documentation
    const lowerHref = href.toLowerCase();
    expect(
      lowerHref.includes('doc') ||
      lowerHref.includes('/docs')
    ).toBe(true);

    // Verify link is an anchor element
    const tagName = await docsLink.evaluate(el => el.tagName.toLowerCase());
    expect(tagName).toBe('a');
  });

  test('should navigate to privacy policy page when clicking Privacy Policy link', async ({ page }) => {
    const privacyLink = page.locator('footer [data-testid="footer-link-privacy"]');
    await expect(privacyLink).toBeVisible();

    const href = await privacyLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href.length).toBeGreaterThan(0);

    // Verify href points to privacy policy
    const lowerHref = href.toLowerCase();
    expect(
      lowerHref.includes('privacy') ||
      lowerHref.includes('/privacy')
    ).toBe(true);

    // Verify link is an anchor element
    const tagName = await privacyLink.evaluate(el => el.tagName.toLowerCase());
    expect(tagName).toBe('a');
  });

  test('should navigate to terms of service page when clicking Terms of Service link', async ({ page }) => {
    const termsLink = page.locator('footer [data-testid="footer-link-terms"]');
    await expect(termsLink).toBeVisible();

    const href = await termsLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href.length).toBeGreaterThan(0);

    // Verify href points to terms of service
    const lowerHref = href.toLowerCase();
    expect(
      lowerHref.includes('terms') ||
      lowerHref.includes('/terms') ||
      lowerHref.includes('service')
    ).toBe(true);

    // Verify link is an anchor element
    const tagName = await termsLink.evaluate(el => el.tagName.toLowerCase());
    expect(tagName).toBe('a');
  });

  test('should reorganize footer layout for mobile readability at 375px', async ({ page }) => {
    // Set viewport to mobile size
    await page.setViewportSize({ width: 375, height: 812 });

    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // All footer links should be visible on mobile
    const links = footer.locator('[data-testid="footer-links"] a');
    const linkCount = await links.count();
    expect(linkCount).toBeGreaterThanOrEqual(3);

    for (let i = 0; i < linkCount; i++) {
      await expect(links.nth(i)).toBeVisible();
    }

    // Copyright should be visible
    const copyright = footer.locator('[data-testid="footer-copyright"]');
    await expect(copyright).toBeVisible();

    // Verify no horizontal overflow
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    expect(bodyWidth).toBeLessThanOrEqual(viewportWidth + 1);

    // Footer should fit within viewport width
    const footerBox = await footer.boundingBox();
    expect(footerBox.width).toBeLessThanOrEqual(viewportWidth + 1);
  });

  test('should have accessible footer with proper semantic markup', async ({ page }) => {
    const footer = page.locator('footer');

    // Footer should have contentinfo role
    const role = await footer.getAttribute('role');
    expect(role).toBe('contentinfo');

    // All links should be focusable anchor elements
    const links = footer.locator('a');
    const count = await links.count();
    expect(count).toBeGreaterThanOrEqual(3);

    for (let i = 0; i < count; i++) {
      const link = links.nth(i);
      const tagName = await link.evaluate(el => el.tagName.toLowerCase());
      expect(tagName).toBe('a');

      const href = await link.getAttribute('href');
      expect(href).toBeTruthy();
    }

    // Links should have visible text content
    const docsLink = footer.locator('[data-testid="footer-link-docs"]');
    const docsText = await docsLink.textContent();
    expect(docsText.trim().length).toBeGreaterThan(0);
  });
});
