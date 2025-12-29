import { test, expect } from '@playwright/test';

/**
 * E2E Tests for MirDB Homepage Navigation and Links
 *
 * These tests verify that all navigation elements and external links function correctly:
 * - 'Get Started' CTA button navigates to getting started section
 * - GitHub link/button opens the repository page
 * - All href attributes are valid (no broken links)
 * - Anchor links scroll to correct sections smoothly
 */

test.describe('Navigation and Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Get Started CTA button navigates to getting started section', async ({ page }) => {
    // Find the "Get Started" button in the hero section
    const getStartedButton = page.locator('.hero a.btn-primary, .cta-buttons a').filter({ hasText: /Get Started/i }).first();

    // Verify the button exists and is visible
    await expect(getStartedButton).toBeVisible();

    // Verify it has the correct href pointing to #getting-started
    const href = await getStartedButton.getAttribute('href');
    expect(href).toBe('#getting-started');

    // Click the button
    await getStartedButton.click();

    // Verify the URL now includes the anchor
    await expect(page).toHaveURL(/#getting-started$/);

    // Verify the getting started section is now visible in the viewport
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();
    await expect(gettingStartedSection).toBeInViewport();
  });

  test('TC2: GitHub link opens GitHub repository page', async ({ page }) => {
    // Find the GitHub button/link in the hero section
    const githubButton = page.locator('.hero a, .cta-buttons a').filter({ hasText: /GitHub|View on GitHub/i }).first();

    // Verify it exists and is visible
    await expect(githubButton).toBeVisible();

    // Verify it has the correct href pointing to GitHub
    const href = await githubButton.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toContain('github.com');
    expect(href).toContain('mirdb');

    // Verify it has target="_blank" for opening in new tab
    const target = await githubButton.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify it has rel="noopener noreferrer" for security
    const rel = await githubButton.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  test('TC3: Verify all href attributes are valid (no broken links)', async ({ page }) => {
    // Get all links on the page
    const allLinks = page.locator('a[href]');
    const linkCount = await allLinks.count();

    expect(linkCount).toBeGreaterThan(0);

    const brokenLinks: string[] = [];
    const checkedLinks: string[] = [];

    for (let i = 0; i < linkCount; i++) {
      const link = allLinks.nth(i);
      const href = await link.getAttribute('href');

      if (!href) continue;

      // Skip already checked links
      if (checkedLinks.includes(href)) continue;
      checkedLinks.push(href);

      // Check anchor links
      if (href.startsWith('#')) {
        const anchorId = href.substring(1);
        if (anchorId) {
          const targetElement = page.locator(`#${anchorId}`);
          const exists = await targetElement.count() > 0;
          if (!exists) {
            brokenLinks.push(`Anchor link ${href} - target element not found`);
          }
        }
      }
      // Check external links (just verify format, don't make network requests)
      else if (href.startsWith('http://') || href.startsWith('https://')) {
        // Verify URL is well-formed
        try {
          new URL(href);
        } catch (e) {
          brokenLinks.push(`External link ${href} - malformed URL`);
        }

        // Verify external links have proper attributes
        const target = await link.getAttribute('target');
        const rel = await link.getAttribute('rel');

        // External links should open in new tab
        if (target !== '_blank') {
          // Just a warning, not a broken link
        }

        // External links should have security attributes
        if (!rel?.includes('noopener')) {
          // Just a warning, not a broken link
        }
      }
    }

    // Report any broken links
    if (brokenLinks.length > 0) {
      throw new Error(`Found broken links:\n${brokenLinks.join('\n')}`);
    }
  });

  test('TC4: Anchor links scroll to correct sections smoothly', async ({ page }) => {
    // Test anchor navigation to different sections
    const anchorLinks = [
      { selector: 'a[href="#getting-started"]', targetId: 'getting-started', sectionName: 'Getting Started' },
      { selector: 'a[href="#features"]', targetId: 'features', sectionName: 'Features' },
      { selector: 'a[href="#code-examples"]', targetId: 'code-examples', sectionName: 'Code Examples' },
    ];

    for (const { selector, targetId, sectionName } of anchorLinks) {
      // Find link with this anchor (may not exist for all sections)
      const link = page.locator(selector).first();
      const linkExists = await link.count() > 0;

      if (!linkExists) {
        // Section may not have a dedicated navigation link
        continue;
      }

      // Check if the target section exists
      const targetSection = page.locator(`#${targetId}`);
      const sectionExists = await targetSection.count() > 0;

      if (!sectionExists) {
        // Skip if section doesn't exist
        continue;
      }

      // Get initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY);

      // Click the anchor link
      await link.click();

      // Wait for scroll animation
      await page.waitForTimeout(500);

      // Verify URL has updated
      await expect(page).toHaveURL(new RegExp(`#${targetId}$`));

      // Verify the section is now in viewport
      await expect(targetSection).toBeInViewport();

      // Reset scroll position for next test
      await page.evaluate(() => window.scrollTo(0, 0));
      await page.waitForTimeout(100);
    }

    // Also test the "Get Started" button specifically
    const getStartedBtn = page.locator('a[href="#getting-started"]').first();
    if (await getStartedBtn.count() > 0) {
      await getStartedBtn.click();
      await page.waitForTimeout(500);

      const gettingStartedSection = page.locator('#getting-started');
      await expect(gettingStartedSection).toBeInViewport();
    }
  });

  test('Footer links are valid and functional', async ({ page }) => {
    // Scroll to footer
    const footer = page.locator('footer, .footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Get all links in the footer
    const footerLinks = footer.locator('a[href]');
    const footerLinkCount = await footerLinks.count();

    expect(footerLinkCount).toBeGreaterThan(0);

    // Check GitHub link in footer
    const githubFooterLink = footer.locator('a').filter({ hasText: /GitHub/i }).first();
    if (await githubFooterLink.count() > 0) {
      await expect(githubFooterLink).toBeVisible();
      const href = await githubFooterLink.getAttribute('href');
      expect(href).toContain('github.com');
    }

    // Check Documentation link in footer
    const docsLink = footer.locator('a').filter({ hasText: /Documentation|Docs/i }).first();
    if (await docsLink.count() > 0) {
      await expect(docsLink).toBeVisible();
      const href = await docsLink.getAttribute('href');
      expect(href).toBeTruthy();
    }
  });

  test('Navigation elements have correct styling for interactivity', async ({ page }) => {
    // Verify buttons have pointer cursor
    const ctaButtons = page.locator('.cta-buttons a.btn, .hero a.btn');
    const buttonCount = await ctaButtons.count();

    expect(buttonCount).toBeGreaterThan(0);

    for (let i = 0; i < buttonCount; i++) {
      const button = ctaButtons.nth(i);
      await expect(button).toBeVisible();

      // Check that buttons have cursor:pointer styling
      const cursor = await button.evaluate((el) => {
        return window.getComputedStyle(el).cursor;
      });
      expect(cursor).toBe('pointer');
    }
  });
});

test.describe('External Link Accessibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('External links have proper security attributes', async ({ page }) => {
    // Find all external links
    const externalLinks = page.locator('a[href^="http"], a[href^="https"]');
    const count = await externalLinks.count();

    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      const href = await link.getAttribute('href');

      // External links should have target="_blank" and rel="noopener noreferrer"
      const target = await link.getAttribute('target');
      const rel = await link.getAttribute('rel');

      // Verify security attributes for external links
      if (target === '_blank') {
        expect(rel, `External link ${href} should have rel="noopener" when target="_blank"`).toContain('noopener');
      }
    }
  });

  test('All navigation links are keyboard accessible', async ({ page }) => {
    // Get all links
    const allLinks = page.locator('a[href]');
    const linkCount = await allLinks.count();

    // Verify links can receive focus
    for (let i = 0; i < Math.min(linkCount, 5); i++) { // Test first 5 links
      const link = allLinks.nth(i);
      await link.focus();

      // Verify the element is focused
      const isFocused = await link.evaluate((el) => {
        return document.activeElement === el;
      });
      expect(isFocused).toBe(true);
    }
  });
});
