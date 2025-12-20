// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

/**
 * Footer Section Tests - REQ-12
 * Verify that the footer includes relevant links and project information
 */
test.describe('Footer Section', () => {
  test.beforeEach(async ({ page }) => {
    const filePath = path.resolve(__dirname, '../index.html');
    await page.goto(`file://${filePath}`);
  });

  // Test Case 1: Check for footer element on the page
  // Expected: Page contains a semantic footer element
  test('TC1: Page contains a semantic footer element', async ({ page }) => {
    // Check that a semantic <footer> element exists
    const footer = page.locator('footer');
    await expect(footer).toBeAttached();
    await expect(footer).toBeVisible();

    // Verify it's a semantic footer element (not just a div with a class)
    const footerTagName = await footer.evaluate(el => el.tagName.toLowerCase());
    expect(footerTagName).toBe('footer');

    // Verify footer is at the bottom of the page (after main content)
    const footerBoundingBox = await footer.boundingBox();
    expect(footerBoundingBox).not.toBeNull();
    expect(footerBoundingBox.y).toBeGreaterThan(0);

    // Footer should have proper data-testid for accessibility
    const footerTestId = page.locator('[data-testid="footer"]');
    await expect(footerTestId).toBeVisible();
  });

  // Test Case 2: Verify footer contains navigation links
  // Expected: Footer contains at least GitHub and documentation links
  test('TC2: Footer contains at least GitHub and documentation links', async ({ page }) => {
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Check for GitHub link in footer
    const githubLink = footer.locator('[data-testid="github-link"]');
    await expect(githubLink).toBeVisible();

    const githubHref = await githubLink.getAttribute('href');
    expect(githubHref).toMatch(/github\.com/);
    expect(githubHref).toBeTruthy();

    const githubText = await githubLink.textContent();
    expect(githubText?.toLowerCase()).toContain('github');

    // Check for Documentation link in footer
    const docsLink = footer.locator('[data-testid="docs-link"]');
    await expect(docsLink).toBeVisible();

    const docsHref = await docsLink.getAttribute('href');
    expect(docsHref).toBeTruthy();
    expect(docsHref.length).toBeGreaterThan(0);

    const docsText = await docsLink.textContent();
    expect(docsText?.toLowerCase()).toContain('doc');

    // Verify both links are clickable (have href and are not disabled)
    const githubIsEnabled = await githubLink.isEnabled();
    const docsIsEnabled = await docsLink.isEnabled();
    expect(githubIsEnabled).toBe(true);
    expect(docsIsEnabled).toBe(true);

    // Verify external links have proper security attributes
    const githubTarget = await githubLink.getAttribute('target');
    const githubRel = await githubLink.getAttribute('rel');
    if (githubTarget === '_blank') {
      expect(githubRel).toContain('noopener');
    }

    const docsTarget = await docsLink.getAttribute('target');
    const docsRel = await docsLink.getAttribute('rel');
    if (docsTarget === '_blank') {
      expect(docsRel).toContain('noopener');
    }
  });

  // Test Case 3: Check footer displays project information
  // Expected: Footer shows project name or copyright information
  test('TC3: Footer shows project name or copyright information', async ({ page }) => {
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Get all text content from footer
    const footerText = await footer.textContent();
    expect(footerText).toBeTruthy();

    // Check for project name (MirDB)
    const projectNameRegex = /mirdb/i;
    expect(footerText).toMatch(projectNameRegex);

    // Check for copyright or license information
    const copyrightOrLicenseRegex = /(copyright|©|license|mit|open\s*source|built\s*with)/i;
    expect(footerText).toMatch(copyrightOrLicenseRegex);

    // Verify footer brand section exists with project name
    const footerBrand = footer.locator('.footer-brand, .footer-logo, h3, h4').first();
    await expect(footerBrand).toBeVisible();
    const brandText = await footerBrand.textContent();
    expect(brandText?.toLowerCase()).toContain('mirdb');

    // Verify there's a tagline or description in footer
    const taglineElement = footer.locator('.footer-tagline, .footer-description, p').first();
    await expect(taglineElement).toBeVisible();
  });

  // Additional test: Footer has proper structure and semantic markup
  test('TC-Structure: Footer has proper semantic structure', async ({ page }) => {
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Check for navigation element within footer for links
    const footerNav = footer.locator('nav');
    const navCount = await footerNav.count();
    expect(navCount).toBeGreaterThan(0);

    // Verify nav has aria-label for accessibility
    for (let i = 0; i < navCount; i++) {
      const nav = footerNav.nth(i);
      const ariaLabel = await nav.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
    }

    // Check for proper heading hierarchy in footer
    const footerHeadings = footer.locator('h3, h4');
    const headingsCount = await footerHeadings.count();
    expect(headingsCount).toBeGreaterThan(0);

    // Check for link list elements
    const footerLists = footer.locator('ul');
    const listsCount = await footerLists.count();
    expect(listsCount).toBeGreaterThan(0);
  });

  // Additional test: Footer links are accessible and have visible text
  test('TC-Accessibility: Footer links are accessible', async ({ page }) => {
    const footer = page.locator('footer');
    const footerLinks = footer.locator('a');
    const linksCount = await footerLinks.count();

    expect(linksCount).toBeGreaterThanOrEqual(2);

    // Verify all links have visible text or accessible name
    for (let i = 0; i < linksCount; i++) {
      const link = footerLinks.nth(i);
      await expect(link).toBeVisible();

      const text = await link.textContent();
      const ariaLabel = await link.getAttribute('aria-label');

      // Link must have either visible text or aria-label
      const hasAccessibleName = (text && text.trim().length > 0) || (ariaLabel && ariaLabel.trim().length > 0);
      expect(hasAccessibleName).toBe(true);

      // Verify href is present and not empty
      const href = await link.getAttribute('href');
      expect(href).toBeTruthy();
      expect(href.length).toBeGreaterThan(0);
    }
  });

  // Additional test: Footer is visible at bottom of page after scrolling
  test('TC-Position: Footer is positioned at the bottom of the page', async ({ page }) => {
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Use evaluate to check footer position relative to document
    const footerPosition = await page.evaluate(() => {
      const footer = document.querySelector('footer');
      const footerRect = footer.getBoundingClientRect();
      const pageHeight = document.body.scrollHeight;
      const footerOffsetTop = footer.offsetTop;
      const footerHeight = footerRect.height;

      return {
        footerOffsetTop,
        footerHeight,
        pageHeight,
        footerBottom: footerOffsetTop + footerHeight
      };
    });

    expect(footerPosition.footerOffsetTop).toBeGreaterThan(0);

    // Footer's bottom edge should be at the page's bottom (within small tolerance for margins)
    const difference = Math.abs(footerPosition.footerBottom - footerPosition.pageHeight);
    expect(difference).toBeLessThan(50);

    // Scroll to footer and verify it's in viewport
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeInViewport();
  });
});
