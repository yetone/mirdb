/**
 * External Links Navigation E2E Tests
 * Owner: Scenario 4 - External Links Navigation
 *
 * Tests for documentation and source code repository links
 * in header, footer, and body sections.
 *
 * Navigation and Smooth Scroll Tests
 * Owner: Scenario 12 - Navigation and Smooth Scroll
 *
 * Tests for sticky navigation, smooth scrolling, and nav link functionality.
 */

import { test, expect } from '@playwright/test';

// Constants for external URLs
const GITHUB_URL = 'https://github.com/mirdb/mirdb';
const DOCS_URL = 'https://mirdb.dev/docs';

test.describe('External Links Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: documentation link exists on homepage', async ({ page }) => {
    // Find link element containing 'documentation' or 'docs' text/aria
    const docsLink = page.locator('a').filter({
      has: page.locator('text=/documentation|docs/i')
    }).or(
      page.locator('a[aria-label*="documentation" i], a[aria-label*="docs" i]')
    ).first();

    await expect(docsLink).toBeVisible();

    // Verify the href attribute exists and contains a valid URL
    const href = await docsLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toMatch(/^https?:\/\//);
  });

  test('TC2: clicking documentation link navigates correctly', async ({ page, context }) => {
    // Find documentation link
    const docsLink = page.locator('a:has-text("Documentation")').first();
    await expect(docsLink).toBeVisible();

    // Get the href to verify navigation target
    const href = await docsLink.getAttribute('href');
    expect(href).toBe(DOCS_URL);

    // Since it opens in new tab (target="_blank"), verify the attribute
    const target = await docsLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify rel attribute for security
    const rel = await docsLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  test('TC3: source code link exists in footer', async ({ page }) => {
    // Navigate to footer section
    const footer = page.locator('#footer, footer');
    await expect(footer).toBeVisible();

    // Find source code link in footer
    const sourceCodeLink = footer.locator('a').filter({
      has: page.locator('text=/source|code|github|repository/i')
    }).first();

    await expect(sourceCodeLink).toBeVisible();

    // Verify the link has a valid href
    const href = await sourceCodeLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toMatch(/^https?:\/\//);
  });

  test('TC4: clicking source code link navigates to repository', async ({ page }) => {
    // Find source code link in footer
    const footer = page.locator('#footer, footer');
    const sourceCodeLink = footer.locator('a:has-text("Source Code"), a:has-text("GitHub")').first();

    await expect(sourceCodeLink).toBeVisible();

    // Get the href to verify it points to a valid repository
    const href = await sourceCodeLink.getAttribute('href');
    expect(href).toBe(GITHUB_URL);

    // Verify it opens in new tab
    const target = await sourceCodeLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify rel attribute for security
    const rel = await sourceCodeLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  test('header contains GitHub navigation link', async ({ page }) => {
    // Find header navigation
    const header = page.locator('#header, header');
    await expect(header).toBeVisible();

    // Find GitHub link in header nav
    const githubLink = header.locator('a:has-text("GitHub")').first();
    await expect(githubLink).toBeVisible();

    // Verify it points to the repository
    const href = await githubLink.getAttribute('href');
    expect(href).toBe(GITHUB_URL);

    // Verify it opens in new tab
    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');
  });

  test('all external links have secure attributes', async ({ page }) => {
    // Find all external links (starting with http/https)
    const externalLinks = page.locator('a[href^="http"]');
    const count = await externalLinks.count();

    expect(count).toBeGreaterThan(0);

    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      const href = await link.getAttribute('href');
      const target = await link.getAttribute('target');
      const rel = await link.getAttribute('rel');

      // External links should open in new tab and have noopener
      if (target === '_blank') {
        expect(rel).toContain('noopener');
      }

      // Verify href is a valid URL
      expect(href).toMatch(/^https?:\/\/.+/);
    }
  });

  test('footer contains both documentation and source code links', async ({ page }) => {
    const footer = page.locator('#footer, footer');
    await expect(footer).toBeVisible();

    // Verify documentation link exists
    const docsLink = footer.locator('a:has-text("Documentation")');
    await expect(docsLink).toBeVisible();

    // Verify source code link exists
    const sourceLink = footer.locator('a:has-text("Source Code"), a:has-text("GitHub")').first();
    await expect(sourceLink).toBeVisible();
  });
});

/**
 * Sticky Navigation and Smooth Scroll E2E Tests
 * Owner: Scenario 12 - Navigation and Smooth Scroll
 */
test.describe('Sticky Navigation and Smooth Scroll', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: navigation bar remains visible after scrolling 500px', async ({ page }) => {
    // Get the header element
    const header = page.locator('#header');
    await expect(header).toBeVisible();

    // Get initial header position
    const initialBoundingBox = await header.boundingBox();
    expect(initialBoundingBox).toBeTruthy();

    // Verify the header has position: sticky applied (this is the key requirement)
    const position = await header.evaluate((el) => window.getComputedStyle(el).position);
    expect(position).toBe('sticky');

    // Verify top: 0 is set for sticky positioning
    const topValue = await header.evaluate((el) => window.getComputedStyle(el).top);
    expect(topValue).toBe('0px');

    // Scroll down 500px
    await page.evaluate(() => window.scrollTo(0, 500));

    // Wait for scroll to complete
    await page.waitForTimeout(200);

    // Verify header is still visible after scrolling
    await expect(header).toBeVisible();

    // For sticky elements, verify it's accessible by checking if the element
    // can be found and is visible (sticky elements remain in the viewport)
    const isVisible = await header.isVisible();
    expect(isVisible).toBe(true);

    // Verify z-index is set high enough to overlay content
    const zIndex = await header.evaluate((el) => window.getComputedStyle(el).zIndex);
    expect(parseInt(zIndex)).toBeGreaterThanOrEqual(100);
  });

  test('TC2: clicking Features link scrolls to Features section', async ({ page }) => {
    // Find the Features link in navigation
    const featuresLink = page.locator('.nav__link[href="#features"]');
    await expect(featuresLink).toBeVisible();

    // Click the Features link
    await featuresLink.click();

    // Wait for smooth scroll to complete
    await page.waitForTimeout(500);

    // Verify the Features section is now in the viewport
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();

    // Verify URL hash has been updated
    const currentUrl = page.url();
    expect(currentUrl).toContain('#features');
  });

  test('smooth scroll works when clicking navigation links', async ({ page }) => {
    // Click Quick Start link
    const quickStartLink = page.locator('.nav__link[href="#quickstart"]');
    await expect(quickStartLink).toBeVisible();

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click the link
    await quickStartLink.click();

    // Wait for scroll to complete
    await page.waitForTimeout(500);

    // Verify page scrolled
    const finalScrollY = await page.evaluate(() => window.scrollY);
    expect(finalScrollY).toBeGreaterThan(initialScrollY);

    // Verify Quick Start section is visible
    const quickStartSection = page.locator('#quickstart');
    await expect(quickStartSection).toBeInViewport();
  });

  test('all navigation anchor links point to existing sections', async ({ page }) => {
    // Get all internal navigation links
    const navLinks = page.locator('.nav__links .nav__link[href^="#"]');
    const count = await navLinks.count();

    expect(count).toBeGreaterThan(0);

    for (let i = 0; i < count; i++) {
      const link = navLinks.nth(i);
      const href = await link.getAttribute('href');
      expect(href).toBeTruthy();
      expect(href).toMatch(/^#[a-z]+/);

      // Verify the target section exists
      const sectionId = href!.substring(1);
      const section = page.locator(`#${sectionId}`);
      await expect(section).toBeAttached();
    }
  });

  test('navigation remains fixed during scroll', async ({ page }) => {
    const header = page.locator('#header');

    // Verify header has sticky positioning
    const position = await header.evaluate((el) => window.getComputedStyle(el).position);
    expect(position).toBe('sticky');

    // Scroll to different positions and verify header stays visible
    const scrollPositions = [100, 300, 500, 800, 1000];

    for (const scrollY of scrollPositions) {
      await page.evaluate((y) => window.scrollTo(0, y), scrollY);
      await page.waitForTimeout(100);

      // Header should still be visible regardless of scroll position
      await expect(header).toBeVisible();

      // Verify we can interact with header (it's not hidden behind content)
      const headerLogo = page.locator('.nav__logo');
      await expect(headerLogo).toBeVisible();
    }

    // Verify header stays at top: 0 position in CSS
    const topValue = await header.evaluate((el) => window.getComputedStyle(el).top);
    expect(topValue).toBe('0px');
  });

  test('clicking Get Started CTA scrolls to Quick Start section', async ({ page }) => {
    // Find the Get Started CTA button
    const ctaButton = page.locator('.hero__cta');
    await expect(ctaButton).toBeVisible();

    // Verify it links to quickstart
    const href = await ctaButton.getAttribute('href');
    expect(href).toBe('#quickstart');

    // Click the CTA
    await ctaButton.click();

    // Wait for smooth scroll
    await page.waitForTimeout(500);

    // Verify Quick Start section is in viewport
    const quickStartSection = page.locator('#quickstart');
    await expect(quickStartSection).toBeInViewport();
  });

  test('navigation has correct section links', async ({ page }) => {
    // Verify expected navigation links exist
    const expectedLinks = ['#features', '#quickstart', '#techspecs'];

    for (const expectedHref of expectedLinks) {
      const link = page.locator(`.nav__link[href="${expectedHref}"]`);
      await expect(link).toBeVisible();
    }
  });
});
