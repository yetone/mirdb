/**
 * E2E tests for Footer section.
 * Owner: Scenario 8 - Footer Section
 *
 * Test cases 4 and 5:
 * - GitHub link opens in new tab
 * - Footer content stacks vertically on mobile
 */

import { test, expect } from '@playwright/test';

test.describe('Footer Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for footer section to be rendered
    await page.waitForSelector('#footer');
  });

  test('Test case 4: GitHub link opens in new tab', async ({ page, context }) => {
    // Find the GitHub link in the footer
    const githubLink = page.locator('#footer a[href*="github.com"]').first();
    await expect(githubLink).toBeVisible();

    // Verify the link has target="_blank"
    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify the link has rel="noopener noreferrer"
    const rel = await githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');
    expect(rel).toContain('noreferrer');

    // Verify the href points to the correct repository
    const href = await githubLink.getAttribute('href');
    expect(href).toContain('github.com');
    expect(href).toContain('mirdb');

    // Test that clicking opens a new tab
    const [newPage] = await Promise.all([
      context.waitForEvent('page'),
      githubLink.click(),
    ]);

    // Verify new tab opened with correct URL
    expect(newPage.url()).toContain('github.com');
    await newPage.close();
  });

  test('Test case 5: Footer content stacks vertically on mobile viewport', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });

    // Give the page time to reflow
    await page.waitForTimeout(100);

    // Scroll to the footer
    const footer = page.locator('#footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Get the footer top section
    const footerTop = page.locator('.footer-top');
    await expect(footerTop).toBeVisible();

    // Check if the layout is stacked (flex-direction: column)
    const footerTopStyle = await footerTop.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        flexDirection: style.flexDirection,
        alignItems: style.alignItems,
      };
    });
    expect(footerTopStyle.flexDirection).toBe('column');
    expect(footerTopStyle.alignItems).toBe('center');

    // Get the footer bottom section
    const footerBottom = page.locator('.footer-bottom');
    await expect(footerBottom).toBeVisible();

    // Check if footer bottom is also stacked
    const footerBottomStyle = await footerBottom.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        flexDirection: style.flexDirection,
        textAlign: style.textAlign,
      };
    });
    expect(footerBottomStyle.flexDirection).toBe('column');
    expect(footerBottomStyle.textAlign).toBe('center');
  });

  test('Footer is readable on mobile viewport', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });

    // Scroll to the footer
    const footer = page.locator('#footer');
    await footer.scrollIntoViewIfNeeded();

    // Check that all key elements are visible
    const brandTitle = page.locator('.footer-brand-title');
    await expect(brandTitle).toBeVisible();

    const license = page.locator('.footer-license');
    await expect(license).toBeVisible();

    const credits = page.locator('.footer-credits');
    await expect(credits).toBeVisible();

    // Check that GitHub link is still accessible
    const githubLink = page.locator('#footer a[href*="github.com"]').first();
    await expect(githubLink).toBeVisible();
  });

  test('Footer content fits within viewport on mobile', async ({ page }) => {
    // Set small mobile viewport
    await page.setViewportSize({ width: 320, height: 568 });

    // Scroll to the footer
    const footer = page.locator('#footer');
    await footer.scrollIntoViewIfNeeded();

    // Get footer dimensions
    const footerBox = await footer.boundingBox();
    expect(footerBox).not.toBeNull();

    // Footer width should not exceed viewport width
    expect(footerBox!.width).toBeLessThanOrEqual(320);

    // Check no horizontal overflow
    const hasOverflow = await page.evaluate(() => {
      const footer = document.getElementById('footer');
      if (!footer) return false;
      return footer.scrollWidth > footer.clientWidth;
    });
    expect(hasOverflow).toBe(false);
  });
});
