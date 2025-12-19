import { test, expect } from '@playwright/test';

test.describe('Footer Content and Links (REQ-7)', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage
    await page.goto('file://' + process.cwd() + '/public/index.html');
  });

  test('TC1: Footer is visible at the bottom of the page', async ({ page }) => {
    // Scroll to the bottom of the page
    await page.evaluate(() => {
      window.scrollTo(0, document.body.scrollHeight);
    });

    // Wait for scroll to complete
    await page.waitForTimeout(500);

    // Verify footer element exists and is visible
    const footer = page.locator('footer[data-testid="footer"]');
    await expect(footer).toBeVisible();

    // Verify footer is at the bottom of the page (below all main content)
    const footerBox = await footer.boundingBox();
    const mainContent = page.locator('main');
    const mainBox = await mainContent.boundingBox();

    expect(footerBox).toBeTruthy();
    expect(mainBox).toBeTruthy();

    // Footer should be below the main content
    expect(footerBox!.y).toBeGreaterThanOrEqual(mainBox!.y + mainBox!.height - 10);
  });

  test('TC2: Footer displays project license (MIT)', async ({ page }) => {
    // Scroll to footer
    await page.evaluate(() => {
      window.scrollTo(0, document.body.scrollHeight);
    });
    await page.waitForTimeout(300);

    // Find the footer
    const footer = page.locator('footer[data-testid="footer"]');
    await expect(footer).toBeVisible();

    // Check for license information - MIT license should be displayed
    const licenseText = page.locator('[data-testid="footer-license"]');
    await expect(licenseText).toBeVisible();

    const licenseContent = await licenseText.textContent();
    // Verify it mentions MIT license
    expect(licenseContent?.toLowerCase()).toContain('mit');
  });

  test('TC3: Footer displays project attribution or copyright notice', async ({ page }) => {
    // Scroll to footer
    await page.evaluate(() => {
      window.scrollTo(0, document.body.scrollHeight);
    });
    await page.waitForTimeout(300);

    // Find the footer
    const footer = page.locator('footer[data-testid="footer"]');
    await expect(footer).toBeVisible();

    // Check for copyright/attribution element
    const copyright = page.locator('[data-testid="footer-copyright"]');
    await expect(copyright).toBeVisible();

    const copyrightText = await copyright.textContent();
    // Verify it contains copyright symbol or year and project name
    expect(
      copyrightText?.includes('©') ||
      copyrightText?.includes('Copyright') ||
      copyrightText?.toLowerCase().includes('mirdb')
    ).toBe(true);

    // Verify it mentions MirDB project
    expect(copyrightText?.toLowerCase()).toContain('mirdb');
  });

  test('TC4: Footer contains link to GitHub repository', async ({ page }) => {
    // Scroll to footer
    await page.evaluate(() => {
      window.scrollTo(0, document.body.scrollHeight);
    });
    await page.waitForTimeout(300);

    // Find the footer
    const footer = page.locator('footer[data-testid="footer"]');
    await expect(footer).toBeVisible();

    // Find GitHub link in footer specifically
    const githubLink = footer.locator('a[data-testid="footer-github-link"]');
    await expect(githubLink).toBeVisible();

    // Verify href points to GitHub
    const href = await githubLink.getAttribute('href');
    expect(href).toContain('github.com');
    expect(href).toContain('mirdb');

    // Verify link text
    const linkText = await githubLink.textContent();
    expect(linkText?.toLowerCase()).toContain('github');

    // Verify it opens in new tab
    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');
  });

  test('TC5: All footer links are functional and navigate correctly', async ({ page }) => {
    // Scroll to footer
    await page.evaluate(() => {
      window.scrollTo(0, document.body.scrollHeight);
    });
    await page.waitForTimeout(300);

    // Find footer
    const footer = page.locator('footer[data-testid="footer"]');
    await expect(footer).toBeVisible();

    // Get all links in the footer
    const footerLinks = footer.locator('a');
    const linkCount = await footerLinks.count();

    // Verify we have at least 2 links (GitHub and at least one other)
    expect(linkCount).toBeGreaterThanOrEqual(2);

    // Verify each link has required attributes and valid href
    for (let i = 0; i < linkCount; i++) {
      const link = footerLinks.nth(i);

      // Link should be visible
      await expect(link).toBeVisible();

      // Link should have valid href
      const href = await link.getAttribute('href');
      expect(href).toBeTruthy();
      expect(href!.length).toBeGreaterThan(0);

      // External links should have proper security attributes
      const target = await link.getAttribute('target');
      if (target === '_blank') {
        const rel = await link.getAttribute('rel');
        expect(rel).toContain('noopener');
        expect(rel).toContain('noreferrer');
      }
    }
  });

  test('TC6: Footer contains documentation link', async ({ page }) => {
    // Scroll to footer
    await page.evaluate(() => {
      window.scrollTo(0, document.body.scrollHeight);
    });
    await page.waitForTimeout(300);

    // Find the footer
    const footer = page.locator('footer[data-testid="footer"]');
    await expect(footer).toBeVisible();

    // Find docs link in footer
    const docsLink = footer.locator('a[data-testid="footer-docs-link"]');
    await expect(docsLink).toBeVisible();

    // Verify link text mentions documentation
    const linkText = await docsLink.textContent();
    expect(
      linkText?.toLowerCase().includes('doc') ||
      linkText?.toLowerCase().includes('guide') ||
      linkText?.toLowerCase().includes('readme')
    ).toBe(true);
  });

  test('TC7: Footer contains crates.io link', async ({ page }) => {
    // Scroll to footer
    await page.evaluate(() => {
      window.scrollTo(0, document.body.scrollHeight);
    });
    await page.waitForTimeout(300);

    // Find the footer
    const footer = page.locator('footer[data-testid="footer"]');
    await expect(footer).toBeVisible();

    // Find crates.io link in footer
    const cratesLink = footer.locator('a[data-testid="footer-crates-link"]');
    await expect(cratesLink).toBeVisible();

    // Verify href points to crates.io
    const href = await cratesLink.getAttribute('href');
    expect(href).toContain('crates.io');

    // Verify it opens in new tab
    const target = await cratesLink.getAttribute('target');
    expect(target).toBe('_blank');
  });

  test('TC8: Footer has proper styling and layout', async ({ page }) => {
    // Scroll to footer
    await page.evaluate(() => {
      window.scrollTo(0, document.body.scrollHeight);
    });
    await page.waitForTimeout(300);

    // Find the footer
    const footer = page.locator('footer[data-testid="footer"]');
    await expect(footer).toBeVisible();

    // Check footer has proper styling
    const styles = await footer.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        backgroundColor: computed.backgroundColor,
        padding: computed.padding,
        display: computed.display
      };
    });

    // Footer should have non-transparent background
    expect(styles.backgroundColor).not.toBe('rgba(0, 0, 0, 0)');

    // Footer should have padding
    expect(styles.padding).not.toBe('0px');
  });

  test('TC9: Footer is responsive on mobile viewport', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });

    // Navigate and scroll to footer
    await page.goto('file://' + process.cwd() + '/public/index.html');
    await page.evaluate(() => {
      window.scrollTo(0, document.body.scrollHeight);
    });
    await page.waitForTimeout(300);

    // Find the footer
    const footer = page.locator('footer[data-testid="footer"]');
    await expect(footer).toBeVisible();

    // Footer content should still be visible
    const footerContent = footer.locator('.footer-content');
    await expect(footerContent).toBeVisible();

    // All links should still be visible
    const footerLinks = footer.locator('.footer-links a');
    const linkCount = await footerLinks.count();
    for (let i = 0; i < linkCount; i++) {
      await expect(footerLinks.nth(i)).toBeVisible();
    }
  });

  test('TC10: Footer links have proper keyboard accessibility', async ({ page }) => {
    // Scroll to footer
    await page.evaluate(() => {
      window.scrollTo(0, document.body.scrollHeight);
    });
    await page.waitForTimeout(300);

    // Find the footer
    const footer = page.locator('footer[data-testid="footer"]');
    await expect(footer).toBeVisible();

    // Get all links in footer
    const footerLinks = footer.locator('a');
    const linkCount = await footerLinks.count();

    // Each link should be focusable and have visible focus indicator
    for (let i = 0; i < linkCount; i++) {
      const link = footerLinks.nth(i);

      // Focus the link
      await link.focus();

      // Verify the link is focused
      const isFocused = await link.evaluate((el) => document.activeElement === el);
      expect(isFocused).toBe(true);
    }
  });
});
