/**
 * E2E Tests for Footer Section
 * Owner: Scenario 7 - Footer Section
 *
 * Test Cases:
 * - TC2: Click GitHub repository link - Opens https://github.com/yetone/mirdb in new tab
 * - Visual verification of footer content
 * - Responsive design testing
 */

const { test, expect } = require('@playwright/test');

test.describe('Footer Section E2E Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Footer Visibility and Content', () => {
    test('should display footer at the bottom of the page', async ({ page }) => {
      const footer = page.locator('footer.footer');
      await expect(footer).toBeVisible();
    });

    test('should display footer when scrolling to bottom', async ({ page }) => {
      // Scroll to the footer
      await page.locator('footer.footer').scrollIntoViewIfNeeded();
      const footer = page.locator('footer.footer');
      await expect(footer).toBeVisible();
    });

    test('should display GitHub link text', async ({ page }) => {
      await page.locator('footer.footer').scrollIntoViewIfNeeded();
      const githubLink = page.locator('footer.footer a[href="https://github.com/yetone/mirdb"]');
      await expect(githubLink).toBeVisible();
    });

    test('should display author attribution (yetone)', async ({ page }) => {
      await page.locator('footer.footer').scrollIntoViewIfNeeded();
      const footer = page.locator('footer.footer');
      await expect(footer).toContainText('yetone');
    });

    test('should display license information (MIT)', async ({ page }) => {
      await page.locator('footer.footer').scrollIntoViewIfNeeded();
      const footer = page.locator('footer.footer');
      await expect(footer).toContainText('MIT');
    });

    test('should display copyright notice', async ({ page }) => {
      await page.locator('footer.footer').scrollIntoViewIfNeeded();
      const footer = page.locator('footer.footer');
      // Check for copyright symbol (© or &copy;) or year
      const footerText = await footer.textContent();
      expect(footerText).toMatch(/(©|\u00A9|Copyright|2024|2025|2026)/);
    });
  });

  test.describe('TC2: GitHub Repository Link Functionality', () => {
    test('should have GitHub repository link with correct href', async ({ page }) => {
      await page.locator('footer.footer').scrollIntoViewIfNeeded();
      const githubLink = page.locator('footer.footer a[href="https://github.com/yetone/mirdb"]');
      await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    });

    test('should have GitHub link configured to open in new tab', async ({ page }) => {
      await page.locator('footer.footer').scrollIntoViewIfNeeded();
      const githubLink = page.locator('footer.footer a[href="https://github.com/yetone/mirdb"]');
      await expect(githubLink).toHaveAttribute('target', '_blank');
    });

    test('should have GitHub link with noopener noreferrer for security', async ({ page }) => {
      await page.locator('footer.footer').scrollIntoViewIfNeeded();
      const githubLink = page.locator('footer.footer a[href="https://github.com/yetone/mirdb"]');
      await expect(githubLink).toHaveAttribute('rel', /noopener/);
      await expect(githubLink).toHaveAttribute('rel', /noreferrer/);
    });

    test('clicking GitHub link should open new tab with correct URL', async ({ page, context }) => {
      await page.locator('footer.footer').scrollIntoViewIfNeeded();

      // Listen for new page (tab) to open
      const pagePromise = context.waitForEvent('page');

      // Click the GitHub repository link in footer
      const githubLink = page.locator('footer.footer a[href="https://github.com/yetone/mirdb"]');
      await githubLink.click();

      // Get the new page/tab
      const newPage = await pagePromise;

      // Verify the URL is correct
      expect(newPage.url()).toContain('github.com/yetone/mirdb');
    });
  });

  test.describe('Author Link Functionality', () => {
    test('should have author link to GitHub profile', async ({ page }) => {
      await page.locator('footer.footer').scrollIntoViewIfNeeded();
      const authorLink = page.locator('footer.footer a[href="https://github.com/yetone"]');
      await expect(authorLink).toBeVisible();
    });

    test('author link should open in new tab', async ({ page }) => {
      await page.locator('footer.footer').scrollIntoViewIfNeeded();
      const authorLink = page.locator('footer.footer a[href="https://github.com/yetone"]');
      await expect(authorLink).toHaveAttribute('target', '_blank');
    });
  });

  test.describe('Footer Styling and Layout', () => {
    test('should have proper background color', async ({ page }) => {
      await page.locator('footer.footer').scrollIntoViewIfNeeded();
      const footer = page.locator('footer.footer');

      const bgColor = await footer.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Should have a dark background (matching the theme)
      expect(bgColor).not.toBe('rgba(0, 0, 0, 0)');
      expect(bgColor).not.toBe('transparent');
    });

    test('should have proper text color for readability', async ({ page }) => {
      await page.locator('footer.footer').scrollIntoViewIfNeeded();
      const footer = page.locator('footer.footer');

      const textColor = await footer.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });

      // Should have light text color (matching the theme)
      expect(textColor).not.toBe('rgb(0, 0, 0)');
    });

    test('should have padding for proper spacing', async ({ page }) => {
      await page.locator('footer.footer').scrollIntoViewIfNeeded();
      const footer = page.locator('footer.footer');

      const padding = await footer.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          top: parseFloat(styles.paddingTop),
          bottom: parseFloat(styles.paddingBottom)
        };
      });

      // Should have vertical padding
      expect(padding.top).toBeGreaterThan(0);
      expect(padding.bottom).toBeGreaterThan(0);
    });
  });

  test.describe('Responsive Design', () => {
    test.describe('Mobile viewport', () => {
      test.use({ viewport: { width: 375, height: 667 } });

      test('footer should be visible on mobile', async ({ page }) => {
        await page.locator('footer.footer').scrollIntoViewIfNeeded();
        const footer = page.locator('footer.footer');
        await expect(footer).toBeVisible();
      });

      test('footer content should fit within mobile viewport', async ({ page }) => {
        await page.locator('footer.footer').scrollIntoViewIfNeeded();
        const footer = page.locator('footer.footer');

        const box = await footer.boundingBox();
        expect(box.width).toBeLessThanOrEqual(375);
      });

      test('all footer links should be accessible on mobile', async ({ page }) => {
        await page.locator('footer.footer').scrollIntoViewIfNeeded();
        const links = page.locator('footer.footer a');
        const count = await links.count();

        for (let i = 0; i < count; i++) {
          await expect(links.nth(i)).toBeVisible();
        }
      });
    });

    test.describe('Desktop viewport', () => {
      test.use({ viewport: { width: 1280, height: 720 } });

      test('footer should span full width on desktop', async ({ page }) => {
        await page.locator('footer.footer').scrollIntoViewIfNeeded();
        const footer = page.locator('footer.footer');

        const box = await footer.boundingBox();
        expect(box.width).toBeGreaterThanOrEqual(1000);
      });
    });
  });

  test.describe('Accessibility', () => {
    test('footer links should be focusable via keyboard', async ({ page }) => {
      await page.locator('footer.footer').scrollIntoViewIfNeeded();

      // Focus the first link in footer using keyboard navigation
      const firstLink = page.locator('footer.footer a').first();
      await firstLink.focus();
      await expect(firstLink).toBeFocused();
    });

    test('footer should use semantic HTML', async ({ page }) => {
      const footer = page.locator('footer');
      await expect(footer).toBeVisible();
    });
  });
});
