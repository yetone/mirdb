/**
 * Navigation E2E Tests
 * Owner: Scenario 8 - Navigation and Header
 *
 * Tests:
 * - Header visibility
 * - Navigation links
 * - Smooth scroll functionality
 * - Semantic nav structure
 */

const { test, expect } = require('@playwright/test');

test.describe('Navigation and Header', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Test Case 1: Header Logo/Brand Element', () => {
    test('header contains MirDB logo or brand text', async ({ page }) => {
      // Check that header exists and is visible
      const header = page.locator('header');
      await expect(header).toBeVisible();

      // Check for logo/brand element
      const logo = page.locator('.logo');
      await expect(logo).toBeVisible();

      // Verify logo contains MirDB text
      await expect(logo).toContainText('MirDB');
    });

    test('logo is positioned in the header', async ({ page }) => {
      const header = page.locator('header');
      const logo = header.locator('.logo');
      await expect(logo).toBeVisible();
    });

    test('logo links to home/top of page', async ({ page }) => {
      const logo = page.locator('.logo');
      const href = await logo.getAttribute('href');
      expect(href === '#' || href === '/' || href === '#hero').toBeTruthy();
    });

    test('logo has accessible aria-label', async ({ page }) => {
      const logo = page.locator('.logo');
      const ariaLabel = await logo.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel.toLowerCase()).toContain('home');
    });
  });

  test.describe('Test Case 2: Navigation Links', () => {
    test('navigation contains link to Home section', async ({ page }) => {
      // Check for home link (either explicit or via logo)
      const homeLink = page.locator('nav a[href="#"], nav a[href="#hero"], .logo');
      await expect(homeLink.first()).toBeVisible();
    });

    test('navigation contains link to Features section', async ({ page }) => {
      const featuresLink = page.locator('nav a[href="#features"]');
      await expect(featuresLink).toBeVisible();
      await expect(featuresLink).toContainText(/features/i);
    });

    test('navigation contains link to Docs/Quick Start section', async ({ page }) => {
      // Check for docs or quickstart link
      const docsLink = page.locator('nav a[href="#quickstart"], nav a[href="#docs"]');
      await expect(docsLink.first()).toBeVisible();
    });

    test('navigation contains GitHub link', async ({ page }) => {
      const githubLink = page.locator('nav a[href*="github.com"]');
      await expect(githubLink).toBeVisible();
      await expect(githubLink).toContainText(/github/i);

      // GitHub link should open in new tab
      const target = await githubLink.getAttribute('target');
      expect(target).toBe('_blank');

      // Should have rel attribute for security
      const rel = await githubLink.getAttribute('rel');
      expect(rel).toContain('noopener');
    });

    test('all main navigation links are visible and accessible', async ({ page }) => {
      const navLinks = page.locator('.nav-links a');
      const count = await navLinks.count();
      expect(count).toBeGreaterThanOrEqual(4); // Features, Architecture, Quick Start, GitHub at minimum

      for (let i = 0; i < count; i++) {
        await expect(navLinks.nth(i)).toBeVisible();
      }
    });
  });

  test.describe('Test Case 3: Navigation Functionality', () => {
    test('clicking Features link scrolls to Features section', async ({ page }) => {
      const featuresLink = page.locator('nav a[href="#features"]');
      await featuresLink.click();

      // Wait for scroll to complete
      await page.waitForTimeout(1000);

      // Verify the Features section is in view
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport();
    });

    test('clicking Architecture link navigates to Architecture section', async ({ page }) => {
      const architectureLink = page.locator('nav a[href="#architecture"]');
      await architectureLink.click();

      await page.waitForTimeout(1000);

      const architectureSection = page.locator('#architecture');
      await expect(architectureSection).toBeInViewport();
    });

    test('clicking Quick Start link navigates to Quick Start section', async ({ page }) => {
      const quickstartLink = page.locator('nav a[href="#quickstart"]');
      await quickstartLink.click();

      await page.waitForTimeout(1000);

      const quickstartSection = page.locator('#quickstart');
      await expect(quickstartSection).toBeInViewport();
    });

    test('navigation uses smooth scrolling', async ({ page }) => {
      // Check that smooth scrolling is enabled via CSS
      const htmlScrollBehavior = await page.evaluate(() => {
        return getComputedStyle(document.documentElement).scrollBehavior;
      });
      expect(htmlScrollBehavior).toBe('smooth');
    });

    test('internal navigation links work correctly', async ({ page }) => {
      // Get all internal navigation links
      const navLinks = page.locator('.nav-links a[href^="#"]');
      const count = await navLinks.count();

      for (let i = 0; i < count; i++) {
        const link = navLinks.nth(i);
        const href = await link.getAttribute('href');
        if (href && href.startsWith('#') && href.length > 1) {
          // Click the link
          await link.click();
          await page.waitForTimeout(500);

          // Verify the target section exists and is visible
          const targetId = href.substring(1);
          const targetSection = page.locator(`#${targetId}`);
          await expect(targetSection).toBeAttached();
        }
      }
    });
  });

  test.describe('Test Case 4: Semantic HTML and ARIA', () => {
    test('navigation uses <nav> element', async ({ page }) => {
      const nav = page.locator('header nav');
      await expect(nav).toBeAttached();
    });

    test('navigation has role="navigation"', async ({ page }) => {
      const nav = page.locator('header nav');
      const role = await nav.getAttribute('role');
      expect(role).toBe('navigation');
    });

    test('navigation has aria-label attribute', async ({ page }) => {
      const nav = page.locator('header nav');
      const ariaLabel = await nav.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel.toLowerCase()).toContain('navigation');
    });

    test('header has role="banner"', async ({ page }) => {
      const header = page.locator('header');
      const role = await header.getAttribute('role');
      expect(role).toBe('banner');
    });

    test('navigation links are in a list structure', async ({ page }) => {
      const navList = page.locator('nav ul');
      await expect(navList).toBeAttached();

      const listItems = page.locator('nav ul li');
      const count = await listItems.count();
      expect(count).toBeGreaterThan(0);
    });
  });

  test.describe('Header Visibility and Positioning', () => {
    test('header is fixed at the top of the page', async ({ page }) => {
      const header = page.locator('header');
      const position = await header.evaluate((el) => getComputedStyle(el).position);
      expect(position).toBe('fixed');
    });

    test('header remains visible when scrolling', async ({ page }) => {
      // Scroll down the page
      await page.evaluate(() => window.scrollTo(0, 1000));
      await page.waitForTimeout(500);

      // Header should still be visible
      const header = page.locator('header');
      await expect(header).toBeVisible();
    });

    test('header has appropriate z-index to stay on top', async ({ page }) => {
      const header = page.locator('header');
      const zIndex = await header.evaluate((el) => getComputedStyle(el).zIndex);
      expect(parseInt(zIndex)).toBeGreaterThanOrEqual(100);
    });
  });

  test.describe('Keyboard Navigation', () => {
    test('navigation links are focusable', async ({ page }) => {
      const navLinks = page.locator('.nav-links a');
      const count = await navLinks.count();

      for (let i = 0; i < count; i++) {
        const link = navLinks.nth(i);
        await link.focus();
        await expect(link).toBeFocused();
      }
    });

    test('logo is focusable', async ({ page }) => {
      const logo = page.locator('.logo');
      await logo.focus();
      await expect(logo).toBeFocused();
    });

    test('focus is visible on navigation elements', async ({ page }) => {
      const firstNavLink = page.locator('.nav-links a').first();
      await firstNavLink.focus();

      // Check that focus styles are applied (outline or similar)
      const outlineWidth = await firstNavLink.evaluate((el) => {
        const styles = getComputedStyle(el);
        return styles.outlineWidth || styles.boxShadow;
      });

      // Focus should be visible (non-zero outline or box-shadow)
      expect(outlineWidth).toBeTruthy();
    });
  });
});
