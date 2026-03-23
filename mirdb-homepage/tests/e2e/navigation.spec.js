/**
 * Navigation and User Flow Tests
 * Owner: Scenario 10 - Navigation and User Flow
 *
 * Test cases:
 * 1. Navigation links scroll smoothly to sections
 * 2. URL hash updates on navigation
 * 3. Direct URL with hash scrolls to section
 * 4. External links open in new tab
 * 5. Sticky navigation on scroll
 */
const { test, expect } = require('@playwright/test');

test.describe('Navigation and User Flow', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('http://localhost:3000/index.html');
    // Wait for the page to be fully loaded
    await page.waitForLoadState('domcontentloaded');
  });

  test.describe('Navigation Link Scrolling', () => {
    test('TC1: Click Features navigation link scrolls to Features section and updates URL hash', async ({ page }) => {
      // Get initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY);

      // Click the Features navigation link
      await page.click('nav a[href="#features"]');

      // Wait for smooth scroll animation to complete
      await page.waitForTimeout(1000);

      // Verify URL hash is updated
      await expect(page).toHaveURL(/.*#features$/);

      // Get the Features section position
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();

      // Verify we scrolled to the Features section (it should be near the top of viewport)
      const sectionBounds = await featuresSection.boundingBox();
      const navHeight = await page.evaluate(() => {
        const nav = document.querySelector('nav');
        return nav ? nav.offsetHeight : 0;
      });

      // The section should be scrolled into view (near the top, accounting for sticky nav)
      const viewportTop = await page.evaluate(() => window.scrollY);
      expect(viewportTop).toBeGreaterThan(initialScrollY);
    });

    test('TC2: Click Getting Started navigation link scrolls to Getting Started section and updates URL hash', async ({ page }) => {
      // Get initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY);

      // Click the Getting Started navigation link
      await page.click('nav a[href="#getting-started"]');

      // Wait for smooth scroll animation
      await page.waitForTimeout(1000);

      // Verify URL hash is updated
      await expect(page).toHaveURL(/.*#getting-started$/);

      // Verify the Getting Started section is visible
      const gettingStartedSection = page.locator('#getting-started');
      await expect(gettingStartedSection).toBeVisible();

      // Verify we scrolled down
      const currentScrollY = await page.evaluate(() => window.scrollY);
      expect(currentScrollY).toBeGreaterThan(initialScrollY);
    });

    test('TC3: Click Architecture navigation link scrolls to Architecture section and updates URL hash', async ({ page }) => {
      // Get initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY);

      // Click the Architecture navigation link
      await page.click('nav a[href="#architecture"]');

      // Wait for smooth scroll animation
      await page.waitForTimeout(1000);

      // Verify URL hash is updated
      await expect(page).toHaveURL(/.*#architecture$/);

      // Verify the Architecture section is visible
      const architectureSection = page.locator('#architecture');
      await expect(architectureSection).toBeVisible();

      // Verify we scrolled down
      const currentScrollY = await page.evaluate(() => window.scrollY);
      expect(currentScrollY).toBeGreaterThan(initialScrollY);
    });
  });

  test.describe('Direct URL with Hash Navigation', () => {
    test('TC4: Loading page with #features hash scrolls directly to Features section', async ({ page }) => {
      // Navigate directly to the page with hash
      await page.goto('http://localhost:3000/index.html#features');

      // Wait for page to load and scroll to position
      await page.waitForLoadState('domcontentloaded');
      await page.waitForTimeout(500);

      // Verify the Features section is visible
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();

      // Verify the section is in the viewport
      const isInViewport = await featuresSection.evaluate((el) => {
        const rect = el.getBoundingClientRect();
        return rect.top < window.innerHeight && rect.bottom > 0;
      });
      expect(isInViewport).toBe(true);
    });
  });

  test.describe('External Link Behavior', () => {
    test('TC5: Click GitHub link opens in new tab and original page remains', async ({ page, context }) => {
      // Find the GitHub link in the hero section or footer
      const githubLink = page.locator('a[href*="github.com"][target="_blank"]').first();
      await expect(githubLink).toBeVisible();

      // Verify the link has target="_blank" attribute
      await expect(githubLink).toHaveAttribute('target', '_blank');

      // Verify the link has rel="noopener noreferrer" for security
      const relAttribute = await githubLink.getAttribute('rel');
      expect(relAttribute).toContain('noopener');

      // Click the link and verify new page opens (listen for popup)
      const [newPage] = await Promise.all([
        context.waitForEvent('page'),
        githubLink.click(),
      ]);

      // Verify original page URL hasn't changed
      expect(page.url()).toContain('localhost:3000');

      // Close the new tab
      await newPage.close();
    });
  });

  test.describe('Sticky Navigation', () => {
    test('TC6: Navigation header remains visible when scrolling down (sticky positioning)', async ({ page }) => {
      // Verify navigation is visible initially
      const nav = page.locator('nav.nav');
      await expect(nav).toBeVisible();

      // Get initial nav position
      const initialNavBounds = await nav.boundingBox();
      expect(initialNavBounds).not.toBeNull();

      // Scroll down significantly
      await page.evaluate(() => window.scrollTo(0, 1000));
      await page.waitForTimeout(500);

      // Verify navigation is still visible after scrolling
      await expect(nav).toBeVisible();

      // Get nav position after scrolling
      const afterScrollNavBounds = await nav.boundingBox();
      expect(afterScrollNavBounds).not.toBeNull();

      // The nav should be at the top of the viewport (sticky behavior)
      // It should have top position close to 0
      expect(afterScrollNavBounds.y).toBeLessThanOrEqual(5);

      // Verify the nav has sticky positioning via CSS
      const navPosition = await nav.evaluate((el) => {
        return window.getComputedStyle(el).position;
      });
      expect(navPosition).toBe('sticky');
    });

    test('Navigation stays fixed at top when scrolling through multiple sections', async ({ page }) => {
      const nav = page.locator('nav.nav');

      // Scroll to different sections and verify nav is always visible at top
      const scrollPositions = [500, 1500, 2500, 3500];

      for (const scrollY of scrollPositions) {
        await page.evaluate((y) => window.scrollTo(0, y), scrollY);
        await page.waitForTimeout(300);

        // Nav should still be visible
        await expect(nav).toBeVisible();

        // Nav should be at the top of viewport
        const bounds = await nav.boundingBox();
        expect(bounds).not.toBeNull();
        expect(bounds.y).toBeLessThanOrEqual(5);
      }
    });
  });

  test.describe('Navigation Link Structure', () => {
    test('All navigation links point to valid section IDs', async ({ page }) => {
      // Get all navigation links
      const navLinks = page.locator('nav.nav ul.nav__links a');
      const linkCount = await navLinks.count();

      expect(linkCount).toBeGreaterThan(0);

      for (let i = 0; i < linkCount; i++) {
        const link = navLinks.nth(i);
        const href = await link.getAttribute('href');

        // Skip external links
        if (href && href.startsWith('#')) {
          const sectionId = href.substring(1);
          const section = page.locator(`#${sectionId}`);
          await expect(section).toBeVisible();
        }
      }
    });

    test('Navigation includes links to all major sections', async ({ page }) => {
      // Verify essential navigation links exist
      await expect(page.locator('nav a[href="#features"]')).toBeVisible();
      await expect(page.locator('nav a[href="#getting-started"]')).toBeVisible();
      await expect(page.locator('nav a[href="#architecture"]')).toBeVisible();
      await expect(page.locator('nav a[href="#configuration"]')).toBeVisible();
      await expect(page.locator('nav a[href="#commands"]')).toBeVisible();
    });

    test('Navigation logo links to home/top', async ({ page }) => {
      const logo = page.locator('nav a.nav__logo');
      await expect(logo).toBeVisible();
      await expect(logo).toHaveText('MirDB');

      // Logo should link to top of page
      const href = await logo.getAttribute('href');
      expect(href).toBe('#');
    });
  });

  test.describe('Smooth Scroll Behavior', () => {
    test('HTML has scroll-behavior: smooth applied', async ({ page }) => {
      const scrollBehavior = await page.evaluate(() => {
        return window.getComputedStyle(document.documentElement).scrollBehavior;
      });
      expect(scrollBehavior).toBe('smooth');
    });
  });
});
