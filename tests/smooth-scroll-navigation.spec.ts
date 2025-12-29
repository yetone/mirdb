import { test, expect } from '@playwright/test';

test.describe('Smooth Scroll Navigation', () => {
  test.describe('Test Case 1: Click navigation link to features section', () => {
    test('should smoothly scroll to features section when clicking nav link', async ({ page }) => {
      // Input: Click navigation link to features section
      // Expected: Page smoothly scrolls to features section

      await page.goto('/');

      // Get initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY);

      // Get the features section position before clicking
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();

      // Click the features navigation link
      const featuresNavLink = page.locator('a.nav-link[href="#features"]');
      await featuresNavLink.click();

      // Wait a bit for smooth scroll animation to start
      await page.waitForTimeout(100);

      // Verify that scrolling has started (scroll position changed)
      const scrollYAfterClick = await page.evaluate(() => window.scrollY);
      expect(scrollYAfterClick).toBeGreaterThan(initialScrollY);

      // Wait for smooth scroll to complete (should take less than 1 second)
      await page.waitForTimeout(800);

      // Verify the features section is now in the viewport
      await expect(featuresSection).toBeInViewport();

      // Verify scroll position is at or near the features section
      const featuresSectionTop = await featuresSection.evaluate(el => el.getBoundingClientRect().top);
      // The section should be near the top of the viewport (accounting for fixed header)
      expect(Math.abs(featuresSectionTop)).toBeLessThan(100);
    });

    test('should smoothly scroll to getting-started section', async ({ page }) => {
      await page.goto('/');

      const initialScrollY = await page.evaluate(() => window.scrollY);
      const gettingStartedSection = page.locator('#getting-started');

      // Click the getting started navigation link
      const navLink = page.locator('a.nav-link[href="#getting-started"]');
      await navLink.click();

      // Wait for scroll animation
      await page.waitForTimeout(100);

      // Verify scrolling has started
      const scrollYAfterClick = await page.evaluate(() => window.scrollY);
      expect(scrollYAfterClick).toBeGreaterThan(initialScrollY);

      // Wait for smooth scroll to complete
      await page.waitForTimeout(800);

      // Verify the section is in viewport
      await expect(gettingStartedSection).toBeInViewport();
    });

    test('should smoothly scroll to configuration section', async ({ page }) => {
      await page.goto('/');

      const configSection = page.locator('#configuration');

      // Click the configuration navigation link
      const navLink = page.locator('a.nav-link[href="#configuration"]');
      await navLink.click();

      // Wait for smooth scroll to complete
      await page.waitForTimeout(900);

      // Verify the section is in viewport
      await expect(configSection).toBeInViewport();
    });

    test('should smoothly scroll to technical-specs section', async ({ page }) => {
      await page.goto('/');

      const specsSection = page.locator('#technical-specs');

      // Click the specs navigation link
      const navLink = page.locator('a.nav-link[href="#technical-specs"]');
      await navLink.click();

      // Wait for smooth scroll to complete
      await page.waitForTimeout(900);

      // Verify the section is in viewport
      await expect(specsSection).toBeInViewport();
    });

    test('should smoothly scroll to architecture section', async ({ page }) => {
      await page.goto('/');

      const architectureSection = page.locator('#architecture');

      // Click the architecture navigation link
      const navLink = page.locator('a.nav-link[href="#architecture"]');
      await navLink.click();

      // Wait for smooth scroll to complete
      await page.waitForTimeout(900);

      // Verify the section is in viewport
      await expect(architectureSection).toBeInViewport();
    });

    test('should use smooth scroll animation (not instant jump)', async ({ page }) => {
      await page.goto('/');

      // Track scroll positions over time to verify smooth animation
      const scrollPositions: number[] = [];

      // Set up scroll position tracking
      await page.evaluate(() => {
        (window as any).__scrollPositions = [];
        const trackScroll = () => {
          (window as any).__scrollPositions.push(window.scrollY);
        };
        window.addEventListener('scroll', trackScroll);
      });

      // Click a navigation link to a section far down the page
      const navLink = page.locator('a.nav-link[href="#architecture"]');
      await navLink.click();

      // Wait for scroll animation
      await page.waitForTimeout(500);

      // Get recorded scroll positions
      const recordedPositions = await page.evaluate(() => (window as any).__scrollPositions);

      // Verify that we have multiple scroll events (indicating animation, not instant jump)
      expect(recordedPositions.length).toBeGreaterThan(1);

      // Verify scroll positions are increasing (scrolling down)
      for (let i = 1; i < recordedPositions.length; i++) {
        expect(recordedPositions[i]).toBeGreaterThanOrEqual(recordedPositions[i - 1]);
      }
    });
  });

  test.describe('Test Case 2: URL hash after navigation', () => {
    test('should update URL hash when clicking features navigation link', async ({ page }) => {
      // Input: Verify URL hash after navigation
      // Expected: URL contains hash corresponding to target section

      await page.goto('/');

      // Click the features navigation link
      const featuresNavLink = page.locator('a.nav-link[href="#features"]');
      await featuresNavLink.click();

      // Wait for navigation to complete
      await page.waitForTimeout(100);

      // Verify URL hash is updated
      const url = page.url();
      expect(url).toContain('#features');
    });

    test('should update URL hash when clicking getting-started link', async ({ page }) => {
      await page.goto('/');

      const navLink = page.locator('a.nav-link[href="#getting-started"]');
      await navLink.click();

      await page.waitForTimeout(100);

      const url = page.url();
      expect(url).toContain('#getting-started');
    });

    test('should update URL hash when clicking configuration link', async ({ page }) => {
      await page.goto('/');

      const navLink = page.locator('a.nav-link[href="#configuration"]');
      await navLink.click();

      await page.waitForTimeout(100);

      const url = page.url();
      expect(url).toContain('#configuration');
    });

    test('should update URL hash when clicking technical-specs link', async ({ page }) => {
      await page.goto('/');

      const navLink = page.locator('a.nav-link[href="#technical-specs"]');
      await navLink.click();

      await page.waitForTimeout(100);

      const url = page.url();
      expect(url).toContain('#technical-specs');
    });

    test('should update URL hash when clicking architecture link', async ({ page }) => {
      await page.goto('/');

      const navLink = page.locator('a.nav-link[href="#architecture"]');
      await navLink.click();

      await page.waitForTimeout(100);

      const url = page.url();
      expect(url).toContain('#architecture');
    });

    test('should update URL hash when clicking CTA button link', async ({ page }) => {
      await page.goto('/');

      // Click the "Get Started" CTA button
      const ctaButton = page.locator('a.btn-primary[href="#getting-started"]');
      await ctaButton.click();

      await page.waitForTimeout(100);

      const url = page.url();
      expect(url).toContain('#getting-started');
    });

    test('should allow browser back navigation after hash update', async ({ page }) => {
      await page.goto('/');

      // Click features link
      const featuresLink = page.locator('a.nav-link[href="#features"]');
      await featuresLink.click();
      await page.waitForTimeout(100);

      // Click configuration link
      const configLink = page.locator('a.nav-link[href="#configuration"]');
      await configLink.click();
      await page.waitForTimeout(100);

      // Verify current URL has configuration hash
      expect(page.url()).toContain('#configuration');

      // Go back in browser history
      await page.goBack();
      await page.waitForTimeout(200);

      // Verify URL now has features hash
      expect(page.url()).toContain('#features');
    });
  });

  test.describe('Test Case 3: Load page with section hash in URL', () => {
    test('should scroll to features section when URL contains #features hash', async ({ page }) => {
      // Input: Load page with section hash in URL
      // Expected: Page scrolls to correct section on initial load

      // Navigate directly to the page with hash
      await page.goto('/#features');

      // Wait for page load and smooth scroll
      await page.waitForTimeout(500);

      // Verify the features section is in viewport
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport();

      // Verify scroll position is not at top
      const scrollY = await page.evaluate(() => window.scrollY);
      expect(scrollY).toBeGreaterThan(0);
    });

    test('should scroll to getting-started section when URL contains hash', async ({ page }) => {
      await page.goto('/#getting-started');

      await page.waitForTimeout(500);

      const section = page.locator('#getting-started');
      await expect(section).toBeInViewport();

      const scrollY = await page.evaluate(() => window.scrollY);
      expect(scrollY).toBeGreaterThan(0);
    });

    test('should scroll to configuration section when URL contains hash', async ({ page }) => {
      await page.goto('/#configuration');

      await page.waitForTimeout(500);

      const section = page.locator('#configuration');
      await expect(section).toBeInViewport();

      const scrollY = await page.evaluate(() => window.scrollY);
      expect(scrollY).toBeGreaterThan(0);
    });

    test('should scroll to technical-specs section when URL contains hash', async ({ page }) => {
      await page.goto('/#technical-specs');

      await page.waitForTimeout(500);

      const section = page.locator('#technical-specs');
      await expect(section).toBeInViewport();

      const scrollY = await page.evaluate(() => window.scrollY);
      expect(scrollY).toBeGreaterThan(0);
    });

    test('should scroll to architecture section when URL contains hash', async ({ page }) => {
      await page.goto('/#architecture');

      await page.waitForTimeout(500);

      const section = page.locator('#architecture');
      await expect(section).toBeInViewport();

      const scrollY = await page.evaluate(() => window.scrollY);
      expect(scrollY).toBeGreaterThan(0);
    });

    test('should preserve hash in URL after initial scroll', async ({ page }) => {
      await page.goto('/#configuration');

      await page.waitForTimeout(500);

      // Verify URL still contains the hash
      const url = page.url();
      expect(url).toContain('#configuration');
    });

    test('should handle invalid hash gracefully', async ({ page }) => {
      // Navigate with a non-existent hash
      await page.goto('/#nonexistent-section');

      // Page should load without errors
      await page.waitForTimeout(500);

      // Hero section should still be visible
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // URL should still contain the hash
      const url = page.url();
      expect(url).toContain('#nonexistent-section');
    });
  });

  test.describe('Additional smooth scroll tests', () => {
    test('should have smooth scroll CSS property on html element', async ({ page }) => {
      await page.goto('/');

      // Verify CSS scroll-behavior is set to smooth
      const scrollBehavior = await page.evaluate(() => {
        return getComputedStyle(document.documentElement).scrollBehavior;
      });

      expect(scrollBehavior).toBe('smooth');
    });

    test('should scroll smoothly from footer back to top via CTA', async ({ page }) => {
      await page.goto('/#architecture');

      // Wait for initial scroll
      await page.waitForTimeout(500);

      // Now click the features link to scroll up
      const featuresLink = page.locator('a.nav-link[href="#features"]');
      await featuresLink.click();

      // Wait for smooth scroll
      await page.waitForTimeout(800);

      // Verify features section is in viewport
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport();
    });

    test('all navigation links should have corresponding sections with IDs', async ({ page }) => {
      await page.goto('/');

      // Get all navigation links
      const navLinks = page.locator('.nav-link[href^="#"]');
      const count = await navLinks.count();

      // Verify each navigation link has a corresponding section
      for (let i = 0; i < count; i++) {
        const href = await navLinks.nth(i).getAttribute('href');
        if (href) {
          const sectionId = href.substring(1);
          const section = page.locator(`#${sectionId}`);
          await expect(section).toBeVisible();
        }
      }
    });
  });
});
