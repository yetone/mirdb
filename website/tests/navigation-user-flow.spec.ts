import { test, expect } from '@playwright/test';

/**
 * Navigation and User Flow Tests
 *
 * These tests verify smooth navigation and user journey through the homepage.
 * Tests cover:
 * - Smooth scrolling to sections
 * - CTA button functionality
 * - External link behavior
 * - Back to top functionality
 */

test.describe('Navigation and User Flow', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('TC1: Click navigation link to features section - Page smoothly scrolls to features section', async ({ page }) => {
    // Test smooth scrolling to features section via navigation
    const featuresNavLink = page.locator('nav a[href="#features"], .nav-link[href="#features"]');

    // If nav link exists, click it
    if (await featuresNavLink.count() > 0) {
      await featuresNavLink.first().click();
      await page.waitForTimeout(500); // Wait for smooth scroll
    } else {
      // Fallback: scroll directly to verify smooth scroll behavior is enabled
      await page.evaluate(() => {
        const featuresSection = document.querySelector('#features');
        if (featuresSection) {
          featuresSection.scrollIntoView({ behavior: 'smooth' });
        }
      });
      await page.waitForTimeout(500);
    }

    // Verify features section is in viewport
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();

    // Verify smooth scroll behavior is enabled in CSS
    const scrollBehavior = await page.evaluate(() => {
      return window.getComputedStyle(document.documentElement).scrollBehavior;
    });
    expect(scrollBehavior).toBe('smooth');
  });

  test('TC2: Click Get Started CTA button - User is directed to quick-start section', async ({ page }) => {
    // Find and click the Get Started button in the hero section CTA area
    const getStartedButton = page.locator('.hero-section .cta-primary');
    await expect(getStartedButton).toBeVisible();

    // Verify the button text
    await expect(getStartedButton).toContainText('Get Started');

    // Click the button
    await getStartedButton.click();

    // Wait for scroll animation to complete
    await page.waitForTimeout(800);

    // Verify the quick-start section is now in viewport
    const quickStartSection = page.locator('#quickstart');
    await expect(quickStartSection).toBeInViewport();
  });

  test('TC3: Click View on GitHub button - GitHub repository opens in new tab', async ({ page }) => {
    // Find the View on GitHub button
    const githubButton = page.locator('.cta-secondary, a[href*="github.com"]').first();
    await expect(githubButton).toBeVisible();

    // Get the href attribute
    const href = await githubButton.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toMatch(/^https:\/\/github\.com\//);

    // Verify target="_blank" for new tab
    const target = await githubButton.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify rel="noopener" for security
    const rel = await githubButton.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  test('TC4: Test all internal navigation links - All internal links navigate to correct sections', async ({ page }) => {
    // Get all internal navigation links
    const internalLinks = page.locator('a[href^="#"]');
    const linkCount = await internalLinks.count();

    // Collect all href values
    const hrefs: string[] = [];
    for (let i = 0; i < linkCount; i++) {
      const href = await internalLinks.nth(i).getAttribute('href');
      if (href && href.startsWith('#') && href.length > 1) {
        hrefs.push(href);
      }
    }

    // Verify each internal link has a corresponding section
    for (const href of hrefs) {
      const sectionId = href.substring(1); // Remove the # prefix
      const section = page.locator(`#${sectionId}`);

      // The section should exist in the DOM
      const sectionCount = await section.count();
      expect(sectionCount).toBeGreaterThan(0);
    }

    // Test clicking a few key internal links
    const keyLinks = ['#features', '#architecture', '#quickstart', '#project-status'];
    for (const link of keyLinks) {
      // Scroll back to top first
      await page.evaluate(() => window.scrollTo(0, 0));
      await page.waitForTimeout(200);

      // Find and click the link if it exists
      const navLink = page.locator(`a[href="${link}"]`).first();
      if (await navLink.count() > 0 && await navLink.isVisible()) {
        await navLink.click();
        await page.waitForTimeout(500);

        // Verify the target section is now in viewport
        const targetSection = page.locator(link);
        await expect(targetSection).toBeInViewport();
      } else {
        // Direct scroll test
        const section = page.locator(link);
        if (await section.count() > 0) {
          await section.scrollIntoViewIfNeeded();
          await page.waitForTimeout(300);
          await expect(section).toBeInViewport();
        }
      }
    }
  });

  test('TC5: Check external links open in new tab - External links have target=_blank and rel=noopener', async ({ page }) => {
    // Get all external links (links that don't start with # or /)
    const externalLinks = page.locator('a[href^="http"], a[href^="https"]');
    const linkCount = await externalLinks.count();

    expect(linkCount).toBeGreaterThan(0);

    // Check each external link has proper attributes
    for (let i = 0; i < linkCount; i++) {
      const link = externalLinks.nth(i);
      const href = await link.getAttribute('href');

      // Skip if it's an internal link (same domain)
      if (href && !href.includes('localhost') && !href.includes('127.0.0.1')) {
        const target = await link.getAttribute('target');
        const rel = await link.getAttribute('rel');

        // Verify target="_blank"
        expect(target).toBe('_blank');

        // Verify rel contains "noopener" for security
        expect(rel).toBeTruthy();
        expect(rel).toContain('noopener');
      }
    }
  });

  test('TC6: Navigate back to top from footer - Back to top functionality exists and works', async ({ page }) => {
    // First scroll to the bottom of the page to make footer visible
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(500);

    // Find the back to top button in the footer
    const backToTopButton = page.locator('.site-footer .back-to-top');

    // The button should exist
    const buttonCount = await backToTopButton.count();
    expect(buttonCount).toBeGreaterThan(0);

    // Wait for the button to be visible
    await expect(backToTopButton.first()).toBeVisible();

    // Get the initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click the back to top button
    await backToTopButton.first().click();

    // Wait for smooth scroll animation to complete
    await page.waitForTimeout(1200);

    // Verify we scrolled significantly up (near the top of the page)
    // Allow some margin because of the fixed nav bar and scroll-margin-top
    const finalScrollY = await page.evaluate(() => window.scrollY);

    // Should have scrolled up significantly
    expect(finalScrollY).toBeLessThan(initialScrollY);

    // Should be near the top (within 200px to account for any browser differences)
    expect(finalScrollY).toBeLessThan(200);

    // Verify the hero section is now visible
    const heroSection = page.locator('.hero-section');
    await expect(heroSection).toBeInViewport();
  });
});

test.describe('Navigation Smooth Scroll Behavior', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('Smooth scroll CSS is properly configured', async ({ page }) => {
    // Check that scroll-behavior: smooth is set on html element
    const scrollBehavior = await page.evaluate(() => {
      return window.getComputedStyle(document.documentElement).scrollBehavior;
    });

    expect(scrollBehavior).toBe('smooth');
  });

  test('Navigation links use fragment identifiers correctly', async ({ page }) => {
    // Get all internal links
    const internalLinks = page.locator('a[href^="#"]');
    const count = await internalLinks.count();

    if (count > 0) {
      // Each fragment identifier should correspond to an existing element
      for (let i = 0; i < count; i++) {
        const href = await internalLinks.nth(i).getAttribute('href');
        if (href && href.length > 1) {
          const targetId = href.substring(1);
          const targetElement = page.locator(`#${targetId}`);

          // The target element should exist
          await expect(targetElement).toHaveCount(1);
        }
      }
    }
  });
});

test.describe('External Link Security', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('GitHub link has proper security attributes', async ({ page }) => {
    const githubLink = page.locator('a[href*="github.com"]');

    if (await githubLink.count() > 0) {
      const link = githubLink.first();

      // Check target="_blank"
      const target = await link.getAttribute('target');
      expect(target).toBe('_blank');

      // Check rel contains noopener
      const rel = await link.getAttribute('rel');
      expect(rel).toBeTruthy();
      expect(rel).toContain('noopener');
    }
  });

  test('All external links follow security best practices', async ({ page }) => {
    // Find all links with external URLs
    const links = await page.locator('a').all();

    for (const link of links) {
      const href = await link.getAttribute('href');

      // Check if it's an external link (starts with http and not localhost)
      if (href && href.startsWith('http') && !href.includes('localhost')) {
        const target = await link.getAttribute('target');
        const rel = await link.getAttribute('rel');

        // External links should open in new tab with security attributes
        expect(target).toBe('_blank');
        expect(rel).toBeTruthy();
        expect(rel).toContain('noopener');
      }
    }
  });
});
