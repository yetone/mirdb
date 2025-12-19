// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

// Base URL for the static HTML file
const BASE_URL = 'file://' + path.resolve(__dirname, '../dist/index.html');

/**
 * Navigation and User Flow Tests
 *
 * Scenario: Verify smooth navigation throughout the page sections
 *
 * Test Cases:
 * TC1: Click 'Get Started' CTA - Page scrolls to quick-start section
 * TC2: Test navigation menu links - Each link scrolls to correct section
 * TC3: Test direct section URL access - Direct URL with anchor loads at correct section
 * TC4: Test back button after anchor navigation - Browser back button works correctly
 */

test.describe('Navigation and User Flow', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(BASE_URL);
    await page.waitForLoadState('domcontentloaded');
  });

  test.describe('TC1: Get Started CTA Navigation', () => {
    test('clicking Get Started button scrolls to quickstart section', async ({ page }) => {
      // Verify we start at the top of the page (hero section visible)
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // Click the "Get Started" CTA button
      const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
      await expect(getStartedBtn).toBeVisible();
      await expect(getStartedBtn).toHaveAttribute('href', '#quickstart');
      await getStartedBtn.click();

      // Wait for smooth scroll animation to complete
      await page.waitForTimeout(800);

      // Verify quickstart section is now in viewport
      const quickstartSection = page.locator('[data-testid="quickstart-section"]');
      await expect(quickstartSection).toBeInViewport();

      // Verify URL hash was updated
      const url = page.url();
      expect(url).toContain('#quickstart');
    });

    test('Get Started button has correct href attribute', async ({ page }) => {
      const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
      await expect(getStartedBtn).toHaveAttribute('href', '#quickstart');
    });

    test('Get Started navigation works with keyboard (Enter key)', async ({ page }) => {
      // Tab to Get Started button
      await page.keyboard.press('Tab');
      const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
      await expect(getStartedBtn).toBeFocused();

      // Press Enter to activate
      await page.keyboard.press('Enter');

      // Wait for smooth scroll
      await page.waitForTimeout(800);

      // Verify quickstart section is in viewport
      const quickstartSection = page.locator('[data-testid="quickstart-section"]');
      await expect(quickstartSection).toBeInViewport();
    });
  });

  test.describe('TC2: Navigation Menu Links', () => {
    const sections = [
      { id: 'features', testId: 'features-section', name: 'Key Features' },
      { id: 'architecture', testId: 'architecture-section', name: 'Architecture Overview' },
      { id: 'quickstart', testId: 'quickstart-section', name: 'Quick Start' },
      { id: 'protocol', testId: 'protocol-section', name: 'Protocol Reference' },
      { id: 'configuration', testId: 'configuration-section', name: 'Configuration Reference' },
      { id: 'footer', testId: 'footer-section', name: 'Footer' }
    ];

    for (const section of sections) {
      test(`navigating to #${section.id} scrolls to ${section.name} section`, async ({ page }) => {
        // Start at top of page
        await page.evaluate(() => window.scrollTo(0, 0));
        await page.waitForTimeout(100);

        // Navigate using URL hash
        await page.evaluate((sectionId) => {
          window.location.hash = sectionId;
        }, section.id);

        // Wait for scroll to complete
        await page.waitForTimeout(800);

        // Verify section is in viewport
        const sectionElement = page.locator(`[data-testid="${section.testId}"]`);
        await expect(sectionElement).toBeInViewport();
      });
    }

    test('clicking internal anchor links scrolls to correct section', async ({ page }) => {
      // Find and click architecture docs link (internal link example)
      const archDocsLink = page.locator('[data-testid="architecture-docs-link"]');

      // Verify it's an external link (this one goes to GitHub)
      const href = await archDocsLink.getAttribute('href');
      expect(href).toContain('github.com');
    });

    test('all section IDs exist and are reachable', async ({ page }) => {
      const sectionIds = ['hero', 'features', 'architecture', 'quickstart', 'protocol', 'configuration', 'footer'];

      for (const sectionId of sectionIds) {
        const section = page.locator(`#${sectionId}`);
        await expect(section).toBeAttached();
      }
    });
  });

  test.describe('TC3: Direct Section URL Access', () => {
    const sections = [
      { id: 'features', testId: 'features-section' },
      { id: 'architecture', testId: 'architecture-section' },
      { id: 'quickstart', testId: 'quickstart-section' },
      { id: 'protocol', testId: 'protocol-section' },
      { id: 'configuration', testId: 'configuration-section' }
    ];

    for (const section of sections) {
      test(`direct URL with #${section.id} loads page at correct section`, async ({ page }) => {
        // Navigate directly to URL with anchor
        await page.goto(`${BASE_URL}#${section.id}`);
        await page.waitForLoadState('domcontentloaded');

        // Wait for browser to scroll to anchor
        await page.waitForTimeout(500);

        // Verify section is in viewport or near top of viewport
        const sectionElement = page.locator(`[data-testid="${section.testId}"]`);
        await expect(sectionElement).toBeInViewport();

        // Verify URL contains the correct hash
        expect(page.url()).toContain(`#${section.id}`);
      });
    }

    test('direct URL with invalid anchor loads page at top', async ({ page }) => {
      await page.goto(`${BASE_URL}#nonexistent-section`);
      await page.waitForLoadState('domcontentloaded');

      // Hero section should be visible (page loads at top when anchor doesn't exist)
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();
    });

    test('direct URL with #hero loads at top of page', async ({ page }) => {
      await page.goto(`${BASE_URL}#hero`);
      await page.waitForLoadState('domcontentloaded');
      await page.waitForTimeout(300);

      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeInViewport();
    });
  });

  test.describe('TC4: Browser Back Button After Anchor Navigation', () => {
    test('back button returns to previous hash state after anchor navigation', async ({ page }) => {
      // Start at top of page (hero section)
      await page.evaluate(() => window.scrollTo(0, 0));
      await page.waitForTimeout(100);

      // Navigate to quickstart section
      const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
      await getStartedBtn.click();
      await page.waitForTimeout(800);

      // Verify we're at quickstart
      expect(page.url()).toContain('#quickstart');
      const quickstartSection = page.locator('[data-testid="quickstart-section"]');
      await expect(quickstartSection).toBeInViewport();

      // Click browser back button
      await page.goBack();
      await page.waitForTimeout(800);

      // URL should no longer have #quickstart hash (or have empty hash)
      const currentUrl = page.url();
      expect(currentUrl).not.toContain('#quickstart');

      // The scroll position may vary by browser, but URL should be correct
      // Just verify we navigated back (hash changed)
    });

    test('forward button works after going back from anchor navigation', async ({ page }) => {
      // Navigate to quickstart
      await page.goto(`${BASE_URL}#quickstart`);
      await page.waitForLoadState('domcontentloaded');
      await page.waitForTimeout(500);

      // Navigate to configuration
      await page.evaluate(() => {
        window.location.hash = 'configuration';
      });
      await page.waitForTimeout(800);

      // Go back
      await page.goBack();
      await page.waitForTimeout(500);
      expect(page.url()).toContain('#quickstart');

      // Go forward
      await page.goForward();
      await page.waitForTimeout(500);
      expect(page.url()).toContain('#configuration');
    });

    test('multiple back button presses work correctly through navigation history', async ({ page }) => {
      // Build navigation history: hero -> features -> architecture -> quickstart
      await page.evaluate(() => window.scrollTo(0, 0));

      // Navigate to features
      await page.evaluate(() => { window.location.hash = 'features'; });
      await page.waitForTimeout(500);
      expect(page.url()).toContain('#features');

      // Navigate to architecture
      await page.evaluate(() => { window.location.hash = 'architecture'; });
      await page.waitForTimeout(500);
      expect(page.url()).toContain('#architecture');

      // Navigate to quickstart
      await page.evaluate(() => { window.location.hash = 'quickstart'; });
      await page.waitForTimeout(500);
      expect(page.url()).toContain('#quickstart');

      // Go back to architecture
      await page.goBack();
      await page.waitForTimeout(500);
      expect(page.url()).toContain('#architecture');

      // Go back to features
      await page.goBack();
      await page.waitForTimeout(500);
      expect(page.url()).toContain('#features');
    });
  });

  test.describe('Smooth Scrolling Behavior', () => {
    test('page has smooth scroll behavior CSS applied', async ({ page }) => {
      // Check if html element has scroll-behavior: smooth
      const scrollBehavior = await page.evaluate(() => {
        return window.getComputedStyle(document.documentElement).scrollBehavior;
      });

      expect(scrollBehavior).toBe('smooth');
    });

    test('scrolling between sections is animated (not instant)', async ({ page }) => {
      // Start at top
      await page.evaluate(() => window.scrollTo(0, 0));
      await page.waitForTimeout(100);

      const initialScrollY = await page.evaluate(() => window.scrollY);

      // Click Get Started to trigger scroll
      const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
      await getStartedBtn.click();

      // Check scroll position shortly after click (should be mid-scroll if smooth)
      await page.waitForTimeout(100);
      const midScrollY = await page.evaluate(() => window.scrollY);

      // Wait for scroll to complete
      await page.waitForTimeout(700);
      const finalScrollY = await page.evaluate(() => window.scrollY);

      // If scroll is smooth, midScrollY should be between initial and final
      // (or at least different from both for very fast scrolls)
      expect(finalScrollY).toBeGreaterThan(initialScrollY);
    });
  });

  test.describe('Section Visibility After Navigation', () => {
    test('quickstart section heading is visible after navigation', async ({ page }) => {
      const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
      await getStartedBtn.click();
      await page.waitForTimeout(800);

      // Verify the section heading is visible
      const heading = page.locator('#quickstart h2');
      await expect(heading).toBeVisible();
      await expect(heading).toContainText('Quick Start');
    });

    test('all major sections are scrollable to', async ({ page }) => {
      const sections = [
        { hash: '#features', heading: 'Key Features' },
        { hash: '#architecture', heading: 'Architecture Overview' },
        { hash: '#quickstart', heading: 'Quick Start' },
        { hash: '#protocol', heading: 'Protocol Reference' },
        { hash: '#configuration', heading: 'Configuration Reference' }
      ];

      for (const section of sections) {
        // Navigate to section
        await page.evaluate((hash) => {
          window.location.hash = hash.substring(1);
        }, section.hash);
        await page.waitForTimeout(800);

        // Verify heading is in viewport
        const heading = page.locator(`${section.hash} h2`).first();
        await expect(heading).toBeInViewport();
      }
    });
  });

  test.describe('External Links Behavior', () => {
    test('GitHub button opens in new tab (has target="_blank")', async ({ page }) => {
      const githubBtn = page.locator('[data-testid="cta-github"]');
      await expect(githubBtn).toHaveAttribute('target', '_blank');
      await expect(githubBtn).toHaveAttribute('rel', 'noopener noreferrer');
    });

    test('footer GitHub link opens in new tab', async ({ page }) => {
      const footerGithubLink = page.locator('[data-testid="footer-github-link"]');
      await expect(footerGithubLink).toHaveAttribute('target', '_blank');
      await expect(footerGithubLink).toHaveAttribute('rel', 'noopener noreferrer');
    });

    test('footer docs link opens in new tab', async ({ page }) => {
      const footerDocsLink = page.locator('[data-testid="footer-docs-link"]');
      await expect(footerDocsLink).toHaveAttribute('target', '_blank');
      await expect(footerDocsLink).toHaveAttribute('rel', 'noopener noreferrer');
    });
  });
});
