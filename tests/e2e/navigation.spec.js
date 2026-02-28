/**
 * Navigation E2E Tests
 * Owner: Scenarios 4, 14, 16
 *
 * Test cases:
 * - Navigation menu presence
 * - Internal link smooth scrolling
 * - External links open in new tab with rel=noopener
 * - Mobile hamburger menu toggle
 * - GitHub and CircleCI link validation
 * - Keyboard navigation through links
 */

const { test, expect } = require('@playwright/test');
const { VIEWPORTS, waitForPageLoad } = require('./test-utils');

test.describe('Navigation Menu Functionality', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForPageLoad(page);
  });

  test('Test Case 1: Navigation element is present in header', async ({ page }) => {
    // Check that header exists
    const header = page.locator('header.header');
    await expect(header).toBeVisible();

    // Check that nav element exists with aria-label
    const nav = page.locator('nav.nav');
    await expect(nav).toBeVisible();
    await expect(nav).toHaveAttribute('aria-label', 'Main navigation');

    // Check that navigation menu has links
    const navMenu = page.locator('.nav-menu');
    await expect(navMenu).toBeVisible();

    // Verify all expected links are present
    const featuresLink = page.locator('.nav-menu a[href="#features"]');
    const quickstartLink = page.locator('.nav-menu a[href="#quickstart"]');
    const statusLink = page.locator('.nav-menu a[href="#status"]');

    await expect(featuresLink).toBeVisible();
    await expect(quickstartLink).toBeVisible();
    await expect(statusLink).toBeVisible();
  });

  test('Test Case 2: Clicking Features link scrolls to features section', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click the Features link
    const featuresLink = page.locator('.nav-menu a[href="#features"]');
    await featuresLink.click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Check that the page has scrolled
    const finalScrollY = await page.evaluate(() => window.scrollY);
    expect(finalScrollY).toBeGreaterThan(initialScrollY);

    // Check that the features section is in view
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });

  test('Test Case 3: GitHub link opens in new tab with correct attributes', async ({ page }) => {
    // Find the GitHub link
    const githubLink = page.locator('.nav-menu a[href*="github.com/yetone/mirdb"]');
    await expect(githubLink).toBeVisible();

    // Check target="_blank" attribute
    await expect(githubLink).toHaveAttribute('target', '_blank');

    // Check rel="noopener noreferrer" attribute
    const relAttr = await githubLink.getAttribute('rel');
    expect(relAttr).toContain('noopener');

    // Check for external link indicator
    const externalIcon = githubLink.locator('.external-icon');
    await expect(externalIcon).toBeVisible();
  });

  test('Test Case 4: Navigation collapses to hamburger menu on mobile viewport', async ({ page }) => {
    // Set viewport to mobile width and reload to ensure CSS media queries are applied
    await page.setViewportSize(VIEWPORTS.mobile);
    await page.reload();
    await waitForPageLoad(page);

    // Check that hamburger button is visible
    const hamburger = page.locator('.nav-toggle');
    await expect(hamburger).toBeVisible();

    // Check that the hamburger toggle has display: flex (indicating it's shown on mobile)
    const toggleDisplay = await hamburger.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(toggleDisplay).toBe('flex');

    // Check that nav menu has hidden visibility
    const navMenu = page.locator('.nav-menu');
    const menuStyles = await navMenu.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        visibility: styles.visibility,
        opacity: styles.opacity,
        transform: styles.transform
      };
    });
    // Nav menu should be hidden (visibility: hidden OR opacity: 0)
    expect(menuStyles.visibility === 'hidden' || menuStyles.opacity === '0').toBe(true);
  });

  test('Test Case 5: Mobile hamburger menu expands when clicked', async ({ page }) => {
    // Set viewport to mobile width
    await page.setViewportSize(VIEWPORTS.mobile);
    await page.waitForTimeout(100);

    // Get hamburger button and click it
    const hamburger = page.locator('.nav-toggle');
    await expect(hamburger).toBeVisible();
    await hamburger.click();

    // Wait for animation
    await page.waitForTimeout(300);

    // Check that aria-expanded is true
    await expect(hamburger).toHaveAttribute('aria-expanded', 'true');

    // Check that nav menu is now visible
    const navMenu = page.locator('.nav-menu');
    const isVisible = await navMenu.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return styles.visibility === 'visible' && styles.opacity === '1';
    });
    expect(isVisible).toBe(true);

    // Verify all navigation links are visible
    const featuresLink = page.locator('.nav-menu a[href="#features"]');
    await expect(featuresLink).toBeVisible();
  });

  test('Test Case 6: Keyboard navigation through all links with visible focus indicators', async ({ page }) => {
    // Start by focusing the skip link
    await page.keyboard.press('Tab');

    // Tab through navigation links and check focus
    const navLinks = page.locator('.nav-menu a');
    const linkCount = await navLinks.count();

    // Tab to nav brand first
    await page.keyboard.press('Tab');

    // Then tab through each nav menu link
    for (let i = 0; i < linkCount; i++) {
      await page.keyboard.press('Tab');

      // Get the currently focused element
      const focusedElement = page.locator(':focus');

      // Check that the focused element has a visible focus indicator
      const outlineStyle = await focusedElement.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          outlineWidth: styles.outlineWidth,
          outlineStyle: styles.outlineStyle,
          outlineColor: styles.outlineColor,
        };
      });

      // Focus indicator should not be 'none' or '0px'
      expect(outlineStyle.outlineStyle).not.toBe('none');
    }
  });

  test('Quick Start link scrolls to quickstart section', async ({ page }) => {
    // Click the Quick Start link
    const quickstartLink = page.locator('.nav-menu a[href="#quickstart"]');
    await quickstartLink.click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Check that the quickstart section is in view
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport();
  });

  test('Status link scrolls to status section', async ({ page }) => {
    // Click the Status link
    const statusLink = page.locator('.nav-menu a[href="#status"]');
    await statusLink.click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Check that the status section is in view
    const statusSection = page.locator('#status');
    await expect(statusSection).toBeInViewport();
  });

  test('Documentation link has correct external link attributes', async ({ page }) => {
    // Find the Docs link
    const docsLink = page.locator('.nav-menu a[href*="memcached"]');
    await expect(docsLink).toBeVisible();

    // Check target="_blank" attribute
    await expect(docsLink).toHaveAttribute('target', '_blank');

    // Check rel="noopener noreferrer" attribute
    const relAttr = await docsLink.getAttribute('rel');
    expect(relAttr).toContain('noopener');
  });

  test('Mobile menu closes when escape key is pressed', async ({ page }) => {
    // Set viewport to mobile width
    await page.setViewportSize(VIEWPORTS.mobile);
    await page.waitForTimeout(100);

    // Open the menu
    const hamburger = page.locator('.nav-toggle');
    await hamburger.click();
    await page.waitForTimeout(300);

    // Verify menu is open
    await expect(hamburger).toHaveAttribute('aria-expanded', 'true');

    // Press Escape
    await page.keyboard.press('Escape');
    await page.waitForTimeout(300);

    // Verify menu is closed
    await expect(hamburger).toHaveAttribute('aria-expanded', 'false');
  });

  test('Mobile menu closes when a link is clicked', async ({ page }) => {
    // Set viewport to mobile width
    await page.setViewportSize(VIEWPORTS.mobile);
    await page.waitForTimeout(100);

    // Open the menu
    const hamburger = page.locator('.nav-toggle');
    await hamburger.click();
    await page.waitForTimeout(300);

    // Click a link
    const featuresLink = page.locator('.nav-menu a[href="#features"]');
    await featuresLink.click();
    await page.waitForTimeout(300);

    // Verify menu is closed
    await expect(hamburger).toHaveAttribute('aria-expanded', 'false');
  });

  test('Nav brand link exists and links to hero', async ({ page }) => {
    const brandLink = page.locator('.nav-brand a');
    await expect(brandLink).toBeVisible();
    await expect(brandLink).toHaveAttribute('href', '#hero');
    await expect(brandLink).toHaveText('MirDB');
  });

  test('Header has fixed position', async ({ page }) => {
    const header = page.locator('header.header');
    const position = await header.evaluate((el) => {
      return window.getComputedStyle(el).position;
    });
    expect(position).toBe('fixed');
  });
});

/**
 * External Link Validation Tests
 * Owner: Scenario 14 - External Link Validation
 *
 * Test cases:
 * - GitHub repository link href is valid
 * - CircleCI badge links to correct project
 * - All external links have target=_blank
 * - All external links with target=_blank have rel=noopener
 * - Memcached protocol documentation link is valid
 */
/**
 * Smooth Scroll Behavior Tests
 * Owner: Scenario 16 - Smooth Scroll Behavior
 *
 * Test cases:
 * - Navigation links trigger smooth scroll to sections
 * - CTA button triggers smooth scroll to quick start
 * - Target sections are visible after scroll completes
 * - prefers-reduced-motion disables smooth scroll
 */
test.describe('Smooth Scroll Behavior', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForPageLoad(page);
  });

  test('Test Case 1: CSS has scroll-behavior: smooth on html element', async ({ page }) => {
    // Check that the html element has scroll-behavior: smooth
    const scrollBehavior = await page.evaluate(() => {
      return window.getComputedStyle(document.documentElement).scrollBehavior;
    });
    expect(scrollBehavior).toBe('smooth');
  });

  test('Test Case 2: Click navigation link to features section triggers smooth scroll', async ({ page }) => {
    // Scroll to top first to ensure we can measure scroll
    await page.evaluate(() => window.scrollTo(0, 0));
    await page.waitForTimeout(100);

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);
    expect(initialScrollY).toBe(0);

    // Click the Features link
    const featuresLink = page.locator('.nav-menu a[href="#features"]');
    await featuresLink.click();

    // Wait a brief moment for scroll to start
    await page.waitForTimeout(50);

    // Check that scroll is in progress (intermediate position)
    const midScrollY = await page.evaluate(() => window.scrollY);

    // Wait for scroll to complete
    await page.waitForTimeout(500);

    // Get final scroll position
    const finalScrollY = await page.evaluate(() => window.scrollY);

    // Verify page has scrolled
    expect(finalScrollY).toBeGreaterThan(initialScrollY);

    // Verify features section is now in view
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();

    // Check that the section title is visible at or near the top
    const featuresTitleBoundingBox = await page.locator('#features-title').boundingBox();
    expect(featuresTitleBoundingBox).not.toBeNull();
    // Title should be in the upper portion of the viewport
    expect(featuresTitleBoundingBox.y).toBeLessThan(300);
  });

  test('Test Case 3: Click CTA button triggers smooth scroll to quick start section', async ({ page }) => {
    // Scroll to top first
    await page.evaluate(() => window.scrollTo(0, 0));
    await page.waitForTimeout(100);

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click the CTA button (Get Started)
    const ctaButton = page.locator('.hero__cta[href="#quickstart"]');
    await ctaButton.click();

    // Wait for scroll animation to complete
    await page.waitForTimeout(500);

    // Get final scroll position
    const finalScrollY = await page.evaluate(() => window.scrollY);

    // Verify page has scrolled
    expect(finalScrollY).toBeGreaterThan(initialScrollY);

    // Verify quickstart section is now in view
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport();

    // Check that the section title is visible
    const quickstartTitle = page.locator('#quickstart-title');
    await expect(quickstartTitle).toBeVisible();
  });

  test('Test Case 4: Target section header is visible at or near top after scroll', async ({ page }) => {
    // Click the Status link
    const statusLink = page.locator('.nav-menu a[href="#status"]');
    await statusLink.click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Get the status section title position
    const statusTitleBoundingBox = await page.locator('#status-title').boundingBox();

    // Verify the title exists
    expect(statusTitleBoundingBox).not.toBeNull();

    // The title should be visible in the viewport
    // Taking into account the fixed header and potential padding
    // The title should be within the upper 60% of the viewport
    const viewportHeight = await page.evaluate(() => window.innerHeight);
    expect(statusTitleBoundingBox.y).toBeLessThan(viewportHeight * 0.6);
  });

  test('Test Case 5: Smooth scroll works for all in-page navigation links', async ({ page }) => {
    const sections = ['#features', '#quickstart', '#status'];

    for (const sectionId of sections) {
      // Scroll to top
      await page.evaluate(() => window.scrollTo(0, 0));
      await page.waitForTimeout(100);

      // Click the link
      const link = page.locator(`.nav-menu a[href="${sectionId}"]`);
      await link.click();

      // Wait for scroll
      await page.waitForTimeout(500);

      // Verify section is in view
      const section = page.locator(sectionId);
      await expect(section).toBeInViewport();
    }
  });
});

/**
 * Reduced Motion Preference Tests
 * Owner: Scenario 16 - Smooth Scroll Behavior
 */
test.describe('Reduced Motion Preference', () => {
  test('Test Case: prefers-reduced-motion disables smooth scroll', async ({ page }) => {
    // Emulate prefers-reduced-motion
    await page.emulateMedia({ reducedMotion: 'reduce' });
    await page.goto('/');
    await waitForPageLoad(page);

    // Check that scroll-behavior is auto when reduced motion is preferred
    const scrollBehavior = await page.evaluate(() => {
      return window.getComputedStyle(document.documentElement).scrollBehavior;
    });
    expect(scrollBehavior).toBe('auto');
  });

  test('Test Case: scroll-behavior remains smooth without reduced motion preference', async ({ page }) => {
    // Emulate no preference for reduced motion
    await page.emulateMedia({ reducedMotion: 'no-preference' });
    await page.goto('/');
    await waitForPageLoad(page);

    // Check that scroll-behavior is smooth
    const scrollBehavior = await page.evaluate(() => {
      return window.getComputedStyle(document.documentElement).scrollBehavior;
    });
    expect(scrollBehavior).toBe('smooth');
  });
});

test.describe('External Link Validation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForPageLoad(page);
  });

  test('Test Case 1: GitHub repository link points to valid GitHub URL', async ({ page }) => {
    // Find the GitHub link in navigation
    const githubLink = page.locator('a[href*="github.com/yetone/mirdb"]').first();
    await expect(githubLink).toBeVisible();

    // Verify the href points to the correct GitHub repository
    const href = await githubLink.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');

    // Verify it's a valid GitHub URL format
    expect(href).toMatch(/^https:\/\/github\.com\/[\w-]+\/[\w-]+$/);
  });

  test('Test Case 2: CircleCI badge links to correct project URL', async ({ page }) => {
    // Find the CircleCI badge link in status section
    const circleciLink = page.locator('a[href*="circleci.com/gh/yetone/mirdb"]');
    await expect(circleciLink).toBeVisible();

    // Verify the href points to the correct CircleCI project
    const href = await circleciLink.getAttribute('href');
    expect(href).toBe('https://circleci.com/gh/yetone/mirdb');

    // Verify the badge image is present within the link
    const badgeImage = circleciLink.locator('img');
    await expect(badgeImage).toBeVisible();
    await expect(badgeImage).toHaveAttribute('alt', 'CircleCI build status badge');
  });

  test('Test Case 3: All external links have target=_blank attribute', async ({ page }) => {
    // Get all external links (links that start with http:// or https://)
    const externalLinks = page.locator('a[href^="http://"], a[href^="https://"]');
    const count = await externalLinks.count();

    expect(count).toBeGreaterThan(0);

    // Verify each external link has target="_blank"
    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      const href = await link.getAttribute('href');
      const target = await link.getAttribute('target');
      expect(target, `External link ${href} should have target="_blank"`).toBe('_blank');
    }
  });

  test('Test Case 4: All external links with target=_blank have rel=noopener', async ({ page }) => {
    // Get all external links with target="_blank"
    const externalLinks = page.locator('a[target="_blank"]');
    const count = await externalLinks.count();

    expect(count).toBeGreaterThan(0);

    // Verify each link has rel containing "noopener"
    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      const href = await link.getAttribute('href');
      const rel = await link.getAttribute('rel');

      // rel should contain "noopener" to prevent tabnabbing security vulnerability
      expect(rel, `External link ${href} should have rel containing "noopener"`).toContain('noopener');
    }
  });

  test('Test Case 5: Memcached protocol documentation link points to valid URL', async ({ page }) => {
    // Find the documentation link (pointing to memcached docs)
    const docsLink = page.locator('a[href*="memcached"]').first();
    await expect(docsLink).toBeVisible();

    // Verify the href points to valid memcached documentation
    const href = await docsLink.getAttribute('href');
    expect(href).toMatch(/^https:\/\/github\.com\/memcached\/memcached/);

    // Verify it has proper external link attributes
    await expect(docsLink).toHaveAttribute('target', '_blank');
    const rel = await docsLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });
});
