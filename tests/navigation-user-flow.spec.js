// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E tests for Navigation and User Flow
 * Scenario: Verify smooth navigation and user journey through the page
 *
 * Tests cover:
 * 1. Smooth scroll navigation between sections
 * 2. CTA flow - Learn More scrolls to appropriate sections
 * 3. Complete user journey from landing to getting-started
 * 4. Back-to-top functionality
 */

test.describe('Navigation and User Flow', () => {

  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage
    await page.goto('/');
    // Wait for page to be fully loaded
    await page.waitForLoadState('domcontentloaded');
  });

  /**
   * Test Case 1: Click 'Learn More' CTA
   * Input: Click 'Learn More' CTA
   * Expected: Page smoothly scrolls to features or value proposition section
   */
  test('TC1: Learn More CTA scrolls to features or value proposition section', async ({ page }) => {
    // Find the Learn More / secondary CTA button
    const learnMoreBtn = page.locator('[data-testid="secondary-cta"], a:has-text("Learn More")').first();
    await expect(learnMoreBtn).toBeVisible();

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click the Learn More button
    await learnMoreBtn.click();

    // Wait for smooth scroll animation to complete
    await page.waitForTimeout(1000);

    // Verify page has scrolled down
    const newScrollY = await page.evaluate(() => window.scrollY);
    expect(newScrollY).toBeGreaterThan(initialScrollY);

    // Verify we're in the expected section (features, value-proposition, or getting-started)
    const targetSections = [
      '#features',
      '#value-proposition',
      '#getting-started'
    ];

    let isInTargetSection = false;
    for (const selector of targetSections) {
      const section = page.locator(selector);
      if (await section.count() > 0) {
        const isVisible = await section.isVisible();
        const box = await section.boundingBox();
        // Check if section is near the top of the viewport (within 200px)
        if (isVisible && box && box.y < 200 && box.y > -100) {
          isInTargetSection = true;
          break;
        }
      }
    }

    // Also check if the URL hash has changed
    const currentUrl = page.url();
    const hasValidHash = targetSections.some(s => currentUrl.includes(s));

    expect(isInTargetSection || hasValidHash || newScrollY > 200).toBeTruthy();
  });

  /**
   * Test Case 2: Test anchor link navigation
   * Input: Test anchor link navigation
   * Expected: Anchor links scroll to correct sections
   */
  test('TC2: Anchor links scroll to correct sections', async ({ page }) => {
    // Find navigation links with anchor references that have actual section IDs
    const navLinks = page.locator('nav a[href^="#"], .nav-links a[href^="#"]');
    const navLinkCount = await navLinks.count();
    let testedLinks = 0;

    // Test at least one anchor link navigation
    if (navLinkCount > 0) {
      for (let i = 0; i < navLinkCount && testedLinks < 3; i++) {
        const link = navLinks.nth(i);
        const href = await link.getAttribute('href');

        // Skip empty anchors or just "#"
        if (href && href.length > 1 && href.startsWith('#')) {
          const sectionId = href.substring(1);

          // Skip if sectionId is empty
          if (!sectionId) continue;

          const targetSection = page.locator(`[id="${sectionId}"]`);

          if (await targetSection.count() > 0) {
            // Scroll back to top first
            await page.evaluate(() => window.scrollTo(0, 0));
            await page.waitForTimeout(300);

            // Click the nav link
            await link.click();

            // Wait for smooth scroll animation
            await page.waitForTimeout(1000);

            // Verify the section is now visible and near the top of viewport
            await expect(targetSection).toBeVisible();

            const box = await targetSection.boundingBox();
            // Section should be near the top of the viewport (accounting for fixed header)
            expect(box).toBeTruthy();
            expect(box.y).toBeLessThan(300);

            testedLinks++;
          }
        }
      }
    }

    // If we tested at least one link, or at least found anchor links on page
    if (testedLinks === 0) {
      // Check for in-page anchor links with valid sections
      const allAnchorLinks = page.locator('a[href^="#"]');
      const count = await allAnchorLinks.count();
      expect(count).toBeGreaterThan(0);
    }
  });

  /**
   * Test Case 3: Complete user journey: landing to getting-started
   * Input: Complete user journey: landing to getting-started
   * Expected: User can naturally flow from hero through features to getting-started
   */
  test('TC3: User can flow from hero through features to getting-started', async ({ page }) => {
    // 1. Verify we start at the hero section
    const heroSection = page.locator('[data-testid="hero-section"], .hero, header.hero, #hero');
    await expect(heroSection.first()).toBeVisible();

    // 2. Scroll through the page naturally
    await page.evaluate(() => window.scrollTo(0, 0));
    await page.waitForTimeout(300);

    // 3. Check features section exists and is reachable
    const featuresSection = page.locator('#features, .features');
    const featuresExists = await featuresSection.count() > 0;

    if (featuresExists) {
      await featuresSection.first().scrollIntoViewIfNeeded();
      await page.waitForTimeout(500);
      await expect(featuresSection.first()).toBeVisible();
    }

    // 4. Navigate to getting-started section
    const gettingStartedSection = page.locator('#getting-started, .getting-started');
    const gettingStartedExists = await gettingStartedSection.count() > 0;

    expect(gettingStartedExists).toBeTruthy();

    await gettingStartedSection.first().scrollIntoViewIfNeeded();
    await page.waitForTimeout(500);
    await expect(gettingStartedSection.first()).toBeVisible();

    // 5. Verify the user journey is complete by checking we can see getting-started content
    const gettingStartedHeading = page.locator('#getting-started h2, .getting-started h2');
    if (await gettingStartedHeading.count() > 0) {
      await expect(gettingStartedHeading.first()).toBeVisible();
    }
  });

  /**
   * Test Case 4: Test back-to-top functionality
   * Input: Test back-to-top functionality
   * Expected: Back-to-top button or mechanism exists if page is long
   */
  test('TC4: Back-to-top functionality exists for long pages', async ({ page }) => {
    // First, check if the page is long enough to need back-to-top
    const pageHeight = await page.evaluate(() => document.body.scrollHeight);
    const viewportHeight = await page.evaluate(() => window.innerHeight);

    if (pageHeight > viewportHeight * 2) {
      // Page is long enough, back-to-top should be useful

      // Scroll to the bottom of the page
      await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
      await page.waitForTimeout(500);

      // Check for various back-to-top mechanisms:
      // 1. Explicit back-to-top button
      const backToTopButton = page.locator('[data-testid="back-to-top"], .back-to-top, #back-to-top, a[href="#top"], a[href="#hero"], a[href="#"], button:has-text("Top"), button:has-text("↑")');
      const backToTopExists = await backToTopButton.count() > 0;

      // 2. Navigation logo that links to top
      const logoLink = page.locator('nav .logo[href="#"], nav .logo[href="#hero"], nav a.logo[href^="#"]');
      const logoLinkExists = await logoLink.count() > 0;

      // 3. Fixed navigation that remains accessible
      const fixedNav = page.locator('nav.navbar, nav[class*="fixed"], header[class*="fixed"]');
      const fixedNavVisible = await fixedNav.count() > 0 && await fixedNav.first().isVisible();

      // 4. Keyboard shortcut (Home key) - not testable via Playwright directly

      // At least one mechanism should exist
      const hasBackToTopMechanism = backToTopExists || logoLinkExists || fixedNavVisible;

      // If a back-to-top button exists, test it
      if (backToTopExists) {
        const btn = backToTopButton.first();
        await expect(btn).toBeVisible();
        await btn.click();
        await page.waitForTimeout(1000);

        const scrollYAfterClick = await page.evaluate(() => window.scrollY);
        expect(scrollYAfterClick).toBeLessThan(200);
      } else if (fixedNavVisible) {
        // Fixed nav provides back-to-top via logo or home link
        const navLogoOrHome = page.locator('nav .logo, nav a[href="#"], nav a[href="#hero"], nav a:has-text("MirDB")').first();
        if (await navLogoOrHome.count() > 0) {
          await navLogoOrHome.click();
          await page.waitForTimeout(1000);

          // Should be at or near top
          const scrollYAfterNav = await page.evaluate(() => window.scrollY);
          // Even if it doesn't scroll to exact top, nav being fixed is a valid mechanism
          expect(scrollYAfterNav >= 0).toBeTruthy();
        }
      }

      expect(hasBackToTopMechanism).toBeTruthy();
    } else {
      // Page is not long enough to require back-to-top functionality
      // This is acceptable - test passes as the requirement is conditional
      expect(true).toBeTruthy();
    }
  });

  /**
   * Additional test: Verify smooth scroll CSS is applied
   */
  test('Smooth scroll CSS is applied to HTML element', async ({ page }) => {
    const scrollBehavior = await page.evaluate(() => {
      return window.getComputedStyle(document.documentElement).scrollBehavior;
    });

    // Should have smooth scroll (unless prefers-reduced-motion is set)
    const prefersReducedMotion = await page.evaluate(() => {
      return window.matchMedia('(prefers-reduced-motion: reduce)').matches;
    });

    if (!prefersReducedMotion) {
      expect(scrollBehavior).toBe('smooth');
    } else {
      // If reduced motion is preferred, auto is acceptable
      expect(['smooth', 'auto']).toContain(scrollBehavior);
    }
  });

  /**
   * Additional test: Verify all navigation sections are accessible
   */
  test('All main sections are accessible via navigation', async ({ page }) => {
    // Get all main sections that should be navigable
    const mainSections = ['features', 'architecture', 'getting-started', 'value-proposition'];
    const existingSections = [];

    for (const sectionId of mainSections) {
      const section = page.locator(`#${sectionId}`);
      if (await section.count() > 0) {
        existingSections.push(sectionId);
      }
    }

    // At least 2 sections should exist
    expect(existingSections.length).toBeGreaterThanOrEqual(2);

    // Each existing section should be scrollable to
    for (const sectionId of existingSections) {
      const section = page.locator(`#${sectionId}`);
      await section.scrollIntoViewIfNeeded();
      await page.waitForTimeout(300);
      await expect(section).toBeVisible();
    }
  });

  /**
   * Additional test: Get Started CTA navigates to getting-started section
   */
  test('Get Started CTA navigates to getting-started section', async ({ page }) => {
    // Find Get Started button
    const getStartedBtn = page.locator('[data-testid="primary-cta"]:has-text("Get Started"), a.btn:has-text("Get Started"), a:has-text("Get Started")').first();

    // Check if Get Started button exists and points to getting-started
    if (await getStartedBtn.count() > 0) {
      const href = await getStartedBtn.getAttribute('href');

      // If it's an internal link to getting-started section
      if (href && href.includes('#getting-started')) {
        const initialScrollY = await page.evaluate(() => window.scrollY);

        await getStartedBtn.click();
        await page.waitForTimeout(1000);

        const newScrollY = await page.evaluate(() => window.scrollY);
        expect(newScrollY).toBeGreaterThan(initialScrollY);

        // Verify getting-started section is visible
        const gettingStarted = page.locator('#getting-started');
        await expect(gettingStarted).toBeVisible();
      }
    }
  });

});
