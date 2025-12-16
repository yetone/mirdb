// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Interactive Elements and User Interactions
 * Scenario 20: Verify all interactive elements have appropriate hover states and feedback
 *
 * Test Cases:
 * TC1: CTA buttons show visual hover state
 * TC2: Navigation links show visual hover state
 * TC3: Mobile menu toggle (if present)
 * TC4: Collapsible sections (if present)
 */

test.describe('Interactive Elements and User Interactions (Scenario 20)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  /**
   * Test Case 1: CTA buttons show visual hover state
   * Input: Hover over CTA buttons
   * Expected: Buttons show visual hover state (color change, shadow, etc.)
   */
  test('TC1: CTA buttons show visual hover state (color change, shadow, transform)', async ({ page }) => {
    // Test Get Started button (primary CTA)
    const getStartedBtn = page.locator('[data-testid="get-started-btn"]');
    await expect(getStartedBtn).toBeVisible();

    // Get initial styles before hover
    const initialBgColor = await getStartedBtn.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    const initialTransform = await getStartedBtn.evaluate((el) => {
      return window.getComputedStyle(el).transform;
    });

    // Hover over the button
    await getStartedBtn.hover();

    // Wait for transition to complete
    await page.waitForTimeout(350);

    // Get styles after hover
    const hoverBgColor = await getStartedBtn.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    const hoverTransform = await getStartedBtn.evaluate((el) => {
      return window.getComputedStyle(el).transform;
    });

    // Verify hover state changes occurred
    // Primary button should change from white to transparent on hover
    expect(hoverBgColor).not.toBe(initialBgColor);
    // Button should have translateY transform on hover
    expect(hoverTransform).not.toBe(initialTransform);

    // Test GitHub button (secondary CTA)
    const githubBtn = page.locator('[data-testid="github-btn"]');
    await expect(githubBtn).toBeVisible();

    // Get initial styles for secondary button
    const initialGithubBg = await githubBtn.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Hover over the GitHub button
    await githubBtn.hover();
    await page.waitForTimeout(350);

    // Get hover styles
    const hoverGithubBg = await githubBtn.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Secondary button should change from transparent to white on hover
    expect(hoverGithubBg).not.toBe(initialGithubBg);
  });

  /**
   * Test Case 2: Navigation links show visual hover state
   * Input: Hover over navigation links
   * Expected: Links show visual hover state (underline, color change)
   */
  test('TC2: Navigation links show visual hover state (underline, color change, opacity)', async ({ page }) => {
    // Scroll to footer where navigation links are located
    await page.locator('[data-testid="footer-section"]').scrollIntoViewIfNeeded();

    // Test footer navigation links
    const footerNavLinks = page.locator('.footer-nav a');
    const linkCount = await footerNavLinks.count();
    expect(linkCount).toBeGreaterThan(0);

    // Test first footer navigation link
    const firstLink = footerNavLinks.first();
    await expect(firstLink).toBeVisible();

    // Get initial opacity
    const initialOpacity = await firstLink.evaluate((el) => {
      return window.getComputedStyle(el).opacity;
    });

    // Hover over the link
    await firstLink.hover();
    await page.waitForTimeout(350);

    // Get hover styles
    const hoverOpacity = await firstLink.evaluate((el) => {
      return window.getComputedStyle(el).opacity;
    });
    const hoverTextDecoration = await firstLink.evaluate((el) => {
      return window.getComputedStyle(el).textDecoration;
    });

    // Footer nav links should increase opacity on hover (0.8 -> 1)
    expect(parseFloat(hoverOpacity)).toBeGreaterThanOrEqual(parseFloat(initialOpacity));
    // Links should show underline on hover
    expect(hoverTextDecoration).toContain('underline');
  });

  /**
   * Test Case 2b: Documentation links show visual hover state
   */
  test('TC2b: Documentation buttons show visual hover state', async ({ page }) => {
    // Scroll to getting started section
    await page.locator('[data-testid="getting-started-section"]').scrollIntoViewIfNeeded();

    // Test primary docs button
    const docsBtn = page.locator('[data-testid="docs-link"]');
    await expect(docsBtn).toBeVisible();

    // Get initial background color
    const initialBg = await docsBtn.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Hover
    await docsBtn.hover();
    await page.waitForTimeout(350);

    // Get hover background - should darken
    const hoverBg = await docsBtn.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Background should change on hover
    expect(hoverBg).not.toBe(initialBg);

    // Test secondary docs button (wiki link)
    const wikiBtn = page.locator('[data-testid="wiki-link"]');
    await expect(wikiBtn).toBeVisible();

    // Get initial background (transparent)
    const initialWikiBg = await wikiBtn.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Hover
    await wikiBtn.hover();
    await page.waitForTimeout(350);

    // Get hover background - should fill with color
    const hoverWikiBg = await wikiBtn.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Background should change on hover (from transparent to filled)
    expect(hoverWikiBg).not.toBe(initialWikiBg);
  });

  /**
   * Test Case 2c: Feature cards show visual hover state
   */
  test('TC2c: Feature cards show visual hover state (transform, shadow)', async ({ page }) => {
    // Scroll to features section
    await page.locator('[data-testid="features-section"]').scrollIntoViewIfNeeded();

    // Get first feature card
    const featureCard = page.locator('.feature-card').first();
    await expect(featureCard).toBeVisible();

    // Get initial styles
    const initialTransform = await featureCard.evaluate((el) => {
      return window.getComputedStyle(el).transform;
    });
    const initialBoxShadow = await featureCard.evaluate((el) => {
      return window.getComputedStyle(el).boxShadow;
    });

    // Hover over the card
    await featureCard.hover();
    await page.waitForTimeout(350);

    // Get hover styles
    const hoverTransform = await featureCard.evaluate((el) => {
      return window.getComputedStyle(el).transform;
    });
    const hoverBoxShadow = await featureCard.evaluate((el) => {
      return window.getComputedStyle(el).boxShadow;
    });

    // Card should lift up (translateY transform) on hover
    expect(hoverTransform).not.toBe(initialTransform);
    // Box shadow should change (become more prominent)
    expect(hoverBoxShadow).not.toBe(initialBoxShadow);
  });

  /**
   * Test Case 3: Mobile menu toggle (if present)
   * Input: Test mobile menu toggle
   * Expected: Mobile menu opens and closes smoothly on toggle
   *
   * Note: This site does not have a mobile hamburger menu - it's a simple single-page
   * static site. The test checks for existence and gracefully handles the case where
   * no mobile menu exists.
   */
  test('TC3: Mobile menu toggle (if present) opens and closes smoothly', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });

    // Check for mobile navigation elements
    const mobileMenuToggle = page.locator('[data-testid="mobile-menu-toggle"], .hamburger-menu, .mobile-nav-toggle, [aria-label="Toggle navigation"]');
    const hasMobileMenu = await mobileMenuToggle.count() > 0;

    if (hasMobileMenu) {
      // Mobile menu exists - test its functionality
      await expect(mobileMenuToggle).toBeVisible();

      // Click to open
      await mobileMenuToggle.click();
      await page.waitForTimeout(300);

      // Check if mobile menu/nav is now visible
      const mobileNav = page.locator('.mobile-nav, [data-testid="mobile-nav"], .nav-menu.open');
      if (await mobileNav.count() > 0) {
        await expect(mobileNav).toBeVisible();

        // Click to close
        await mobileMenuToggle.click();
        await page.waitForTimeout(300);

        // Menu should be hidden or have different state
      }
    } else {
      // No mobile menu - verify footer navigation is accessible as fallback
      const footerNav = page.locator('[data-testid="footer-nav"]');
      await footerNav.scrollIntoViewIfNeeded();
      await expect(footerNav).toBeVisible();

      // Verify all footer links are accessible on mobile
      const footerLinks = footerNav.locator('a');
      const linkCount = await footerLinks.count();
      expect(linkCount).toBeGreaterThan(0);

      // Test passes - no mobile menu present, but navigation is accessible
      // This is acceptable for a simple single-page site
    }
  });

  /**
   * Test Case 4: Collapsible sections (if present)
   * Input: Test collapsible sections
   * Expected: Sections expand and collapse with smooth animation
   *
   * Note: This site does not have collapsible/accordion sections in the current
   * implementation. The test checks for their existence and gracefully handles
   * the case where none exist.
   */
  test('TC4: Collapsible sections (if present) expand and collapse smoothly', async ({ page }) => {
    // Check for collapsible elements using common patterns
    const collapsibleTriggers = page.locator(
      '[data-testid*="collapsible"], ' +
      '.accordion-toggle, ' +
      '.collapsible-toggle, ' +
      '[aria-expanded], ' +
      '.expand-toggle, ' +
      '[data-collapse]'
    );
    const hasCollapsible = await collapsibleTriggers.count() > 0;

    if (hasCollapsible) {
      // Collapsible sections exist - test their functionality
      const firstTrigger = collapsibleTriggers.first();
      await expect(firstTrigger).toBeVisible();

      // Get initial expanded state
      const initialExpanded = await firstTrigger.getAttribute('aria-expanded');

      // Click to toggle
      await firstTrigger.click();
      await page.waitForTimeout(500); // Allow for animation

      // Get new expanded state
      const newExpanded = await firstTrigger.getAttribute('aria-expanded');

      // State should have changed
      if (initialExpanded !== null) {
        expect(newExpanded).not.toBe(initialExpanded);
      }

      // Look for associated content panel
      const contentId = await firstTrigger.getAttribute('aria-controls');
      if (contentId) {
        const contentPanel = page.locator(`#${contentId}`);
        if (await contentPanel.count() > 0) {
          // Verify content visibility matches expanded state
          const isVisible = await contentPanel.isVisible();
          expect(isVisible).toBe(newExpanded === 'true');
        }
      }
    } else {
      // No collapsible sections - verify the content is properly displayed
      // This is acceptable as not all pages need collapsible sections

      // Check that main content sections are visible and accessible
      const mainSections = [
        '[data-testid="features-section"]',
        '[data-testid="how-it-works-section"]',
        '[data-testid="code-examples-section"]',
        '[data-testid="getting-started-section"]'
      ];

      for (const sectionSelector of mainSections) {
        const section = page.locator(sectionSelector);
        if (await section.count() > 0) {
          await section.scrollIntoViewIfNeeded();
          await expect(section).toBeVisible();
        }
      }

      // Test passes - no collapsible sections, but content is accessible
      // This is acceptable for this page design
    }
  });

  /**
   * Additional Test: Copy button hover states
   */
  test('TC5: Copy buttons show visual hover state', async ({ page }) => {
    // Scroll to code examples section
    await page.locator('[data-testid="code-examples-section"]').scrollIntoViewIfNeeded();

    // Get first copy button
    const copyBtn = page.locator('.copy-btn').first();
    await expect(copyBtn).toBeVisible();

    // Get initial background
    const initialBg = await copyBtn.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    const initialColor = await copyBtn.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    // Hover
    await copyBtn.hover();
    await page.waitForTimeout(350);

    // Get hover styles
    const hoverBg = await copyBtn.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    const hoverColor = await copyBtn.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    // Background and/or color should change on hover
    const styleChanged = hoverBg !== initialBg || hoverColor !== initialColor;
    expect(styleChanged).toBe(true);
  });

  /**
   * Additional Test: Footer badge link hover states
   */
  test('TC6: Footer badge links show visual hover state', async ({ page }) => {
    // Scroll to footer
    await page.locator('[data-testid="footer-section"]').scrollIntoViewIfNeeded();

    // Get status badge link
    const badgeLink = page.locator('[data-testid="project-status-badge"]');
    await expect(badgeLink).toBeVisible();

    // Get initial transform/opacity
    const initialTransform = await badgeLink.evaluate((el) => {
      return window.getComputedStyle(el).transform;
    });

    // Hover
    await badgeLink.hover();
    await page.waitForTimeout(350);

    // Get hover transform - should have translateY
    const hoverTransform = await badgeLink.evaluate((el) => {
      return window.getComputedStyle(el).transform;
    });

    // Transform should change on hover
    expect(hoverTransform).not.toBe(initialTransform);
  });

  /**
   * Additional Test: Command card hover states
   */
  test('TC7: Command cards show visual hover state', async ({ page }) => {
    // Scroll to code examples section
    await page.locator('[data-testid="code-examples-section"]').scrollIntoViewIfNeeded();

    // Get first command card
    const commandCard = page.locator('.command-card').first();
    await expect(commandCard).toBeVisible();

    // Get initial transform and box-shadow
    const initialTransform = await commandCard.evaluate((el) => {
      return window.getComputedStyle(el).transform;
    });
    const initialShadow = await commandCard.evaluate((el) => {
      return window.getComputedStyle(el).boxShadow;
    });

    // Hover
    await commandCard.hover();
    await page.waitForTimeout(350);

    // Get hover styles
    const hoverTransform = await commandCard.evaluate((el) => {
      return window.getComputedStyle(el).transform;
    });
    const hoverShadow = await commandCard.evaluate((el) => {
      return window.getComputedStyle(el).boxShadow;
    });

    // Transform and shadow should change on hover
    expect(hoverTransform).not.toBe(initialTransform);
    expect(hoverShadow).not.toBe(initialShadow);
  });

  /**
   * Additional Test: Data path cards hover states
   */
  test('TC8: Data path cards show visual hover state', async ({ page }) => {
    // Scroll to how it works section
    await page.locator('[data-testid="how-it-works-section"]').scrollIntoViewIfNeeded();

    // Get write path card
    const dataPathCard = page.locator('[data-testid="write-path-card"]');
    await expect(dataPathCard).toBeVisible();

    // Get initial transform and shadow
    const initialTransform = await dataPathCard.evaluate((el) => {
      return window.getComputedStyle(el).transform;
    });
    const initialShadow = await dataPathCard.evaluate((el) => {
      return window.getComputedStyle(el).boxShadow;
    });

    // Hover
    await dataPathCard.hover();
    await page.waitForTimeout(350);

    // Get hover styles
    const hoverTransform = await dataPathCard.evaluate((el) => {
      return window.getComputedStyle(el).transform;
    });
    const hoverShadow = await dataPathCard.evaluate((el) => {
      return window.getComputedStyle(el).boxShadow;
    });

    // Transform and shadow should change on hover (card lifts up)
    expect(hoverTransform).not.toBe(initialTransform);
    expect(hoverShadow).not.toBe(initialShadow);
  });
});
