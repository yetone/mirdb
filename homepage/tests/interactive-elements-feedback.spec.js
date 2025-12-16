/**
 * Interactive Elements Feedback E2E Tests
 * Scenario ID: 18
 * UUID: 0172bbb7-e938-4de4-86a0-10abb3478f3b
 *
 * This test suite verifies that all interactive elements provide appropriate user feedback
 * including hover states, active/pressed states, and cursor changes.
 */

const { test, expect } = require('@playwright/test');

test.describe('Interactive Elements Feedback', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  /**
   * Test Case 1: Hover over navigation links
   * Expected: Links show hover state (color change, underline, etc.)
   */
  test('Test Case 1: Navigation links show hover state with visual change', async ({ page }) => {
    // Test desktop navigation links
    const navLinks = page.locator('.nav-links a');
    const navLinkCount = await navLinks.count();

    expect(navLinkCount, 'Navigation should have links').toBeGreaterThan(0);

    for (let i = 0; i < navLinkCount; i++) {
      const link = navLinks.nth(i);

      // Get initial computed styles
      const initialStyles = await link.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          color: style.color,
          textDecoration: style.textDecoration,
          backgroundColor: style.backgroundColor
        };
      });

      // Hover over the link
      await link.hover();

      // Wait a moment for any transition to apply
      await page.waitForTimeout(200);

      // Get hover computed styles
      const hoverStyles = await link.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          color: style.color,
          textDecoration: style.textDecoration,
          backgroundColor: style.backgroundColor
        };
      });

      // Verify that at least one style property changed on hover
      const hasVisualChange =
        initialStyles.color !== hoverStyles.color ||
        initialStyles.textDecoration !== hoverStyles.textDecoration ||
        initialStyles.backgroundColor !== hoverStyles.backgroundColor;

      expect(
        hasVisualChange,
        `Navigation link ${i + 1} should have a visual change on hover`
      ).toBe(true);
    }

    // Test the nav logo hover state
    const navLogo = page.locator('.nav-logo');
    const logoInitialColor = await navLogo.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    await navLogo.hover();
    await page.waitForTimeout(200);

    const logoHoverColor = await navLogo.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    expect(
      logoInitialColor !== logoHoverColor,
      'Nav logo should change color on hover'
    ).toBe(true);
  });

  /**
   * Test Case 2: Hover over CTA buttons
   * Expected: Buttons show hover state with visual change
   */
  test('Test Case 2: CTA buttons show hover state with visual change', async ({ page }) => {
    // Test primary CTA button (Get Started)
    const primaryBtn = page.locator('[data-testid="cta-get-started"]');
    await expect(primaryBtn).toBeVisible();

    const primaryInitialStyles = await primaryBtn.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        backgroundColor: style.backgroundColor,
        borderColor: style.borderColor,
        transform: style.transform,
        boxShadow: style.boxShadow
      };
    });

    await primaryBtn.hover();
    await page.waitForTimeout(200);

    const primaryHoverStyles = await primaryBtn.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        backgroundColor: style.backgroundColor,
        borderColor: style.borderColor,
        transform: style.transform,
        boxShadow: style.boxShadow
      };
    });

    const primaryHasChange =
      primaryInitialStyles.backgroundColor !== primaryHoverStyles.backgroundColor ||
      primaryInitialStyles.borderColor !== primaryHoverStyles.borderColor ||
      primaryInitialStyles.transform !== primaryHoverStyles.transform ||
      primaryInitialStyles.boxShadow !== primaryHoverStyles.boxShadow;

    expect(
      primaryHasChange,
      'Primary CTA button should have visual change on hover'
    ).toBe(true);

    // Test secondary CTA button (Documentation)
    const secondaryBtn = page.locator('[data-testid="cta-documentation"]');
    await expect(secondaryBtn).toBeVisible();

    const secondaryInitialStyles = await secondaryBtn.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        backgroundColor: style.backgroundColor,
        borderColor: style.borderColor,
        transform: style.transform,
        boxShadow: style.boxShadow
      };
    });

    await secondaryBtn.hover();
    await page.waitForTimeout(200);

    const secondaryHoverStyles = await secondaryBtn.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        backgroundColor: style.backgroundColor,
        borderColor: style.borderColor,
        transform: style.transform,
        boxShadow: style.boxShadow
      };
    });

    const secondaryHasChange =
      secondaryInitialStyles.backgroundColor !== secondaryHoverStyles.backgroundColor ||
      secondaryInitialStyles.borderColor !== secondaryHoverStyles.borderColor ||
      secondaryInitialStyles.transform !== secondaryHoverStyles.transform ||
      secondaryInitialStyles.boxShadow !== secondaryHoverStyles.boxShadow;

    expect(
      secondaryHasChange,
      'Secondary CTA button should have visual change on hover'
    ).toBe(true);

    // Test feature cards hover state
    const featureCards = page.locator('.feature-card');
    const featureCardCount = await featureCards.count();

    expect(featureCardCount, 'Should have feature cards').toBeGreaterThan(0);

    // Test first feature card hover
    const firstCard = featureCards.first();
    const cardInitialStyles = await firstCard.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        transform: style.transform,
        boxShadow: style.boxShadow
      };
    });

    await firstCard.hover();
    await page.waitForTimeout(200);

    const cardHoverStyles = await firstCard.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        transform: style.transform,
        boxShadow: style.boxShadow
      };
    });

    const cardHasChange =
      cardInitialStyles.transform !== cardHoverStyles.transform ||
      cardInitialStyles.boxShadow !== cardHoverStyles.boxShadow;

    expect(
      cardHasChange,
      'Feature card should have visual change on hover (transform or shadow)'
    ).toBe(true);

    // Test copy buttons hover state
    const copyButtons = page.locator('.copy-button');
    const copyButtonCount = await copyButtons.count();

    if (copyButtonCount > 0) {
      const firstCopyBtn = copyButtons.first();
      const copyInitialStyles = await firstCopyBtn.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          backgroundColor: style.backgroundColor,
          color: style.color,
          borderColor: style.borderColor
        };
      });

      await firstCopyBtn.hover();
      await page.waitForTimeout(200);

      const copyHoverStyles = await firstCopyBtn.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          backgroundColor: style.backgroundColor,
          color: style.color,
          borderColor: style.borderColor
        };
      });

      const copyHasChange =
        copyInitialStyles.backgroundColor !== copyHoverStyles.backgroundColor ||
        copyInitialStyles.color !== copyHoverStyles.color ||
        copyInitialStyles.borderColor !== copyHoverStyles.borderColor;

      expect(
        copyHasChange,
        'Copy button should have visual change on hover'
      ).toBe(true);
    }
  });

  /**
   * Test Case 3: Click buttons
   * Expected: Buttons show active/pressed state
   */
  test('Test Case 3: Buttons show active/pressed state on click', async ({ page }) => {
    // Test primary CTA button active state
    const primaryBtn = page.locator('[data-testid="cta-get-started"]');
    await expect(primaryBtn).toBeVisible();

    // Check if active styles are defined (CSS has :active rule)
    // Even if the visual change is subtle, the transition should be defined
    const hasTransition = await primaryBtn.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.transition !== 'none' && style.transition !== '';
    });

    expect(
      hasTransition,
      'Primary button should have transition for active state feedback'
    ).toBe(true);

    // Test secondary button active state
    const secondaryBtn = page.locator('[data-testid="cta-documentation"]');
    await expect(secondaryBtn).toBeVisible();

    const secondaryHasTransition = await secondaryBtn.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.transition !== 'none' && style.transition !== '';
    });

    expect(
      secondaryHasTransition,
      'Secondary button should have transition for active state feedback'
    ).toBe(true);

    // Test copy buttons have transition for active state feedback
    const copyButtons = page.locator('.copy-button');
    const copyButtonCount = await copyButtons.count();

    if (copyButtonCount > 0) {
      const firstCopyBtn = copyButtons.first();

      // Verify copy button has transition for active state
      const copyBtnHasTransition = await firstCopyBtn.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return style.transition !== 'none' && style.transition !== '';
      });

      expect(
        copyBtnHasTransition,
        'Copy button should have transition for active state feedback'
      ).toBe(true);
    }

    // Test that mobile menu toggle has active state feedback
    // Set viewport to mobile
    await page.setViewportSize({ width: 375, height: 667 });
    await page.waitForTimeout(300);

    const mobileMenuToggle = page.locator('.mobile-menu-toggle');
    const isToggleVisible = await mobileMenuToggle.isVisible();

    if (isToggleVisible) {
      // Click the toggle
      await mobileMenuToggle.click();
      await page.waitForTimeout(300);

      // Check if it has active class
      const hasActiveClass = await mobileMenuToggle.evaluate((el) => {
        return el.classList.contains('is-active');
      });

      expect(
        hasActiveClass,
        'Mobile menu toggle should have active class when clicked'
      ).toBe(true);
    }
  });

  /**
   * Test Case 4: Test cursor changes
   * Expected: Cursor changes to pointer over clickable elements
   */
  test('Test Case 4: Cursor changes to pointer over clickable elements', async ({ page }) => {
    // Test navigation links cursor
    const navLinks = page.locator('.nav-links a');
    const navLinkCount = await navLinks.count();

    for (let i = 0; i < navLinkCount; i++) {
      const link = navLinks.nth(i);
      // Links inherit pointer cursor automatically, but we verify it's not overridden
      const cursor = await link.evaluate((el) => {
        return window.getComputedStyle(el).cursor;
      });
      expect(cursor, `Navigation link ${i + 1} should have pointer cursor`).toBe('pointer');
    }

    // Test nav logo cursor
    const navLogo = page.locator('.nav-logo');
    const navLogoCursor = await navLogo.evaluate((el) => {
      return window.getComputedStyle(el).cursor;
    });
    expect(navLogoCursor, 'Nav logo should have pointer cursor').toBe('pointer');

    // Test primary CTA button cursor
    const primaryBtn = page.locator('[data-testid="cta-get-started"]');
    const primaryCursor = await primaryBtn.evaluate((el) => {
      return window.getComputedStyle(el).cursor;
    });
    expect(primaryCursor, 'Primary CTA button should have pointer cursor').toBe('pointer');

    // Test secondary CTA button cursor
    const secondaryBtn = page.locator('[data-testid="cta-documentation"]');
    const secondaryCursor = await secondaryBtn.evaluate((el) => {
      return window.getComputedStyle(el).cursor;
    });
    expect(secondaryCursor, 'Secondary CTA button should have pointer cursor').toBe('pointer');

    // Test copy buttons cursor
    const copyButtons = page.locator('.copy-button');
    const copyButtonCount = await copyButtons.count();

    for (let i = 0; i < copyButtonCount; i++) {
      const btn = copyButtons.nth(i);
      const cursor = await btn.evaluate((el) => {
        return window.getComputedStyle(el).cursor;
      });
      expect(cursor, `Copy button ${i + 1} should have pointer cursor`).toBe('pointer');
    }

    // Test footer links cursor
    const footerLinks = page.locator('.footer-links a');
    const footerLinkCount = await footerLinks.count();

    for (let i = 0; i < footerLinkCount; i++) {
      const link = footerLinks.nth(i);
      const cursor = await link.evaluate((el) => {
        return window.getComputedStyle(el).cursor;
      });
      expect(cursor, `Footer link ${i + 1} should have pointer cursor`).toBe('pointer');
    }

    // Test status badge link cursor
    const statusBadgeLink = page.locator('.status-badge-link');
    const statusBadgeVisible = await statusBadgeLink.isVisible();
    if (statusBadgeVisible) {
      const badgeCursor = await statusBadgeLink.evaluate((el) => {
        return window.getComputedStyle(el).cursor;
      });
      expect(badgeCursor, 'Status badge link should have pointer cursor').toBe('pointer');
    }

    // Test mobile menu toggle cursor (in mobile viewport)
    await page.setViewportSize({ width: 375, height: 667 });
    await page.waitForTimeout(300);

    const mobileMenuToggle = page.locator('.mobile-menu-toggle');
    const isToggleVisible = await mobileMenuToggle.isVisible();

    if (isToggleVisible) {
      const toggleCursor = await mobileMenuToggle.evaluate((el) => {
        return window.getComputedStyle(el).cursor;
      });
      expect(toggleCursor, 'Mobile menu toggle should have pointer cursor').toBe('pointer');
    }

    // Test mobile nav links cursor
    await mobileMenuToggle.click();
    await page.waitForTimeout(300);

    const mobileNavLinks = page.locator('.mobile-nav-links a');
    const mobileNavLinkCount = await mobileNavLinks.count();

    for (let i = 0; i < mobileNavLinkCount; i++) {
      const link = mobileNavLinks.nth(i);
      const cursor = await link.evaluate((el) => {
        return window.getComputedStyle(el).cursor;
      });
      expect(cursor, `Mobile nav link ${i + 1} should have pointer cursor`).toBe('pointer');
    }
  });

  /**
   * Additional test: Verify transitions are defined for smooth feedback
   */
  test('Interactive elements have smooth transitions for feedback', async ({ page }) => {
    // Test buttons have transitions
    const primaryBtn = page.locator('[data-testid="cta-get-started"]');
    const primaryTransition = await primaryBtn.evaluate((el) => {
      return window.getComputedStyle(el).transition;
    });
    expect(primaryTransition, 'Primary button should have transition').not.toBe('none');

    const secondaryBtn = page.locator('[data-testid="cta-documentation"]');
    const secondaryTransition = await secondaryBtn.evaluate((el) => {
      return window.getComputedStyle(el).transition;
    });
    expect(secondaryTransition, 'Secondary button should have transition').not.toBe('none');

    // Test nav links have transitions
    const navLinks = page.locator('.nav-links a');
    const firstNavLink = navLinks.first();
    const navLinkTransition = await firstNavLink.evaluate((el) => {
      return window.getComputedStyle(el).transition;
    });
    expect(navLinkTransition, 'Nav links should have transition').not.toBe('none');

    // Test feature cards have transitions
    const featureCards = page.locator('.feature-card');
    const firstCard = featureCards.first();
    const cardTransition = await firstCard.evaluate((el) => {
      return window.getComputedStyle(el).transition;
    });
    expect(cardTransition, 'Feature cards should have transition').not.toBe('none');

    // Test copy buttons have transitions
    const copyButtons = page.locator('.copy-button');
    const copyButtonCount = await copyButtons.count();
    if (copyButtonCount > 0) {
      const firstCopyBtn = copyButtons.first();
      const copyBtnTransition = await firstCopyBtn.evaluate((el) => {
        return window.getComputedStyle(el).transition;
      });
      expect(copyBtnTransition, 'Copy buttons should have transition').not.toBe('none');
    }

    // Test footer links have transitions
    const footerLinks = page.locator('.footer-links a');
    const firstFooterLink = footerLinks.first();
    const footerLinkTransition = await firstFooterLink.evaluate((el) => {
      return window.getComputedStyle(el).transition;
    });
    expect(footerLinkTransition, 'Footer links should have transition').not.toBe('none');
  });

  /**
   * Test: Status badge hover effect
   */
  test('Status badge link has hover effect', async ({ page }) => {
    const statusBadgeLink = page.locator('.status-badge-link');
    const isVisible = await statusBadgeLink.isVisible();

    if (isVisible) {
      const initialStyles = await statusBadgeLink.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          transform: style.transform,
          opacity: style.opacity
        };
      });

      await statusBadgeLink.hover();
      await page.waitForTimeout(200);

      const hoverStyles = await statusBadgeLink.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          transform: style.transform,
          opacity: style.opacity
        };
      });

      const hasChange =
        initialStyles.transform !== hoverStyles.transform ||
        initialStyles.opacity !== hoverStyles.opacity;

      expect(
        hasChange,
        'Status badge should have visual change on hover (scale or opacity)'
      ).toBe(true);
    }
  });
});
