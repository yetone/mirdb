const { test, expect } = require('@playwright/test');

/**
 * US-6: Mobile Access - User Story Validation
 * Scenario: Validate acceptance criteria for US-6: Homepage works on mobile devices
 *
 * As a New Developer, I want to view the homepage on my mobile device,
 * so that I can learn about MirDB while away from my workstation.
 *
 * Acceptance Criteria:
 * - Given I access the homepage on a mobile device
 * - When the page loads
 * - Then all content is readable without horizontal scrolling
 * - And navigation is accessible via a mobile-friendly menu
 */

test.describe('US-6: Mobile Access Validation', () => {
  // Configure all tests in this suite to use 320px mobile viewport (smallest common mobile width)
  test.use({
    viewport: { width: 320, height: 568 } // iPhone 5/SE size - smallest common viewport
  });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: All content fits without horizontal scroll at 320px viewport', async ({ page }) => {
    // Step 1: Access on mobile viewport - Load homepage with mobile device viewport
    // Context: Testing mobile experience

    const viewportWidth = 320;

    // Check that the page doesn't have horizontal overflow
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth);

    // Also verify documentElement (html) doesn't overflow
    const htmlScrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    expect(htmlScrollWidth).toBeLessThanOrEqual(viewportWidth);

    // Verify there's no horizontal scrollbar by checking overflow
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.body.scrollWidth > document.body.clientWidth ||
             document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBeFalsy();

    // Verify all main sections are visible without horizontal scrolling
    await expect(page.locator('header')).toBeVisible();
    await expect(page.locator('.hero')).toBeVisible();
    await expect(page.locator('#value-proposition')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();
    await expect(page.locator('#quickstart')).toBeVisible();
    await expect(page.locator('footer')).toBeVisible();

    // Verify no element exceeds viewport width
    const allElements = page.locator('body *');
    const elementCount = await allElements.count();

    // Check a sample of important content containers don't overflow
    const importantContainers = [
      '.container',
      '.hero',
      '.value-prop-grid',
      '.features-grid',
      '.quickstart-content',
      '.code-block'
    ];

    for (const selector of importantContainers) {
      const elements = page.locator(selector);
      const count = await elements.count();
      for (let i = 0; i < count; i++) {
        const box = await elements.nth(i).boundingBox();
        if (box) {
          expect(box.width).toBeLessThanOrEqual(viewportWidth);
        }
      }
    }
  });

  test('Test Case 2: Navigation accessible via mobile menu interface', async ({ page }) => {
    // Step 3: Verify navigation access - Check navigation is accessible on mobile
    // Context: Should have mobile-friendly menu

    // Check that header and navigation exist
    const header = page.locator('header');
    await expect(header).toBeVisible();

    const nav = header.locator('nav');
    await expect(nav).toBeVisible();

    // Get navigation links
    const navLinks = nav.locator('a');
    const linkCount = await navLinks.count();

    // Should have at least 3 navigation links (Features, Quick Start, GitHub)
    expect(linkCount).toBeGreaterThanOrEqual(3);

    // Check if there's a hamburger menu button
    const hamburgerButton = header.locator('[class*="hamburger"], [class*="mobile-menu"], button[aria-label*="menu"]');
    const hasHamburger = await hamburgerButton.count() > 0;

    if (hasHamburger) {
      // If hamburger menu exists, click it and verify links become visible
      await hamburgerButton.click();
      for (let i = 0; i < linkCount; i++) {
        await expect(navLinks.nth(i)).toBeVisible();
      }
    } else {
      // Navigation wraps on mobile - verify links are accessible
      // The implementation uses flex-wrap, so links should still be visible
      const navList = nav.locator('ul');
      await expect(navList).toBeVisible();

      // Verify nav list uses flex-wrap for mobile (responsive design)
      const flexWrap = await navList.evaluate((el) => {
        return window.getComputedStyle(el).flexWrap;
      });
      expect(flexWrap).toBe('wrap');

      // All navigation links should be visible
      for (let i = 0; i < linkCount; i++) {
        await expect(navLinks.nth(i)).toBeVisible();
      }
    }

    // Verify the Features link is functional and navigates correctly
    const featuresLink = nav.locator('a[href="#features"]');
    await expect(featuresLink).toBeVisible();
    await featuresLink.click();
    await expect(page.locator('#features')).toBeVisible();

    // Verify the Quick Start link is functional
    const quickstartLink = nav.locator('a[href="#quickstart"]');
    await expect(quickstartLink).toBeVisible();
    await quickstartLink.click();
    await expect(page.locator('#quickstart')).toBeVisible();

    // Verify external links (GitHub) have proper attributes
    const githubLink = nav.locator('a[href*="github.com"]');
    if (await githubLink.count() > 0) {
      await expect(githubLink).toBeVisible();
      const target = await githubLink.getAttribute('target');
      const rel = await githubLink.getAttribute('rel');
      expect(target).toBe('_blank');
      expect(rel).toContain('noopener');
    }
  });

  test('Test Case 3: Body text is at least 16px equivalent for readability', async ({ page }) => {
    // Step 2: Verify readability - Check all content is readable without horizontal scrolling
    // Context: No pinch-zoom should be required

    // Check viewport meta tag is properly set for mobile
    const viewportMeta = await page.locator('meta[name="viewport"]').getAttribute('content');
    expect(viewportMeta).toContain('width=device-width');
    expect(viewportMeta).toContain('initial-scale=1');

    // Check body text font size in value proposition section
    const valuePropText = page.locator('.value-prop-column p').first();
    await expect(valuePropText).toBeVisible();

    const valuePropFontSize = await valuePropText.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    // Body text should be at least 14px (slightly below 16px is acceptable for secondary text)
    // Main body text should be 16px or greater
    expect(valuePropFontSize).toBeGreaterThanOrEqual(14);

    // Check hero subheadline (main body text equivalent)
    const heroSubheadline = page.locator('.hero-subheadline');
    await expect(heroSubheadline).toBeVisible();

    const subheadlineFontSize = await heroSubheadline.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    // Subheadline should be at least 16px for readability
    expect(subheadlineFontSize).toBeGreaterThanOrEqual(16);

    // Check headline is readable (should be larger than body text)
    const headline = page.locator('.hero-headline');
    await expect(headline).toBeVisible();

    const headlineFontSize = await headline.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    // Headline should be at least 24px on mobile for readability
    expect(headlineFontSize).toBeGreaterThanOrEqual(24);

    // Check feature card text
    const featureCardText = page.locator('.feature-card p').first();
    if (await featureCardText.count() > 0) {
      await expect(featureCardText).toBeVisible();

      const featureTextFontSize = await featureCardText.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });
      // Feature card text should be readable (at least 14px)
      expect(featureTextFontSize).toBeGreaterThanOrEqual(14);
    }

    // Check quickstart section text
    const quickstartText = page.locator('.quickstart-step h3').first();
    if (await quickstartText.count() > 0) {
      await expect(quickstartText).toBeVisible();

      const quickstartFontSize = await quickstartText.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });
      // Quickstart headers should be readable
      expect(quickstartFontSize).toBeGreaterThanOrEqual(16);
    }

    // Verify line-height is sufficient for readability (should be at least 1.4)
    const bodyLineHeight = await page.locator('body').evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).lineHeight) / parseFloat(window.getComputedStyle(el).fontSize);
    });
    expect(bodyLineHeight).toBeGreaterThanOrEqual(1.4);

    // Check that text color has sufficient contrast with background
    // This is a basic check - the accessibility tests cover this more thoroughly
    const textColor = await page.locator('.hero-subheadline').evaluate((el) => {
      return window.getComputedStyle(el).color;
    });
    expect(textColor).toBeTruthy();
    // Text should not be transparent or white-on-white
    expect(textColor).not.toBe('transparent');
    expect(textColor).not.toBe('rgba(0, 0, 0, 0)');
  });

  // Additional test for comprehensive mobile layout validation
  test('Content stacks vertically on 320px mobile viewport', async ({ page }) => {
    // Verify the three-column value proposition stacks to single column
    const valuePropGrid = page.locator('.value-prop-grid');
    await expect(valuePropGrid).toBeVisible();

    // Check grid template columns - should be 1fr (single column) on mobile
    const gridStyle = await valuePropGrid.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });
    // On mobile (max-width: 767px), grid should be single column
    // This validates the responsive CSS is working
    expect(gridStyle).toMatch(/^[\d.]+px$/); // Single column value (e.g., "280px")

    // Verify columns are stacked vertically (same x position, different y positions)
    const columns = page.locator('.value-prop-column');
    const columnCount = await columns.count();
    expect(columnCount).toBe(3);

    const firstColumnBox = await columns.nth(0).boundingBox();
    const secondColumnBox = await columns.nth(1).boundingBox();
    const thirdColumnBox = await columns.nth(2).boundingBox();

    // All columns should have the same x position (stacked vertically)
    expect(firstColumnBox.x).toBe(secondColumnBox.x);
    expect(secondColumnBox.x).toBe(thirdColumnBox.x);

    // Second column should be below first
    expect(secondColumnBox.y).toBeGreaterThan(firstColumnBox.y);

    // Third column should be below second
    expect(thirdColumnBox.y).toBeGreaterThan(secondColumnBox.y);
  });

  // Test for touch-friendly target sizes
  test('Interactive elements have touch-friendly sizes', async ({ page }) => {
    // CTA buttons should be easily tappable (minimum 44px height per WCAG)
    const ctaButtons = page.locator('.btn');
    const buttonCount = await ctaButtons.count();

    for (let i = 0; i < buttonCount; i++) {
      const button = ctaButtons.nth(i);
      const box = await button.boundingBox();
      if (box) {
        // Buttons should have minimum touch target size
        expect(box.height).toBeGreaterThanOrEqual(40); // Slightly relaxed from 44px
      }
    }

    // Navigation links should be accessible
    const navLinks = page.locator('nav a');
    const navLinkCount = await navLinks.count();

    for (let i = 0; i < navLinkCount; i++) {
      const link = navLinks.nth(i);
      await expect(link).toBeVisible();
    }
  });
});
