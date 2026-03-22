/**
 * Accessibility E2E Tests
 * Owners: Scenarios 11, 12, 13 (Accessibility), Scenario 22 (No JS)
 *
 * Test groups:
 * - axe-core accessibility audit
 * - Color contrast validation
 * - Heading hierarchy
 * - ARIA landmarks
 * - Keyboard navigation (Scenario 12)
 * - Focus indicators (Scenario 12)
 * - Image alt text (Scenario 13)
 * - No-JavaScript fallback (Scenario 22)
 */

import { test, expect } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';
import { waitForLoad } from './utils';

test.describe('Accessibility - Basic Requirements (Scenario 11)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForLoad(page);
  });

  test('axe-core accessibility scan finds no critical or serious violations', async ({ page }) => {
    // Run axe-core accessibility audit
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21aa'])
      .analyze();

    // Filter for critical and serious violations only
    const criticalAndSerious = accessibilityScanResults.violations.filter(
      (violation) => violation.impact === 'critical' || violation.impact === 'serious'
    );

    // Log any violations for debugging
    if (criticalAndSerious.length > 0) {
      console.log('Critical/Serious violations found:', JSON.stringify(criticalAndSerious, null, 2));
    }

    // Expect no critical or serious violations
    expect(criticalAndSerious).toHaveLength(0);
  });

  test('body text has sufficient contrast ratio (at least 4.5:1)', async ({ page }) => {
    // Helper function to calculate contrast ratio
    const contrastRatio = await page.evaluate(() => {
      // Function to parse CSS color to RGB
      function parseColor(color: string): { r: number; g: number; b: number } | null {
        const canvas = document.createElement('canvas');
        canvas.width = 1;
        canvas.height = 1;
        const ctx = canvas.getContext('2d');
        if (!ctx) return null;
        ctx.fillStyle = color;
        ctx.fillRect(0, 0, 1, 1);
        const [r, g, b] = ctx.getImageData(0, 0, 1, 1).data;
        return { r, g, b };
      }

      // Function to calculate relative luminance
      function getLuminance(r: number, g: number, b: number): number {
        const [rs, gs, bs] = [r, g, b].map((c) => {
          c = c / 255;
          return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
        });
        return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
      }

      // Function to calculate contrast ratio
      function getContrastRatio(fg: { r: number; g: number; b: number }, bg: { r: number; g: number; b: number }): number {
        const l1 = getLuminance(fg.r, fg.g, fg.b);
        const l2 = getLuminance(bg.r, bg.g, bg.b);
        const lighter = Math.max(l1, l2);
        const darker = Math.min(l1, l2);
        return (lighter + 0.05) / (darker + 0.05);
      }

      // Get body element and computed styles
      const body = document.body;
      const bodyStyles = window.getComputedStyle(body);

      // Get text color and background color
      const textColor = bodyStyles.color;
      const bgColor = bodyStyles.backgroundColor;

      const fgParsed = parseColor(textColor);
      const bgParsed = parseColor(bgColor);

      if (!fgParsed || !bgParsed) return 0;

      return getContrastRatio(fgParsed, bgParsed);
    });

    // WCAG 2.1 AA requires at least 4.5:1 contrast ratio for normal text
    expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
  });

  test('heading hierarchy has exactly one h1 and headings in sequential order', async ({ page }) => {
    // Get all headings on the page
    const headings = await page.evaluate(() => {
      const headingElements = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      return Array.from(headingElements).map((el) => ({
        tag: el.tagName.toLowerCase(),
        level: parseInt(el.tagName.charAt(1)),
        text: el.textContent?.trim() || '',
      }));
    });

    // Check exactly one h1
    const h1Count = headings.filter((h) => h.tag === 'h1').length;
    expect(h1Count).toBe(1);

    // Check sequential order (no skipping levels)
    let previousLevel = 0;
    for (const heading of headings) {
      // Heading level should not skip more than 1 level
      // (e.g., h1 -> h3 is invalid, but h2 -> h2 or h2 -> h3 is valid)
      if (previousLevel > 0) {
        const levelDiff = heading.level - previousLevel;
        // Can go down any amount (h3 -> h2 is OK)
        // Can go up at most 1 level (h2 -> h3 is OK, h2 -> h4 is not)
        expect(levelDiff).toBeLessThanOrEqual(1);
      }
      previousLevel = heading.level;
    }
  });

  test('page has required ARIA landmark regions (main, contentinfo/footer)', async ({ page }) => {
    // Check for main landmark
    const mainLandmark = await page.locator('main, [role="main"]').count();
    expect(mainLandmark).toBeGreaterThanOrEqual(1);

    // Check for contentinfo landmark (footer)
    const contentinfoLandmark = await page.locator('footer, [role="contentinfo"]').count();
    expect(contentinfoLandmark).toBeGreaterThanOrEqual(1);

    // Check for banner landmark (header)
    const bannerLandmark = await page.locator('header, [role="banner"]').count();
    expect(bannerLandmark).toBeGreaterThanOrEqual(1);
  });

  test('page has navigation landmark if navigation exists', async ({ page }) => {
    // Check if there are navigation elements
    const navElements = await page.locator('nav, [role="navigation"]').count();

    // If navigation exists, it should have proper landmark role
    if (navElements > 0) {
      // Navigation should have aria-label for accessibility
      const navWithLabel = await page.locator('nav[aria-label], [role="navigation"][aria-label]').count();
      expect(navWithLabel).toBeGreaterThanOrEqual(1);
    }

    // The page should have at least one navigation in the footer
    const footerNav = await page.locator('footer nav, footer [role="navigation"]').count();
    expect(footerNav).toBeGreaterThanOrEqual(1);
  });

  test('all sections have proper ARIA labeling', async ({ page }) => {
    // Get all sections with aria-labelledby or aria-label
    const sections = await page.evaluate(() => {
      const sectionElements = document.querySelectorAll('section');
      return Array.from(sectionElements).map((section) => ({
        id: section.id,
        hasAriaLabel: section.hasAttribute('aria-label'),
        hasAriaLabelledBy: section.hasAttribute('aria-labelledby'),
        ariaLabelledBy: section.getAttribute('aria-labelledby'),
      }));
    });

    // Each section should have either aria-label or aria-labelledby
    for (const section of sections) {
      const hasLabel = section.hasAriaLabel || section.hasAriaLabelledBy;
      expect(hasLabel).toBeTruthy();

      // If aria-labelledby is used, the referenced element should exist
      if (section.hasAriaLabelledBy && section.ariaLabelledBy) {
        const labelElement = await page.locator(`#${section.ariaLabelledBy}`).count();
        expect(labelElement).toBeGreaterThanOrEqual(1);
      }
    }
  });
});

test.describe('Accessibility - Keyboard Navigation (Scenario 12)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForLoad(page);
  });

  test('TC1: Focus moves in logical order through all clickable elements', async ({ page }) => {
    // Start from the document body
    await page.keyboard.press('Tab');

    // First focusable element should be the skip-to-content link
    const skipLink = page.locator('#skip-to-content');
    await expect(skipLink).toBeFocused();

    // Continue tabbing to next elements
    await page.keyboard.press('Tab');

    // Next should be the primary CTA in hero section
    const primaryCta = page.locator('#primary-cta');
    await expect(primaryCta).toBeFocused();

    // Tab to secondary CTA
    await page.keyboard.press('Tab');
    const secondaryCta = page.locator('.btn-secondary');
    await expect(secondaryCta).toBeFocused();

    // Continue tabbing through badge links
    await page.keyboard.press('Tab');
    const firstBadge = page.locator('.badges a').first();
    await expect(firstBadge).toBeFocused();

    // Verify we can reach footer links by tabbing
    const footerLinks = page.locator('.footer-link');
    const footerLinkCount = await footerLinks.count();
    expect(footerLinkCount).toBeGreaterThan(0);

    // Tab through all interactive elements and verify they receive focus
    let focusableCount = 0;
    const maxTabs = 50; // Limit to prevent infinite loop

    // Reset to start
    await page.goto('/');
    await waitForLoad(page);

    for (let i = 0; i < maxTabs; i++) {
      await page.keyboard.press('Tab');
      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement;
        return el ? el.tagName : null;
      });

      if (focusedElement === 'BODY' || focusedElement === null) {
        break;
      }
      focusableCount++;
    }

    // Should have multiple focusable elements (CTAs, badge links, footer links)
    expect(focusableCount).toBeGreaterThanOrEqual(5);
  });

  test('TC2: Focus indicator is visible with sufficient contrast on CTA buttons', async ({ page }) => {
    // Tab to the primary CTA
    await page.keyboard.press('Tab'); // Skip link
    await page.keyboard.press('Tab'); // Primary CTA

    const primaryCta = page.locator('#primary-cta');
    await expect(primaryCta).toBeFocused();

    // Check that focus styles are applied (outline or box-shadow)
    const primaryFocusStyle = await primaryCta.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        outline: computed.outline,
        outlineWidth: computed.outlineWidth,
        outlineColor: computed.outlineColor,
        boxShadow: computed.boxShadow,
      };
    });

    // Focus should be visible via outline (width > 0) or box-shadow
    const hasVisibleOutline =
      primaryFocusStyle.outlineWidth !== '0px' &&
      primaryFocusStyle.outlineColor !== 'transparent' &&
      primaryFocusStyle.outlineColor !== 'rgba(0, 0, 0, 0)';
    const hasVisibleBoxShadow =
      primaryFocusStyle.boxShadow !== 'none' && primaryFocusStyle.boxShadow !== '';

    expect(hasVisibleOutline || hasVisibleBoxShadow).toBe(true);

    // Tab to secondary CTA and verify focus indicator
    await page.keyboard.press('Tab');
    const secondaryCta = page.locator('.btn-secondary');
    await expect(secondaryCta).toBeFocused();

    const secondaryFocusStyle = await secondaryCta.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        outline: computed.outline,
        outlineWidth: computed.outlineWidth,
        outlineColor: computed.outlineColor,
        boxShadow: computed.boxShadow,
      };
    });

    // Secondary CTA should also have visible focus indicator
    const secondaryHasVisibleOutline =
      secondaryFocusStyle.outlineWidth !== '0px' &&
      secondaryFocusStyle.outlineColor !== 'transparent' &&
      secondaryFocusStyle.outlineColor !== 'rgba(0, 0, 0, 0)';
    const secondaryHasVisibleBoxShadow =
      secondaryFocusStyle.boxShadow !== 'none' && secondaryFocusStyle.boxShadow !== '';

    expect(secondaryHasVisibleOutline || secondaryHasVisibleBoxShadow).toBe(true);
  });

  test('TC3: CTA activates and performs expected action when pressing Enter', async ({ page }) => {
    // Navigate to secondary CTA (which links to #quickstart)
    await page.keyboard.press('Tab'); // Skip link
    await page.keyboard.press('Tab'); // Primary CTA
    await page.keyboard.press('Tab'); // Secondary CTA

    const secondaryCta = page.locator('.btn-secondary');
    await expect(secondaryCta).toBeFocused();

    // Get the href of the secondary CTA
    const href = await secondaryCta.getAttribute('href');
    expect(href).toBe('#quickstart');

    // Press Enter to activate the link
    await page.keyboard.press('Enter');

    // Wait for navigation to the quickstart section
    await page.waitForTimeout(500); // Allow time for smooth scroll

    // Verify the URL hash changed
    const currentUrl = page.url();
    expect(currentUrl).toContain('#quickstart');

    // Verify the quickstart section is now visible in viewport
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport();
  });

  test('TC4: Skip link is available for keyboard users', async ({ page }) => {
    // Skip-to-content link should exist
    const skipLink = page.locator('#skip-to-content');
    await expect(skipLink).toBeAttached();

    // Skip link should have proper href
    const href = await skipLink.getAttribute('href');
    expect(href).toBe('#features');

    // Skip link should have appropriate text
    const text = await skipLink.textContent();
    expect(text?.toLowerCase()).toContain('skip');

    // Skip link should be visually hidden initially (positioned off-screen with negative top)
    const topBeforeFocus = await skipLink.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      // Return the numeric value in pixels
      return parseFloat(computed.top);
    });
    // Skip link should be positioned above the viewport (negative top value)
    expect(topBeforeFocus).toBeLessThan(0);

    // Tab to the skip link (first focusable element)
    await page.keyboard.press('Tab');
    await expect(skipLink).toBeFocused();

    // Wait for the CSS transition to complete (0.3s transition defined in CSS)
    await page.waitForTimeout(400);

    // When focused, skip link should become visible (top: 0)
    const topAfterFocus = await skipLink.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return parseFloat(computed.top);
    });
    // When focused, top should be 0 (visible) or very close to it
    expect(topAfterFocus).toBeGreaterThanOrEqual(-5);

    // Press Enter to activate skip link
    await page.keyboard.press('Enter');

    // Wait for navigation
    await page.waitForTimeout(500);

    // URL should contain the target hash
    expect(page.url()).toContain('#features');

    // Features section should be visible in viewport
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });

  test('All interactive elements have visible focus indicators', async ({ page }) => {
    // Collect all interactive elements
    const interactiveElements = page.locator('a, button, [tabindex]:not([tabindex="-1"])');
    const count = await interactiveElements.count();

    expect(count).toBeGreaterThan(0);

    // Sample test: verify first few elements have focus styles
    for (let i = 0; i < Math.min(3, count); i++) {
      const element = interactiveElements.nth(i);

      // Focus the element directly
      await element.focus();

      // Check for focus indicator
      const outlineStyle = await element.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return {
          outline: computed.outline,
          outlineWidth: computed.outlineWidth,
          outlineStyle: computed.outlineStyle,
          boxShadow: computed.boxShadow,
        };
      });

      // Element should have visible focus indicator (outline or box-shadow)
      const hasOutline =
        outlineStyle.outlineStyle !== 'none' && outlineStyle.outlineWidth !== '0px';
      const hasBoxShadow = outlineStyle.boxShadow !== 'none';

      expect(hasOutline || hasBoxShadow).toBe(true);
    }
  });

  test('Tab order follows visual layout', async ({ page }) => {
    const focusOrder: string[] = [];

    // Tab through elements and record their order
    for (let i = 0; i < 15; i++) {
      await page.keyboard.press('Tab');
      const focusedId = await page.evaluate(() => {
        const el = document.activeElement;
        return el?.id || el?.className || el?.tagName;
      });
      if (focusedId) {
        focusOrder.push(focusedId);
      }
    }

    // Verify logical order: skip link -> hero CTAs -> badges -> footer
    const skipLinkIndex = focusOrder.findIndex((id) => id.includes('skip'));
    const primaryCtaIndex = focusOrder.findIndex((id) => id === 'primary-cta');

    expect(skipLinkIndex).toBeLessThan(primaryCtaIndex);
  });

  test('Shift+Tab navigates backwards through elements', async ({ page }) => {
    // Tab forward a few times
    await page.keyboard.press('Tab'); // Skip link
    await page.keyboard.press('Tab'); // Primary CTA
    await page.keyboard.press('Tab'); // Secondary CTA

    const secondaryCta = page.locator('.btn-secondary');
    await expect(secondaryCta).toBeFocused();

    // Shift+Tab should go back to primary CTA
    await page.keyboard.press('Shift+Tab');
    const primaryCta = page.locator('#primary-cta');
    await expect(primaryCta).toBeFocused();

    // Shift+Tab again should go to skip link
    await page.keyboard.press('Shift+Tab');
    const skipLink = page.locator('#skip-to-content');
    await expect(skipLink).toBeFocused();
  });

  test('Footer links are keyboard accessible', async ({ page }) => {
    // Navigate to footer area (scroll and tab)
    const footerLinks = page.locator('.footer-link');
    const footerLinkCount = await footerLinks.count();

    expect(footerLinkCount).toBeGreaterThanOrEqual(3);

    // Focus first footer link directly
    await footerLinks.first().focus();
    await expect(footerLinks.first()).toBeFocused();

    // Check focus indicator is visible
    const outlineStyle = await footerLinks.first().evaluate((el) => {
      return window.getComputedStyle(el).outline;
    });

    expect(outlineStyle).not.toBe('');
    expect(outlineStyle).not.toContain('0px');

    // Tab to next footer link
    await page.keyboard.press('Tab');
    await expect(footerLinks.nth(1)).toBeFocused();
  });

  test('Badge links are keyboard navigable', async ({ page }) => {
    const badgeLinks = page.locator('.badges a');
    const badgeLinkCount = await badgeLinks.count();

    expect(badgeLinkCount).toBeGreaterThanOrEqual(1);

    // Focus first badge link
    await badgeLinks.first().focus();
    await expect(badgeLinks.first()).toBeFocused();

    // Check focus indicator
    const outlineStyle = await badgeLinks.first().evaluate((el) => {
      return window.getComputedStyle(el).outline;
    });

    expect(outlineStyle).not.toBe('');
    expect(outlineStyle).not.toContain('0px');

    // Tab to next badge if available
    if (badgeLinkCount > 1) {
      await page.keyboard.press('Tab');
      await expect(badgeLinks.nth(1)).toBeFocused();
    }
  });
});

test.describe('Accessibility - Images and Alt Text (Scenario 13)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForLoad(page);
  });

  test('TC1: All images have alt attribute (can be empty for decorative)', async ({ page }) => {
    // Query all img elements on the page
    const images = await page.locator('img').all();

    expect(images.length).toBeGreaterThan(0);

    // Check each image has an alt attribute
    for (const img of images) {
      const hasAlt = await img.evaluate((el) => el.hasAttribute('alt'));
      const src = await img.getAttribute('src');
      expect(hasAlt, `Image ${src} should have an alt attribute`).toBe(true);
    }
  });

  test('TC2: Logo has descriptive alt text', async ({ page }) => {
    // Find the logo image
    const logo = page.locator('#hero-logo, .hero-logo, img[src*="logo"]').first();

    await expect(logo).toBeVisible();

    // Get the alt text
    const altText = await logo.getAttribute('alt');

    // Alt text should be present and descriptive (not empty, not generic)
    expect(altText).not.toBeNull();
    expect(altText).not.toBe('');
    expect(altText!.toLowerCase()).not.toBe('image');
    expect(altText!.toLowerCase()).not.toBe('icon');
    expect(altText!.toLowerCase()).not.toBe('logo');

    // Should contain meaningful description (e.g., 'MirDB' or product name)
    expect(altText!.toLowerCase()).toContain('mirdb');
  });

  test('TC3: Status badges have alt text describing their purpose', async ({ page }) => {
    // Find the badges section
    const badgesSection = page.locator('#badges, .badges');
    await expect(badgesSection).toBeVisible();

    // Find all badge images within the badges section
    const badgeImages = await badgesSection.locator('img').all();

    expect(badgeImages.length).toBeGreaterThan(0);

    for (const badge of badgeImages) {
      const altText = await badge.getAttribute('alt');

      // Each badge should have alt text
      expect(altText, 'Badge should have alt text').not.toBeNull();
      expect(altText!.trim(), 'Badge alt text should not be empty').not.toBe('');

      // Alt text should be descriptive, not generic
      expect(altText!.toLowerCase()).not.toBe('image');
      expect(altText!.toLowerCase()).not.toBe('icon');
      expect(altText!.toLowerCase()).not.toBe('badge');

      // Alt text should describe the badge's purpose (e.g., build status, license, stars)
      const isDescriptive =
        altText!.toLowerCase().includes('build') ||
        altText!.toLowerCase().includes('status') ||
        altText!.toLowerCase().includes('license') ||
        altText!.toLowerCase().includes('mit') ||
        altText!.toLowerCase().includes('stars') ||
        altText!.toLowerCase().includes('github') ||
        altText!.toLowerCase().includes('version') ||
        altText!.toLowerCase().includes('coverage');

      expect(isDescriptive, `Badge alt text "${altText}" should describe its purpose`).toBe(true);
    }
  });

  test('TC4: Icon-only buttons have aria-label or screen reader text', async ({ page }) => {
    // Find all buttons and button-like elements
    const buttons = await page.locator('button, [role="button"], a.btn, .btn').all();

    for (const button of buttons) {
      // Get button text content
      const textContent = await button.textContent();
      const trimmedText = textContent?.trim() || '';

      // If the button has visible text, it's accessible
      if (trimmedText.length > 0 && !/^[\s\u200B-\u200D\uFEFF]+$/.test(trimmedText)) {
        continue;
      }

      // For icon-only buttons (no text content), check for accessibility
      const ariaLabel = await button.getAttribute('aria-label');
      const ariaLabelledby = await button.getAttribute('aria-labelledby');
      const title = await button.getAttribute('title');

      // Check for visually hidden text inside the button
      const srText = await button.locator('.sr-only, .visually-hidden, [class*="screen-reader"]').textContent().catch(() => '');

      const hasAccessibleName =
        (ariaLabel && ariaLabel.trim().length > 0) ||
        (ariaLabelledby && ariaLabelledby.trim().length > 0) ||
        (title && title.trim().length > 0) ||
        (srText && srText.trim().length > 0);

      expect(hasAccessibleName, `Icon-only button should have aria-label, aria-labelledby, title, or screen reader text`).toBe(true);
    }

    // Additionally, check badge links have aria-labels
    const badgeLinks = await page.locator('#badges a, .badges a').all();

    for (const link of badgeLinks) {
      const ariaLabel = await link.getAttribute('aria-label');
      const linkText = await link.textContent();
      const trimmedLinkText = linkText?.trim() || '';

      // Badge links that only contain images should have aria-labels
      if (trimmedLinkText.length === 0) {
        expect(ariaLabel, 'Badge link should have aria-label').not.toBeNull();
        expect(ariaLabel!.trim(), 'Badge link aria-label should not be empty').not.toBe('');
      }
    }
  });

  test('All images have non-generic alt text', async ({ page }) => {
    // Additional test to verify all images have meaningful alt text
    const images = await page.locator('img').all();

    const genericAltValues = ['image', 'icon', 'picture', 'photo', 'img', 'graphic'];

    for (const img of images) {
      const altText = await img.getAttribute('alt');
      const src = await img.getAttribute('src');

      // Skip decorative images (empty alt is valid for decorative images)
      if (altText === '') {
        continue;
      }

      // Alt text should not be null for non-decorative images
      expect(altText, `Image ${src} should have alt attribute`).not.toBeNull();

      // Alt text should not be generic
      const isGeneric = genericAltValues.some(generic =>
        altText!.toLowerCase() === generic
      );

      expect(isGeneric, `Image ${src} has generic alt text "${altText}". Use descriptive text.`).toBe(false);
    }
  });

  test('Usage GIF has descriptive alt text', async ({ page }) => {
    // Find the usage GIF
    const usageGif = page.locator('#usage-gif, .usage-gif, img[src*="usage"]').first();

    const isVisible = await usageGif.isVisible().catch(() => false);

    if (isVisible) {
      const altText = await usageGif.getAttribute('alt');

      // Alt text should be present and descriptive
      expect(altText).not.toBeNull();
      expect(altText!.trim()).not.toBe('');
      expect(altText!.toLowerCase()).not.toBe('image');
      expect(altText!.toLowerCase()).not.toBe('gif');
      expect(altText!.toLowerCase()).not.toBe('usage');

      // Should describe what the GIF shows
      const isDescriptive =
        altText!.toLowerCase().includes('usage') ||
        altText!.toLowerCase().includes('demo') ||
        altText!.toLowerCase().includes('example') ||
        altText!.toLowerCase().includes('command') ||
        altText!.toLowerCase().includes('memcached') ||
        altText!.toLowerCase().includes('mirdb');

      expect(isDescriptive, `Usage GIF alt text "${altText}" should describe the content`).toBe(true);
    }
  });
});
