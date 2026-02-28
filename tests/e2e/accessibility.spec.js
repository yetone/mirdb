/**
 * Accessibility E2E Tests
 * Owner: Scenarios 8, 9, 10, 18
 *
 * This file contains keyboard navigation tests for Scenario 8.
 * Other scenarios (9, 10, 18) will add their tests to this file.
 *
 * Test cases for Scenario 8 - Keyboard Navigation:
 * - Skip navigation link appears on first Tab
 * - All interactive elements receive visible focus
 * - Keyboard activation works on buttons and links
 * - Focus order matches visual order
 */

const { test, expect } = require('@playwright/test');
const {
  waitForPageLoad,
  VIEWPORTS,
  measureContrastRatio,
  calculateContrastRatio,
  getEffectiveBackgroundColor,
  WCAG_AA_NORMAL_TEXT,
  WCAG_AA_LARGE_TEXT,
} = require('./test-utils');

test.describe('Accessibility - Keyboard Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForPageLoad(page);
  });

  test('skip navigation link becomes visible on first Tab press', async ({ page }) => {
    // Skip link should be hidden initially (positioned off-screen with negative top)
    const skipLink = page.locator('.skip-link');

    // Check initial state - should be positioned off-screen (negative top value)
    const initialTop = await skipLink.evaluate((el) => {
      return parseInt(window.getComputedStyle(el).top, 10);
    });
    expect(initialTop).toBeLessThan(0);

    // Press Tab to focus the skip link
    await page.keyboard.press('Tab');

    // Wait for the skip link to become visible
    await expect(skipLink).toBeFocused();

    // Wait for CSS transition to complete (150ms defined in styles.css)
    await page.waitForTimeout(200);

    // Check that skip link is now visible (top should be positive or close to 0)
    const visibleTop = await skipLink.evaluate((el) => {
      return parseInt(window.getComputedStyle(el).top, 10);
    });
    expect(visibleTop).toBeGreaterThanOrEqual(0);

    // Verify the skip link text
    await expect(skipLink).toHaveText('Skip to main content');
  });

  test('each navigation link receives visible focus outline', async ({ page }) => {
    // Tab to skip link first
    await page.keyboard.press('Tab');

    // Tab to nav brand link
    await page.keyboard.press('Tab');
    const navBrandLink = page.locator('.nav-brand a');
    await expect(navBrandLink).toBeFocused();

    // Verify focus outline exists
    const brandOutline = await navBrandLink.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        outline: styles.outline,
        outlineColor: styles.outlineColor,
        outlineStyle: styles.outlineStyle,
        outlineWidth: styles.outlineWidth,
      };
    });
    expect(brandOutline.outlineStyle).not.toBe('none');

    // Tab through navigation menu links
    const navLinks = ['Features', 'Quick Start', 'Status', 'GitHub', 'Docs'];

    for (const linkText of navLinks) {
      await page.keyboard.press('Tab');
      const currentFocused = page.locator(':focus');
      const focusedText = await currentFocused.textContent();

      // Verify we're on a nav link (may include external icon)
      expect(focusedText).toContain(linkText.split(' ')[0]);

      // Verify focus outline is visible
      const focusOutline = await currentFocused.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return styles.outlineStyle;
      });
      expect(focusOutline).not.toBe('none');
    }
  });

  test('CTA button action is triggered via Enter key (scroll to section)', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Focus the CTA button directly
    const ctaButton = page.locator('.hero__cta');
    await ctaButton.focus();

    // Verify CTA button is focused
    await expect(ctaButton).toBeFocused();
    await expect(ctaButton).toHaveText('Get Started');

    // Press Enter to activate
    await page.keyboard.press('Enter');

    // Wait for smooth scroll to complete
    await page.waitForTimeout(500);

    // Verify scroll occurred (page should scroll to quickstart section)
    const finalScrollY = await page.evaluate(() => window.scrollY);
    expect(finalScrollY).toBeGreaterThan(initialScrollY);

    // Verify we scrolled to the quickstart section
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport();
  });

  test('copy button action is triggered via keyboard', async ({ page }) => {
    // First, scroll to the quickstart section to ensure copy buttons are accessible
    await page.locator('#quickstart').scrollIntoViewIfNeeded();

    // Focus on the first copy button directly
    const firstCopyButton = page.locator('.copy-button').first();
    await firstCopyButton.focus();

    // Verify it's focused
    await expect(firstCopyButton).toBeFocused();

    // Get initial button text
    const initialText = await firstCopyButton.locator('.copy-text').textContent();
    expect(initialText).toBe('Copy');

    // Press Enter to trigger copy
    await page.keyboard.press('Enter');

    // Wait for feedback
    await page.waitForTimeout(100);

    // Verify copy feedback is shown
    const feedbackText = await firstCopyButton.locator('.copy-text').textContent();
    expect(feedbackText).toBe('Copied!');

    // Verify the button has the copied class
    await expect(firstCopyButton).toHaveClass(/copied/);
  });

  test('focus order matches visual order (logical tab sequence)', async ({ page }) => {
    const expectedFocusOrder = [
      { selector: '.skip-link', description: 'Skip link' },
      { selector: '.nav-brand a', description: 'Nav brand' },
      { selector: '.nav-menu a[href="#features"]', description: 'Features link' },
      { selector: '.nav-menu a[href="#quickstart"]', description: 'Quick Start link' },
      { selector: '.nav-menu a[href="#status"]', description: 'Status link' },
      { selector: '.nav-menu a[href="https://github.com/yetone/mirdb"]', description: 'GitHub link' },
      { selector: '.nav-menu a[href="https://github.com/memcached/memcached/wiki/Commands"]', description: 'Docs link' },
      { selector: '.hero__cta', description: 'CTA button' },
    ];

    for (let i = 0; i < expectedFocusOrder.length; i++) {
      await page.keyboard.press('Tab');
      const expected = expectedFocusOrder[i];
      const focusedElement = page.locator(expected.selector);

      // Verify the expected element is focused
      await expect(focusedElement, `${expected.description} should be focused at position ${i + 1}`).toBeFocused();
    }
  });

  test('all interactive elements can be activated via keyboard', async ({ page }) => {
    // Test navigation links with Enter key
    await page.keyboard.press('Tab'); // Skip link
    await page.keyboard.press('Tab'); // Nav brand
    await page.keyboard.press('Tab'); // Features link

    // Verify Features link is focused
    const featuresLink = page.locator('.nav-menu a[href="#features"]');
    await expect(featuresLink).toBeFocused();

    // Press Enter to navigate
    await page.keyboard.press('Enter');

    // Wait for smooth scroll
    await page.waitForTimeout(500);

    // Verify scroll to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });

  test('mobile hamburger menu is keyboard accessible', async ({ page }) => {
    // Set viewport to mobile size
    await page.setViewportSize(VIEWPORTS.mobile);

    // Reload page with new viewport
    await page.reload();
    await waitForPageLoad(page);

    // Tab to skip link, then nav brand, then hamburger toggle
    await page.keyboard.press('Tab'); // Skip link
    await page.keyboard.press('Tab'); // Nav brand
    await page.keyboard.press('Tab'); // Hamburger toggle

    const navToggle = page.locator('.nav-toggle');
    await expect(navToggle).toBeFocused();

    // Verify menu is closed
    await expect(navToggle).toHaveAttribute('aria-expanded', 'false');

    // Press Enter to open menu
    await page.keyboard.press('Enter');

    // Verify menu is open
    await expect(navToggle).toHaveAttribute('aria-expanded', 'true');

    // Press Escape to close menu
    await page.keyboard.press('Escape');

    // Verify menu is closed again
    await expect(navToggle).toHaveAttribute('aria-expanded', 'false');
  });

  test('focus is visible on all interactive states', async ({ page }) => {
    // Check focus visibility on multiple elements
    const interactiveElements = [
      '.skip-link',
      '.nav-brand a',
      '.nav-menu a',
      '.hero__cta',
      '.copy-button',
    ];

    for (const selector of interactiveElements) {
      const element = page.locator(selector).first();
      await element.focus();

      // Verify focus indicator is visible (outline or ring)
      const focusStyles = await element.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          outline: styles.outline,
          outlineStyle: styles.outlineStyle,
          outlineWidth: styles.outlineWidth,
          outlineColor: styles.outlineColor,
        };
      });

      // Focus should have some visible indicator (outline)
      const hasOutline = focusStyles.outlineStyle !== 'none' &&
                         focusStyles.outlineWidth !== '0px';

      expect(hasOutline, `${selector} should have visible focus outline`).toBe(true);
    }
  });

  test('skip link activates and moves focus to main content', async ({ page }) => {
    // Press Tab to focus skip link
    await page.keyboard.press('Tab');

    const skipLink = page.locator('.skip-link');
    await expect(skipLink).toBeFocused();

    // Press Enter to activate skip link
    await page.keyboard.press('Enter');

    // Wait for navigation
    await page.waitForTimeout(300);

    // Verify URL hash changed or main content is in view
    const url = page.url();
    expect(url).toContain('#main-content');

    // Main content should be in viewport
    const mainContent = page.locator('#main-content');
    await expect(mainContent).toBeInViewport();
  });
});

/**
 * Accessibility - Color Contrast Tests
 * Owner: Scenario 9 - Accessibility - Color Contrast
 *
 * Test cases:
 * - Body text meets 4.5:1 contrast ratio (WCAG AA)
 * - Heading text meets contrast requirements (3:1 for large text)
 * - CTA button text contrast is at least 4.5:1
 * - Code block text has sufficient contrast
 * - Focus indicators have sufficient contrast
 */
test.describe('Accessibility - Color Contrast', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForPageLoad(page);
  });

  test('body text meets WCAG AA contrast ratio of 4.5:1', async ({ page }) => {
    // Test body text in different sections
    const bodyTextSelectors = [
      '.hero__tagline',
      '.feature-description',
    ];

    for (const selector of bodyTextSelectors) {
      const element = page.locator(selector).first();
      const isVisible = await element.isVisible();

      if (isVisible) {
        const { ratio, foreground, background } = await measureContrastRatio(page, element);

        expect(
          ratio,
          `${selector} should have contrast ratio >= ${WCAG_AA_NORMAL_TEXT}:1, got ${ratio.toFixed(2)}:1 (fg: ${foreground}, bg: ${background})`
        ).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      }
    }
  });

  test('heading text meets WCAG AA contrast ratio for large text (3:1)', async ({ page }) => {
    // Test headings - large text can use 3:1 ratio per WCAG AA
    const headingSelectors = [
      'h1',
      'h2',
      'h3',
    ];

    for (const selector of headingSelectors) {
      const elements = page.locator(selector);
      const count = await elements.count();

      for (let i = 0; i < count; i++) {
        const element = elements.nth(i);
        const isVisible = await element.isVisible();

        if (isVisible) {
          const { ratio, foreground, background } = await measureContrastRatio(page, element);

          expect(
            ratio,
            `${selector}[${i}] should have contrast ratio >= ${WCAG_AA_LARGE_TEXT}:1 for large text, got ${ratio.toFixed(2)}:1 (fg: ${foreground}, bg: ${background})`
          ).toBeGreaterThanOrEqual(WCAG_AA_LARGE_TEXT);
        }
      }
    }
  });

  test('CTA button text meets WCAG AA contrast ratio of 4.5:1', async ({ page }) => {
    // Test CTA button
    const ctaButton = page.locator('.hero__cta');
    await expect(ctaButton).toBeVisible();

    const { ratio, foreground, background } = await measureContrastRatio(page, ctaButton);

    expect(
      ratio,
      `CTA button should have contrast ratio >= ${WCAG_AA_NORMAL_TEXT}:1, got ${ratio.toFixed(2)}:1 (fg: ${foreground}, bg: ${background})`
    ).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
  });

  test('code block text has sufficient contrast', async ({ page }) => {
    // Scroll to quickstart section
    await page.locator('#quickstart').scrollIntoViewIfNeeded();

    // Test main code block text
    const codeBlock = page.locator('.code-block').first();
    await expect(codeBlock).toBeVisible();

    const { ratio, foreground, background } = await measureContrastRatio(page, codeBlock);

    expect(
      ratio,
      `Code block should have contrast ratio >= ${WCAG_AA_NORMAL_TEXT}:1, got ${ratio.toFixed(2)}:1 (fg: ${foreground}, bg: ${background})`
    ).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);

    // Test syntax highlighting elements
    const syntaxElements = [
      { selector: '.code-comment', description: 'code comment' },
      { selector: '.code-keyword', description: 'code keyword' },
      { selector: '.code-number', description: 'code number' },
      { selector: '.code-response', description: 'code response' },
    ];

    for (const { selector, description } of syntaxElements) {
      const element = page.locator(selector).first();
      const isVisible = await element.isVisible().catch(() => false);

      if (isVisible) {
        const result = await measureContrastRatio(page, element);

        expect(
          result.ratio,
          `${description} should have contrast ratio >= ${WCAG_AA_NORMAL_TEXT}:1, got ${result.ratio.toFixed(2)}:1`
        ).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      }
    }
  });

  test('focus indicators have sufficient contrast against all backgrounds', async ({ page }) => {
    // Test focus indicator visibility on various elements
    // Note: Skip-link has its own solid background, so we check outline against page background
    const elementsToTest = [
      { selector: '.nav-menu a', background: 'navigation' },
      { selector: '.hero__cta', background: 'hero section' },
      { selector: '.copy-button', background: 'code block' },
    ];

    for (const { selector, background } of elementsToTest) {
      const element = page.locator(selector).first();
      const isVisible = await element.isVisible().catch(() => false);

      if (isVisible) {
        // Ensure element is in view first
        await element.scrollIntoViewIfNeeded();

        // Focus the element
        await element.focus();

        // Get focus outline styles
        const focusStyles = await element.evaluate((el) => {
          const styles = window.getComputedStyle(el);
          return {
            outlineStyle: styles.outlineStyle,
            outlineWidth: styles.outlineWidth,
            outlineColor: styles.outlineColor,
            outlineOffset: styles.outlineOffset,
          };
        });

        // Verify focus indicator is visible
        const hasVisibleOutline =
          focusStyles.outlineStyle !== 'none' &&
          focusStyles.outlineWidth !== '0px';

        expect(
          hasVisibleOutline,
          `${selector} should have visible focus indicator on ${background}`
        ).toBe(true);

        // If outline is visible, check contrast of outline color against background
        // We need to get the background of the area behind the element (parent or page)
        if (hasVisibleOutline && focusStyles.outlineColor !== 'rgba(0, 0, 0, 0)') {
          const bgColor = await element.evaluate((el) => {
            // Get background color from parent element or page
            let parent = el.parentElement;
            while (parent) {
              const style = window.getComputedStyle(parent);
              const bg = style.backgroundColor;
              if (bg !== 'rgba(0, 0, 0, 0)' && bg !== 'transparent') {
                return bg;
              }
              parent = parent.parentElement;
            }
            return 'rgb(13, 17, 23)'; // Default dark theme background
          });
          const outlineRatio = calculateContrastRatio(focusStyles.outlineColor, bgColor);

          // Focus indicators should have at least 3:1 contrast per WCAG 2.1 Success Criterion 1.4.11
          expect(
            outlineRatio,
            `${selector} focus outline should have contrast ratio >= 3:1 against ${background}, got ${outlineRatio.toFixed(2)}:1`
          ).toBeGreaterThanOrEqual(3.0);
        }
      }
    }

    // Skip link has its own solid background (accent blue) with dark text
    // We verify its text/background contrast separately since it's a special element
    // that provides its own background color
    const skipLink = page.locator('.skip-link');

    // Get the skip-link's own background and text colors directly
    const skipLinkColors = await skipLink.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        color: style.color,
        backgroundColor: style.backgroundColor,
      };
    });

    // Calculate contrast between skip-link's text and its own background
    const skipLinkRatio = calculateContrastRatio(
      skipLinkColors.color,
      skipLinkColors.backgroundColor
    );

    expect(
      skipLinkRatio,
      `Skip link text should have good contrast against its own background (${skipLinkColors.color} vs ${skipLinkColors.backgroundColor})`
    ).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
  });

  test('navigation link text has sufficient contrast', async ({ page }) => {
    // Test navigation links
    const navLinks = page.locator('.nav-menu a');
    const count = await navLinks.count();

    for (let i = 0; i < count; i++) {
      const link = navLinks.nth(i);
      const { ratio, foreground, background } = await measureContrastRatio(page, link);

      expect(
        ratio,
        `Navigation link ${i} should have contrast ratio >= ${WCAG_AA_NORMAL_TEXT}:1, got ${ratio.toFixed(2)}:1`
      ).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
    }
  });

  test('feature card text has sufficient contrast', async ({ page }) => {
    // Scroll to features section
    await page.locator('#features').scrollIntoViewIfNeeded();

    // Test feature titles
    const featureTitles = page.locator('.feature-title');
    const titleCount = await featureTitles.count();

    for (let i = 0; i < titleCount; i++) {
      const title = featureTitles.nth(i);
      const { ratio } = await measureContrastRatio(page, title);

      expect(
        ratio,
        `Feature title ${i} should have contrast ratio >= ${WCAG_AA_LARGE_TEXT}:1, got ${ratio.toFixed(2)}:1`
      ).toBeGreaterThanOrEqual(WCAG_AA_LARGE_TEXT);
    }

    // Test feature descriptions
    const featureDescriptions = page.locator('.feature-description');
    const descCount = await featureDescriptions.count();

    for (let i = 0; i < descCount; i++) {
      const desc = featureDescriptions.nth(i);
      const { ratio } = await measureContrastRatio(page, desc);

      expect(
        ratio,
        `Feature description ${i} should have contrast ratio >= ${WCAG_AA_NORMAL_TEXT}:1, got ${ratio.toFixed(2)}:1`
      ).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
    }
  });

  test('status section text has sufficient contrast', async ({ page }) => {
    // Scroll to status section
    await page.locator('#status').scrollIntoViewIfNeeded();

    // Test feature checklist items
    const featureNames = page.locator('.feature-name');
    const count = await featureNames.count();

    for (let i = 0; i < count; i++) {
      const name = featureNames.nth(i);
      const { ratio } = await measureContrastRatio(page, name);

      expect(
        ratio,
        `Status feature name ${i} should have contrast ratio >= ${WCAG_AA_NORMAL_TEXT}:1, got ${ratio.toFixed(2)}:1`
      ).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
    }

    // Test badge if visible
    const badge = page.locator('.badge');
    const badgeVisible = await badge.isVisible().catch(() => false);

    if (badgeVisible) {
      const { ratio } = await measureContrastRatio(page, badge);

      expect(
        ratio,
        `Status badge should have contrast ratio >= ${WCAG_AA_NORMAL_TEXT}:1, got ${ratio.toFixed(2)}:1`
      ).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
    }
  });
});

/**
 * Accessibility E2E Tests - Alt Text and ARIA
 * Owner: Scenario 10 - Accessibility - Alt Text and ARIA
 *
 * Test cases:
 * - Logo image has descriptive alt text
 * - Usage GIF has descriptive alt text
 * - Copy buttons have aria-label
 * - Decorative icons have aria-hidden
 * - No critical ARIA or alt text violations
 */
test.describe('Accessibility - Alt Text and ARIA', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForPageLoad(page);
  });

  test('logo image has descriptive alt text for MirDB branding', async ({ page }) => {
    // Locate the logo image in the hero section
    const logoImg = page.locator('.hero__logo');

    // Verify the image exists
    await expect(logoImg).toBeVisible();

    // Verify alt attribute exists and is not empty
    const altText = await logoImg.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText.length).toBeGreaterThan(0);

    // Verify alt text describes MirDB branding
    expect(altText.toLowerCase()).toContain('mirdb');
    expect(altText.toLowerCase()).toContain('logo');

    // Verify the src points to logo.gif
    const src = await logoImg.getAttribute('src');
    expect(src).toContain('logo.gif');
  });

  test('usage GIF has descriptive alt text for demonstration', async ({ page }) => {
    // Scroll to quickstart section where usage GIF is
    await page.locator('#quickstart').scrollIntoViewIfNeeded();

    // Locate the usage GIF
    const usageGif = page.locator('.usage-gif');

    // Verify the image exists
    await expect(usageGif).toBeVisible();

    // Verify alt attribute exists and is not empty
    const altText = await usageGif.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText.length).toBeGreaterThan(0);

    // Verify alt text describes usage demonstration
    const altLower = altText.toLowerCase();
    expect(altLower).toMatch(/usage|demo|demonstration|terminal|operations/);

    // Verify the src points to usage.gif
    const src = await usageGif.getAttribute('src');
    expect(src).toContain('usage.gif');
  });

  test('copy buttons have aria-label describing the action', async ({ page }) => {
    // Get all copy buttons on the page
    const copyButtons = page.locator('.copy-button');
    const count = await copyButtons.count();

    // There should be at least one copy button
    expect(count).toBeGreaterThan(0);

    // Check each copy button has an aria-label
    for (let i = 0; i < count; i++) {
      const button = copyButtons.nth(i);
      const ariaLabel = await button.getAttribute('aria-label');

      // Verify aria-label exists
      expect(ariaLabel, `Copy button ${i + 1} should have aria-label`).toBeTruthy();

      // Verify aria-label describes clipboard action
      const labelLower = ariaLabel.toLowerCase();
      expect(labelLower).toMatch(/copy.*clipboard|clipboard.*copy/);
    }
  });

  test('external link icons are marked aria-hidden true', async ({ page }) => {
    // Get all external link icons (decorative icons)
    const externalIcons = page.locator('.external-icon');
    const count = await externalIcons.count();

    // There should be at least one external icon (GitHub and Docs links)
    expect(count).toBeGreaterThan(0);

    // Check each icon has aria-hidden="true"
    for (let i = 0; i < count; i++) {
      const icon = externalIcons.nth(i);
      const ariaHidden = await icon.getAttribute('aria-hidden');

      // Verify aria-hidden is "true"
      expect(ariaHidden, `External icon ${i + 1} should have aria-hidden="true"`).toBe('true');
    }
  });

  test('no critical ARIA or alt text violations in accessibility audit', async ({ page }) => {
    // Check all images have alt text (not empty string)
    const allImages = page.locator('img');
    const imageCount = await allImages.count();

    for (let i = 0; i < imageCount; i++) {
      const img = allImages.nth(i);
      const alt = await img.getAttribute('alt');
      const src = await img.getAttribute('src');

      // Every image must have alt attribute (can be empty for decorative)
      expect(alt !== null, `Image ${src} should have alt attribute`).toBe(true);

      // Non-decorative images should have meaningful alt text
      // CircleCI badge can have simple alt text
      if (!src.includes('circleci')) {
        expect(alt.length, `Image ${src} should have non-empty alt text`).toBeGreaterThan(0);
      }
    }

    // Check buttons have accessible names (aria-label, aria-labelledby, or visible text)
    const allButtons = page.locator('button');
    const buttonCount = await allButtons.count();

    for (let i = 0; i < buttonCount; i++) {
      const button = allButtons.nth(i);
      const ariaLabel = await button.getAttribute('aria-label');
      const ariaLabelledby = await button.getAttribute('aria-labelledby');
      const innerText = await button.innerText();

      // Button must have accessible name
      const hasAccessibleName = !!(ariaLabel || ariaLabelledby || innerText.trim().length > 0);
      expect(hasAccessibleName, `Button ${i + 1} must have accessible name`).toBe(true);
    }

    // Check decorative elements are hidden from assistive technology
    const decorativeIcons = page.locator('.feature-icon, .copy-icon, .checkmark');
    const decorativeCount = await decorativeIcons.count();

    for (let i = 0; i < decorativeCount; i++) {
      const icon = decorativeIcons.nth(i);
      const ariaHidden = await icon.getAttribute('aria-hidden');

      // Decorative icons should be hidden from AT
      expect(ariaHidden, `Decorative icon ${i + 1} should have aria-hidden`).toBe('true');
    }

    // Check navigation has aria-label
    const nav = page.locator('nav');
    const navAriaLabel = await nav.getAttribute('aria-label');
    expect(navAriaLabel, 'Navigation should have aria-label').toBeTruthy();

    // Check sections have aria-labelledby
    const sections = page.locator('section[aria-labelledby]');
    const sectionCount = await sections.count();
    expect(sectionCount, 'Sections should use aria-labelledby').toBeGreaterThan(0);
  });

  test('feature icons in features section have aria-hidden', async ({ page }) => {
    // Scroll to features section
    await page.locator('#features').scrollIntoViewIfNeeded();

    // Check feature icons (SVG icons) are hidden from AT
    const featureIcons = page.locator('.feature-icon');
    const count = await featureIcons.count();

    expect(count).toBeGreaterThan(0);

    for (let i = 0; i < count; i++) {
      const icon = featureIcons.nth(i);
      const ariaHidden = await icon.getAttribute('aria-hidden');
      expect(ariaHidden, `Feature icon ${i + 1} should be aria-hidden`).toBe('true');
    }
  });

  test('copy button icons are decorative and aria-hidden', async ({ page }) => {
    // Check copy icons within buttons are hidden
    const copyIcons = page.locator('.copy-icon');
    const count = await copyIcons.count();

    expect(count).toBeGreaterThan(0);

    for (let i = 0; i < count; i++) {
      const icon = copyIcons.nth(i);
      const ariaHidden = await icon.getAttribute('aria-hidden');
      expect(ariaHidden, `Copy icon ${i + 1} should be aria-hidden`).toBe('true');
    }
  });

  test('status checkmarks are decorative and aria-hidden', async ({ page }) => {
    // Scroll to status section
    await page.locator('#status').scrollIntoViewIfNeeded();

    // Check checkmark icons are hidden from AT
    const checkmarks = page.locator('.checkmark');
    const count = await checkmarks.count();

    expect(count).toBeGreaterThan(0);

    for (let i = 0; i < count; i++) {
      const checkmark = checkmarks.nth(i);
      const ariaHidden = await checkmark.getAttribute('aria-hidden');
      expect(ariaHidden, `Checkmark ${i + 1} should be aria-hidden`).toBe('true');
    }
  });

  test('CircleCI badge has alt text', async ({ page }) => {
    // Scroll to status section
    await page.locator('#status').scrollIntoViewIfNeeded();

    // Find the CircleCI badge image
    const badge = page.locator('.status-badge img');

    // Verify badge exists
    await expect(badge).toBeVisible();

    // Verify it has alt text
    const altText = await badge.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText.toLowerCase()).toMatch(/circleci|build|status|badge/);
  });
});

/**
 * Accessibility - Hover and Focus States Tests
 * Owner: Scenario 18 - Hover and Focus States
 *
 * Test cases:
 * - CTA button shows visible hover state (color change, shadow, etc.)
 * - Navigation links show visible hover state (underline, color change, etc.)
 * - CTA button shows distinct focus outline/ring
 * - Copy button shows hover state indicating interactivity
 * - Focus and hover states are distinguishable or combined appropriately
 */
test.describe('Accessibility - Hover and Focus States', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForPageLoad(page);
  });

  test('CTA button shows visible hover state with color change', async ({ page }) => {
    const ctaButton = page.locator('.hero__cta');
    await expect(ctaButton).toBeVisible();

    // Get initial styles before hover
    const initialStyles = await ctaButton.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        backgroundColor: styles.backgroundColor,
        transform: styles.transform,
        color: styles.color,
      };
    });

    // Hover over the CTA button
    await ctaButton.hover();

    // Wait for transition to complete
    await page.waitForTimeout(200);

    // Get styles after hover
    const hoverStyles = await ctaButton.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        backgroundColor: styles.backgroundColor,
        transform: styles.transform,
        color: styles.color,
      };
    });

    // Verify at least one visual change occurred (background color or transform)
    const hasBackgroundChange = initialStyles.backgroundColor !== hoverStyles.backgroundColor;
    const hasTransformChange = initialStyles.transform !== hoverStyles.transform;

    expect(
      hasBackgroundChange || hasTransformChange,
      `CTA button should show visible hover state change. Initial: bg=${initialStyles.backgroundColor}, transform=${initialStyles.transform}. Hover: bg=${hoverStyles.backgroundColor}, transform=${hoverStyles.transform}`
    ).toBe(true);
  });

  test('navigation links show visible hover state with color change', async ({ page }) => {
    // Test each navigation link for hover state
    const navLinks = page.locator('.nav-menu a');
    const count = await navLinks.count();

    expect(count).toBeGreaterThan(0);

    // Test the first navigation link (internal link)
    const firstLink = navLinks.first();

    // Get initial color
    const initialColor = await firstLink.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    // Hover over the link
    await firstLink.hover();

    // Wait for transition
    await page.waitForTimeout(200);

    // Get hover color
    const hoverColor = await firstLink.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    // Verify color changed on hover
    expect(
      initialColor !== hoverColor,
      `Navigation link should change color on hover. Initial: ${initialColor}, Hover: ${hoverColor}`
    ).toBe(true);
  });

  test('CTA button shows distinct focus outline/ring when focused', async ({ page }) => {
    const ctaButton = page.locator('.hero__cta');

    // Focus the button via keyboard simulation
    await ctaButton.focus();

    // Get focus styles
    const focusStyles = await ctaButton.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        outline: styles.outline,
        outlineStyle: styles.outlineStyle,
        outlineWidth: styles.outlineWidth,
        outlineColor: styles.outlineColor,
        outlineOffset: styles.outlineOffset,
      };
    });

    // Verify focus indicator is visible
    const hasOutline = focusStyles.outlineStyle !== 'none' && focusStyles.outlineWidth !== '0px';

    expect(
      hasOutline,
      `CTA button should have visible focus outline. Got: style=${focusStyles.outlineStyle}, width=${focusStyles.outlineWidth}, color=${focusStyles.outlineColor}`
    ).toBe(true);

    // Verify outline offset provides visual separation
    const outlineOffset = parseInt(focusStyles.outlineOffset, 10);
    expect(
      outlineOffset >= 0,
      `CTA button focus outline should have positive offset. Got: ${focusStyles.outlineOffset}`
    ).toBe(true);
  });

  test('copy button shows hover state indicating interactivity', async ({ page }) => {
    // Scroll to quickstart section to make copy button visible
    await page.locator('#quickstart').scrollIntoViewIfNeeded();

    const copyButton = page.locator('.copy-button').first();
    await expect(copyButton).toBeVisible();

    // Get initial styles
    const initialStyles = await copyButton.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        backgroundColor: styles.backgroundColor,
        color: styles.color,
        borderColor: styles.borderColor,
        cursor: styles.cursor,
      };
    });

    // Verify cursor indicates interactivity
    expect(initialStyles.cursor).toBe('pointer');

    // Hover over the copy button
    await copyButton.hover();

    // Wait for transition
    await page.waitForTimeout(200);

    // Get hover styles
    const hoverStyles = await copyButton.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        backgroundColor: styles.backgroundColor,
        color: styles.color,
        borderColor: styles.borderColor,
      };
    });

    // Verify at least one visual change occurred
    const hasBackgroundChange = initialStyles.backgroundColor !== hoverStyles.backgroundColor;
    const hasColorChange = initialStyles.color !== hoverStyles.color;
    const hasBorderChange = initialStyles.borderColor !== hoverStyles.borderColor;

    expect(
      hasBackgroundChange || hasColorChange || hasBorderChange,
      `Copy button should show visible hover state. Initial: bg=${initialStyles.backgroundColor}, color=${initialStyles.color}. Hover: bg=${hoverStyles.backgroundColor}, color=${hoverStyles.color}`
    ).toBe(true);
  });

  test('focus and hover states are distinguishable or combined appropriately', async ({ page }) => {
    const ctaButton = page.locator('.hero__cta');
    await expect(ctaButton).toBeVisible();

    // Get initial/normal state
    const normalStyles = await ctaButton.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        backgroundColor: styles.backgroundColor,
        outline: styles.outline,
        outlineStyle: styles.outlineStyle,
        outlineWidth: styles.outlineWidth,
      };
    });

    // Get hover state
    await ctaButton.hover();
    await page.waitForTimeout(200);

    const hoverStyles = await ctaButton.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        backgroundColor: styles.backgroundColor,
        outline: styles.outline,
        outlineStyle: styles.outlineStyle,
        outlineWidth: styles.outlineWidth,
      };
    });

    // Move mouse away and focus the button
    await page.mouse.move(0, 0);
    await ctaButton.focus();

    const focusStyles = await ctaButton.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        backgroundColor: styles.backgroundColor,
        outline: styles.outline,
        outlineStyle: styles.outlineStyle,
        outlineWidth: styles.outlineWidth,
      };
    });

    // Verify that focus state has an outline (distinct from hover)
    const focusHasOutline =
      focusStyles.outlineStyle !== 'none' && focusStyles.outlineWidth !== '0px';

    // Hover primarily changes background, focus primarily adds outline
    // They should either be distinguishable or combined appropriately
    const hoverChangesBackground = normalStyles.backgroundColor !== hoverStyles.backgroundColor;
    const focusAddsOutline =
      (normalStyles.outlineStyle === 'none' || normalStyles.outlineWidth === '0px') &&
      focusHasOutline;

    // Either focus adds distinct outline, or the states are combined appropriately
    expect(
      focusAddsOutline || focusHasOutline,
      `Focus state should be distinguishable via outline. Normal outline: ${normalStyles.outlineStyle}/${normalStyles.outlineWidth}. Focus outline: ${focusStyles.outlineStyle}/${focusStyles.outlineWidth}`
    ).toBe(true);

    // Additionally verify that when both hover and focus are active, the state is visible
    await ctaButton.hover();
    await page.waitForTimeout(100);

    const combinedStyles = await ctaButton.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        backgroundColor: styles.backgroundColor,
        outlineStyle: styles.outlineStyle,
        outlineWidth: styles.outlineWidth,
      };
    });

    // Combined state should maintain focus outline
    const combinedHasOutline =
      combinedStyles.outlineStyle !== 'none' && combinedStyles.outlineWidth !== '0px';
    expect(
      combinedHasOutline,
      `Combined hover+focus state should maintain focus outline. Got: ${combinedStyles.outlineStyle}/${combinedStyles.outlineWidth}`
    ).toBe(true);
  });

  test('navigation brand link shows hover state', async ({ page }) => {
    const brandLink = page.locator('.nav-brand a');

    // Get initial color
    const initialColor = await brandLink.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    // Hover over the brand link
    await brandLink.hover();
    await page.waitForTimeout(200);

    // Get hover color
    const hoverColor = await brandLink.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    // Verify color change on hover
    expect(
      initialColor !== hoverColor,
      `Navigation brand link should change color on hover. Initial: ${initialColor}, Hover: ${hoverColor}`
    ).toBe(true);
  });

  test('all navigation links have hover state', async ({ page }) => {
    const navLinks = page.locator('.nav-menu a');
    const count = await navLinks.count();

    // Test each navigation link
    for (let i = 0; i < count; i++) {
      const link = navLinks.nth(i);

      // Get initial color
      const initialColor = await link.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });

      // Hover
      await link.hover();
      await page.waitForTimeout(150);

      // Get hover color
      const hoverColor = await link.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });

      // Verify hover state change
      expect(
        initialColor !== hoverColor,
        `Navigation link ${i + 1} should show hover state. Initial: ${initialColor}, Hover: ${hoverColor}`
      ).toBe(true);
    }
  });

  test('all copy buttons have focus state', async ({ page }) => {
    // Scroll to quickstart section
    await page.locator('#quickstart').scrollIntoViewIfNeeded();

    const copyButtons = page.locator('.copy-button');
    const count = await copyButtons.count();

    expect(count).toBeGreaterThan(0);

    for (let i = 0; i < count; i++) {
      const button = copyButtons.nth(i);
      await button.scrollIntoViewIfNeeded();

      // Focus the button
      await button.focus();

      // Get focus styles
      const focusStyles = await button.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          outlineStyle: styles.outlineStyle,
          outlineWidth: styles.outlineWidth,
        };
      });

      // Verify focus outline
      const hasOutline = focusStyles.outlineStyle !== 'none' && focusStyles.outlineWidth !== '0px';
      expect(
        hasOutline,
        `Copy button ${i + 1} should have visible focus outline. Got: style=${focusStyles.outlineStyle}, width=${focusStyles.outlineWidth}`
      ).toBe(true);
    }
  });
});
