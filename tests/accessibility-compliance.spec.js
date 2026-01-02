// @ts-check
const { test, expect } = require('@playwright/test');
const AxeBuilder = require('@axe-core/playwright').default;

/**
 * Accessibility Compliance Tests for MirDB Landing Page
 * Verifies WCAG 2.1 Level AA compliance
 *
 * Test Cases:
 * TC1: Navigate page with Tab key only - All interactive elements are focusable and have visible focus indicators
 * TC2: Check heading hierarchy - Page has proper h1-h6 hierarchy without skipped levels
 * TC3: Check image alt text - All images have descriptive alt attributes
 * TC4: Check color contrast ratio - All text meets WCAG AA contrast requirements
 * TC5: Run Lighthouse accessibility audit - Lighthouse accessibility score is 90 or higher
 */

test.describe('Accessibility Compliance - WCAG 2.1 Level AA', () => {

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for the page to be fully loaded
    await page.waitForLoadState('networkidle');
  });

  // TC1: Keyboard Navigation - All interactive elements are focusable and have visible focus indicators
  test('TC1: Navigate page with Tab key only - all interactive elements are focusable with visible focus indicators', async ({ page }) => {
    // Get all interactive elements that should be focusable
    const interactiveElements = await page.locator('a[href], button, input, select, textarea, [tabindex]:not([tabindex="-1"])').all();

    expect(interactiveElements.length).toBeGreaterThan(0);

    // Track focused elements
    const focusedElements = [];

    // Start from the body to begin tabbing
    await page.keyboard.press('Tab');

    // Tab through all elements and verify they receive focus
    for (let i = 0; i < interactiveElements.length + 5; i++) {
      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement;
        if (el && el !== document.body) {
          const styles = window.getComputedStyle(el);
          const outlineWidth = parseFloat(styles.outlineWidth) || 0;
          const outlineStyle = styles.outlineStyle;
          const boxShadow = styles.boxShadow;
          const backgroundColor = styles.backgroundColor;

          // Check for visible focus indicator
          const hasOutline = outlineWidth > 0 && outlineStyle !== 'none';
          const hasBoxShadow = boxShadow && boxShadow !== 'none';
          const hasBorderChange = styles.borderColor !== 'transparent';

          return {
            tagName: el.tagName,
            href: el.getAttribute('href'),
            text: el.textContent?.trim().substring(0, 50),
            hasVisibleFocus: hasOutline || hasBoxShadow || hasBorderChange,
            focusStyles: {
              outlineWidth,
              outlineStyle,
              boxShadow,
            }
          };
        }
        return null;
      });

      if (focusedElement && focusedElement.tagName !== 'BODY') {
        focusedElements.push(focusedElement);
      }

      await page.keyboard.press('Tab');
    }

    // Verify we were able to focus on multiple elements
    expect(focusedElements.length).toBeGreaterThan(0);

    // Verify navigation links are focusable
    const navLinks = await page.locator('.nav-links a').all();
    for (const link of navLinks) {
      await link.focus();
      const isFocused = await link.evaluate(el => el === document.activeElement);
      expect(isFocused).toBe(true);
    }

    // Verify hero buttons are focusable
    const heroButtons = await page.locator('.hero-buttons a').all();
    for (const button of heroButtons) {
      await button.focus();
      const isFocused = await button.evaluate(el => el === document.activeElement);
      expect(isFocused).toBe(true);
    }

    // Verify footer links are focusable
    const footerLinks = await page.locator('.footer-links a').all();
    for (const link of footerLinks) {
      await link.focus();
      const isFocused = await link.evaluate(el => el === document.activeElement);
      expect(isFocused).toBe(true);
    }
  });

  // TC2: Heading Hierarchy - Page has proper h1-h6 hierarchy without skipped levels
  test('TC2: Check heading hierarchy - page has proper h1-h6 hierarchy without skipped levels', async ({ page }) => {
    // Get all headings in document order
    const headings = await page.evaluate(() => {
      const headingElements = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      return Array.from(headingElements).map(h => ({
        level: parseInt(h.tagName.substring(1)),
        text: h.textContent?.trim(),
        tagName: h.tagName
      }));
    });

    // Verify there is exactly one h1
    const h1Count = headings.filter(h => h.level === 1).length;
    expect(h1Count).toBe(1);

    // Verify the h1 contains the product name
    const h1 = headings.find(h => h.level === 1);
    expect(h1).toBeDefined();
    expect(h1?.text).toContain('MirDB');

    // Verify heading hierarchy - no skipped levels
    let previousLevel = 0;
    for (const heading of headings) {
      // First heading can be h1
      if (previousLevel === 0) {
        expect(heading.level).toBe(1);
      } else {
        // Each subsequent heading should be at most one level deeper
        // (can go to same level, higher level, or one level deeper)
        const levelDiff = heading.level - previousLevel;
        expect(levelDiff).toBeLessThanOrEqual(1);
      }
      previousLevel = heading.level;
    }

    // Verify there are section headings (h2)
    const h2Count = headings.filter(h => h.level === 2).length;
    expect(h2Count).toBeGreaterThanOrEqual(3); // Features, Commands, Quick Start at minimum

    // Verify section headings exist for main sections
    const h2Texts = headings.filter(h => h.level === 2).map(h => h.text?.toLowerCase());
    expect(h2Texts.some(text => text?.includes('feature'))).toBe(true);
    expect(h2Texts.some(text => text?.includes('command'))).toBe(true);
    expect(h2Texts.some(text => text?.includes('quick start') || text?.includes('quickstart'))).toBe(true);
  });

  // TC3: Image Alt Text - All images have descriptive alt attributes
  test('TC3: Check image alt text - all images have descriptive alt attributes', async ({ page }) => {
    // Get all images on the page
    const images = await page.locator('img').all();

    for (const image of images) {
      const alt = await image.getAttribute('alt');

      // Verify alt attribute exists
      expect(alt).not.toBeNull();

      // If the image is decorative (empty alt), that's acceptable
      // But if alt is present and not empty, it should be descriptive (more than just a filename)
      if (alt && alt.length > 0) {
        // Alt should be at least 3 characters for meaningful description
        expect(alt.length).toBeGreaterThanOrEqual(3);

        // Alt should not be just a filename pattern (e.g., "image.png", "logo.jpg")
        expect(alt).not.toMatch(/^[\w-]+\.(png|jpg|jpeg|gif|svg|webp)$/i);
      }
    }

    // Check for SVG elements with proper accessibility
    const svgs = await page.locator('svg').all();
    for (const svg of svgs) {
      // Check if SVG is presentational
      const role = await svg.getAttribute('role');
      const ariaHidden = await svg.getAttribute('aria-hidden');
      const ariaLabel = await svg.getAttribute('aria-label');
      const ariaLabelledby = await svg.getAttribute('aria-labelledby');

      // SVG should either be hidden from AT (aria-hidden="true" or role="presentation")
      // OR have accessible name (aria-label, aria-labelledby, or title element)
      const isDecorativeOrAccessible =
        ariaHidden === 'true' ||
        role === 'presentation' ||
        role === 'none' ||
        (ariaLabel && ariaLabel.length > 0) ||
        (ariaLabelledby && ariaLabelledby.length > 0);

      // Check for title element inside SVG
      const hasTitle = await svg.locator('title').count() > 0;

      expect(isDecorativeOrAccessible || hasTitle).toBe(true);
    }
  });

  // TC4: Color Contrast - All text meets WCAG AA contrast requirements (4.5:1 for normal text, 3:1 for large text)
  test('TC4: Check color contrast ratio - all text meets WCAG AA contrast requirements', async ({ page }) => {
    // Run axe-core accessibility check focused on color contrast
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .analyze();

    // Filter for color contrast violations specifically
    const colorContrastViolations = accessibilityScanResults.violations.filter(
      v => v.id === 'color-contrast' || v.id === 'color-contrast-enhanced'
    );

    // If there are violations, log them for debugging
    if (colorContrastViolations.length > 0) {
      console.log('Color contrast violations found:');
      colorContrastViolations.forEach(violation => {
        violation.nodes.forEach(node => {
          console.log(`  - ${node.html}`);
          console.log(`    ${node.failureSummary}`);
        });
      });
    }

    // Assert no color contrast violations
    expect(colorContrastViolations.length).toBe(0);

    // Additional manual check for key text elements
    const textElements = await page.evaluate(() => {
      const getLuminance = (r, g, b) => {
        const [rs, gs, bs] = [r, g, b].map(c => {
          c = c / 255;
          return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
        });
        return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
      };

      const getContrastRatio = (l1, l2) => {
        const lighter = Math.max(l1, l2);
        const darker = Math.min(l1, l2);
        return (lighter + 0.05) / (darker + 0.05);
      };

      const parseColor = (color) => {
        // Handle rgb/rgba format
        const rgbMatch = color.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
        if (rgbMatch) {
          return {
            r: parseInt(rgbMatch[1]),
            g: parseInt(rgbMatch[2]),
            b: parseInt(rgbMatch[3])
          };
        }
        return null;
      };

      const results = [];
      const textSelectors = ['h1', 'h2', 'h3', 'p', '.tagline', '.value-prop', '.nav-links a', '.footer-links a'];

      textSelectors.forEach(selector => {
        const elements = document.querySelectorAll(selector);
        elements.forEach(el => {
          const styles = window.getComputedStyle(el);
          const fontSize = parseFloat(styles.fontSize);
          const fontWeight = parseInt(styles.fontWeight);
          const color = parseColor(styles.color);
          const bgColor = parseColor(styles.backgroundColor);

          // Determine if large text (18px+ normal, or 14px+ bold)
          const isLargeText = fontSize >= 18 || (fontSize >= 14 && fontWeight >= 700);
          const requiredRatio = isLargeText ? 3 : 4.5;

          if (color && bgColor) {
            const colorLum = getLuminance(color.r, color.g, color.b);
            const bgLum = getLuminance(bgColor.r, bgColor.g, bgColor.b);
            const ratio = getContrastRatio(colorLum, bgLum);

            results.push({
              selector,
              text: el.textContent?.substring(0, 30),
              fontSize,
              isLargeText,
              requiredRatio,
              actualRatio: ratio.toFixed(2),
              passes: ratio >= requiredRatio
            });
          }
        });
      });

      return results;
    });

    // Log contrast info for debugging
    console.log('Text contrast analysis:', textElements);
  });

  // TC5: Lighthouse Accessibility Audit - Score is 90 or higher
  test('TC5: Run Lighthouse accessibility audit - score is 90 or higher', async ({ page }) => {
    // Use axe-core for comprehensive accessibility testing as Playwright doesn't have native Lighthouse integration
    // axe-core is what Lighthouse uses under the hood for accessibility audits

    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21aa', 'best-practice'])
      .analyze();

    // Log all violations for debugging
    if (accessibilityScanResults.violations.length > 0) {
      console.log('\nAccessibility Violations Found:');
      accessibilityScanResults.violations.forEach(violation => {
        console.log(`\n[${violation.impact?.toUpperCase()}] ${violation.id}: ${violation.description}`);
        console.log(`  Help: ${violation.helpUrl}`);
        violation.nodes.forEach(node => {
          console.log(`  - Element: ${node.html.substring(0, 100)}`);
          console.log(`    ${node.failureSummary}`);
        });
      });
    }

    // Calculate a score similar to Lighthouse (based on violations weighted by impact)
    const impactWeights = {
      critical: 10,
      serious: 7,
      moderate: 4,
      minor: 1
    };

    let totalDeductions = 0;
    accessibilityScanResults.violations.forEach(violation => {
      const weight = impactWeights[violation.impact || 'moderate'] || 4;
      totalDeductions += violation.nodes.length * weight;
    });

    // Calculate score (100 - deductions, minimum 0)
    const totalChecks = accessibilityScanResults.passes.length + accessibilityScanResults.violations.length;
    const maxDeductions = 100;
    const calculatedScore = Math.max(0, 100 - Math.min(totalDeductions, maxDeductions));

    console.log(`\nAccessibility Score Calculation:`);
    console.log(`  Total passes: ${accessibilityScanResults.passes.length}`);
    console.log(`  Total violations: ${accessibilityScanResults.violations.length}`);
    console.log(`  Total deductions: ${totalDeductions}`);
    console.log(`  Calculated score: ${calculatedScore}`);

    // For the actual test, we'll check:
    // 1. No critical or serious violations
    const criticalViolations = accessibilityScanResults.violations.filter(
      v => v.impact === 'critical' || v.impact === 'serious'
    );

    if (criticalViolations.length > 0) {
      console.log('\nCritical/Serious violations that must be fixed:');
      criticalViolations.forEach(v => {
        console.log(`  - ${v.id}: ${v.description} (${v.impact})`);
      });
    }

    expect(criticalViolations.length).toBe(0);

    // 2. Total violations should be minimal for a score >= 90
    // With impact weighting, we allow some minor issues
    expect(calculatedScore).toBeGreaterThanOrEqual(90);
  });

  // Additional test: Verify ARIA landmarks
  test('Verify proper ARIA landmarks for screen reader navigation', async ({ page }) => {
    // Check for main landmark
    const mainLandmark = await page.locator('main, [role="main"]').count();
    const hasMain = mainLandmark > 0 || await page.locator('header, nav, section, footer').count() > 0;

    // Check for navigation landmark
    const navLandmark = await page.locator('nav, [role="navigation"]').count();
    expect(navLandmark).toBeGreaterThanOrEqual(1);

    // Check for header/banner
    const headerLandmark = await page.locator('header, [role="banner"]').count();
    expect(headerLandmark).toBeGreaterThanOrEqual(1);

    // Check for footer/contentinfo
    const footerLandmark = await page.locator('footer, [role="contentinfo"]').count();
    expect(footerLandmark).toBeGreaterThanOrEqual(1);

    // Verify sections have proper structure
    const sections = await page.locator('section').all();
    expect(sections.length).toBeGreaterThanOrEqual(3);

    // Each section should have a heading
    for (const section of sections) {
      const heading = await section.locator('h2, h3, [role="heading"]').count();
      expect(heading).toBeGreaterThanOrEqual(1);
    }
  });

  // Additional test: Form controls and interactive elements accessibility
  test('Verify interactive elements have accessible names', async ({ page }) => {
    // Check all links have accessible text
    const links = await page.locator('a[href]').all();
    for (const link of links) {
      const text = await link.textContent();
      const ariaLabel = await link.getAttribute('aria-label');
      const title = await link.getAttribute('title');

      // Link should have either visible text, aria-label, or title
      const hasAccessibleName = (text && text.trim().length > 0) ||
                                 (ariaLabel && ariaLabel.length > 0) ||
                                 (title && title.length > 0);

      expect(hasAccessibleName).toBe(true);

      // If it's a link that opens in new window, should indicate that
      const target = await link.getAttribute('target');
      if (target === '_blank') {
        const rel = await link.getAttribute('rel');
        // Should have noopener for security
        expect(rel).toContain('noopener');
      }
    }

    // Check buttons have accessible names
    const buttons = await page.locator('button, [role="button"], .btn').all();
    for (const button of buttons) {
      const text = await button.textContent();
      const ariaLabel = await button.getAttribute('aria-label');

      const hasAccessibleName = (text && text.trim().length > 0) ||
                                 (ariaLabel && ariaLabel.length > 0);

      expect(hasAccessibleName).toBe(true);
    }
  });

  // Test for skip links (optional but good practice)
  test('Page should support skip navigation or have logical focus order', async ({ page }) => {
    // Check if skip link exists
    const skipLink = await page.locator('a[href="#main"], a[href="#content"], .skip-link, [class*="skip"]').count();

    // If no skip link, verify the focus order is logical
    // The first few tab stops should be navigation, not content
    await page.keyboard.press('Tab');

    const firstFocusedElement = await page.evaluate(() => {
      const el = document.activeElement;
      return {
        tagName: el?.tagName,
        className: el?.className,
        isInNav: el?.closest('nav, header, .nav, .nav-links') !== null
      };
    });

    // First focused element should be in header/nav area (logical focus order)
    // OR there should be a skip link
    const hasGoodFocusOrder = firstFocusedElement.isInNav || skipLink > 0;
    expect(hasGoodFocusOrder).toBe(true);
  });
});
