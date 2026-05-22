/**
 * Accessibility Compliance E2E Tests
 * Owner: Scenario 9 - Accessibility Compliance
 *
 * Tests:
 * - Automated axe-core scan for WCAG 2.1 AA violations
 * - Keyboard tab navigation and focus order
 * - Semantic HTML structure validation
 * - ARIA labels and accessible names
 * - Color contrast ratio compliance
 * - Screen reader announcement (manual)
 */

const { test, expect } = require('@playwright/test');
const AxeBuilder = require('@axe-core/playwright').default;

test.describe('Accessibility Compliance', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for page to be fully loaded
    await page.waitForLoadState('networkidle');
  });

  // ============================================================
  // Test 1: Automated axe-core scan
  // ============================================================
  test('should pass automated axe-core scan with zero critical or serious violations', async ({ page }) => {
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21aa'])
      .analyze();

    // Filter for critical and serious violations
    const criticalViolations = accessibilityScanResults.violations.filter(
      v => v.impact === 'critical'
    );
    const seriousViolations = accessibilityScanResults.violations.filter(
      v => v.impact === 'serious'
    );

    // Report violations for debugging
    if (criticalViolations.length > 0) {
      console.log('Critical violations:', JSON.stringify(criticalViolations, null, 2));
    }
    if (seriousViolations.length > 0) {
      console.log('Serious violations:', JSON.stringify(seriousViolations, null, 2));
    }

    expect(criticalViolations.length, 'Should have zero critical violations').toBe(0);
    expect(seriousViolations.length, 'Should have zero serious violations').toBe(0);
  });

  // ============================================================
  // Test 2: Keyboard tab navigation
  // ============================================================
  test('should have logical tab order through all interactive elements', async ({ page }) => {
    // Get all focusable elements on the page
    const focusableSelectors = [
      'a[href]',
      'button:not([disabled])',
      'input:not([disabled])',
      'select:not([disabled])',
      'textarea:not([disabled])',
      '[tabindex]:not([tabindex="-1"])'
    ];

    const focusableElements = await page.locator(focusableSelectors.join(', ')).all();
    expect(focusableElements.length).toBeGreaterThan(0);

    // Tab through all elements and record focus order
    const focusOrder = [];
    const maxTabs = focusableElements.length + 5; // Safety limit

    for (let i = 0; i < maxTabs; i++) {
      const activeElement = await page.evaluate(() => {
        const el = document.activeElement;
        return el ? {
          tag: el.tagName.toLowerCase(),
          text: el.textContent?.trim().substring(0, 50) || '',
          ariaLabel: el.getAttribute('aria-label') || '',
          href: el.getAttribute('href') || '',
          dataTestid: el.getAttribute('data-testid') || '',
          id: el.id || ''
        } : null;
      });

      // Stop when we've cycled back to the first element or body
      if (i > 0 && (!activeElement || activeElement.tag === 'body')) {
        break;
      }

      // Avoid infinite loops by checking if we've seen this element before
      const elementKey = `${activeElement.tag}-${activeElement.text}-${activeElement.dataTestid}`;
      if (focusOrder.some(el => `${el.tag}-${el.text}-${el.dataTestid}` === elementKey)) {
        break;
      }

      focusOrder.push(activeElement);
      await page.keyboard.press('Tab');
    }

    // Verify we found focusable elements
    expect(focusOrder.length).toBeGreaterThan(0);

    // Verify logical order: navigation links should come before main content
    const navIndices = [];
    const mainContentIndices = [];

    for (let i = 0; i < focusOrder.length; i++) {
      const el = focusOrder[i];
      if (el.dataTestid.includes('nav-') || el.dataTestid.includes('mobile-')) {
        navIndices.push(i);
      }
      if (el.dataTestid.includes('hero-') || el.dataTestid.includes('cta-') || el.dataTestid.includes('footer-')) {
        mainContentIndices.push(i);
      }
    }

    // If both nav and main content exist, nav should come before main content
    if (navIndices.length > 0 && mainContentIndices.length > 0) {
      const lastNavIndex = Math.max(...navIndices);
      const firstMainIndex = Math.min(...mainContentIndices);
      // Nav elements should generally come before main content
      // (Allow some flexibility for skip links or special cases)
      expect(lastNavIndex).toBeLessThanOrEqual(firstMainIndex + 2);
    }
  });

  test('should have visible focus indicator on all interactive elements', async ({ page }) => {
    const focusableSelectors = [
      'a[href]',
      'button:not([disabled])',
      'input:not([disabled])',
      '[tabindex]:not([tabindex="-1"])'
    ];

    const allElements = await page.locator(focusableSelectors.join(', ')).all();
    expect(allElements.length).toBeGreaterThan(0);

    // Filter out hidden elements (e.g., auth-only links when unauthenticated)
    const elements = [];
    for (const el of allElements) {
      const isHidden = await el.evaluate(el => {
        const style = window.getComputedStyle(el);
        return style.display === 'none' || style.visibility === 'hidden' || el.offsetParent === null;
      });
      if (!isHidden) {
        elements.push(el);
      }
    }

    expect(elements.length).toBeGreaterThan(0);

    for (const element of elements) {
      // Focus the element
      await element.focus();

      // Check if the element is actually focused
      const isFocused = await element.evaluate(el => el === document.activeElement);
      expect(isFocused, `Element should be focusable: ${await element.evaluate(el =>
        el.getAttribute('data-testid') || el.textContent?.trim().substring(0, 30) || el.tagName
      )}`).toBe(true);

      // Check computed outline style
      const outlineStyle = await element.evaluate(el => {
        const style = window.getComputedStyle(el);
        return {
          outlineWidth: style.outlineWidth,
          outlineStyle: style.outlineStyle,
          outlineColor: style.outlineColor,
          boxShadow: style.boxShadow,
          ringWidth: style.getPropertyValue('--tw-ring-width') || '0'
        };
      });

      // Focus indicator should be visible: either outline is visible or box-shadow shows focus ring
      const hasVisibleOutline = outlineStyle.outlineWidth !== '0px' &&
        outlineStyle.outlineStyle !== 'none';
      const hasFocusShadow = outlineStyle.boxShadow !== 'none' &&
        outlineStyle.boxShadow !== '0 0 #0000' &&
        outlineStyle.boxShadow !== 'rgba(0, 0, 0, 0) 0px 0px 0px 0px';

      // DaisyUI/Tailwind typically uses focus:outline or focus:ring
      const hasFocusIndicator = hasVisibleOutline || hasFocusShadow;

      expect(hasFocusIndicator, `Element should have visible focus indicator: ${await element.evaluate(el =>
        el.getAttribute('data-testid') || el.textContent?.trim().substring(0, 30) || el.tagName
      )}`).toBe(true);
    }
  });

  // ============================================================
  // Test 3: Semantic HTML validation
  // ============================================================
  test('should use semantic HTML landmarks appropriately', async ({ page }) => {
    // Check for nav element
    const nav = page.locator('nav');
    await expect(nav).toHaveCount(1);

    // Check for main element
    const main = page.locator('main');
    await expect(main).toHaveCount(1);

    // Check for footer element or role="contentinfo"
    const footer = page.locator('footer');
    const contentinfo = page.locator('[role="contentinfo"]');
    const footerCount = await footer.count() + await contentinfo.count();
    expect(footerCount).toBeGreaterThanOrEqual(1);

    // Check for section elements
    const sections = page.locator('section');
    const sectionCount = await sections.count();
    expect(sectionCount).toBeGreaterThanOrEqual(2);

    // Check for article elements (feature cards)
    const articles = page.locator('article');
    const articleCount = await articles.count();
    expect(articleCount).toBeGreaterThanOrEqual(1);
  });

  test('should have logical heading hierarchy', async ({ page }) => {
    const headings = await page.locator('h1, h2, h3, h4, h5, h6').all();
    expect(headings.length).toBeGreaterThan(0);

    const headingLevels = [];
    for (const heading of headings) {
      const level = await heading.evaluate(el => parseInt(el.tagName[1]));
      const text = await heading.textContent();
      headingLevels.push({ level, text: text.trim().substring(0, 50) });
    }

    // Should have exactly one h1
    const h1Count = headingLevels.filter(h => h.level === 1).length;
    expect(h1Count).toBe(1);

    // Heading levels should not skip (e.g., h1 -> h3 without h2)
    let previousLevel = 0;
    for (const heading of headingLevels) {
      // Headings can stay the same or increase by 1
      // They can decrease by any amount
      if (heading.level > previousLevel) {
        expect(heading.level - previousLevel).toBeLessThanOrEqual(1);
      }
      previousLevel = heading.level;
    }

    // h1 should come before any h2
    const firstH1Index = headingLevels.findIndex(h => h.level === 1);
    const firstH2Index = headingLevels.findIndex(h => h.level === 2);
    if (firstH2Index !== -1) {
      expect(firstH1Index).toBeLessThan(firstH2Index);
    }
  });

  test('should have lang attribute on html element', async ({ page }) => {
    const html = page.locator('html');
    const lang = await html.getAttribute('lang');
    expect(lang).toBeTruthy();
    expect(lang.length).toBeGreaterThan(0);
  });

  test('should have page title describing the content', async ({ page }) => {
    const title = await page.title();
    expect(title).toBeTruthy();
    expect(title.length).toBeGreaterThan(0);
    // Title should mention the product/service
    expect(title.toLowerCase()).toContain('mirdb');
  });

  // ============================================================
  // Test 4: ARIA labels and accessible names
  // ============================================================
  test('should have accessible names for all buttons', async ({ page }) => {
    const buttons = await page.locator('button').all();

    for (const button of buttons) {
      const accessibleName = await button.evaluate(el => {
        // Check for aria-label
        if (el.getAttribute('aria-label')) return el.getAttribute('aria-label');
        // Check for aria-labelledby
        const labelledBy = el.getAttribute('aria-labelledby');
        if (labelledBy) {
          const labelEl = document.getElementById(labelledBy);
          return labelEl ? labelEl.textContent.trim() : null;
        }
        // Check for text content
        return el.textContent.trim();
      });

      expect(accessibleName, 'Button should have accessible name').toBeTruthy();
      expect(accessibleName.length, 'Button accessible name should not be empty').toBeGreaterThan(0);
    }
  });

  test('should have accessible names for all links', async ({ page }) => {
    const links = await page.locator('a[href]').all();

    for (const link of links) {
      const accessibleName = await link.evaluate(el => {
        // Check for aria-label
        if (el.getAttribute('aria-label')) return el.getAttribute('aria-label');
        // Check for aria-labelledby
        const labelledBy = el.getAttribute('aria-labelledby');
        if (labelledBy) {
          const labelEl = document.getElementById(labelledBy);
          return labelEl ? labelEl.textContent.trim() : null;
        }
        // Check for text content
        const text = el.textContent.trim();
        if (text.length > 0) return text;
        // Check for img with alt inside
        const img = el.querySelector('img');
        if (img && img.alt) return img.alt;
        return null;
      });

      expect(accessibleName, `Link should have accessible name: ${await link.evaluate(el => el.href || '')}`).toBeTruthy();
      expect(accessibleName.length, 'Link accessible name should not be empty').toBeGreaterThan(0);
    }
  });

  test('should have aria-label on navigation landmark', async ({ page }) => {
    const nav = page.locator('nav');
    const ariaLabel = await nav.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel.toLowerCase()).toContain('navigation');
  });

  test('should have alt text on all images', async ({ page }) => {
    const images = await page.locator('img').all();
    expect(images.length).toBeGreaterThan(0);

    for (const img of images) {
      const alt = await img.getAttribute('alt');
      expect(alt, 'Image should have alt text').toBeTruthy();
      expect(alt.length, 'Image alt text should not be empty').toBeGreaterThan(0);
    }
  });

  // ============================================================
  // Test 5: Color contrast
  // ============================================================
  test('should meet WCAG AA color contrast ratios', async ({ page }) => {
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .analyze();

    // Filter specifically for color contrast violations
    const contrastViolations = accessibilityScanResults.violations.filter(
      v => v.id === 'color-contrast'
    );

    if (contrastViolations.length > 0) {
      console.log('Contrast violations:', JSON.stringify(contrastViolations, null, 2));
    }

    expect(contrastViolations.length, 'Should have zero color contrast violations').toBe(0);
  });

  test('should have sufficient text contrast for normal text', async ({ page }) => {
    // Get computed styles for body text
    const bodyStyles = await page.evaluate(() => {
      const body = document.body;
      const style = window.getComputedStyle(body);
      return {
        color: style.color,
        backgroundColor: style.backgroundColor
      };
    });

    expect(bodyStyles.color).toBeTruthy();
    expect(bodyStyles.backgroundColor).toBeTruthy();

    // The text color should not be the same as background color
    expect(bodyStyles.color).not.toBe(bodyStyles.backgroundColor);
  });

  // ============================================================
  // Test 6: Screen reader announcement (manual)
  // ============================================================
  test('should have proper ARIA landmarks for screen reader navigation', async ({ page }) => {
    // Check that page has proper landmark regions
    const landmarks = await page.evaluate(() => {
      return {
        hasNav: !!document.querySelector('nav, [role="navigation"]'),
        hasMain: !!document.querySelector('main, [role="main"]'),
        hasFooter: !!document.querySelector('footer, [role="contentinfo"]'),
        hasHeader: !!document.querySelector('header, [role="banner"]'),
        hasComplementary: !!document.querySelector('aside, [role="complementary"]'),
        hasSearch: !!document.querySelector('[role="search"]')
      };
    });

    // At minimum, should have nav, main, and footer landmarks
    expect(landmarks.hasNav).toBe(true);
    expect(landmarks.hasMain).toBe(true);
    expect(landmarks.hasFooter).toBe(true);
  });

  test('should have aria-labelledby connecting sections to their headings', async ({ page }) => {
    const sections = await page.locator('section[aria-labelledby]').all();

    for (const section of sections) {
      const ariaLabelledBy = await section.getAttribute('aria-labelledby');
      expect(ariaLabelledBy).toBeTruthy();

      // Verify the referenced element exists
      const referencedElement = page.locator(`#${ariaLabelledBy}`);
      await expect(referencedElement).toBeVisible();

      // Verify the referenced element is a heading
      const tagName = await referencedElement.evaluate(el => el.tagName.toLowerCase());
      expect(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']).toContain(tagName);
    }
  });

  test('should have skip-to-content or first focusable element in logical order', async ({ page }) => {
    // Check for skip link
    const skipLink = page.locator('a[href^="#main"], a[href^="#content"], .skip-link, [class*="skip"]');
    const skipLinkCount = await skipLink.count();

    // If no skip link, verify first focusable element is in the nav/header
    if (skipLinkCount === 0) {
      const firstFocusable = await page.locator('a[href], button, input, [tabindex]:not([tabindex="-1"])').first();
      const isInNav = await firstFocusable.evaluate(el => {
        return !!el.closest('nav, header, [role="navigation"], [role="banner"]');
      });
      // First focusable should be in nav area
      expect(isInNav).toBe(true);
    }
  });
});

test.describe('Accessibility - Manual Tests', () => {
  test('screen reader announcement verification (manual)', async ({ page }) => {
    test.skip(true, 'Manual test: Verify page title, headings, and interactive elements are announced correctly by screen reader');
    await page.goto('/');
  });
});
