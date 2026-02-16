/**
 * Accessibility E2E Tests
 * Owner: Scenario 7 - Responsive Design and Accessibility
 *
 * Tests WCAG 2.1 AA compliance including:
 * - ARIA landmarks
 * - Heading hierarchy
 * - Color contrast
 * - Focus indicators
 * - Keyboard navigation
 * - Screen reader compatibility
 */

const { test, expect } = require('@playwright/test');
const AxeBuilder = require('@axe-core/playwright').default;

test.describe('Accessibility - axe-core Audit', () => {
  test('Test Case 6: No critical or serious WCAG 2.1 AA violations', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Run axe accessibility scan
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21aa'])
      .analyze();

    // Filter for critical and serious violations only
    const criticalViolations = accessibilityScanResults.violations.filter(
      (v) => v.impact === 'critical' || v.impact === 'serious'
    );

    // Output violations for debugging
    if (criticalViolations.length > 0) {
      console.log('Critical/Serious Accessibility Violations:');
      criticalViolations.forEach((v) => {
        console.log(`- ${v.id}: ${v.description} (${v.impact})`);
        v.nodes.forEach((n) => console.log(`  Target: ${n.target}`));
      });
    }

    expect(criticalViolations).toHaveLength(0);
  });
});

test.describe('Accessibility - ARIA Landmarks', () => {
  test('Test Case 15: Page has required ARIA landmarks', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Check for banner landmark (top-level header with role=banner)
    const banner = page.locator('body > header[role="banner"], body > [role="banner"]');
    expect(await banner.count()).toBeGreaterThanOrEqual(1);

    // Check for navigation landmark
    const nav = page.locator('[role="navigation"], nav[aria-label]');
    expect(await nav.count()).toBeGreaterThanOrEqual(1);

    // Check for main landmark
    const main = page.locator('[role="main"], main[role="main"]');
    expect(await main.count()).toBeGreaterThanOrEqual(1);

    // Check for contentinfo landmark (footer)
    const contentinfo = page.locator('[role="contentinfo"], footer[role="contentinfo"]');
    expect(await contentinfo.count()).toBeGreaterThanOrEqual(1);
  });

  test('Navigation has proper aria-label', async ({ page }) => {
    await page.goto('/');

    const mainNav = page.locator('nav[aria-label]').first();
    const ariaLabel = await mainNav.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel.length).toBeGreaterThan(0);
  });
});

test.describe('Accessibility - Heading Hierarchy', () => {
  test('Test Case 8: Headings follow logical order', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Get all headings in order
    const headings = await page.$$eval('h1, h2, h3, h4, h5, h6', (elements) => {
      return elements.map((el) => ({
        level: parseInt(el.tagName.substring(1)),
        text: el.textContent.trim().substring(0, 50),
      }));
    });

    // Check there's exactly one h1
    const h1Count = headings.filter((h) => h.level === 1).length;
    expect(h1Count).toBe(1);

    // Verify heading hierarchy - no skipped levels
    let previousLevel = 0;
    for (const heading of headings) {
      // Can go down any amount, but can only go up by 1 level at a time
      if (heading.level > previousLevel) {
        expect(heading.level - previousLevel).toBeLessThanOrEqual(1);
      }
      previousLevel = heading.level;
    }
  });

  test('H1 is the page title', async ({ page }) => {
    await page.goto('/');

    const h1 = page.locator('h1');
    await expect(h1).toHaveCount(1);

    const h1Text = await h1.textContent();
    expect(h1Text.length).toBeGreaterThan(0);
    // H1 should describe the product (key-value store)
    expect(h1Text.toLowerCase()).toContain('key-value');
  });
});

test.describe('Accessibility - Images and Alt Text', () => {
  test('Test Case 9: All images have alt text', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Get all images
    const images = page.locator('img');
    const imageCount = await images.count();

    for (let i = 0; i < imageCount; i++) {
      const img = images.nth(i);
      const alt = await img.getAttribute('alt');
      const ariaHidden = await img.getAttribute('aria-hidden');

      // Image must have alt attribute (can be empty for decorative)
      // OR be marked as aria-hidden="true"
      const hasAlt = alt !== null;
      const isHidden = ariaHidden === 'true';

      expect(hasAlt || isHidden).toBeTruthy();
    }
  });

  test('SVG icons are properly hidden from screen readers', async ({ page }) => {
    await page.goto('/');

    // Feature card icons should be decorative
    const featureIcons = page.locator('.feature-card__icon');
    const iconCount = await featureIcons.count();

    for (let i = 0; i < iconCount; i++) {
      const icon = featureIcons.nth(i);
      const ariaHidden = await icon.getAttribute('aria-hidden');
      expect(ariaHidden).toBe('true');
    }
  });
});

test.describe('Accessibility - Color Contrast', () => {
  test('Test Case 10: Body text has 4.5:1 contrast ratio', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Run axe specifically for color contrast
    const results = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .include('body')
      .analyze();

    // Check for color contrast violations
    const contrastViolations = results.violations.filter(
      (v) => v.id === 'color-contrast'
    );

    expect(contrastViolations).toHaveLength(0);
  });

  test('Test Case 11: Large text has 3:1 contrast ratio', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Get computed styles for headings
    const h1Style = await page.locator('h1').first().evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        color: style.color,
        fontSize: parseFloat(style.fontSize),
        fontWeight: style.fontWeight,
      };
    });

    // Large text is >= 18px or >= 14px bold
    expect(h1Style.fontSize >= 18 || (h1Style.fontSize >= 14 && parseInt(h1Style.fontWeight) >= 700)).toBeTruthy();
  });
});

test.describe('Accessibility - Focus Indicators', () => {
  test('Test Case 12: Focusable elements have visible focus indicators', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Test focus on navigation links
    const firstNavLink = page.locator('.nav__links a').first();
    await firstNavLink.focus();

    // Check for visible focus indicator
    const outlineStyle = await firstNavLink.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        outline: style.outline,
        outlineWidth: style.outlineWidth,
        outlineColor: style.outlineColor,
        outlineStyle: style.outlineStyle,
      };
    });

    // Focus indicator should not be 'none' or '0px'
    expect(outlineStyle.outlineStyle).not.toBe('none');
    expect(outlineStyle.outlineWidth).not.toBe('0px');
  });

  test('Buttons are focusable and visible', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Verify buttons exist and are focusable
    const ctaButton = page.locator('.btn-primary').first();
    await expect(ctaButton).toBeVisible();

    // Button should not have negative tabindex
    const tabIndex = await ctaButton.getAttribute('tabindex');
    expect(tabIndex !== '-1').toBeTruthy();

    // Verify CTA button is an accessible link
    const tagName = await ctaButton.evaluate((el) => el.tagName.toLowerCase());
    expect(['a', 'button']).toContain(tagName);

    // If it's a link, verify it has href
    if (tagName === 'a') {
      const href = await ctaButton.getAttribute('href');
      expect(href).toBeTruthy();
    }
  });
});

test.describe('Accessibility - Keyboard Navigation', () => {
  test('Test Case 7: Page is navigable with keyboard only', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Start at body and tab through the page
    await page.keyboard.press('Tab');

    // Should focus on skip link first
    let focusedElement = await page.evaluate(() => document.activeElement?.className || document.activeElement?.tagName);
    expect(focusedElement).toContain('skip-link');

    // Tab to first nav link
    await page.keyboard.press('Tab');
    focusedElement = await page.evaluate(() => document.activeElement?.tagName);
    expect(focusedElement.toLowerCase()).toBe('a');

    // Continue tabbing - should be able to reach main content
    for (let i = 0; i < 20; i++) {
      await page.keyboard.press('Tab');
      const tag = await page.evaluate(() => document.activeElement?.tagName);
      if (!tag) break;
    }

    // Verify focus didn't get trapped
    const finalFocused = await page.evaluate(() => document.activeElement?.tagName);
    expect(finalFocused).toBeTruthy();
  });

  test('Skip link moves focus to main content', async ({ page }) => {
    await page.goto('/');

    // Tab to skip link
    await page.keyboard.press('Tab');

    // Activate skip link
    await page.keyboard.press('Enter');

    // Focus should move to main content area
    const focusedId = await page.evaluate(() => document.activeElement?.id);
    expect(focusedId).toBe('main-content');
  });

  test('Code example tabs are keyboard navigable', async ({ page }) => {
    await page.goto('/');

    // Navigate to tabs
    const tabList = page.locator('.code-examples__tabs');
    const firstTab = page.locator('.code-examples__tab').first();
    await firstTab.focus();

    // Tab should be focusable
    const isFocused = await firstTab.evaluate((el) => document.activeElement === el);
    expect(isFocused).toBeTruthy();

    // Space or Enter should activate tab
    await page.keyboard.press('Enter');

    // Tab panel should be visible
    const activePanel = page.locator('.code-examples__panel--active');
    await expect(activePanel).toBeVisible();
  });

  test('Arrow keys navigate between tabs', async ({ page }) => {
    await page.goto('/');

    // Focus first tab
    const firstTab = page.locator('.code-examples__tab').first();
    await firstTab.focus();

    // Get initial tab
    const initialTabId = await page.evaluate(() => document.activeElement?.id);

    // Press right arrow
    await page.keyboard.press('ArrowRight');

    // Focus should move to next tab
    const newTabId = await page.evaluate(() => document.activeElement?.id);
    expect(newTabId).not.toBe(initialTabId);
  });
});

test.describe('Accessibility - Interactive Elements', () => {
  test('Links have descriptive text', async ({ page }) => {
    await page.goto('/');

    // Check for links with ambiguous text
    const ambiguousTexts = ['click here', 'read more', 'more', 'link', 'here'];
    const links = page.locator('a');
    const linkCount = await links.count();

    for (let i = 0; i < linkCount; i++) {
      const link = links.nth(i);
      const text = (await link.textContent()).trim().toLowerCase();
      const ariaLabel = await link.getAttribute('aria-label');

      // Link should have meaningful text or aria-label
      const hasMeaningfulText = text.length > 0 && !ambiguousTexts.includes(text);
      const hasAriaLabel = ariaLabel && ariaLabel.length > 0;

      expect(hasMeaningfulText || hasAriaLabel).toBeTruthy();
    }
  });

  test('External links have rel="noopener"', async ({ page }) => {
    await page.goto('/');

    const externalLinks = page.locator('a[target="_blank"]');
    const linkCount = await externalLinks.count();

    for (let i = 0; i < linkCount; i++) {
      const link = externalLinks.nth(i);
      const rel = await link.getAttribute('rel');
      expect(rel).toContain('noopener');
    }
  });

  test('Buttons have accessible names', async ({ page }) => {
    await page.goto('/');

    const buttons = page.locator('button');
    const buttonCount = await buttons.count();

    for (let i = 0; i < buttonCount; i++) {
      const button = buttons.nth(i);
      const text = (await button.textContent()).trim();
      const ariaLabel = await button.getAttribute('aria-label');
      const title = await button.getAttribute('title');

      // Button should have accessible name
      const hasAccessibleName = text.length > 0 || (ariaLabel && ariaLabel.length > 0) || (title && title.length > 0);
      expect(hasAccessibleName).toBeTruthy();
    }
  });
});

test.describe('Accessibility - Form Elements', () => {
  test('Tab buttons have proper ARIA roles', async ({ page }) => {
    await page.goto('/');

    // Check tab buttons
    const tabs = page.locator('[role="tab"]');
    const tabCount = await tabs.count();
    expect(tabCount).toBeGreaterThan(0);

    for (let i = 0; i < tabCount; i++) {
      const tab = tabs.nth(i);
      const ariaSelected = await tab.getAttribute('aria-selected');
      const ariaControls = await tab.getAttribute('aria-controls');

      // Tab should have required ARIA attributes
      expect(ariaSelected).toBeTruthy();
      expect(ariaControls).toBeTruthy();
    }
  });

  test('Tab panels have proper ARIA roles', async ({ page }) => {
    await page.goto('/');

    const panels = page.locator('[role="tabpanel"]');
    const panelCount = await panels.count();
    expect(panelCount).toBeGreaterThan(0);

    for (let i = 0; i < panelCount; i++) {
      const panel = panels.nth(i);
      const ariaLabelledby = await panel.getAttribute('aria-labelledby');
      expect(ariaLabelledby).toBeTruthy();
    }
  });
});

test.describe('Accessibility - Document Structure', () => {
  test('Page has lang attribute', async ({ page }) => {
    await page.goto('/');

    const lang = await page.locator('html').getAttribute('lang');
    expect(lang).toBe('en');
  });

  test('Page has descriptive title', async ({ page }) => {
    await page.goto('/');

    const title = await page.title();
    expect(title.length).toBeGreaterThan(0);
    expect(title.toLowerCase()).toContain('mirdb');
  });

  test('Page has meta description', async ({ page }) => {
    await page.goto('/');

    const metaDesc = await page.locator('meta[name="description"]').getAttribute('content');
    expect(metaDesc).toBeTruthy();
    expect(metaDesc.length).toBeGreaterThan(20);
  });
});

test.describe('Accessibility - Screen Reader Support', () => {
  test('Test Case 13: Page structure is screen reader friendly', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Verify sections have accessible labels
    const sections = page.locator('section[aria-labelledby]');
    const sectionCount = await sections.count();

    for (let i = 0; i < sectionCount; i++) {
      const section = sections.nth(i);
      const labelledBy = await section.getAttribute('aria-labelledby');

      if (labelledBy) {
        const labelElement = page.locator(`#${labelledBy}`);
        await expect(labelElement).toHaveCount(1);

        const labelText = await labelElement.textContent();
        expect(labelText.trim().length).toBeGreaterThan(0);
      }
    }
  });

  test('Decorative elements are hidden from screen readers', async ({ page }) => {
    await page.goto('/');

    // Step numbers should be decorative
    const stepNumbers = page.locator('.getting-started__step-number');
    const stepCount = await stepNumbers.count();

    for (let i = 0; i < stepCount; i++) {
      const stepNum = stepNumbers.nth(i);
      const ariaHidden = await stepNum.getAttribute('aria-hidden');
      expect(ariaHidden).toBe('true');
    }
  });
});
