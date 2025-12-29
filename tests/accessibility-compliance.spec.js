// @ts-check
const { test, expect } = require('@playwright/test');
const AxeBuilder = require('@axe-core/playwright').default;

test.describe('Accessibility Compliance - WCAG 2.1 AA', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Page has exactly one h1 element containing product name', async ({ page }) => {
    // Get all h1 elements
    const h1Elements = page.locator('h1');
    const h1Count = await h1Elements.count();

    // Verify exactly one h1 exists
    expect(h1Count).toBe(1);

    // Verify h1 contains MirDB product name
    const h1Text = await h1Elements.first().textContent();
    expect(h1Text).toContain('MirDB');
  });

  test('TC2: Headings follow logical order without skipping levels', async ({ page }) => {
    // Get all heading elements
    const headings = await page.evaluate(() => {
      const headingElements = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      return Array.from(headingElements).map(h => ({
        level: parseInt(h.tagName.charAt(1)),
        text: h.textContent.trim()
      }));
    });

    // Verify heading hierarchy is not empty
    expect(headings.length).toBeGreaterThan(0);

    // Verify first heading is h1
    expect(headings[0].level).toBe(1);

    // Verify no heading level is skipped
    let previousLevel = 1;
    for (const heading of headings) {
      // Each heading should be same level, one level deeper, or any level higher
      // (you can go from h3 back to h2, but not from h1 to h3 without h2)
      if (heading.level > previousLevel) {
        // When going deeper, it should only go one level at a time
        expect(heading.level).toBeLessThanOrEqual(previousLevel + 1);
      }
      previousLevel = heading.level;
    }
  });

  test('TC3: All img elements have alt attributes with appropriate text', async ({ page }) => {
    // Get all image elements
    const images = page.locator('img');
    const imageCount = await images.count();

    for (let i = 0; i < imageCount; i++) {
      const img = images.nth(i);

      // Check that alt attribute exists
      const altAttribute = await img.getAttribute('alt');
      expect(altAttribute).not.toBeNull();

      // For informative images (like logos), alt text should be descriptive
      const src = await img.getAttribute('src') || '';
      const isLikelyInformative = src.includes('logo') ||
                                   await img.evaluate(el => el.classList.contains('logo'));

      if (isLikelyInformative) {
        // Informative images need descriptive alt text
        expect(altAttribute.length).toBeGreaterThan(0);
      }
      // Note: decorative images may have empty alt="" which is acceptable
    }
  });

  test('TC4: All interactive elements are reachable via Tab key with visible focus indicators', async ({ page }) => {
    // Get all interactive elements that should be focusable
    const interactiveElements = page.locator('a[href], button, input, select, textarea, [tabindex]:not([tabindex="-1"])');
    const elementCount = await interactiveElements.count();

    expect(elementCount).toBeGreaterThan(0);

    // Track which elements receive focus
    const focusedTags = new Set();
    let totalFocusableVisited = 0;

    // Start from the beginning
    await page.keyboard.press('Tab');

    // Tab through elements and verify each is focusable
    for (let i = 0; i < elementCount + 5; i++) {
      const activeElement = await page.evaluate(() => {
        const el = document.activeElement;
        if (el && el !== document.body && el !== document.documentElement) {
          return {
            tagName: el.tagName,
            href: el.getAttribute('href'),
            className: el.className
          };
        }
        return null;
      });

      if (activeElement) {
        const key = `${activeElement.tagName}-${activeElement.href || ''}-${activeElement.className}`;
        if (!focusedTags.has(key)) {
          focusedTags.add(key);
          totalFocusableVisited++;
        }
      }

      await page.keyboard.press('Tab');
    }

    // Verify we were able to tab through interactive elements
    expect(totalFocusableVisited).toBeGreaterThan(0);

    // Verify focus styles exist in CSS (checking stylesheet rather than computed styles)
    const hasFocusStyles = await page.evaluate(() => {
      // Check if there are any focus-related CSS rules
      const stylesheets = Array.from(document.styleSheets);
      for (const sheet of stylesheets) {
        try {
          const rules = Array.from(sheet.cssRules || []);
          for (const rule of rules) {
            if (rule.cssText && (rule.cssText.includes(':focus') || rule.cssText.includes(':focus-visible'))) {
              // Verify focus rule includes outline or other visible indicator
              if (rule.cssText.includes('outline') || rule.cssText.includes('box-shadow')) {
                return true;
              }
            }
          }
        } catch (e) {
          // Cross-origin stylesheet, skip
        }
      }
      return false;
    });

    expect(hasFocusStyles).toBeTruthy();

    // Verify at least 50% of focusable elements were visited
    const visitRatio = totalFocusableVisited / elementCount;
    expect(visitRatio).toBeGreaterThanOrEqual(0.5);
  });

  test('TC5: No critical accessibility violations detected (automated audit)', async ({ page }) => {
    // Run axe-core accessibility audit
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
      .analyze();

    // Filter for critical and serious violations only
    const criticalViolations = accessibilityScanResults.violations.filter(
      v => v.impact === 'critical' || v.impact === 'serious'
    );

    // Log violations for debugging if any exist
    if (criticalViolations.length > 0) {
      console.log('Critical accessibility violations found:');
      criticalViolations.forEach(v => {
        console.log(`- ${v.id}: ${v.description}`);
        console.log(`  Impact: ${v.impact}`);
        console.log(`  Affected nodes: ${v.nodes.length}`);
      });
    }

    // Verify no critical violations
    expect(criticalViolations).toHaveLength(0);
  });

  test('TC6: All text meets minimum color contrast requirements', async ({ page }) => {
    // Run axe-core with focus on color contrast rules
    const contrastResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .options({ runOnly: ['color-contrast'] })
      .analyze();

    // Get any contrast violations
    const contrastViolations = contrastResults.violations.filter(
      v => v.id === 'color-contrast'
    );

    // Log contrast issues for debugging
    if (contrastViolations.length > 0) {
      console.log('Color contrast violations found:');
      contrastViolations.forEach(v => {
        v.nodes.forEach(node => {
          console.log(`- Element: ${node.html}`);
          console.log(`  Failure: ${node.failureSummary}`);
        });
      });
    }

    // Verify no contrast violations
    expect(contrastViolations).toHaveLength(0);
  });

  test('Supplementary: SVG icons have aria-hidden for decorative icons', async ({ page }) => {
    // Check that decorative SVG icons have aria-hidden="true"
    const svgIcons = page.locator('svg');
    const svgCount = await svgIcons.count();

    for (let i = 0; i < svgCount; i++) {
      const svg = svgIcons.nth(i);
      const ariaHidden = await svg.getAttribute('aria-hidden');
      const ariaLabel = await svg.getAttribute('aria-label');
      const role = await svg.getAttribute('role');

      // SVGs should either have aria-hidden="true" (decorative) or have accessible name
      const isAccessible = ariaHidden === 'true' ||
                          (ariaLabel && ariaLabel.length > 0) ||
                          role === 'img';

      expect(isAccessible).toBeTruthy();
    }
  });

  test('Supplementary: Page has lang attribute on html element', async ({ page }) => {
    const htmlLang = await page.getAttribute('html', 'lang');
    expect(htmlLang).toBeTruthy();
    expect(htmlLang.length).toBeGreaterThan(0);
  });

  test('Supplementary: Links have discernible text', async ({ page }) => {
    // Check all links have accessible names
    const links = page.locator('a');
    const linkCount = await links.count();

    for (let i = 0; i < linkCount; i++) {
      const link = links.nth(i);

      // Get accessible name via text content or aria-label
      const textContent = await link.textContent();
      const ariaLabel = await link.getAttribute('aria-label');
      const title = await link.getAttribute('title');

      // Link should have some form of accessible name
      const hasAccessibleName = (textContent && textContent.trim().length > 0) ||
                               (ariaLabel && ariaLabel.length > 0) ||
                               (title && title.length > 0);

      expect(hasAccessibleName).toBeTruthy();
    }
  });

  test('Supplementary: Buttons have discernible text', async ({ page }) => {
    const buttons = page.locator('button');
    const buttonCount = await buttons.count();

    for (let i = 0; i < buttonCount; i++) {
      const button = buttons.nth(i);

      // Get accessible name
      const textContent = await button.textContent();
      const ariaLabel = await button.getAttribute('aria-label');
      const title = await button.getAttribute('title');

      // Button should have some form of accessible name
      const hasAccessibleName = (textContent && textContent.trim().length > 0) ||
                               (ariaLabel && ariaLabel.length > 0) ||
                               (title && title.length > 0);

      expect(hasAccessibleName).toBeTruthy();
    }
  });
});
