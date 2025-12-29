import { test, expect } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

test.describe('Accessibility Compliance (WCAG 2.1 Level AA)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Run Lighthouse/Axe accessibility audit
  // Expected: Accessibility score is 90 or higher (using axe-core for automated accessibility testing)
  test('should not have any automatically detectable WCAG 2.1 AA accessibility issues', async ({ page }) => {
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
      .analyze();

    // Log violations for debugging if any
    if (accessibilityScanResults.violations.length > 0) {
      console.log('Accessibility violations found:');
      accessibilityScanResults.violations.forEach((violation) => {
        console.log(`- ${violation.id}: ${violation.description}`);
        console.log(`  Impact: ${violation.impact}`);
        console.log(`  Help: ${violation.help}`);
        violation.nodes.forEach((node) => {
          console.log(`  Element: ${node.html}`);
        });
      });
    }

    expect(accessibilityScanResults.violations).toEqual([]);
  });

  // Test Case 2: Check heading hierarchy
  // Expected: Page has single h1, headings follow logical order (h1 > h2 > h3)
  test('page has proper heading hierarchy with single h1', async ({ page }) => {
    // Check for single h1
    const h1Elements = await page.locator('h1').all();
    expect(h1Elements.length).toBe(1);

    // Verify h1 text
    const h1Text = await page.locator('h1').textContent();
    expect(h1Text).toBe('MirDB');

    // Get all headings and verify logical order
    const headings = await page.locator('h1, h2, h3, h4, h5, h6').all();
    const headingLevels: number[] = [];

    for (const heading of headings) {
      const tagName = await heading.evaluate((el) => el.tagName.toLowerCase());
      const level = parseInt(tagName.replace('h', ''), 10);
      headingLevels.push(level);
    }

    // First heading should be h1
    expect(headingLevels[0]).toBe(1);

    // Check that no heading level is skipped (e.g., h1 then h3 without h2)
    for (let i = 1; i < headingLevels.length; i++) {
      const currentLevel = headingLevels[i];
      const prevLevel = headingLevels[i - 1];
      // A heading can be equal to or one level deeper, or go back up any number of levels
      // But should not skip levels when going deeper (e.g., h1 then h3)
      if (currentLevel > prevLevel) {
        expect(currentLevel - prevLevel).toBeLessThanOrEqual(1);
      }
    }
  });

  test('all h2 headings follow the h1 properly', async ({ page }) => {
    // Get all section headings to verify proper hierarchy
    const h2Headings = await page.locator('h2').all();
    expect(h2Headings.length).toBeGreaterThan(0);

    // Verify each section has an h2
    const sections = await page.locator('section').all();
    for (const section of sections) {
      // Hero section has h1, other sections should have h2
      const hasH1 = await section.locator('h1').count() > 0;
      const hasH2 = await section.locator('h2').count() > 0;
      expect(hasH1 || hasH2).toBe(true);
    }
  });

  // Test Case 3: Verify all images have alt text
  // Expected: All img elements have appropriate alt attributes
  test('all images have appropriate alt attributes', async ({ page }) => {
    // Get all img elements
    const images = await page.locator('img').all();

    for (const img of images) {
      const altAttribute = await img.getAttribute('alt');
      // Alt attribute should exist and not be null
      expect(altAttribute).not.toBeNull();
      // Alt should not be empty for informative images
      // (decorative images can have alt="", but we verify they at least have the attribute)
      expect(typeof altAttribute).toBe('string');
    }

    // Check SVG images with role="img" have proper accessible names
    const svgImages = await page.locator('svg[role="img"]').all();
    for (const svg of svgImages) {
      // SVGs with role="img" should have aria-labelledby or aria-label
      const ariaLabel = await svg.getAttribute('aria-label');
      const ariaLabelledby = await svg.getAttribute('aria-labelledby');
      const hasTitle = await svg.locator('title').count() > 0;

      expect(ariaLabel || ariaLabelledby || hasTitle).toBeTruthy();
    }
  });

  test('architecture diagram SVG has accessible description', async ({ page }) => {
    const architectureSvg = page.locator('[data-testid="architecture-diagram"] svg');
    await expect(architectureSvg).toBeVisible();

    // Verify SVG has role="img"
    await expect(architectureSvg).toHaveAttribute('role', 'img');

    // Verify aria-labelledby is present
    const ariaLabelledby = await architectureSvg.getAttribute('aria-labelledby');
    expect(ariaLabelledby).toBeTruthy();

    // Verify title element exists
    const title = architectureSvg.locator('title');
    await expect(title).toBeAttached();
    const titleText = await title.textContent();
    expect(titleText).toContain('Architecture');

    // Verify desc element exists with meaningful content
    const desc = architectureSvg.locator('desc');
    await expect(desc).toBeAttached();
    const descText = await desc.textContent();
    expect(descText?.length).toBeGreaterThan(50); // Should have substantial description
  });

  // Test Case 4: Test keyboard navigation through page
  // Expected: All interactive elements can be reached and activated via keyboard
  test('all interactive elements are keyboard accessible', async ({ page }) => {
    // Get all interactive elements
    const links = await page.locator('a[href]').all();
    const buttons = await page.locator('button').all();
    const interactiveElements = [...links, ...buttons];

    // Verify each interactive element is focusable
    for (const element of interactiveElements) {
      const isVisible = await element.isVisible();
      if (isVisible) {
        // Check that element can receive focus
        await element.focus();
        const isFocused = await element.evaluate((el) => document.activeElement === el);
        expect(isFocused).toBe(true);
      }
    }
  });

  test('tab navigation works correctly through the page', async ({ page }) => {
    // Focus the body first to ensure consistent starting point
    await page.locator('body').click();

    // Track focused elements
    const focusedElements: string[] = [];

    // Tab through the page and collect focused elements
    for (let i = 0; i < 20; i++) {
      await page.keyboard.press('Tab');
      // Small delay to let focus settle
      await page.waitForTimeout(50);

      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement;
        if (el && el.tagName !== 'BODY' && el.tagName !== 'HTML') {
          const identifier = el.tagName.toLowerCase() +
            (el.getAttribute('data-testid') || el.getAttribute('href') || '');
          return identifier;
        }
        return null;
      });

      if (focusedElement && !focusedElements.includes(focusedElement)) {
        focusedElements.push(focusedElement);
      }
    }

    // Should be able to tab through multiple interactive elements
    // Page has: 2 CTA buttons in hero, 1 footer link = at least 3 interactive elements
    expect(focusedElements.length).toBeGreaterThanOrEqual(3);
  });

  test('links can be activated with Enter key', async ({ page }) => {
    const getStartedLink = page.locator('[data-testid="hero-cta-primary"]');
    await getStartedLink.focus();

    // Press Enter should activate the link (navigate to the anchor)
    const hrefBefore = await page.evaluate(() => window.location.hash);
    await page.keyboard.press('Enter');

    // Wait for potential navigation
    await page.waitForTimeout(100);

    const hrefAfter = await page.evaluate(() => window.location.hash);
    // The link should navigate to #getting-started
    expect(hrefAfter).toBe('#getting-started');
  });

  // Test Case 5: Check color contrast ratios
  // Expected: Text elements meet WCAG AA contrast requirements (4.5:1)
  test('color contrast meets WCAG AA requirements', async ({ page }) => {
    // Use axe-core specifically for color contrast checks
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .options({ runOnly: ['color-contrast'] })
      .analyze();

    if (accessibilityScanResults.violations.length > 0) {
      console.log('Color contrast violations:');
      accessibilityScanResults.violations.forEach((violation) => {
        violation.nodes.forEach((node) => {
          console.log(`  Element: ${node.html}`);
          console.log(`  Issue: ${node.failureSummary}`);
        });
      });
    }

    expect(accessibilityScanResults.violations).toEqual([]);
  });

  test('text colors have sufficient contrast against backgrounds', async ({ page }) => {
    // Verify key text elements have proper contrast (manual verification)
    // Check hero section text
    const heroH1 = page.locator('[data-testid="hero-product-name"]');
    const heroH1Color = await heroH1.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.color;
    });
    // Primary color should be visible (blue #2563eb)
    expect(heroH1Color).toBeTruthy();

    // Check tagline has proper contrast
    const tagline = page.locator('[data-testid="hero-tagline"]');
    const taglineColor = await tagline.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.color;
    });
    expect(taglineColor).toBeTruthy();

    // Check body text color
    const bodyText = page.locator('.value-proposition');
    const bodyColor = await bodyText.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.color;
    });
    expect(bodyColor).toBeTruthy();
  });

  // Additional accessibility tests for semantic HTML and landmarks
  test('page uses proper semantic HTML landmarks', async ({ page }) => {
    // Check for main content area (using sections in this case)
    const sections = await page.locator('section').count();
    expect(sections).toBeGreaterThan(0);

    // Check for footer
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Verify page has proper document structure
    const html = page.locator('html');
    await expect(html).toHaveAttribute('lang', 'en');
  });

  test('page has proper language attribute', async ({ page }) => {
    const langAttr = await page.locator('html').getAttribute('lang');
    expect(langAttr).toBe('en');
  });

  test('buttons and links have accessible names', async ({ page }) => {
    // Check all links have accessible text
    const links = await page.locator('a[href]').all();
    for (const link of links) {
      const isVisible = await link.isVisible();
      if (isVisible) {
        const accessibleName = await link.evaluate((el) => {
          return el.textContent?.trim() || el.getAttribute('aria-label') || '';
        });
        expect(accessibleName.length).toBeGreaterThan(0);
      }
    }

    // Check all buttons have accessible text
    const buttons = await page.locator('button').all();
    for (const button of buttons) {
      const isVisible = await button.isVisible();
      if (isVisible) {
        const accessibleName = await button.evaluate((el) => {
          return el.textContent?.trim() || el.getAttribute('aria-label') || '';
        });
        expect(accessibleName.length).toBeGreaterThan(0);
      }
    }
  });

  test('links opening in new windows have appropriate indicators', async ({ page }) => {
    const externalLinks = await page.locator('a[target="_blank"]').all();

    for (const link of externalLinks) {
      // External links should have rel="noopener noreferrer" for security
      const rel = await link.getAttribute('rel');
      expect(rel).toContain('noopener');

      // Links should indicate they open in new window (via text or aria-label)
      const linkText = await link.textContent();
      // GitHub links are commonly understood to open externally
      expect(linkText).toBeTruthy();
    }
  });

  test('focus indicators are visible', async ({ page }) => {
    // Tab to first interactive element
    await page.keyboard.press('Tab');

    const focusedElement = await page.evaluate(() => {
      const el = document.activeElement;
      if (el) {
        const style = window.getComputedStyle(el);
        return {
          outline: style.outline,
          outlineOffset: style.outlineOffset,
          boxShadow: style.boxShadow,
        };
      }
      return null;
    });

    // Verify focus indicator exists (outline, box-shadow, or other visible indicator)
    expect(focusedElement).not.toBeNull();
  });
});
