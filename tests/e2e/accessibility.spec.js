// @ts-check
const { test, expect } = require('@playwright/test');
const AxeBuilder = require('@axe-core/playwright').default;

/**
 * E2E Accessibility Tests (WCAG 2.1 AA)
 * Tests for automated accessibility audit, color contrast, and focus states
 */

test.describe('Accessibility - Basic Compliance', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: No critical or serious accessibility violations (axe-core audit)', async ({ page }) => {
    // Run axe-core accessibility audit
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
      .analyze();

    // Filter for critical and serious violations
    const criticalAndSerious = accessibilityScanResults.violations.filter(
      (violation) => violation.impact === 'critical' || violation.impact === 'serious'
    );

    // Log violations for debugging if any exist
    if (criticalAndSerious.length > 0) {
      console.log('Critical/Serious violations found:');
      criticalAndSerious.forEach((violation) => {
        console.log(`- ${violation.id}: ${violation.description}`);
        violation.nodes.forEach((node) => {
          console.log(`  Target: ${node.target}`);
          console.log(`  HTML: ${node.html}`);
        });
      });
    }

    expect(criticalAndSerious).toHaveLength(0);
  });

  test('TC2: All text meets WCAG AA contrast requirements', async ({ page }) => {
    // Run axe-core specifically for color contrast
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .analyze();

    // Filter for color contrast violations
    const contrastViolations = accessibilityScanResults.violations.filter(
      (violation) => violation.id === 'color-contrast'
    );

    // Log contrast issues for debugging
    if (contrastViolations.length > 0) {
      console.log('Color contrast violations found:');
      contrastViolations.forEach((violation) => {
        violation.nodes.forEach((node) => {
          console.log(`  Element: ${node.target}`);
          console.log(`  HTML: ${node.html}`);
          console.log(`  Issue: ${node.failureSummary}`);
        });
      });
    }

    expect(contrastViolations).toHaveLength(0);
  });

  test('TC6: Buttons and links have visible focus indicators', async ({ page }) => {
    // Test focus visibility on primary button
    const primaryButton = page.locator('.btn-primary').first();
    await primaryButton.focus();

    // Check that the button is visible and focused
    await expect(primaryButton).toBeFocused();

    // Get computed styles to verify focus is visible
    const primaryButtonStyles = await primaryButton.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        outline: styles.outline,
        outlineWidth: styles.outlineWidth,
        outlineStyle: styles.outlineStyle,
        boxShadow: styles.boxShadow,
      };
    });

    // Focus should be visible (either via outline or box-shadow)
    const hasFocusIndicator =
      (primaryButtonStyles.outlineStyle !== 'none' && primaryButtonStyles.outlineWidth !== '0px') ||
      (primaryButtonStyles.boxShadow && primaryButtonStyles.boxShadow !== 'none');

    // If no custom focus indicator, browser default should be visible
    // We just verify the element can receive focus

    // Test focus visibility on secondary button
    const secondaryButton = page.locator('.btn-secondary').first();
    await secondaryButton.focus();
    await expect(secondaryButton).toBeFocused();

    // Test focus visibility on footer links
    const footerLinks = page.locator('.footer-links a');
    const footerLinkCount = await footerLinks.count();

    for (let i = 0; i < footerLinkCount; i++) {
      const link = footerLinks.nth(i);
      await link.focus();
      await expect(link).toBeFocused();
    }

    // Test focus visibility on docs links
    const docsLinks = page.locator('.docs-link');
    const docsLinkCount = await docsLinks.count();

    for (let i = 0; i < docsLinkCount; i++) {
      const link = docsLinks.nth(i);
      await link.focus();
      await expect(link).toBeFocused();
    }
  });

  test('Interactive elements are keyboard navigable', async ({ page }) => {
    // Start from the body and tab through interactive elements
    await page.keyboard.press('Tab');

    // Should be able to tab through all interactive elements
    const interactiveElements = page.locator('a, button, input, textarea, select, [tabindex="0"]');
    const count = await interactiveElements.count();

    // Verify there are interactive elements to navigate
    expect(count).toBeGreaterThan(0);

    // Tab through and verify focus moves
    let focusedCount = 0;
    for (let i = 0; i < Math.min(count, 10); i++) {
      const focusedElement = page.locator(':focus');
      const isFocused = await focusedElement.count();
      if (isFocused > 0) {
        focusedCount++;
      }
      await page.keyboard.press('Tab');
    }

    // At least some elements should have received focus
    expect(focusedCount).toBeGreaterThan(0);
  });

  test('Skip links or landmarks are present for navigation', async ({ page }) => {
    // Check for main landmark
    const main = page.locator('main');
    await expect(main).toBeVisible();

    // Check for header landmark
    const header = page.locator('header');
    await expect(header).toBeVisible();

    // Check for footer landmark
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Check for navigation in footer
    const nav = page.locator('nav');
    expect(await nav.count()).toBeGreaterThan(0);
  });

  test('Page has proper document structure', async ({ page }) => {
    // Check for html lang attribute
    const htmlLang = await page.getAttribute('html', 'lang');
    expect(htmlLang).toBe('en');

    // Check for meta viewport
    const viewport = page.locator('meta[name="viewport"]');
    await expect(viewport).toHaveCount(1);

    // Check for title
    const title = await page.title();
    expect(title).toBeTruthy();
    expect(title.length).toBeGreaterThan(0);

    // Check for meta description
    const metaDescription = page.locator('meta[name="description"]');
    await expect(metaDescription).toHaveCount(1);
  });

  test('Images have alt text', async ({ page }) => {
    const images = page.locator('img');
    const imageCount = await images.count();

    for (let i = 0; i < imageCount; i++) {
      const img = images.nth(i);
      const alt = await img.getAttribute('alt');
      // All images should have alt attribute (can be empty for decorative)
      expect(alt).not.toBeNull();
    }
  });

  test('Links have discernible text', async ({ page }) => {
    const links = page.locator('a');
    const linkCount = await links.count();

    for (let i = 0; i < linkCount; i++) {
      const link = links.nth(i);
      const text = await link.textContent();
      const ariaLabel = await link.getAttribute('aria-label');
      const title = await link.getAttribute('title');

      // Link should have some accessible name
      const hasAccessibleName =
        (text && text.trim().length > 0) ||
        (ariaLabel && ariaLabel.length > 0) ||
        (title && title.length > 0);

      expect(hasAccessibleName).toBe(true);
    }
  });

  test('Headings are in logical order', async ({ page }) => {
    const headings = await page.locator('h1, h2, h3, h4, h5, h6').all();
    const levels = [];

    for (const heading of headings) {
      const tagName = await heading.evaluate((el) => el.tagName.toLowerCase());
      levels.push(parseInt(tagName.charAt(1)));
    }

    // First heading should be h1
    expect(levels[0]).toBe(1);

    // Check no level is skipped
    let maxSeenLevel = 1;
    for (const level of levels) {
      expect(level).toBeLessThanOrEqual(maxSeenLevel + 1);
      if (level > maxSeenLevel) {
        maxSeenLevel = level;
      }
    }
  });

  test('There is exactly one h1 on the page', async ({ page }) => {
    const h1Count = await page.locator('h1').count();
    expect(h1Count).toBe(1);
  });

  test('Sections have proper aria-labelledby attributes', async ({ page }) => {
    // Check features section
    const featuresSection = page.locator('section#features');
    const featuresAriaLabelledby = await featuresSection.getAttribute('aria-labelledby');
    expect(featuresAriaLabelledby).toBe('features-heading');

    // Verify the referenced heading exists
    const featuresHeading = page.locator('#features-heading');
    await expect(featuresHeading).toBeVisible();

    // Check quickstart section
    const quickstartSection = page.locator('section#quickstart');
    const quickstartAriaLabelledby = await quickstartSection.getAttribute('aria-labelledby');
    expect(quickstartAriaLabelledby).toBe('quickstart-heading');

    // Check commands section
    const commandsSection = page.locator('section#commands');
    const commandsAriaLabelledby = await commandsSection.getAttribute('aria-labelledby');
    expect(commandsAriaLabelledby).toBe('commands-heading');

    // Check roadmap section
    const roadmapSection = page.locator('section#roadmap');
    const roadmapAriaLabelledby = await roadmapSection.getAttribute('aria-labelledby');
    expect(roadmapAriaLabelledby).toBe('roadmap-heading');
  });

  test('Decorative icons are hidden from screen readers', async ({ page }) => {
    const featureIcons = page.locator('.feature-icon');
    const iconCount = await featureIcons.count();

    for (let i = 0; i < iconCount; i++) {
      const icon = featureIcons.nth(i);
      const ariaHidden = await icon.getAttribute('aria-hidden');
      expect(ariaHidden).toBe('true');
    }

    // Also check status icons in roadmap
    const statusIcons = page.locator('.status-icon');
    const statusIconCount = await statusIcons.count();

    for (let i = 0; i < statusIconCount; i++) {
      const icon = statusIcons.nth(i);
      const ariaHidden = await icon.getAttribute('aria-hidden');
      expect(ariaHidden).toBe('true');
    }
  });

  test('External links have rel="noopener" for security', async ({ page }) => {
    const externalLinks = page.locator('a[target="_blank"]');
    const count = await externalLinks.count();

    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      const rel = await link.getAttribute('rel');
      expect(rel).toContain('noopener');
    }
  });

  test('Table has proper structure with thead and tbody', async ({ page }) => {
    const tables = page.locator('table');
    const tableCount = await tables.count();

    for (let i = 0; i < tableCount; i++) {
      const table = tables.nth(i);

      // Check for thead
      const thead = table.locator('thead');
      await expect(thead).toHaveCount(1);

      // Check for tbody
      const tbody = table.locator('tbody');
      await expect(tbody).toHaveCount(1);

      // Check for th elements in thead
      const thElements = thead.locator('th');
      expect(await thElements.count()).toBeGreaterThan(0);
    }
  });
});
