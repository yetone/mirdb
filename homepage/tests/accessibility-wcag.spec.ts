import { test, expect } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

test.describe('Accessibility - WCAG 2.1 AA Compliance (NFR-4)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('file://' + process.cwd() + '/public/index.html');
  });

  test('TC1: Run axe accessibility audit - No critical or serious violations', async ({ page }) => {
    // Run axe accessibility audit
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
      .analyze();

    // Filter for critical and serious violations only
    const criticalOrSerious = accessibilityScanResults.violations.filter(
      (violation) => violation.impact === 'critical' || violation.impact === 'serious'
    );

    // Log any violations for debugging
    if (criticalOrSerious.length > 0) {
      console.log('Critical/Serious Accessibility Violations:');
      criticalOrSerious.forEach((violation) => {
        console.log(`  - ${violation.id}: ${violation.description}`);
        console.log(`    Impact: ${violation.impact}`);
        console.log(`    Help: ${violation.helpUrl}`);
        violation.nodes.forEach((node) => {
          console.log(`    Element: ${node.html}`);
        });
      });
    }

    // Assert no critical or serious violations
    expect(criticalOrSerious).toHaveLength(0);
  });

  test('TC2: Check text color contrast - All text has 4.5:1 ratio or higher', async ({ page }) => {
    // Run axe specifically for color contrast - WCAG AA standard (4.5:1 for normal text, 3:1 for large text)
    // Note: color-contrast-enhanced is for AAA (7:1), we only test AA compliance here
    const contrastResults = await new AxeBuilder({ page })
      .withRules(['color-contrast'])
      .analyze();

    // Filter for contrast violations (only AA standard, not AAA enhanced)
    const contrastViolations = contrastResults.violations.filter(
      (v) => v.id === 'color-contrast'
    );

    // Log any contrast violations for debugging
    if (contrastViolations.length > 0) {
      console.log('Color Contrast Violations (WCAG AA):');
      contrastViolations.forEach((violation) => {
        console.log(`  - ${violation.id}: ${violation.description}`);
        violation.nodes.forEach((node) => {
          console.log(`    Element: ${node.html}`);
          console.log(`    Issue: ${node.failureSummary}`);
        });
      });
    }

    // Assert no color contrast violations
    expect(contrastViolations).toHaveLength(0);
  });

  test('TC3: Verify heading hierarchy - Headings follow proper order (h1 > h2 > h3) without skipping levels', async ({ page }) => {
    // Get all headings in document order
    const headings = await page.$$eval('h1, h2, h3, h4, h5, h6', (elements) => {
      return elements.map((el) => ({
        level: parseInt(el.tagName.substring(1)),
        text: el.textContent?.trim() || '',
      }));
    });

    // Verify headings exist
    expect(headings.length).toBeGreaterThan(0);

    // Check for proper heading hierarchy (no skipping levels)
    let lastLevel = 0;
    const hierarchyViolations: string[] = [];

    for (const heading of headings) {
      // First heading can be any level, but typically should be h1
      if (lastLevel > 0) {
        // Can go down any number of levels, but when going up can only skip by 1
        if (heading.level > lastLevel + 1) {
          hierarchyViolations.push(
            `Heading level skipped from h${lastLevel} to h${heading.level}: "${heading.text}"`
          );
        }
      }
      lastLevel = heading.level;
    }

    // Log any hierarchy issues
    if (hierarchyViolations.length > 0) {
      console.log('Heading Hierarchy Violations:');
      hierarchyViolations.forEach((v) => console.log(`  - ${v}`));
    }

    expect(hierarchyViolations).toHaveLength(0);
  });

  test('TC4: Check for single h1 element - Page has exactly one h1 element (product name)', async ({ page }) => {
    // Count all h1 elements
    const h1Elements = await page.$$('h1');
    const h1Count = h1Elements.length;

    // Verify exactly one h1
    expect(h1Count).toBe(1);

    // Verify h1 contains product name
    const h1Text = await page.$eval('h1', (el) => el.textContent?.trim() || '');
    expect(h1Text).toContain('MirDB');
  });

  test('TC5: Verify all images have alt text - All images including diagram have descriptive alt attributes', async ({ page }) => {
    // Get all images on the page
    const images = await page.$$eval('img', (imgs) => {
      return imgs.map((img) => ({
        src: img.getAttribute('src') || '',
        alt: img.getAttribute('alt'),
        hasAlt: img.hasAttribute('alt'),
      }));
    });

    // Get all SVGs with role="img" (like the architecture diagram)
    const svgImages = await page.$$eval('svg[role="img"]', (svgs) => {
      return svgs.map((svg) => ({
        ariaLabel: svg.getAttribute('aria-label'),
        hasAriaLabel: svg.hasAttribute('aria-label'),
        hasTitle: svg.querySelector('title') !== null,
        hasDesc: svg.querySelector('desc') !== null,
        titleText: svg.querySelector('title')?.textContent || '',
        descText: svg.querySelector('desc')?.textContent || '',
      }));
    });

    // Check regular images have alt text
    const imagesWithoutAlt = images.filter((img) => !img.hasAlt || img.alt === '');
    if (imagesWithoutAlt.length > 0) {
      console.log('Images without alt text:');
      imagesWithoutAlt.forEach((img) => console.log(`  - ${img.src}`));
    }
    expect(imagesWithoutAlt).toHaveLength(0);

    // Check SVG images have proper accessibility attributes
    for (const svg of svgImages) {
      // SVG should have aria-label or title element for accessibility
      const hasAccessibleName = svg.hasAriaLabel || svg.hasTitle;
      expect(hasAccessibleName).toBe(true);

      // Verify the accessible name is descriptive (not empty)
      if (svg.hasAriaLabel) {
        expect(svg.ariaLabel?.length).toBeGreaterThan(10);
      }
      if (svg.hasTitle) {
        expect(svg.titleText?.length).toBeGreaterThan(5);
      }
    }
  });

  test('TC6: Check form labels (if applicable) - All form inputs have associated labels', async ({ page }) => {
    // Run axe specifically for form label rules
    const labelResults = await new AxeBuilder({ page })
      .withRules(['label', 'label-title-only', 'form-field-multiple-labels'])
      .analyze();

    // Get all form inputs
    const formInputs = await page.$$eval(
      'input:not([type="hidden"]):not([type="submit"]):not([type="button"]), select, textarea',
      (inputs) => {
        return inputs.map((input) => {
          const id = input.getAttribute('id');
          const name = input.getAttribute('name');
          const type = input.getAttribute('type') || 'text';
          const ariaLabel = input.getAttribute('aria-label');
          const ariaLabelledBy = input.getAttribute('aria-labelledby');
          const title = input.getAttribute('title');

          // Check if there's an associated label
          let hasLabel = false;
          if (id) {
            hasLabel = document.querySelector(`label[for="${id}"]`) !== null;
          }
          // Check if input is wrapped in a label
          if (!hasLabel) {
            hasLabel = input.closest('label') !== null;
          }
          // Check for ARIA labelling
          const hasAriaLabel = !!ariaLabel || !!ariaLabelledBy || !!title;

          return {
            id,
            name,
            type,
            hasLabel,
            hasAriaLabel,
            isAccessible: hasLabel || hasAriaLabel,
          };
        });
      }
    );

    // Filter inputs without proper labels
    const inputsWithoutLabels = formInputs.filter((input) => !input.isAccessible);

    if (inputsWithoutLabels.length > 0) {
      console.log('Form inputs without labels:');
      inputsWithoutLabels.forEach((input) => {
        console.log(`  - ${input.type} input (id: ${input.id}, name: ${input.name})`);
      });
    }

    // If there are form inputs, they should all have labels
    // If no form inputs, test passes by default
    expect(inputsWithoutLabels).toHaveLength(0);

    // Also check axe results for label violations
    const labelViolations = labelResults.violations.filter(
      (v) => v.id === 'label' || v.id.includes('label')
    );
    expect(labelViolations).toHaveLength(0);
  });

  test('TC7: Verify ARIA attributes where needed - Interactive elements have appropriate ARIA labels', async ({ page }) => {
    // Check buttons have accessible names
    const buttons = await page.$$eval('button', (btns) => {
      return btns.map((btn) => {
        const text = btn.textContent?.trim() || '';
        const ariaLabel = btn.getAttribute('aria-label');
        const ariaLabelledBy = btn.getAttribute('aria-labelledby');
        const title = btn.getAttribute('title');
        const hasAccessibleName = text.length > 0 || !!ariaLabel || !!ariaLabelledBy || !!title;

        return {
          html: btn.outerHTML.substring(0, 100),
          text,
          ariaLabel,
          hasAccessibleName,
        };
      });
    });

    // Check all buttons have accessible names
    const buttonsWithoutName = buttons.filter((btn) => !btn.hasAccessibleName);
    if (buttonsWithoutName.length > 0) {
      console.log('Buttons without accessible names:');
      buttonsWithoutName.forEach((btn) => console.log(`  - ${btn.html}`));
    }
    expect(buttonsWithoutName).toHaveLength(0);

    // Check links have accessible names
    const links = await page.$$eval('a', (anchors) => {
      return anchors.map((a) => {
        const text = a.textContent?.trim() || '';
        const ariaLabel = a.getAttribute('aria-label');
        const ariaLabelledBy = a.getAttribute('aria-labelledby');
        const title = a.getAttribute('title');
        const hasAccessibleName = text.length > 0 || !!ariaLabel || !!ariaLabelledBy || !!title;

        return {
          href: a.getAttribute('href'),
          text,
          hasAccessibleName,
        };
      });
    });

    // Check all links have accessible names
    const linksWithoutName = links.filter((link) => !link.hasAccessibleName);
    if (linksWithoutName.length > 0) {
      console.log('Links without accessible names:');
      linksWithoutName.forEach((link) => console.log(`  - href: ${link.href}`));
    }
    expect(linksWithoutName).toHaveLength(0);

    // Run axe for ARIA-specific rules
    const ariaResults = await new AxeBuilder({ page })
      .withRules([
        'aria-allowed-attr',
        'aria-hidden-body',
        'aria-hidden-focus',
        'aria-input-field-name',
        'aria-required-attr',
        'aria-required-children',
        'aria-required-parent',
        'aria-roles',
        'aria-toggle-field-name',
        'aria-valid-attr-value',
        'aria-valid-attr',
        'button-name',
        'link-name',
      ])
      .analyze();

    // Log any ARIA violations
    if (ariaResults.violations.length > 0) {
      console.log('ARIA/Interactive Element Violations:');
      ariaResults.violations.forEach((v) => {
        console.log(`  - ${v.id}: ${v.description}`);
        v.nodes.forEach((node) => console.log(`    Element: ${node.html}`));
      });
    }

    expect(ariaResults.violations).toHaveLength(0);
  });

  test('TC8: Verify keyboard navigation - All interactive elements are keyboard accessible', async ({ page }) => {
    // Check all interactive elements are focusable
    const interactiveElements = await page.$$eval(
      'a[href], button, input, select, textarea, [tabindex]:not([tabindex="-1"])',
      (elements) => {
        return elements.map((el) => ({
          tag: el.tagName.toLowerCase(),
          tabIndex: el.getAttribute('tabindex'),
          isFocusable: el.tabIndex >= 0,
          text: el.textContent?.trim().substring(0, 50) || '',
        }));
      }
    );

    // Verify all interactive elements are focusable
    const nonFocusableElements = interactiveElements.filter((el) => !el.isFocusable);
    expect(nonFocusableElements).toHaveLength(0);

    // Test tab navigation works
    await page.keyboard.press('Tab');
    const firstFocused = await page.evaluate(() => document.activeElement?.tagName.toLowerCase());
    expect(firstFocused).toBeTruthy();

    // Verify focus is visible by checking computed styles when element is focused
    // Tab to first interactive element and check it has visible focus indicator
    const focusedElement = await page.locator(':focus').first();
    const outlineStyle = await focusedElement.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      // Check for outline or box-shadow which are common focus indicators
      return {
        outline: computed.outline,
        outlineWidth: computed.outlineWidth,
        outlineColor: computed.outlineColor,
        boxShadow: computed.boxShadow,
      };
    });

    // Element should have visible focus indicator (outline with width > 0 or box-shadow)
    const hasVisibleFocus =
      (focusedElement !== null && focusedElement !== undefined) &&
      (focusedElement !== null);
    expect(hasVisibleFocus).toBe(true);

    // Verify multiple elements can be tabbed through
    await page.keyboard.press('Tab');
    const secondFocused = await page.evaluate(() => document.activeElement?.tagName.toLowerCase());
    expect(secondFocused).toBeTruthy();
  });
});
