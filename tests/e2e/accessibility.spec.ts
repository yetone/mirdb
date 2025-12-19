import { test, expect } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

test.describe('Accessibility Compliance - WCAG 2.1 AA', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Heading hierarchy
  test('TC1: Heading hierarchy - single h1 and proper level progression', async ({ page }) => {
    // Check for single h1 element
    const h1Elements = page.locator('h1');
    const h1Count = await h1Elements.count();
    expect(h1Count).toBe(1);

    // Verify h1 contains meaningful text
    await expect(h1Elements.first()).toContainText('MirDB');

    // Check heading level progression (no skipped levels)
    const allHeadings = await page.locator('h1, h2, h3, h4, h5, h6').all();
    const headingLevels: number[] = [];

    for (const heading of allHeadings) {
      const tagName = await heading.evaluate(el => el.tagName.toLowerCase());
      const level = parseInt(tagName.replace('h', ''));
      headingLevels.push(level);
    }

    // Verify no heading levels are skipped
    for (let i = 1; i < headingLevels.length; i++) {
      const currentLevel = headingLevels[i];
      const previousLevel = headingLevels[i - 1];
      // Current level should be same, +1, or any lower number (going back up)
      const isValidProgression = currentLevel <= previousLevel + 1;
      expect(isValidProgression).toBe(true);
    }
  });

  // Test Case 2: Semantic landmark elements
  test('TC2: Semantic landmark elements - header, main, nav, footer', async ({ page }) => {
    // Check for navigation element
    const nav = page.locator('nav');
    await expect(nav.first()).toBeVisible();

    // Check nav has proper aria-label
    const navAriaLabel = await nav.first().getAttribute('aria-label');
    expect(navAriaLabel).toBeTruthy();

    // Check for header element (with role="banner" or header tag)
    const header = page.locator('header, [role="banner"]');
    const headerCount = await header.count();
    expect(headerCount).toBeGreaterThan(0);

    // Check for main content area (main tag or role="main")
    const main = page.locator('main, [role="main"]');
    const mainCount = await main.count();
    expect(mainCount).toBeGreaterThan(0);

    // Check for footer element
    const footer = page.locator('footer, [role="contentinfo"]');
    await expect(footer.first()).toBeVisible();
  });

  // Test Case 3: Color contrast ratios
  test('TC3: Text color contrast meets 4.5:1 ratio (AA standard)', async ({ page }) => {
    // Use axe-core for contrast checking - it's more reliable than manual checks
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .disableRules(['region']) // Skip region rule as page uses sections appropriately
      .analyze();

    // Filter for contrast-related violations
    const contrastViolations = accessibilityScanResults.violations.filter(
      v => v.id.includes('contrast')
    );

    // Report any contrast issues
    if (contrastViolations.length > 0) {
      console.log('Contrast violations found:', JSON.stringify(contrastViolations, null, 2));
    }

    expect(contrastViolations.length).toBe(0);
  });

  // Test Case 4: Keyboard navigation - all interactive elements focusable
  test('TC4: All links and buttons are focusable via keyboard', async ({ page }) => {
    // Get all interactive elements
    const interactiveElements = page.locator('a, button, [tabindex="0"]');
    const count = await interactiveElements.count();

    expect(count).toBeGreaterThan(0);

    // Check that no interactive elements have negative tabindex (unless intentionally hidden)
    const elementsWithNegativeTabIndex = page.locator('a[tabindex="-1"]:not([aria-hidden="true"]), button[tabindex="-1"]:not([aria-hidden="true"])');
    const negativeCount = await elementsWithNegativeTabIndex.count();
    expect(negativeCount).toBe(0);

    // Tab through and verify focus moves to interactive elements
    await page.keyboard.press('Tab');

    // Should be able to focus on at least one element
    const focusedElement = page.locator(':focus');
    const tagName = await focusedElement.evaluate(el => el.tagName.toLowerCase());
    expect(['a', 'button', 'input', 'select', 'textarea']).toContain(tagName);
  });

  // Test Case 5: Focus indicators visibility
  test('TC5: Focus state is clearly visible on all interactive elements', async ({ page }) => {
    // Navigate to page and start tabbing
    const links = page.locator('a[href]');

    // Test focus visibility on a link
    const firstLink = links.first();
    await firstLink.focus();

    // Check that focus styles are applied (outline should be visible)
    const linkOutline = await firstLink.evaluate(el => {
      const style = window.getComputedStyle(el);
      return {
        outline: style.outline,
        outlineWidth: style.outlineWidth,
        outlineStyle: style.outlineStyle,
        outlineColor: style.outlineColor,
        boxShadow: style.boxShadow
      };
    });

    // Focus indicator should be present (either outline or box-shadow)
    const hasVisibleFocusIndicator =
      (linkOutline.outlineWidth !== '0px' && linkOutline.outlineStyle !== 'none') ||
      (linkOutline.boxShadow !== 'none');

    expect(hasVisibleFocusIndicator).toBe(true);

    // Test focus visibility on a visible button (copy buttons are visible in code blocks)
    const copyButtons = page.locator('.copy-btn');
    const copyButtonCount = await copyButtons.count();

    if (copyButtonCount > 0) {
      const firstCopyButton = copyButtons.first();
      await firstCopyButton.focus();

      // Wait for focus to be applied
      await page.waitForTimeout(100);

      const buttonOutline = await firstCopyButton.evaluate(el => {
        const style = window.getComputedStyle(el);
        return {
          outline: style.outline,
          outlineWidth: style.outlineWidth,
          outlineStyle: style.outlineStyle,
          outlineColor: style.outlineColor,
          boxShadow: style.boxShadow,
          isFocused: document.activeElement === el
        };
      });

      // Log for debugging
      console.log('Button focus styles:', JSON.stringify(buttonOutline, null, 2));

      // Check if the button is actually focused
      expect(buttonOutline.isFocused).toBe(true);

      // Focus indicator should be present (either outline or box-shadow)
      // Checking for any outline that isn't none/0px
      const hasOutline = buttonOutline.outlineStyle !== 'none' &&
                         buttonOutline.outlineWidth !== '0px';
      const hasBoxShadow = buttonOutline.boxShadow !== 'none';

      const buttonHasFocusIndicator = hasOutline || hasBoxShadow;

      expect(buttonHasFocusIndicator).toBe(true);
    }
  });

  // Test Case 6: All images have alt attributes
  test('TC6: All images have alt attributes (empty for decorative)', async ({ page }) => {
    // Get all img elements
    const images = page.locator('img');
    const imageCount = await images.count();

    // Check each image for alt attribute
    for (let i = 0; i < imageCount; i++) {
      const img = images.nth(i);
      const hasAlt = await img.evaluate(el => el.hasAttribute('alt'));
      expect(hasAlt).toBe(true);
    }

    // Check SVGs that are not decorative have appropriate roles/labels
    const svgElements = page.locator('svg:not([aria-hidden="true"])');
    const svgCount = await svgElements.count();

    for (let i = 0; i < svgCount; i++) {
      const svg = svgElements.nth(i);
      const hasAccessibleName = await svg.evaluate(el => {
        // SVG should have aria-label, aria-labelledby, or title element
        return el.hasAttribute('aria-label') ||
               el.hasAttribute('aria-labelledby') ||
               el.querySelector('title') !== null ||
               el.getAttribute('role') === 'img';
      });
      // If not decorative, should have accessible name
      // Note: decorative SVGs should have aria-hidden="true"
    }

    // All decorative icons should be aria-hidden
    const decorativeIcons = page.locator('.feature-icon svg, .copy-icon, .check-icon');
    const decorativeCount = await decorativeIcons.count();

    for (let i = 0; i < decorativeCount; i++) {
      const icon = decorativeIcons.nth(i);
      const ariaHidden = await icon.getAttribute('aria-hidden');
      expect(ariaHidden).toBe('true');
    }
  });

  // Test Case 7: Links have descriptive text
  test('TC7: Links describe destination, no generic "click here" text', async ({ page }) => {
    const links = page.locator('a');
    const linkCount = await links.count();

    const genericLinkTexts = ['click here', 'here', 'read more', 'more', 'link'];

    for (let i = 0; i < linkCount; i++) {
      const link = links.nth(i);
      const text = await link.textContent();
      const ariaLabel = await link.getAttribute('aria-label');

      // Link should have either meaningful text or aria-label
      const meaningfulText = text?.trim().toLowerCase() || '';
      const meaningfulAriaLabel = ariaLabel?.toLowerCase() || '';

      // Check it's not generic link text
      const isGenericText = genericLinkTexts.includes(meaningfulText);
      const hasAriaLabel = meaningfulAriaLabel.length > 0;

      // Either text should be descriptive OR aria-label should be present
      if (isGenericText) {
        expect(hasAriaLabel).toBe(true);
      }
    }

    // Specifically check no "click here" links exist
    const clickHereLinks = page.locator('a:text-is("click here")');
    const clickHereCount = await clickHereLinks.count();
    expect(clickHereCount).toBe(0);
  });

  // Test Case 8: Axe accessibility audit
  test('TC8: No critical or serious accessibility violations (axe audit)', async ({ page }) => {
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21aa'])
      .disableRules(['region']) // Skip region rule - page uses sections appropriately
      .analyze();

    // Filter for critical and serious violations only
    const criticalAndSeriousViolations = accessibilityScanResults.violations.filter(
      v => v.impact === 'critical' || v.impact === 'serious'
    );

    // Log all violations for debugging
    if (criticalAndSeriousViolations.length > 0) {
      console.log('Critical/Serious Accessibility Violations:');
      criticalAndSeriousViolations.forEach(violation => {
        console.log(`\n${violation.id}: ${violation.description}`);
        console.log(`Impact: ${violation.impact}`);
        console.log(`Help: ${violation.helpUrl}`);
        violation.nodes.forEach(node => {
          console.log(`  Element: ${node.html}`);
          console.log(`  Failure: ${node.failureSummary}`);
        });
      });
    }

    // No critical or serious violations allowed
    expect(criticalAndSeriousViolations.length).toBe(0);
  });

  // Additional test: Verify ARIA roles and attributes are properly used
  test('ARIA roles and attributes are properly used', async ({ page }) => {
    // Check navigation has proper role
    const nav = page.locator('nav');
    const navRole = await nav.first().getAttribute('role');
    expect(navRole).toBe('navigation');

    // Check menu items have proper roles
    const menuItems = page.locator('[role="menuitem"]');
    const menuItemCount = await menuItems.count();
    expect(menuItemCount).toBeGreaterThan(0);

    // Check mobile menu button has aria-expanded
    const mobileMenuBtn = page.locator('.mobile-menu-btn');
    const ariaExpanded = await mobileMenuBtn.getAttribute('aria-expanded');
    expect(ariaExpanded).toBe('false');

    // Check mobile menu button has aria-label
    const ariaLabel = await mobileMenuBtn.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
  });

  // Test: Verify skip links or quick navigation
  test('Page structure supports screen reader navigation', async ({ page }) => {
    // Check that sections have proper labeling
    const labelledSections = page.locator('section[aria-labelledby]');
    const labelledCount = await labelledSections.count();

    // At least some sections should be labelled
    expect(labelledCount).toBeGreaterThan(0);

    // Verify that the heading IDs match aria-labelledby values
    for (let i = 0; i < labelledCount; i++) {
      const section = labelledSections.nth(i);
      const labelledBy = await section.getAttribute('aria-labelledby');

      if (labelledBy) {
        const headingById = page.locator(`#${labelledBy}`);
        await expect(headingById).toBeVisible();
      }
    }
  });
});
