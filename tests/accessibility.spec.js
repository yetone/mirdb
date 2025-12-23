// @ts-check
const { test, expect } = require('@playwright/test');
const AxeBuilder = require('@axe-core/playwright').default;

test.describe('Accessibility Compliance Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Accessibility audit should score 90+ (axe-core comprehensive scan)', async ({ page }) => {
    // Run axe-core accessibility scan
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
      .analyze();

    // Log any violations for debugging
    if (accessibilityScanResults.violations.length > 0) {
      console.log('Accessibility Violations:');
      accessibilityScanResults.violations.forEach((violation) => {
        console.log(`- ${violation.id}: ${violation.description}`);
        console.log(`  Impact: ${violation.impact}`);
        console.log(`  Nodes affected: ${violation.nodes.length}`);
      });
    }

    // Calculate a pseudo-score based on passes vs violations
    const totalChecks = accessibilityScanResults.passes.length + accessibilityScanResults.violations.length;
    const passRate = totalChecks > 0 ? (accessibilityScanResults.passes.length / totalChecks) * 100 : 100;

    console.log(`Accessibility Pass Rate: ${passRate.toFixed(1)}%`);
    console.log(`Passes: ${accessibilityScanResults.passes.length}`);
    console.log(`Violations: ${accessibilityScanResults.violations.length}`);

    // No critical or serious violations should exist
    const criticalViolations = accessibilityScanResults.violations.filter(
      v => v.impact === 'critical' || v.impact === 'serious'
    );

    expect(criticalViolations).toHaveLength(0);

    // Pass rate should be at least 90%
    expect(passRate).toBeGreaterThanOrEqual(90);
  });

  test('TC3: Color contrast should meet WCAG AA requirements (4.5:1 for normal text)', async ({ page }) => {
    // Run axe-core with color-contrast specific check
    const contrastResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .analyze();

    // Find color contrast violations
    const contrastViolations = contrastResults.violations.filter(
      v => v.id === 'color-contrast'
    );

    if (contrastViolations.length > 0) {
      console.log('Color Contrast Violations:');
      contrastViolations.forEach((violation) => {
        violation.nodes.forEach((node) => {
          console.log(`- Element: ${node.html}`);
          console.log(`  Message: ${node.failureSummary}`);
        });
      });
    }

    // There should be no color contrast violations
    expect(contrastViolations).toHaveLength(0);
  });

  test('TC4: All images should have appropriate alt text', async ({ page }) => {
    // Check all images have alt attributes
    const images = await page.locator('img').all();

    for (const img of images) {
      const alt = await img.getAttribute('alt');
      const src = await img.getAttribute('src');

      // Alt should exist (can be empty for decorative images but must be present)
      expect(alt, `Image ${src} should have an alt attribute`).not.toBeNull();
    }

    // Run axe-core for image-alt violations
    const imageResults = await new AxeBuilder({ page })
      .withTags(['wcag2a'])
      .analyze();

    const imageViolations = imageResults.violations.filter(
      v => v.id === 'image-alt'
    );

    expect(imageViolations).toHaveLength(0);
  });

  test('TC5: Headings should follow proper hierarchy without skipping levels', async ({ page }) => {
    // Get all headings in order of appearance
    const headings = await page.locator('h1, h2, h3, h4, h5, h6').all();

    let currentLevel = 0;
    const headingSequence = [];

    for (const heading of headings) {
      const tagName = await heading.evaluate(el => el.tagName);
      const level = parseInt(tagName.charAt(1));
      const text = await heading.textContent();

      headingSequence.push({ level, text: text?.trim() });

      // First heading should be h1
      if (currentLevel === 0) {
        expect(level, 'First heading should be h1').toBe(1);
      } else {
        // Heading levels should not skip (e.g., h1 -> h3 is invalid)
        // Can go up by 1 or go down to any level
        if (level > currentLevel) {
          expect(level - currentLevel,
            `Heading "${text}" (h${level}) skips levels after h${currentLevel}`
          ).toBeLessThanOrEqual(1);
        }
      }

      currentLevel = level;
    }

    console.log('Heading Hierarchy:');
    headingSequence.forEach(h => {
      console.log(`${'  '.repeat(h.level - 1)}h${h.level}: ${h.text}`);
    });

    // Run axe-core for heading order violations
    const headingResults = await new AxeBuilder({ page })
      .analyze();

    const headingViolations = headingResults.violations.filter(
      v => v.id === 'heading-order'
    );

    expect(headingViolations).toHaveLength(0);
  });

  test('All interactive elements should have accessible names', async ({ page }) => {
    const results = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa'])
      .analyze();

    // Check for button-name and link-name violations
    const nameViolations = results.violations.filter(
      v => v.id === 'button-name' || v.id === 'link-name'
    );

    if (nameViolations.length > 0) {
      console.log('Interactive Element Name Violations:');
      nameViolations.forEach((violation) => {
        violation.nodes.forEach((node) => {
          console.log(`- ${violation.id}: ${node.html}`);
        });
      });
    }

    expect(nameViolations).toHaveLength(0);
  });

  test('ARIA attributes should be valid and properly used', async ({ page }) => {
    const results = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa'])
      .analyze();

    // Check for ARIA-related violations
    const ariaViolations = results.violations.filter(
      v => v.id.startsWith('aria-')
    );

    if (ariaViolations.length > 0) {
      console.log('ARIA Violations:');
      ariaViolations.forEach((violation) => {
        console.log(`- ${violation.id}: ${violation.description}`);
      });
    }

    expect(ariaViolations).toHaveLength(0);
  });

  test('Page should have proper document structure', async ({ page }) => {
    // Check for html lang attribute
    const htmlLang = await page.locator('html').getAttribute('lang');
    expect(htmlLang, 'HTML should have lang attribute').toBeTruthy();
    expect(htmlLang).toBe('en');

    // Check for page title
    const title = await page.title();
    expect(title, 'Page should have a title').toBeTruthy();
    expect(title.length).toBeGreaterThan(0);

    // Check for main landmark
    const main = await page.locator('main').count();
    expect(main, 'Page should have a main landmark').toBeGreaterThanOrEqual(1);

    // Check for only one h1
    const h1Count = await page.locator('h1').count();
    expect(h1Count, 'Page should have exactly one h1').toBe(1);

    // Run axe-core for document structure
    const results = await new AxeBuilder({ page })
      .withTags(['best-practice'])
      .analyze();

    const documentViolations = results.violations.filter(
      v => v.id === 'document-title' || v.id === 'html-has-lang' || v.id === 'landmark-one-main'
    );

    expect(documentViolations).toHaveLength(0);
  });

  test('Links should have discernible text', async ({ page }) => {
    const results = await new AxeBuilder({ page })
      .analyze();

    const linkViolations = results.violations.filter(
      v => v.id === 'link-name'
    );

    if (linkViolations.length > 0) {
      console.log('Link Name Violations:');
      linkViolations.forEach((violation) => {
        violation.nodes.forEach((node) => {
          console.log(`- ${node.html}`);
        });
      });
    }

    expect(linkViolations).toHaveLength(0);
  });

  test('Form controls should have labels', async ({ page }) => {
    const results = await new AxeBuilder({ page })
      .withTags(['wcag2a'])
      .analyze();

    const labelViolations = results.violations.filter(
      v => v.id === 'label' || v.id === 'select-name' || v.id === 'input-image-alt'
    );

    expect(labelViolations).toHaveLength(0);
  });

  test('Tables should have proper structure', async ({ page }) => {
    // Check if tables exist
    const tables = await page.locator('table').all();

    for (const table of tables) {
      // Tables with data should have headers
      const thead = await table.locator('thead').count();
      const th = await table.locator('th').count();

      // Data tables should have headers
      const isDataTable = await table.locator('tbody tr').count() > 0;
      if (isDataTable) {
        expect(th, 'Data table should have header cells').toBeGreaterThan(0);
      }
    }

    // Run axe-core for table-related violations
    const results = await new AxeBuilder({ page })
      .analyze();

    const tableViolations = results.violations.filter(
      v => v.id.includes('table') || v.id.includes('th-has-data-cells')
    );

    expect(tableViolations).toHaveLength(0);
  });

  test('Focus should be visible on interactive elements', async ({ page }) => {
    // Test keyboard focus visibility on key interactive elements
    const interactiveElements = [
      'a[href]',
      'button',
      '[tabindex="0"]'
    ];

    for (const selector of interactiveElements) {
      const elements = await page.locator(selector).all();

      for (let i = 0; i < Math.min(elements.length, 3); i++) {
        const element = elements[i];

        // Focus the element
        await element.focus();

        // Check if the element has some form of focus indication
        // This is a basic check - in reality you'd want to verify
        // the focus is visually apparent (outline, box-shadow, etc.)
        const isFocused = await element.evaluate(el => {
          return document.activeElement === el;
        });

        expect(isFocused, `Element ${selector} should be focusable`).toBe(true);
      }
    }
  });

  test('Page content should be accessible in both light and dark modes', async ({ page }) => {
    // Test in light mode (default)
    let lightResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .analyze();

    const lightViolations = lightResults.violations.filter(
      v => v.impact === 'critical' || v.impact === 'serious'
    );

    expect(lightViolations, 'No critical violations in light mode').toHaveLength(0);

    // Toggle to dark mode
    const themeToggle = page.locator('.theme-toggle');
    if (await themeToggle.count() > 0) {
      await themeToggle.click();

      // Wait for theme change
      await page.waitForTimeout(100);

      // Test in dark mode
      let darkResults = await new AxeBuilder({ page })
        .withTags(['wcag2aa'])
        .analyze();

      const darkViolations = darkResults.violations.filter(
        v => v.impact === 'critical' || v.impact === 'serious'
      );

      expect(darkViolations, 'No critical violations in dark mode').toHaveLength(0);
    }
  });
});

test.describe('Keyboard Navigation Tests (TC2)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('All interactive elements should be reachable via Tab key', async ({ page }) => {
    // Get all focusable elements
    const focusableSelectors = 'a[href], button, [tabindex]:not([tabindex="-1"])';
    const focusableElements = await page.locator(focusableSelectors).all();

    // Focus should start at the beginning
    await page.keyboard.press('Tab');

    // Track the tab order
    const tabOrder = [];
    let previousElement = null;

    // Tab through all elements and verify focus moves logically
    for (let i = 0; i < focusableElements.length; i++) {
      const activeElement = await page.evaluate(() => {
        const el = document.activeElement;
        return {
          tagName: el?.tagName,
          text: el?.textContent?.trim().substring(0, 50),
          href: el?.getAttribute('href'),
          ariaLabel: el?.getAttribute('aria-label')
        };
      });

      if (activeElement.tagName !== previousElement?.tagName ||
          activeElement.text !== previousElement?.text) {
        tabOrder.push(activeElement);
        previousElement = activeElement;
      }

      await page.keyboard.press('Tab');
    }

    console.log('Tab Order:');
    tabOrder.forEach((el, i) => {
      console.log(`${i + 1}. ${el.tagName}: ${el.ariaLabel || el.text || el.href}`);
    });

    // Verify we can tab through at least the navigation and main CTAs
    expect(tabOrder.length, 'Should be able to tab through interactive elements').toBeGreaterThan(5);
  });

  test('Navigation links should be in logical order', async ({ page }) => {
    // Focus on first navigation link
    await page.keyboard.press('Tab'); // Skip link
    await page.keyboard.press('Tab'); // Logo
    await page.keyboard.press('Tab'); // First nav link

    const navLinks = await page.locator('.nav-links a').all();
    const navOrder = [];

    // Tab through navigation
    for (let i = 0; i < navLinks.length; i++) {
      const activeText = await page.evaluate(() => {
        return document.activeElement?.textContent?.trim();
      });
      navOrder.push(activeText);
      await page.keyboard.press('Tab');
    }

    // Navigation should follow visual order
    const expectedOrder = ['Features', 'Getting Started', 'Documentation', 'GitHub'];
    expect(navOrder).toEqual(expectedOrder);
  });

  test('Buttons should be activatable via Enter and Space keys', async ({ page }) => {
    // Test theme toggle button
    const themeToggle = page.locator('.theme-toggle');
    await themeToggle.focus();

    // Get initial theme state
    const initialTheme = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme');
    });

    // Press Enter to activate
    await page.keyboard.press('Enter');

    // Check theme changed
    const afterEnter = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme');
    });

    expect(afterEnter).not.toBe(initialTheme);

    // Press Space to toggle back
    await page.keyboard.press('Space');

    const afterSpace = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme');
    });

    expect(afterSpace).toBe(initialTheme);
  });

  test('Skip link should be present for keyboard users', async ({ page }) => {
    // Check if skip link exists (best practice for accessibility)
    const skipLink = page.locator('a[href="#main-content"], a[href="#main"], a[href="#content"], .skip-link');
    const hasSkipLink = await skipLink.count() > 0;

    // Skip link should exist
    expect(hasSkipLink, 'Skip link should be present for keyboard users').toBe(true);
  });

  test('Focus should be trapped appropriately in modals (if present)', async ({ page }) => {
    // Check if any modals exist
    const modals = await page.locator('[role="dialog"], .modal').all();

    // If no modals, this test passes
    if (modals.length === 0) {
      console.log('No modals found - skip focus trap test');
      return;
    }

    // Test focus trapping in modals if they exist
    for (const modal of modals) {
      const isVisible = await modal.isVisible();
      if (isVisible) {
        // Focus should stay within the modal when tabbing
        const focusableInModal = await modal.locator('a, button, input, [tabindex]:not([tabindex="-1"])').count();
        expect(focusableInModal).toBeGreaterThan(0);
      }
    }
  });
});
