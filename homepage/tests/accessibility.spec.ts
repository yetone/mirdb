import { test, expect } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

test.describe('Accessibility Compliance - WCAG 2.1 Level AA', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for page to be fully loaded
    await page.waitForLoadState('networkidle');
  });

  // Test Case 1: Run Lighthouse accessibility audit (using axe-core as Lighthouse alternative)
  test('TC1: No critical accessibility violations reported', async ({ page }) => {
    // Run axe accessibility scan targeting WCAG 2.1 Level AA
    const accessibilityResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
      .analyze();

    // Filter for critical and serious violations only
    const criticalViolations = accessibilityResults.violations.filter(
      (v) => v.impact === 'critical' || v.impact === 'serious'
    );

    // Log violations for debugging
    if (criticalViolations.length > 0) {
      console.log('Critical/Serious Accessibility Violations:');
      criticalViolations.forEach((violation) => {
        console.log(`- ${violation.id}: ${violation.description}`);
        console.log(`  Impact: ${violation.impact}`);
        console.log(`  Help: ${violation.helpUrl}`);
        violation.nodes.forEach((node) => {
          console.log(`  Element: ${node.html.substring(0, 100)}`);
        });
      });
    }

    expect(
      criticalViolations,
      `Expected no critical accessibility violations, but found ${criticalViolations.length}: ${criticalViolations.map((v) => v.id).join(', ')}`
    ).toHaveLength(0);
  });

  // Test Case 2: Tab through all interactive elements
  test('TC2: All buttons, links, and interactive elements are keyboard accessible', async ({
    page,
  }) => {
    // Get all interactive elements that should be focusable
    const interactiveSelectors = [
      'a[href]',
      'button',
      '[role="button"]',
      'input',
      'select',
      'textarea',
      '[tabindex]:not([tabindex="-1"])',
    ];

    const interactiveElements = await page.locator(interactiveSelectors.join(', ')).all();
    expect(interactiveElements.length).toBeGreaterThan(0);

    // Track which elements receive focus during tab navigation
    const focusedElements: string[] = [];
    let previousActiveElement = '';
    let tabCount = 0;
    const maxTabs = 100; // Safety limit

    // Start tabbing from the beginning of the document
    await page.keyboard.press('Tab');

    while (tabCount < maxTabs) {
      // Get current focused element info
      const activeElement = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) return '';
        return el.tagName + (el.id ? `#${el.id}` : '') + (el.className ? `.${el.className.split(' ')[0]}` : '');
      });

      // Break if we've cycled back to start or reached body
      if (activeElement === '' || (activeElement === focusedElements[0] && focusedElements.length > 1)) {
        break;
      }

      if (activeElement !== previousActiveElement && activeElement) {
        focusedElements.push(activeElement);
        previousActiveElement = activeElement;
      }

      await page.keyboard.press('Tab');
      tabCount++;
    }

    // Verify we can tab through multiple elements
    expect(focusedElements.length).toBeGreaterThan(5);

    // Verify specific interactive elements are reachable by keyboard
    // Check navigation links are focusable
    const navLinks = page.locator('[data-testid="desktop-nav"] a');
    const navLinkCount = await navLinks.count();
    for (let i = 0; i < navLinkCount; i++) {
      const link = navLinks.nth(i);
      await link.focus();
      await expect(link).toBeFocused();
    }

    // Check CTA buttons are focusable
    const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
    await getStartedBtn.focus();
    await expect(getStartedBtn).toBeFocused();

    const githubBtn = page.locator('[data-testid="cta-github"]');
    await githubBtn.focus();
    await expect(githubBtn).toBeFocused();

    // Note: Mobile menu button has md:hidden class, so it's not visible/focusable on desktop
    // We verify it has proper keyboard accessibility attributes instead
    const mobileMenuBtn = page.locator('[data-testid="mobile-menu-button"]');
    await expect(mobileMenuBtn).toHaveAttribute('aria-label', /toggle menu/i);
    await expect(mobileMenuBtn).toHaveAttribute('type', 'button');

    // Check copy buttons are focusable
    const copyButtons = page.locator('[data-testid="copy-button"]');
    const copyButtonCount = await copyButtons.count();
    expect(copyButtonCount).toBeGreaterThan(0);
    for (let i = 0; i < Math.min(copyButtonCount, 3); i++) {
      const btn = copyButtons.nth(i);
      await btn.focus();
      await expect(btn).toBeFocused();
    }
  });

  // Test Case 3: Check heading hierarchy
  test('TC3: Semantic HTML with proper h1-h6 hierarchy (no skipped levels)', async ({ page }) => {
    // Get all heading elements
    const headings = await page.evaluate(() => {
      const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      return Array.from(allHeadings).map((h) => ({
        level: parseInt(h.tagName[1]),
        text: h.textContent?.trim().substring(0, 50) || '',
      }));
    });

    expect(headings.length).toBeGreaterThan(0);

    // Verify there is exactly one h1
    const h1Count = headings.filter((h) => h.level === 1).length;
    expect(h1Count, 'Page should have exactly one h1 element').toBe(1);

    // Verify h1 contains MirDB
    const h1 = await page.locator('h1').first();
    await expect(h1).toContainText(/mirdb/i);

    // Check for skipped heading levels
    let previousLevel = 0;
    const skippedLevels: { from: number; to: number; text: string }[] = [];

    for (const heading of headings) {
      // First heading can be any level (usually h1)
      if (previousLevel === 0) {
        previousLevel = heading.level;
        continue;
      }

      // Going deeper: can only skip by 1 level max
      if (heading.level > previousLevel + 1) {
        skippedLevels.push({
          from: previousLevel,
          to: heading.level,
          text: heading.text,
        });
      }

      previousLevel = heading.level;
    }

    // Log any skipped levels
    if (skippedLevels.length > 0) {
      console.log('Skipped heading levels found:');
      skippedLevels.forEach((skip) => {
        console.log(`  h${skip.from} -> h${skip.to}: "${skip.text}"`);
      });
    }

    expect(
      skippedLevels,
      `Found ${skippedLevels.length} skipped heading levels`
    ).toHaveLength(0);

    // Verify section headings exist
    const sectionHeadings = [
      { section: 'features', expectedLevel: 2 },
      { section: 'architecture', expectedLevel: 2 },
      { section: 'quick-start', expectedLevel: 2 },
      { section: 'comparison', expectedLevel: 2 },
      { section: 'documentation', expectedLevel: 2 },
    ];

    for (const { section } of sectionHeadings) {
      const sectionEl = page.locator(`[data-testid="${section}-section"]`);
      const sectionH2 = sectionEl.locator('h2').first();
      await expect(sectionH2).toBeVisible();
    }
  });

  // Test Case 4: Verify all images have alt text
  test('TC4: Every image element has descriptive alt attribute', async ({ page }) => {
    // Get all img elements
    const images = await page.locator('img').all();
    expect(images.length).toBeGreaterThan(0);

    for (const img of images) {
      const alt = await img.getAttribute('alt');
      const src = await img.getAttribute('src');

      // Verify alt attribute exists
      expect(alt, `Image ${src} is missing alt attribute`).not.toBeNull();

      // Verify alt is not empty (decorative images should use alt="" but our images are meaningful)
      expect(
        alt?.trim().length,
        `Image ${src} has empty alt attribute`
      ).toBeGreaterThan(0);
    }

    // Check SVG images with role="img" have proper aria-label or title
    const svgImages = await page.locator('svg[role="img"]').all();
    for (const svg of svgImages) {
      const ariaLabel = await svg.getAttribute('aria-label');
      const title = await svg.locator('title').first();

      // SVG should have either aria-label or title element
      const hasAriaLabel = ariaLabel && ariaLabel.trim().length > 0;
      const hasTitle = (await title.count()) > 0;

      expect(
        hasAriaLabel || hasTitle,
        'SVG with role="img" should have aria-label or title'
      ).toBeTruthy();
    }

    // Specifically check logo images
    const heroLogo = page.locator('[data-testid="hero-logo"]');
    await expect(heroLogo).toHaveAttribute('alt', /mirdb/i);

    const navLogo = page.locator('nav img');
    await expect(navLogo).toHaveAttribute('alt', /mirdb/i);
  });

  // Test Case 5: Test with screen reader - Code blocks are accessible
  test('TC5: Code blocks are accessible and content is properly announced', async ({ page }) => {
    // Check code blocks have appropriate structure
    const codeBlocks = page.locator('.code-block-container');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    for (let i = 0; i < codeBlockCount; i++) {
      const block = codeBlocks.nth(i);

      // Verify pre element exists
      const pre = block.locator('pre');
      await expect(pre).toBeVisible();

      // Verify code element exists inside pre
      const code = pre.locator('code');
      await expect(code).toBeVisible();

      // Code should have a language class for screen reader context
      const codeClass = await code.getAttribute('class');
      expect(codeClass).toMatch(/language-/);

      // Copy button should have aria-label for screen readers
      const copyBtn = block.locator('[data-testid="copy-button"]');
      await expect(copyBtn).toHaveAttribute('aria-label', /copy/i);
    }

    // Check that the LSM tree diagram has proper accessibility
    const lsmDiagram = page.locator('[data-testid="lsm-tree-diagram"]');
    await expect(lsmDiagram).toHaveAttribute('role', 'img');
    await expect(lsmDiagram).toHaveAttribute('aria-label', /.+/);

    // Verify diagram has title and desc for detailed description
    // Note: SVG title/desc elements are not visually displayed but are accessible to screen readers
    const diagramTitle = lsmDiagram.locator('title');
    await expect(diagramTitle).toHaveCount(1);
    const titleText = await diagramTitle.textContent();
    expect(titleText?.length).toBeGreaterThan(0);

    const diagramDesc = lsmDiagram.locator('desc');
    await expect(diagramDesc).toHaveCount(1);
    const descText = await diagramDesc.textContent();
    expect(descText?.length).toBeGreaterThan(0);
  });

  // Test Case 6: Check color contrast ratios
  test('TC6: All text meets WCAG 2.1 AA contrast requirements', async ({ page }) => {
    // Run axe accessibility scan specifically for color contrast
    const contrastResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa'])
      .options({ runOnly: ['color-contrast'] })
      .analyze();

    // Log any contrast violations
    if (contrastResults.violations.length > 0) {
      console.log('Color Contrast Violations:');
      contrastResults.violations.forEach((violation) => {
        console.log(`- ${violation.id}: ${violation.description}`);
        violation.nodes.forEach((node) => {
          console.log(`  Element: ${node.html.substring(0, 100)}`);
          console.log(`  Failure Summary: ${node.failureSummary}`);
        });
      });
    }

    expect(
      contrastResults.violations,
      `Found ${contrastResults.violations.length} color contrast violations`
    ).toHaveLength(0);

    // Additional manual checks for known text elements
    // Verify main text colors meet contrast requirements against dark background

    // Hero section text should be visible
    const heroTagline = page.locator('[data-testid="hero-tagline"]');
    await expect(heroTagline).toBeVisible();

    // Feature descriptions should be visible
    const featureDescriptions = page.locator('.feature-description');
    const descCount = await featureDescriptions.count();
    expect(descCount).toBeGreaterThan(0);

    // Section headings should be prominent (check visible ones only)
    const sectionHeadings = page.locator('h2:visible');
    const headingCount = await sectionHeadings.count();
    expect(headingCount).toBeGreaterThan(0);
    for (let i = 0; i < headingCount; i++) {
      const heading = sectionHeadings.nth(i);
      await expect(heading).toBeVisible();
    }
  });
});
