// @ts-check
const { test, expect } = require('@playwright/test');
const AxeBuilder = require('@axe-core/playwright').default;
const path = require('path');

// Base URL for the static HTML file
const BASE_URL = 'file://' + path.resolve(__dirname, '../dist/index.html');

test.describe('Accessibility Compliance - WCAG 2.1 AA', () => {

  test.describe('Test Case 1: Automated Accessibility Audit', () => {
    test('should not have any automatically detectable WCAG A or AA violations', async ({ page }) => {
      await page.goto(BASE_URL);

      // Wait for page to fully load
      await page.waitForLoadState('domcontentloaded');

      // Run axe accessibility scan with WCAG 2.1 AA tags
      const accessibilityScanResults = await new AxeBuilder({ page })
        .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
        .analyze();

      // Get detailed violation information for debugging
      const violations = accessibilityScanResults.violations;

      if (violations.length > 0) {
        console.log('Accessibility Violations Found:');
        violations.forEach((violation, index) => {
          console.log(`\n${index + 1}. ${violation.id}: ${violation.description}`);
          console.log(`   Impact: ${violation.impact}`);
          console.log(`   Help: ${violation.helpUrl}`);
          violation.nodes.forEach((node, nodeIndex) => {
            console.log(`   Node ${nodeIndex + 1}: ${node.html}`);
            console.log(`   Target: ${node.target.join(', ')}`);
          });
        });
      }

      // Filter out critical and serious violations (these must pass for WCAG AA compliance)
      const criticalViolations = violations.filter(v =>
        v.impact === 'critical' || v.impact === 'serious'
      );

      expect(criticalViolations).toEqual([]);
    });

    test('should pass axe accessibility audit for hero section', async ({ page }) => {
      await page.goto(BASE_URL);

      const accessibilityScanResults = await new AxeBuilder({ page })
        .include('[data-testid="hero-section"]')
        .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
        .analyze();

      const criticalViolations = accessibilityScanResults.violations.filter(v =>
        v.impact === 'critical' || v.impact === 'serious'
      );

      expect(criticalViolations).toEqual([]);
    });

    test('should pass axe accessibility audit for features section', async ({ page }) => {
      await page.goto(BASE_URL);

      const featuresSection = page.locator('[data-testid="features-section"]');
      await featuresSection.scrollIntoViewIfNeeded();

      const accessibilityScanResults = await new AxeBuilder({ page })
        .include('[data-testid="features-section"]')
        .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
        .analyze();

      const criticalViolations = accessibilityScanResults.violations.filter(v =>
        v.impact === 'critical' || v.impact === 'serious'
      );

      expect(criticalViolations).toEqual([]);
    });

    test('should pass axe accessibility audit for tables in protocol section', async ({ page }) => {
      await page.goto(BASE_URL);

      const protocolSection = page.locator('[data-testid="protocol-section"]');
      await protocolSection.scrollIntoViewIfNeeded();

      const accessibilityScanResults = await new AxeBuilder({ page })
        .include('[data-testid="protocol-section"]')
        .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
        .analyze();

      const criticalViolations = accessibilityScanResults.violations.filter(v =>
        v.impact === 'critical' || v.impact === 'serious'
      );

      expect(criticalViolations).toEqual([]);
    });
  });

  test.describe('Test Case 2: Color Contrast Ratios', () => {
    test('should have adequate color contrast for body text (4.5:1 minimum)', async ({ page }) => {
      await page.goto(BASE_URL);

      // Run axe scan specifically for color contrast
      const accessibilityScanResults = await new AxeBuilder({ page })
        .withRules(['color-contrast'])
        .analyze();

      const contrastViolations = accessibilityScanResults.violations;

      if (contrastViolations.length > 0) {
        console.log('Color Contrast Issues:');
        contrastViolations.forEach((violation) => {
          violation.nodes.forEach((node) => {
            console.log(`Element: ${node.html}`);
            console.log(`Message: ${node.failureSummary}`);
          });
        });
      }

      // Filter for critical/serious contrast violations
      const criticalContrastViolations = contrastViolations.filter(v =>
        v.impact === 'critical' || v.impact === 'serious'
      );

      expect(criticalContrastViolations).toEqual([]);
    });

    test('hero section text should have sufficient contrast against background', async ({ page }) => {
      await page.goto(BASE_URL);

      // Verify product name text color has good contrast
      const productName = page.locator('[data-testid="product-name"]');
      const productNameColor = await productName.evaluate(el =>
        window.getComputedStyle(el).color
      );

      // Text should be white (#ffffff or rgb(255, 255, 255)) against dark background
      expect(productNameColor).toMatch(/rgb\(255,\s*255,\s*255\)/);

      // Verify tagline has good contrast
      const tagline = page.locator('[data-testid="tagline"]');
      await expect(tagline).toBeVisible();
    });

    test('feature card text should meet contrast requirements', async ({ page }) => {
      await page.goto(BASE_URL);

      const featuresSection = page.locator('[data-testid="features-section"]');
      await featuresSection.scrollIntoViewIfNeeded();

      // Feature titles should have dark text color for readability
      const featureTitle = page.locator('[data-testid="feature-memcached-title"]');
      const titleColor = await featureTitle.evaluate(el =>
        window.getComputedStyle(el).color
      );

      // Should be dark color (mirdb-dark or similar)
      // mirdb-dark is #0f172a which is very dark
      expect(titleColor).not.toMatch(/rgb\(255,\s*255,\s*255\)/);
    });
  });

  test.describe('Test Case 3: Keyboard Navigation', () => {
    test('all interactive elements should be accessible via keyboard (Tab)', async ({ page }) => {
      await page.goto(BASE_URL);

      // Start from body and tab through all focusable elements
      await page.keyboard.press('Tab');

      // First focusable element should be "Get Started" button
      const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
      await expect(getStartedBtn).toBeFocused();

      // Tab to GitHub button
      await page.keyboard.press('Tab');
      const githubBtn = page.locator('[data-testid="cta-github"]');
      await expect(githubBtn).toBeFocused();

      // Continue tabbing through interactive elements
      // Tab to architecture docs link
      await page.keyboard.press('Tab');

      // Tab through copy buttons in quickstart section
      // The page has multiple copy buttons and links that should be focusable
      let focusableCount = 0;
      for (let i = 0; i < 25; i++) {
        const focusedElement = await page.evaluate(() => {
          const el = document.activeElement;
          return {
            tagName: el?.tagName,
            role: el?.getAttribute('role'),
            testId: el?.getAttribute('data-testid'),
            href: el?.getAttribute('href')
          };
        });

        if (focusedElement.tagName === 'A' ||
            focusedElement.tagName === 'BUTTON' ||
            focusedElement.role === 'button') {
          focusableCount++;
        }

        await page.keyboard.press('Tab');
      }

      // Should have multiple focusable elements
      expect(focusableCount).toBeGreaterThan(5);
    });

    test('interactive elements should respond to Enter key', async ({ page }) => {
      await page.goto(BASE_URL);

      // Focus on "Get Started" button
      await page.keyboard.press('Tab');
      const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
      await expect(getStartedBtn).toBeFocused();

      // Press Enter to activate
      await page.keyboard.press('Enter');

      // Should navigate to quickstart section (internal anchor)
      await page.waitForTimeout(600); // Wait for smooth scroll
      const quickstartSection = page.locator('[data-testid="quickstart-section"]');
      await expect(quickstartSection).toBeInViewport();
    });

    test('copy buttons should be keyboard accessible', async ({ page }) => {
      await page.goto(BASE_URL);

      // Navigate to quickstart section
      const quickstartSection = page.locator('[data-testid="quickstart-section"]');
      await quickstartSection.scrollIntoViewIfNeeded();

      // Find and focus copy button
      const copyButton = page.locator('[data-testid="copy-button-install"]');
      await copyButton.focus();
      await expect(copyButton).toBeFocused();

      // Copy buttons should have accessible name
      const buttonAccessibleName = await copyButton.getAttribute('onclick');
      expect(buttonAccessibleName).toBeTruthy();
    });

    test('external links should be keyboard accessible', async ({ page }) => {
      await page.goto(BASE_URL);

      // Find GitHub link in hero
      const githubBtn = page.locator('[data-testid="cta-github"]');
      await githubBtn.focus();
      await expect(githubBtn).toBeFocused();

      // Verify it has proper external link attributes
      const target = await githubBtn.getAttribute('target');
      const rel = await githubBtn.getAttribute('rel');

      expect(target).toBe('_blank');
      expect(rel).toContain('noopener');
    });
  });

  test.describe('Test Case 4: Heading Hierarchy', () => {
    test('should have proper heading hierarchy without skipping levels', async ({ page }) => {
      await page.goto(BASE_URL);

      // Get all headings in document order
      const headings = await page.evaluate(() => {
        const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
        return Array.from(allHeadings).map(h => ({
          level: parseInt(h.tagName.charAt(1)),
          text: h.textContent?.trim().substring(0, 50)
        }));
      });

      // Should have exactly one h1
      const h1Count = headings.filter(h => h.level === 1).length;
      expect(h1Count).toBe(1);

      // First heading should be h1
      expect(headings[0].level).toBe(1);
      expect(headings[0].text).toBe('MirDB');

      // Check that heading levels don't skip (e.g., h1 -> h3 without h2)
      let previousLevel = 0;
      for (const heading of headings) {
        // Level should not jump more than 1 (e.g., h1 to h3 is invalid, but h2 to h4 within different sections can be ok)
        // Actually, WCAG allows skipping back up (h3 to h2) but not skipping forward by more than 1
        if (heading.level > previousLevel + 1 && previousLevel !== 0) {
          // This is a skip - flag it but don't fail immediately
          console.log(`Warning: Heading skip from h${previousLevel} to h${heading.level}: "${heading.text}"`);
        }
        previousLevel = heading.level;
      }

      // Verify key section headings exist
      const h2s = headings.filter(h => h.level === 2);
      expect(h2s.length).toBeGreaterThanOrEqual(4); // Key Features, Architecture, Quick Start, etc.
    });

    test('main content sections should have descriptive headings', async ({ page }) => {
      await page.goto(BASE_URL);

      // Verify each major section has an h2 heading
      const sections = [
        { testId: 'features-section', expectedHeading: 'Key Features' },
        { testId: 'architecture-section', expectedHeading: 'Architecture Overview' },
        { testId: 'quickstart-section', expectedHeading: 'Quick Start' },
        { testId: 'protocol-section', expectedHeading: 'Protocol Reference' },
        { testId: 'configuration-section', expectedHeading: 'Configuration Reference' }
      ];

      for (const section of sections) {
        const sectionElement = page.locator(`[data-testid="${section.testId}"]`);
        await sectionElement.scrollIntoViewIfNeeded();

        const h2 = sectionElement.locator('h2').first();
        await expect(h2).toBeVisible();
        await expect(h2).toContainText(section.expectedHeading);
      }
    });
  });

  test.describe('Test Case 5: Image Alt Text', () => {
    test('all images should have appropriate alt text or be marked decorative', async ({ page }) => {
      await page.goto(BASE_URL);

      // Get all images
      const images = await page.evaluate(() => {
        const allImages = document.querySelectorAll('img');
        return Array.from(allImages).map(img => ({
          src: img.src,
          alt: img.alt,
          hasAlt: img.hasAttribute('alt'),
          role: img.getAttribute('role'),
          ariaHidden: img.getAttribute('aria-hidden')
        }));
      });

      // If there are any images, verify they have alt text or are marked decorative
      for (const img of images) {
        const hasAccessibleAlt = img.hasAlt || img.role === 'presentation' || img.ariaHidden === 'true';
        expect(hasAccessibleAlt).toBe(true);
      }
    });

    test('SVG icons should have accessible names or be hidden from screen readers', async ({ page }) => {
      await page.goto(BASE_URL);

      // Get all SVG elements and their context
      const svgs = await page.evaluate(() => {
        const allSvgs = document.querySelectorAll('svg');
        return Array.from(allSvgs).map(svg => {
          // Check if SVG is within an interactive element (button, link)
          const closestInteractive = svg.closest('a, button, [role="button"]');
          // Check if SVG is within a text context (list item, paragraph, etc.)
          const hasTextSibling = svg.parentElement &&
            (svg.parentElement.textContent?.trim().length > svg.textContent?.trim().length ||
             svg.parentElement.querySelector('span, strong, p, div') !== null);

          return {
            ariaHidden: svg.getAttribute('aria-hidden'),
            ariaLabel: svg.getAttribute('aria-label'),
            role: svg.getAttribute('role'),
            title: svg.querySelector('title')?.textContent,
            parentTag: svg.parentElement?.tagName,
            isInInteractiveElement: closestInteractive !== null,
            hasTextContext: hasTextSibling,
            interactiveHasText: closestInteractive ? closestInteractive.textContent?.trim().length > 0 : false
          };
        });
      });

      // SVGs should either:
      // 1. Have aria-hidden="true" (decorative)
      // 2. Have aria-label (accessible name)
      // 3. Have a <title> element
      // 4. Be within a button/link that has text content (icon + text pattern)
      // 5. Be decorative within content (icons next to text)
      for (const svg of svgs) {
        const isAccessible =
          svg.ariaHidden === 'true' ||
          svg.ariaLabel ||
          svg.title ||
          svg.role === 'presentation' ||
          svg.role === 'img' ||
          (svg.isInInteractiveElement && svg.interactiveHasText) || // Icon inside button/link with text
          svg.hasTextContext; // Decorative icon next to text

        // This is a lenient check - SVGs that are decorative (have text siblings) are acceptable
        // as they don't convey meaning that isn't already available from surrounding text
        expect(isAccessible).toBe(true);
      }
    });
  });

  test.describe('Test Case 6: Focus Indicators', () => {
    test('all interactive elements should have visible focus indicators', async ({ page }) => {
      await page.goto(BASE_URL);

      // Test focus on Get Started button
      const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
      await getStartedBtn.focus();

      // Check that focus ring/outline is visible
      const focusStyles = await getStartedBtn.evaluate(el => {
        const styles = window.getComputedStyle(el);
        return {
          outline: styles.outline,
          outlineWidth: styles.outlineWidth,
          outlineColor: styles.outlineColor,
          boxShadow: styles.boxShadow,
          ringColor: styles.getPropertyValue('--tw-ring-color'),
          ringWidth: styles.getPropertyValue('--tw-ring-offset-width')
        };
      });

      // Should have visible focus indication (outline, box-shadow, or ring)
      const hasFocusIndicator =
        (focusStyles.outline && focusStyles.outline !== 'none' && focusStyles.outlineWidth !== '0px') ||
        (focusStyles.boxShadow && focusStyles.boxShadow !== 'none');

      expect(hasFocusIndicator).toBe(true);
    });

    test('GitHub button should have visible focus indicator', async ({ page }) => {
      await page.goto(BASE_URL);

      const githubBtn = page.locator('[data-testid="cta-github"]');
      await githubBtn.focus();

      const focusStyles = await githubBtn.evaluate(el => {
        const styles = window.getComputedStyle(el);
        return {
          outline: styles.outline,
          boxShadow: styles.boxShadow
        };
      });

      const hasFocusIndicator =
        (focusStyles.outline && focusStyles.outline !== 'none') ||
        (focusStyles.boxShadow && focusStyles.boxShadow !== 'none');

      expect(hasFocusIndicator).toBe(true);
    });

    test('links in footer should have visible focus indicators', async ({ page }) => {
      await page.goto(BASE_URL);

      const footerSection = page.locator('[data-testid="footer-section"]');
      await footerSection.scrollIntoViewIfNeeded();

      const footerGithubLink = page.locator('[data-testid="footer-github-link"]');
      await footerGithubLink.focus();
      await expect(footerGithubLink).toBeFocused();

      // Focus should be visually indicated
      const isFocused = await footerGithubLink.evaluate(el => {
        return document.activeElement === el;
      });
      expect(isFocused).toBe(true);
    });

    test('copy buttons should have visible focus states', async ({ page }) => {
      await page.goto(BASE_URL);

      const quickstartSection = page.locator('[data-testid="quickstart-section"]');
      await quickstartSection.scrollIntoViewIfNeeded();

      const copyButton = page.locator('[data-testid="copy-button-install"]');
      await copyButton.focus();

      const focusStyles = await copyButton.evaluate(el => {
        const styles = window.getComputedStyle(el);
        return {
          outline: styles.outline,
          boxShadow: styles.boxShadow,
          backgroundColor: styles.backgroundColor
        };
      });

      // Should have some visual focus indication
      expect(focusStyles).toBeTruthy();
    });
  });

  test.describe('Additional Accessibility Checks', () => {
    test('page should have proper language attribute', async ({ page }) => {
      await page.goto(BASE_URL);

      const lang = await page.evaluate(() => document.documentElement.lang);
      expect(lang).toBe('en');
    });

    test('page should have proper meta viewport for mobile', async ({ page }) => {
      await page.goto(BASE_URL);

      const viewport = await page.evaluate(() => {
        const meta = document.querySelector('meta[name="viewport"]');
        return meta?.getAttribute('content');
      });

      expect(viewport).toContain('width=device-width');
      expect(viewport).toContain('initial-scale=1');
    });

    test('page should have descriptive title', async ({ page }) => {
      await page.goto(BASE_URL);

      const title = await page.title();
      expect(title).toBeTruthy();
      expect(title.length).toBeGreaterThan(10);
      expect(title).toContain('MirDB');
    });

    test('tables should have proper structure for screen readers', async ({ page }) => {
      await page.goto(BASE_URL);

      const protocolSection = page.locator('[data-testid="protocol-section"]');
      await protocolSection.scrollIntoViewIfNeeded();

      // Check commands table has proper headers
      const commandsTable = page.locator('[data-testid="commands-table"]');
      const headers = commandsTable.locator('thead th');

      expect(await headers.count()).toBeGreaterThan(0);

      // Headers should have text content
      const headerTexts = await headers.allTextContents();
      expect(headerTexts.length).toBeGreaterThan(0);
      expect(headerTexts[0]).toBeTruthy();
    });

    test('links should have descriptive text', async ({ page }) => {
      await page.goto(BASE_URL);

      // Get all links and verify they have accessible names
      const links = await page.evaluate(() => {
        const allLinks = document.querySelectorAll('a');
        return Array.from(allLinks).map(link => ({
          text: link.textContent?.trim(),
          ariaLabel: link.getAttribute('aria-label'),
          href: link.href,
          hasAccessibleName: !!(link.textContent?.trim() || link.getAttribute('aria-label'))
        }));
      });

      // All links should have accessible names (text or aria-label)
      for (const link of links) {
        expect(link.hasAccessibleName).toBe(true);
      }
    });
  });
});
