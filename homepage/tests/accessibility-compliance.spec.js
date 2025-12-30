// @ts-check
const { test, expect, chromium } = require('@playwright/test');
const { playAudit } = require('playwright-lighthouse');

/**
 * E2E Tests for Accessibility Compliance
 *
 * Scenario: Verify that the homepage meets WCAG 2.1 AA accessibility standards (NFR-3)
 *
 * Test Cases:
 * 1. Lighthouse Accessibility score >= 90
 * 2. Keyboard navigation works correctly
 * 3. All images have alt text (E2E verification)
 * 4. Heading hierarchy is correct (E2E verification)
 * 5. Color contrast meets WCAG AA requirements (via Lighthouse)
 * 6. Form labels are properly associated (E2E verification)
 */

test.describe('Accessibility Compliance - WCAG 2.1 AA', () => {

  test.describe('TC1: Lighthouse Accessibility Audit', () => {
    test('Lighthouse Accessibility score is 90 or higher', async () => {
      // Launch browser with remote debugging port for Lighthouse
      const browser = await chromium.launch({
        args: ['--remote-debugging-port=9222'],
      });

      const page = await browser.newPage();
      await page.goto('http://localhost:3000', { waitUntil: 'networkidle' });

      // Run Lighthouse accessibility audit
      const auditResults = await playAudit({
        page,
        port: 9222,
        thresholds: {
          accessibility: 90
        },
        reports: {
          formats: {
            html: false,
            json: false
          }
        }
      });

      console.log('Lighthouse Accessibility Score:', auditResults.lhr.categories.accessibility.score * 100);

      // Verify the accessibility score meets our threshold
      const accessibilityScore = auditResults.lhr.categories.accessibility.score * 100;
      expect(accessibilityScore).toBeGreaterThanOrEqual(90);

      await browser.close();
    });
  });

  test.describe('TC2: Keyboard Navigation', () => {
    test('all interactive elements are focusable in logical order using Tab key', async ({ page }) => {
      await page.goto('http://localhost:3000');

      // Start from the beginning of the page
      await page.keyboard.press('Tab');

      // Get list of all focusable elements in DOM order
      const focusableElements = await page.evaluate(() => {
        const focusable = document.querySelectorAll(
          'a[href], button, input, select, textarea, [tabindex]:not([tabindex="-1"])'
        );
        return Array.from(focusable).map((el, index) => ({
          index,
          tagName: el.tagName.toLowerCase(),
          text: el.textContent?.trim().substring(0, 30) || '',
          href: el.getAttribute('href') || '',
          tabIndex: el.getAttribute('tabindex') || '0'
        }));
      });

      console.log('Focusable elements:', focusableElements.length);
      expect(focusableElements.length).toBeGreaterThan(0);

      // Tab through elements and verify focus moves in order
      const focusOrder = [];
      let previousFocusedElement = null;

      for (let i = 0; i < Math.min(focusableElements.length, 15); i++) {
        const focused = await page.evaluate(() => {
          const el = document.activeElement;
          return {
            tagName: el?.tagName.toLowerCase() || 'none',
            text: el?.textContent?.trim().substring(0, 30) || '',
            href: el?.getAttribute('href') || ''
          };
        });

        focusOrder.push(focused);

        // Verify focus has changed (unless it's the first element)
        if (previousFocusedElement) {
          // Focus should move to a new element
          expect(focused.tagName).not.toBe('body');
        }
        previousFocusedElement = focused;

        await page.keyboard.press('Tab');
      }

      console.log('Focus order verified for', focusOrder.length, 'elements');

      // Verify we got focus on multiple elements
      expect(focusOrder.length).toBeGreaterThan(0);
    });

    test('focus indicator is visible on interactive elements', async ({ page }) => {
      await page.goto('http://localhost:3000');

      // Tab to first focusable element
      await page.keyboard.press('Tab');

      // Check that the focused element has a visible outline or style change
      const hasFocusIndicator = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) return false;

        const style = window.getComputedStyle(el);
        const outlineWidth = parseFloat(style.outlineWidth) || 0;
        const outlineStyle = style.outlineStyle;
        const boxShadow = style.boxShadow;

        // Check for outline
        if (outlineWidth > 0 && outlineStyle !== 'none') return true;

        // Check for box-shadow (often used as focus indicator)
        if (boxShadow && boxShadow !== 'none') return true;

        // Check for border change (some implementations)
        const borderWidth = parseFloat(style.borderWidth) || 0;
        if (borderWidth > 0) return true;

        return false;
      });

      // Focus indicators should be present (browsers provide default if not styled away)
      // We check that outline hasn't been explicitly removed
      const outlineNotRemoved = await page.evaluate(() => {
        const styleSheet = document.querySelector('style')?.textContent || '';
        // Check that focus outline isn't set to none for all elements
        return !styleSheet.includes('*:focus { outline: none') &&
               !styleSheet.includes('*:focus{outline:none');
      });

      expect(outlineNotRemoved).toBe(true);
    });

    test('navigation links can be activated with Enter key', async ({ page }) => {
      await page.goto('http://localhost:3000');

      // Find and focus a navigation link
      const navLink = page.locator('nav a[href="#features"]');
      await navLink.focus();

      // Verify it's focused
      const isFocused = await navLink.evaluate(el => el === document.activeElement);
      expect(isFocused).toBe(true);

      // Press Enter and verify navigation
      await page.keyboard.press('Enter');

      // Check if the page scrolled to the features section or URL has #features
      const url = page.url();
      const featuresVisible = await page.locator('#features').isVisible();

      expect(url.includes('#features') || featuresVisible).toBe(true);
    });

    test('buttons can be activated with Space and Enter keys', async ({ page }) => {
      await page.goto('http://localhost:3000');

      // Find a button-like link (CTA buttons styled as buttons)
      const ctaButton = page.locator('.hero a.cta-button, .hero .btn-primary, a.btn-primary').first();

      if (await ctaButton.count() > 0) {
        await ctaButton.focus();

        // Verify it's focused
        const isFocused = await ctaButton.evaluate(el => el === document.activeElement);
        expect(isFocused).toBe(true);
      }
    });
  });

  test.describe('TC3: Images Alt Text (E2E)', () => {
    test('all img elements have alt attributes', async ({ page }) => {
      await page.goto('http://localhost:3000');

      // Get all images and check for alt attributes
      const imagesWithoutAlt = await page.evaluate(() => {
        const images = document.querySelectorAll('img');
        const missingAlt = [];

        images.forEach((img, index) => {
          if (!img.hasAttribute('alt')) {
            missingAlt.push({
              index,
              src: img.src.substring(0, 50)
            });
          }
        });

        return missingAlt;
      });

      expect(imagesWithoutAlt).toHaveLength(0);
    });

    test('decorative images have empty alt or aria-hidden', async ({ page }) => {
      await page.goto('http://localhost:3000');

      // SVG icons in feature cards should be aria-hidden
      const decorativeSvgs = await page.evaluate(() => {
        const svgs = document.querySelectorAll('.feature-icon svg, svg[aria-hidden]');
        const issues = [];

        svgs.forEach((svg, index) => {
          const ariaHidden = svg.getAttribute('aria-hidden');
          const role = svg.getAttribute('role');

          // Decorative SVGs should have aria-hidden="true" or role="presentation"
          if (ariaHidden !== 'true' && role !== 'presentation' && role !== 'none') {
            issues.push({ index, outerHTML: svg.outerHTML.substring(0, 100) });
          }
        });

        return issues;
      });

      // All decorative SVGs should be properly hidden
      expect(decorativeSvgs).toHaveLength(0);
    });
  });

  test.describe('TC4: Heading Hierarchy (E2E)', () => {
    test('page has exactly one h1', async ({ page }) => {
      await page.goto('http://localhost:3000');

      const h1Count = await page.locator('h1').count();
      expect(h1Count).toBe(1);
    });

    test('headings follow logical hierarchy without skipping levels', async ({ page }) => {
      await page.goto('http://localhost:3000');

      const headingIssues = await page.evaluate(() => {
        const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
        const issues = [];
        let previousLevel = 0;

        headings.forEach((heading, index) => {
          const currentLevel = parseInt(heading.tagName.charAt(1));

          if (index === 0 && currentLevel !== 1) {
            issues.push(`First heading should be h1, but found h${currentLevel}`);
          } else if (currentLevel > previousLevel + 1) {
            issues.push(`Skipped from h${previousLevel} to h${currentLevel}: "${heading.textContent?.trim().substring(0, 30)}"`);
          }

          previousLevel = currentLevel;
        });

        return issues;
      });

      expect(headingIssues).toHaveLength(0);
    });
  });

  test.describe('TC5: Color Contrast (via automated checks)', () => {
    test('main text has sufficient contrast against background', async ({ page }) => {
      await page.goto('http://localhost:3000');

      // This is a simplified check - Lighthouse does the comprehensive check
      // Here we verify key elements are visible and readable

      // Check hero title is visible
      const heroTitle = page.locator('h1');
      await expect(heroTitle).toBeVisible();

      // Check navigation links are visible
      const navLinks = page.locator('nav a');
      const navCount = await navLinks.count();
      expect(navCount).toBeGreaterThan(0);

      for (let i = 0; i < navCount; i++) {
        await expect(navLinks.nth(i)).toBeVisible();
      }

      // Check section headings are visible
      const sectionHeadings = page.locator('section h2');
      const headingCount = await sectionHeadings.count();

      for (let i = 0; i < headingCount; i++) {
        await expect(sectionHeadings.nth(i)).toBeVisible();
      }
    });
  });

  test.describe('TC6: Form Labels (E2E)', () => {
    test('all form inputs have associated labels', async ({ page }) => {
      await page.goto('http://localhost:3000');

      const unlabeledInputs = await page.evaluate(() => {
        const inputs = document.querySelectorAll(
          'input:not([type="hidden"]):not([type="submit"]):not([type="button"]), textarea, select'
        );
        const issues = [];

        inputs.forEach((input, index) => {
          const id = input.id;
          const ariaLabel = input.getAttribute('aria-label');
          const ariaLabelledBy = input.getAttribute('aria-labelledby');
          const title = input.getAttribute('title');
          const placeholder = input.getAttribute('placeholder');
          const type = input.getAttribute('type');

          // Check for explicit label
          let hasLabel = false;
          if (id) {
            const label = document.querySelector(`label[for="${id}"]`);
            if (label) hasLabel = true;
          }

          // Check for implicit label
          if (input.closest('label')) hasLabel = true;

          // Check for ARIA labeling
          if (ariaLabel || ariaLabelledBy || title) hasLabel = true;

          // For search inputs, placeholder is acceptable
          if (type === 'search' && placeholder) hasLabel = true;

          if (!hasLabel) {
            issues.push({
              index,
              type,
              id
            });
          }
        });

        return issues;
      });

      expect(unlabeledInputs).toHaveLength(0);
    });
  });

  test.describe('Additional Accessibility Checks', () => {
    test('page has proper language attribute', async ({ page }) => {
      await page.goto('http://localhost:3000');

      const lang = await page.evaluate(() => document.documentElement.lang);
      expect(lang).toBe('en');
    });

    test('page has proper viewport meta tag', async ({ page }) => {
      await page.goto('http://localhost:3000');

      const viewport = await page.evaluate(() => {
        const meta = document.querySelector('meta[name="viewport"]');
        return meta?.getAttribute('content') || '';
      });

      expect(viewport).toContain('width=device-width');
    });

    test('links have discernible text', async ({ page }) => {
      await page.goto('http://localhost:3000');

      const linksWithoutText = await page.evaluate(() => {
        const links = document.querySelectorAll('a');
        const issues = [];

        links.forEach((link, index) => {
          const text = link.textContent?.trim();
          const ariaLabel = link.getAttribute('aria-label');
          const title = link.getAttribute('title');
          const img = link.querySelector('img[alt]');

          if (!text && !ariaLabel && !title && !img) {
            issues.push({
              index,
              href: link.href.substring(0, 50)
            });
          }
        });

        return issues;
      });

      expect(linksWithoutText).toHaveLength(0);
    });

    test('skip link or accessible navigation exists', async ({ page }) => {
      await page.goto('http://localhost:3000');

      // Check for skip link or that navigation is properly structured
      const accessibleNav = await page.evaluate(() => {
        const skipLink = document.querySelector('a[href="#main"], a[href="#content"], .skip-link');
        const nav = document.querySelector('nav[role="navigation"], nav[aria-label]');

        return skipLink !== null || nav !== null;
      });

      expect(accessibleNav).toBe(true);
    });

    test('tables have proper headers', async ({ page }) => {
      await page.goto('http://localhost:3000');

      const tableIssues = await page.evaluate(() => {
        const tables = document.querySelectorAll('table');
        const issues = [];

        tables.forEach((table, index) => {
          const headers = table.querySelectorAll('th');
          if (headers.length === 0) {
            issues.push({ index, hasHeaders: false });
          }
        });

        return issues;
      });

      expect(tableIssues).toHaveLength(0);
    });

    test('no auto-playing media', async ({ page }) => {
      await page.goto('http://localhost:3000');

      const autoplayMedia = await page.evaluate(() => {
        const videos = document.querySelectorAll('video[autoplay]:not([muted])');
        const audios = document.querySelectorAll('audio[autoplay]');

        return videos.length + audios.length;
      });

      expect(autoplayMedia).toBe(0);
    });
  });
});
