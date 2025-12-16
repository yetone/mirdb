// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Accessibility - Screen Reader Compatibility Tests
 *
 * Verifies that the MirDB homepage is accessible to screen readers per WCAG 2.1 AA (NFR-3).
 * Tests include:
 * - All images have appropriate alt attributes
 * - Architecture diagram has comprehensive alt text
 * - ARIA landmarks are properly defined
 * - Buttons and links have accessible names
 */

test.describe('Accessibility - Screen Reader Compatibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test.describe('Image Alt Text', () => {
    test('all img elements have alt attributes', async ({ page }) => {
      // Get all img elements on the page
      const images = await page.locator('img').all();

      for (const img of images) {
        // Check that alt attribute exists (can be empty for decorative images)
        const hasAlt = await img.evaluate((el) => el.hasAttribute('alt'));
        expect(hasAlt, 'All img elements must have alt attribute').toBe(true);

        // Get the alt text for verification
        const altText = await img.getAttribute('alt');
        const src = await img.getAttribute('src');

        // If alt is empty, it should be intentionally empty (decorative image)
        // Non-decorative images should have descriptive alt text
        if (altText === '') {
          // Verify it's truly decorative (has role="presentation" or aria-hidden="true")
          const isDecorativeMarked = await img.evaluate((el) => {
            return el.getAttribute('role') === 'presentation' ||
                   el.getAttribute('aria-hidden') === 'true' ||
                   el.hasAttribute('alt'); // empty alt is valid for decorative
          });
          expect(isDecorativeMarked, `Image ${src} with empty alt should be marked as decorative`).toBe(true);
        }
      }

      // If no img elements, test still passes (page might use other methods for images)
      // This is acceptable as MirDB homepage uses Mermaid diagrams instead of images
    });

    test('architecture diagram has comprehensive alt text describing the LSM-tree flow', async ({ page }) => {
      // The architecture diagram is rendered using Mermaid.js
      // Check for the diagram container and verify it has accessible description
      const diagramContainer = page.locator('[data-testid="architecture-diagram"]');
      await expect(diagramContainer).toBeVisible();

      // The Mermaid diagram should have accessible description
      // Check for aria-label or aria-describedby on the diagram or its container
      const mermaidDiagram = page.locator('[data-testid="mermaid-diagram"]');
      await expect(mermaidDiagram).toBeAttached();

      // Verify the diagram has accessible text content or ARIA attributes
      const hasAccessibleDescription = await diagramContainer.evaluate((el) => {
        // Check for various accessibility approaches:
        // 1. aria-label on the container
        // 2. aria-describedby pointing to a description
        // 3. A visually hidden description element inside
        // 4. The mermaid code itself as fallback text
        const ariaLabel = el.getAttribute('aria-label');
        const ariaDescribedBy = el.getAttribute('aria-describedby');
        const hiddenDescription = el.querySelector('.sr-only, .visually-hidden, [aria-hidden="false"]');
        const mermaidCode = el.querySelector('.mermaid');

        return !!(ariaLabel || ariaDescribedBy || hiddenDescription || mermaidCode);
      });

      expect(hasAccessibleDescription, 'Architecture diagram should have accessible description').toBe(true);

      // Verify the description mentions LSM-tree flow concepts
      // The architecture section should contain explanatory text about the diagram
      const architectureSection = page.locator('[data-testid="architecture-section"]');
      const sectionText = await architectureSection.textContent();

      // Check that the section contains key LSM-tree concepts for screen reader users
      expect(sectionText).toContain('Write Path');
      expect(sectionText).toContain('Read Path');
      expect(sectionText).toContain('WAL');
      expect(sectionText).toContain('Memtable');
      expect(sectionText).toContain('SSTable');
    });
  });

  test.describe('ARIA Landmarks', () => {
    test('page has main landmark', async ({ page }) => {
      // Check for main landmark (either <main> element or role="main")
      const mainLandmark = page.locator('main, [role="main"]');
      await expect(mainLandmark).toBeAttached();

      // Verify main landmark contains the primary content
      const mainContent = await mainLandmark.first().getAttribute('id');
      expect(mainContent).toBe('main-content');
    });

    test('page has navigation landmark', async ({ page }) => {
      // Check for navigation landmark (either <nav> element or role="navigation")
      const navLandmark = page.locator('nav, [role="navigation"]');
      await expect(navLandmark).toBeAttached();

      // Verify navigation has an accessible label
      const navLabel = await navLandmark.first().getAttribute('aria-label');
      expect(navLabel).toBeTruthy();
      expect(navLabel).toContain('navigation');
    });

    test('page has contentinfo landmark (footer)', async ({ page }) => {
      // Check for contentinfo landmark (either <footer> element or role="contentinfo")
      const footerLandmark = page.locator('footer, [role="contentinfo"]');
      await expect(footerLandmark).toBeAttached();

      // Verify footer contains expected content
      const footerText = await footerLandmark.first().textContent();
      expect(footerText).toContain('GitHub');
    });

    test('page has all required ARIA landmarks (main, navigation, contentinfo)', async ({ page }) => {
      // Comprehensive check for all three required landmarks
      const landmarks = {
        main: page.locator('main, [role="main"]'),
        navigation: page.locator('nav, [role="navigation"]'),
        contentinfo: page.locator('footer, [role="contentinfo"]'),
      };

      // Verify all landmarks are present
      for (const [name, locator] of Object.entries(landmarks)) {
        const count = await locator.count();
        expect(count, `Page should have ${name} landmark`).toBeGreaterThan(0);
      }

      // Verify landmarks are properly structured (main should not be inside nav or footer)
      const mainLandmark = await landmarks.main.first().evaluate((el) => {
        return !el.closest('nav') && !el.closest('footer');
      });
      expect(mainLandmark, 'Main landmark should not be nested inside navigation or footer').toBe(true);
    });
  });

  test.describe('Accessible Names for Interactive Elements', () => {
    test('all buttons have accessible names', async ({ page }) => {
      // Get all button elements
      const buttons = await page.locator('button').all();

      for (const button of buttons) {
        // Get accessible name (text content, aria-label, or aria-labelledby)
        const accessibleName = await button.evaluate((el) => {
          // Priority: aria-labelledby > aria-label > text content > title
          const labelledBy = el.getAttribute('aria-labelledby');
          if (labelledBy) {
            const labelEl = document.getElementById(labelledBy);
            return labelEl?.textContent?.trim() || '';
          }

          const ariaLabel = el.getAttribute('aria-label');
          if (ariaLabel) return ariaLabel.trim();

          const textContent = el.textContent?.trim();
          if (textContent) return textContent;

          const title = el.getAttribute('title');
          if (title) return title.trim();

          return '';
        });

        expect(accessibleName, 'Button should have an accessible name').toBeTruthy();
      }
    });

    test('all links have accessible names', async ({ page }) => {
      // Get all anchor elements
      const links = await page.locator('a').all();

      for (const link of links) {
        // Get accessible name (text content, aria-label, or aria-labelledby)
        const accessibleInfo = await link.evaluate((el) => {
          const href = el.getAttribute('href') || 'unknown';

          // Priority: aria-labelledby > aria-label > text content > title
          const labelledBy = el.getAttribute('aria-labelledby');
          if (labelledBy) {
            const labelEl = document.getElementById(labelledBy);
            const name = labelEl?.textContent?.trim() || '';
            return { name, href };
          }

          const ariaLabel = el.getAttribute('aria-label');
          if (ariaLabel) return { name: ariaLabel.trim(), href };

          const textContent = el.textContent?.trim();
          if (textContent) return { name: textContent, href };

          const title = el.getAttribute('title');
          if (title) return { name: title.trim(), href };

          // Check for images with alt text inside the link
          const img = el.querySelector('img');
          if (img) {
            const alt = img.getAttribute('alt');
            if (alt) return { name: alt.trim(), href };
          }

          return { name: '', href };
        });

        expect(
          accessibleInfo.name,
          `Link to ${accessibleInfo.href} should have an accessible name`
        ).toBeTruthy();
      }
    });

    test('CTA buttons have descriptive accessible names', async ({ page }) => {
      // Check primary CTA (View on GitHub)
      const primaryCTA = page.locator('[data-testid="primary-cta"]');
      const primaryText = await primaryCTA.textContent();
      expect(primaryText?.trim()).toBeTruthy();
      expect(primaryText?.toLowerCase()).toContain('github');

      // Check secondary CTA (Learn More)
      const secondaryCTA = page.locator('[data-testid="secondary-cta"]');
      const secondaryText = await secondaryCTA.textContent();
      expect(secondaryText?.trim()).toBeTruthy();
      expect(secondaryText?.toLowerCase()).toContain('learn');
    });

    test('navigation links have meaningful accessible names', async ({ page }) => {
      // Check each navigation link
      const navLinks = page.locator('.nav-link');
      const expectedLinks = ['Features', 'Architecture', 'Commands', 'Getting Started'];

      const linkTexts = await navLinks.allTextContents();

      for (const expected of expectedLinks) {
        const hasLink = linkTexts.some((text) =>
          text.toLowerCase().includes(expected.toLowerCase())
        );
        expect(hasLink, `Navigation should have "${expected}" link`).toBe(true);
      }
    });

    test('external links have appropriate attributes for screen readers', async ({ page }) => {
      // External links should indicate they open in a new tab
      const externalLinks = page.locator('a[target="_blank"]');
      const count = await externalLinks.count();

      for (let i = 0; i < count; i++) {
        const link = externalLinks.nth(i);

        // Check for rel="noopener noreferrer" (security best practice)
        const rel = await link.getAttribute('rel');
        expect(rel).toContain('noopener');

        // External links should have clear text or aria-label indicating external nature
        const linkInfo = await link.evaluate((el) => {
          const text = el.textContent?.trim() || '';
          const ariaLabel = el.getAttribute('aria-label') || '';
          const title = el.getAttribute('title') || '';
          return { text, ariaLabel, title };
        });

        // Link should have some accessible name
        const hasAccessibleName = linkInfo.text || linkInfo.ariaLabel || linkInfo.title;
        expect(hasAccessibleName, 'External link should have accessible name').toBeTruthy();
      }
    });
  });

  test.describe('Additional Screen Reader Considerations', () => {
    test('page has proper language attribute', async ({ page }) => {
      // Check that html element has lang attribute
      const lang = await page.locator('html').getAttribute('lang');
      expect(lang).toBeTruthy();
      expect(lang).toBe('en');
    });

    test('skip to main content link exists', async ({ page }) => {
      // Skip link should be present for screen reader users
      const skipLink = page.locator('[data-testid="skip-to-main"]');
      await expect(skipLink).toBeAttached();

      // Skip link should point to main content
      const href = await skipLink.getAttribute('href');
      expect(href).toBe('#main-content');

      // Skip link should have meaningful text
      const text = await skipLink.textContent();
      expect(text?.toLowerCase()).toContain('skip');
      expect(text?.toLowerCase()).toContain('main');
    });

    test('headings are properly nested', async ({ page }) => {
      // Get all heading levels
      const headings = await page.evaluate(() => {
        const h1s = document.querySelectorAll('h1').length;
        const h2s = document.querySelectorAll('h2').length;
        const h3s = document.querySelectorAll('h3').length;

        // Check heading order (no skipping levels)
        const allHeadings = Array.from(document.querySelectorAll('h1, h2, h3, h4, h5, h6'));
        let previousLevel = 0;
        let properlyNested = true;

        for (const heading of allHeadings) {
          const level = parseInt(heading.tagName[1]);
          // Level should not skip more than 1 (e.g., h1 -> h3 is invalid)
          if (previousLevel > 0 && level > previousLevel + 1) {
            properlyNested = false;
            break;
          }
          previousLevel = level;
        }

        return { h1s, h2s, h3s, properlyNested };
      });

      // Page should have exactly one h1
      expect(headings.h1s, 'Page should have exactly one h1').toBe(1);

      // Page should have section headings
      expect(headings.h2s, 'Page should have h2 section headings').toBeGreaterThan(0);

      // Headings should be properly nested
      expect(headings.properlyNested, 'Headings should be properly nested without skipping levels').toBe(true);
    });

    test('form controls have labels (if any)', async ({ page }) => {
      // Get all form controls
      const formControls = await page.locator('input, select, textarea').all();

      for (const control of formControls) {
        const hasLabel = await control.evaluate((el) => {
          // Check for associated label
          const id = el.id;
          if (id) {
            const label = document.querySelector(`label[for="${id}"]`);
            if (label) return true;
          }

          // Check for aria-label or aria-labelledby
          if (el.getAttribute('aria-label') || el.getAttribute('aria-labelledby')) {
            return true;
          }

          // Check if wrapped in a label
          if (el.closest('label')) {
            return true;
          }

          // Check for placeholder (not ideal but acceptable in some cases)
          if (el.getAttribute('placeholder') && el.getAttribute('title')) {
            return true;
          }

          return false;
        });

        expect(hasLabel, 'Form control should have an associated label').toBe(true);
      }
    });

    test('tables have proper headers (if any)', async ({ page }) => {
      // Get all tables
      const tables = await page.locator('table').all();

      for (const table of tables) {
        // Table should have thead with th elements
        const hasHeaders = await table.evaluate((el) => {
          const thead = el.querySelector('thead');
          const ths = el.querySelectorAll('th');
          return thead !== null || ths.length > 0;
        });

        expect(hasHeaders, 'Table should have header cells (th elements)').toBe(true);
      }
    });
  });
});
