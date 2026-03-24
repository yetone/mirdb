/**
 * Accessibility Compliance E2E Tests
 * Owner: Scenario 11 - Accessibility Compliance
 *
 * Tests:
 * - Keyboard navigation (Tab key navigation)
 * - Focus indicators visibility
 * - Skip to main content link
 * - Color contrast (WCAG AA 4.5:1)
 * - Alt text for images
 * - Heading hierarchy
 * - Semantic HTML structure
 */

import { test, expect, Page } from '@playwright/test';

/**
 * Helper function to get computed color values as RGB
 */
async function getColorValues(page: Page, selector: string, property: string): Promise<{ r: number; g: number; b: number }> {
  return page.evaluate(({ sel, prop }) => {
    const el = document.querySelector(sel);
    if (!el) return { r: 0, g: 0, b: 0 };
    const styles = window.getComputedStyle(el);
    const color = styles.getPropertyValue(prop);
    // Parse rgb(r, g, b) or rgba(r, g, b, a)
    const match = color.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
    if (match) {
      return { r: parseInt(match[1]), g: parseInt(match[2]), b: parseInt(match[3]) };
    }
    return { r: 0, g: 0, b: 0 };
  }, { sel: selector, prop: property });
}

/**
 * Calculate relative luminance for WCAG contrast ratio
 * https://www.w3.org/WAI/GL/wiki/Relative_luminance
 */
function getRelativeLuminance(r: number, g: number, b: number): number {
  const srgb = [r, g, b].map(c => {
    const s = c / 255;
    return s <= 0.03928 ? s / 12.92 : Math.pow((s + 0.055) / 1.055, 2.4);
  });
  return 0.2126 * srgb[0] + 0.7152 * srgb[1] + 0.0722 * srgb[2];
}

/**
 * Calculate contrast ratio between two colors
 * https://www.w3.org/WAI/GL/wiki/Contrast_ratio
 */
function getContrastRatio(l1: number, l2: number): number {
  const lighter = Math.max(l1, l2);
  const darker = Math.min(l1, l2);
  return (lighter + 0.05) / (darker + 0.05);
}

test.describe('Accessibility Compliance', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Keyboard Navigation', () => {
    test('TC1: Navigate page with Tab key - All interactive elements are reachable via keyboard', async ({ page }) => {
      // Get all focusable elements
      const focusableSelectors = [
        'a[href]',
        'button',
        'input',
        'select',
        'textarea',
        '[tabindex]:not([tabindex="-1"])'
      ].join(', ');

      const focusableElements = await page.locator(focusableSelectors).all();
      const visibleFocusableCount = await page.locator(focusableSelectors).count();

      // Start from the beginning of the page
      await page.keyboard.press('Tab');

      // Track visited elements
      const visitedElements: string[] = [];
      let previousFocusedElement = '';

      // Tab through all elements (with a reasonable limit)
      for (let i = 0; i < visibleFocusableCount + 5; i++) {
        const focusedElement = await page.evaluate(() => {
          const el = document.activeElement;
          if (el) {
            return {
              tag: el.tagName.toLowerCase(),
              id: el.id || '',
              className: el.className || '',
              href: (el as HTMLAnchorElement).href || '',
              text: el.textContent?.trim().slice(0, 50) || ''
            };
          }
          return null;
        });

        if (focusedElement) {
          const elementIdentifier = `${focusedElement.tag}:${focusedElement.id || focusedElement.className || focusedElement.text}`;

          // Stop if we've cycled back to the start
          if (visitedElements.includes(elementIdentifier) && visitedElements.length > 1) {
            break;
          }

          if (elementIdentifier !== previousFocusedElement) {
            visitedElements.push(elementIdentifier);
            previousFocusedElement = elementIdentifier;
          }
        }

        await page.keyboard.press('Tab');
      }

      // Verify we visited multiple interactive elements
      expect(visitedElements.length).toBeGreaterThanOrEqual(3);

      // Verify key interactive elements are reachable
      // Check for skip link (first focusable element)
      const skipLinkReached = visitedElements.some(el => el.includes('skip-link'));
      expect(skipLinkReached).toBe(true);

      // Check for navigation links or theme toggle (accessible header controls)
      const headerControlReached = visitedElements.some(el =>
        el.includes('header__nav-link') || el.includes('GitHub') ||
        el.includes('Documentation') || el.includes('theme-toggle') ||
        el.includes('header__logo')
      );
      expect(headerControlReached).toBe(true);
    });

    test('Enter key activates focused links', async ({ page }) => {
      // Tab to the skip link
      await page.keyboard.press('Tab');

      // Verify skip link is focused
      const skipLink = page.locator('.skip-link');
      await expect(skipLink).toBeFocused();

      // Press Enter to activate the skip link
      await page.keyboard.press('Enter');

      // After pressing Enter on skip link, main content should be in view
      const main = page.locator('#main');
      await expect(main).toBeInViewport();
    });
  });

  test.describe('Focus Indicators', () => {
    test('TC2: Check focus indicators - All interactive elements have visible focus indicators', async ({ page }) => {
      // List of interactive elements to check
      const interactiveElements = [
        { selector: '.skip-link', name: 'Skip link' },
        { selector: '.header__logo', name: 'Logo link' },
        { selector: '#theme-toggle', name: 'Theme toggle' },
        { selector: '.btn-primary', name: 'Primary CTA button' },
        { selector: '.btn-secondary', name: 'Secondary CTA button' },
        { selector: '.footer__link', name: 'Footer link' }
      ];

      for (const element of interactiveElements) {
        const locator = page.locator(element.selector).first();

        // Check if element exists
        const exists = await locator.count() > 0;
        if (!exists) continue;

        // Focus the element
        await locator.focus();

        // Get the focus styles
        const focusStyles = await locator.evaluate((el) => {
          const styles = window.getComputedStyle(el);
          return {
            outline: styles.outline,
            outlineWidth: styles.outlineWidth,
            outlineColor: styles.outlineColor,
            outlineStyle: styles.outlineStyle,
            boxShadow: styles.boxShadow,
            border: styles.border,
            backgroundColor: styles.backgroundColor
          };
        });

        // Element should have either outline, box-shadow, or visible border change when focused
        const hasVisibleOutline = focusStyles.outlineWidth !== '0px' && focusStyles.outlineStyle !== 'none';
        const hasBoxShadow = focusStyles.boxShadow !== 'none';
        const hasBorder = focusStyles.border !== 'none' && focusStyles.border !== '';

        const hasVisibleFocusIndicator = hasVisibleOutline || hasBoxShadow || hasBorder;

        expect(hasVisibleFocusIndicator,
          `${element.name} (${element.selector}) should have visible focus indicator. ` +
          `Outline: ${focusStyles.outline}, Box-shadow: ${focusStyles.boxShadow}`
        ).toBe(true);
      }
    });

    test('Focus indicators are visible for navigation links', async ({ page }) => {
      // Get viewport size to determine desktop vs mobile
      const viewportSize = page.viewportSize();
      const isMobile = viewportSize && viewportSize.width < 769;

      // On mobile, open hamburger menu first
      if (isMobile) {
        const hamburgerBtn = page.locator('#hamburger-btn');
        await hamburgerBtn.click();
      }

      const navLinks = page.locator('.header__nav-link');
      const count = await navLinks.count();

      for (let i = 0; i < count; i++) {
        const link = navLinks.nth(i);
        await link.focus();

        const focusStyles = await link.evaluate((el) => {
          const styles = window.getComputedStyle(el);
          return {
            outline: styles.outline,
            outlineWidth: styles.outlineWidth,
            boxShadow: styles.boxShadow
          };
        });

        const hasVisibleFocus =
          (focusStyles.outlineWidth !== '0px') ||
          (focusStyles.boxShadow !== 'none');

        expect(hasVisibleFocus, `Nav link ${i} should have visible focus indicator`).toBe(true);
      }
    });
  });

  test.describe('Skip Link', () => {
    test('TC3: Check for skip to main content link - Skip link is available for keyboard users', async ({ page }) => {
      // Check skip link exists
      const skipLink = page.locator('.skip-link');
      await expect(skipLink).toHaveCount(1);

      // Check skip link has correct href
      await expect(skipLink).toHaveAttribute('href', '#main');

      // Check skip link has descriptive text
      const skipLinkText = await skipLink.textContent();
      expect(skipLinkText?.toLowerCase()).toContain('skip');
      expect(skipLinkText?.toLowerCase()).toContain('main');

      // Check that main target exists
      const mainContent = page.locator('#main');
      await expect(mainContent).toHaveCount(1);

      // Tab to skip link and verify it becomes visible
      await page.keyboard.press('Tab');
      await expect(skipLink).toBeFocused();

      // Skip link should become visible when focused
      const isVisible = await skipLink.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        // Check that it's not hidden off-screen or transparent
        return styles.opacity !== '0' &&
               styles.visibility !== 'hidden' &&
               styles.display !== 'none';
      });
      expect(isVisible).toBe(true);
    });

    test('Skip link navigates to main content when activated', async ({ page }) => {
      // Tab to skip link
      await page.keyboard.press('Tab');

      // Activate skip link
      await page.keyboard.press('Enter');

      // Main content should now be scrolled into view
      const main = page.locator('#main');
      await expect(main).toBeInViewport();
    });
  });

  test.describe('Color Contrast', () => {
    test('TC4: Run color contrast check on text - Text has minimum 4.5:1 contrast ratio against background', async ({ page }) => {
      // Test key text elements for contrast
      const textElements = [
        { selector: '.hero-title', name: 'Hero title', minRatio: 4.5 },
        { selector: '.hero-tagline', name: 'Hero tagline', minRatio: 4.5 },
        { selector: '.hero-description', name: 'Hero description', minRatio: 4.5 },
        { selector: '.section-title', name: 'Section title', minRatio: 4.5 },
        { selector: '.feature-title', name: 'Feature title', minRatio: 4.5 },
        { selector: '.feature-description', name: 'Feature description', minRatio: 4.5 },
        { selector: '.footer__copyright', name: 'Footer copyright', minRatio: 4.5 }
      ];

      for (const element of textElements) {
        const locator = page.locator(element.selector).first();
        const exists = await locator.count() > 0;

        if (!exists) continue;

        // Get foreground and background colors
        const colors = await locator.evaluate((el) => {
          const styles = window.getComputedStyle(el);

          // Get foreground color
          const fgColor = styles.color;

          // Get background color (walk up the DOM tree to find non-transparent background)
          let bgColor = 'rgb(255, 255, 255)'; // Default to white
          let current: Element | null = el;
          while (current) {
            const currentStyles = window.getComputedStyle(current);
            const bg = currentStyles.backgroundColor;
            // Check if background is not transparent
            if (bg && !bg.includes('rgba(0, 0, 0, 0)') && bg !== 'transparent') {
              bgColor = bg;
              break;
            }
            current = current.parentElement;
          }

          // Parse colors
          const parseColor = (color: string) => {
            const match = color.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
            if (match) {
              return { r: parseInt(match[1]), g: parseInt(match[2]), b: parseInt(match[3]) };
            }
            return { r: 0, g: 0, b: 0 };
          };

          return {
            fg: parseColor(fgColor),
            bg: parseColor(bgColor)
          };
        });

        // Calculate contrast ratio
        const fgLuminance = getRelativeLuminance(colors.fg.r, colors.fg.g, colors.fg.b);
        const bgLuminance = getRelativeLuminance(colors.bg.r, colors.bg.g, colors.bg.b);
        const contrastRatio = getContrastRatio(fgLuminance, bgLuminance);

        expect(contrastRatio,
          `${element.name} should have contrast ratio >= ${element.minRatio}:1, got ${contrastRatio.toFixed(2)}:1`
        ).toBeGreaterThanOrEqual(element.minRatio);
      }
    });

    test('Button text meets contrast requirements', async ({ page }) => {
      const buttons = ['.btn-primary', '.btn-secondary'];

      for (const buttonSelector of buttons) {
        const button = page.locator(buttonSelector).first();
        const exists = await button.count() > 0;

        if (!exists) continue;

        const colors = await button.evaluate((el) => {
          const styles = window.getComputedStyle(el);
          const parseColor = (color: string) => {
            const match = color.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
            if (match) {
              return { r: parseInt(match[1]), g: parseInt(match[2]), b: parseInt(match[3]) };
            }
            return { r: 255, g: 255, b: 255 }; // Default to white
          };

          const isTransparent = (color: string) => {
            return color === 'transparent' ||
                   color === 'rgba(0, 0, 0, 0)' ||
                   color.includes('rgba') && color.endsWith(', 0)');
          };

          // Get foreground color
          const fgColor = styles.color;

          // Walk up the DOM tree to find non-transparent background
          let bgColor = 'rgb(255, 255, 255)';
          let current: Element | null = el;
          while (current) {
            const currentStyles = window.getComputedStyle(current);
            const bg = currentStyles.backgroundColor;
            if (!isTransparent(bg)) {
              bgColor = bg;
              break;
            }
            current = current.parentElement;
          }

          return {
            fg: parseColor(fgColor),
            bg: parseColor(bgColor)
          };
        });

        const fgLuminance = getRelativeLuminance(colors.fg.r, colors.fg.g, colors.fg.b);
        const bgLuminance = getRelativeLuminance(colors.bg.r, colors.bg.g, colors.bg.b);
        const contrastRatio = getContrastRatio(fgLuminance, bgLuminance);

        // Buttons should have at least 4.5:1 contrast for normal text
        expect(contrastRatio,
          `${buttonSelector} should have contrast ratio >= 4.5:1, got ${contrastRatio.toFixed(2)}:1`
        ).toBeGreaterThanOrEqual(4.5);
      }
    });
  });

  test.describe('Image Alt Text', () => {
    test('TC5: Check images for alt text - All images have descriptive alt attributes', async ({ page }) => {
      // Get all images on the page
      const images = page.locator('img');
      const imageCount = await images.count();

      expect(imageCount).toBeGreaterThan(0);

      for (let i = 0; i < imageCount; i++) {
        const img = images.nth(i);

        // Check that alt attribute exists
        const alt = await img.getAttribute('alt');
        expect(alt, `Image ${i} should have alt attribute`).not.toBeNull();

        // Alt text should not be empty for content images
        // (decorative images can have alt="" but should have aria-hidden="true")
        const ariaHidden = await img.getAttribute('aria-hidden');
        const isDecorative = ariaHidden === 'true' || alt === '';

        if (!isDecorative) {
          expect(alt?.trim().length, `Image ${i} should have non-empty alt text`).toBeGreaterThan(0);
        }

        // Alt text should be descriptive (more than just "image" or "logo")
        if (alt && alt.trim().length > 0) {
          const altLower = alt.toLowerCase();
          expect(
            altLower.length > 5 || altLower.includes('logo'),
            `Image ${i} alt text "${alt}" should be descriptive`
          ).toBe(true);
        }
      }
    });

    test('Logo image has appropriate alt text', async ({ page }) => {
      const logo = page.locator('.header__logo-img');
      await expect(logo).toHaveAttribute('alt', /MirDB/i);
    });

    test('Architecture diagram has descriptive alt text', async ({ page }) => {
      const diagram = page.locator('.architecture-diagram img');
      const exists = await diagram.count() > 0;

      if (exists) {
        const alt = await diagram.getAttribute('alt');
        expect(alt).not.toBeNull();
        expect(alt?.length).toBeGreaterThan(20); // Should be descriptive
        expect(alt?.toLowerCase()).toContain('architecture');
      }
    });
  });

  test.describe('Heading Hierarchy', () => {
    test('TC6: Check heading hierarchy - Headings follow proper hierarchy (h1, h2, h3...)', async ({ page }) => {
      // Get all headings in document order
      const headings = await page.evaluate(() => {
        const headingElements = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
        return Array.from(headingElements).map(h => ({
          level: parseInt(h.tagName[1]),
          text: h.textContent?.trim().slice(0, 50) || ''
        }));
      });

      // Should have at least one heading
      expect(headings.length).toBeGreaterThan(0);

      // Should have exactly one h1
      const h1Count = headings.filter(h => h.level === 1).length;
      expect(h1Count, 'Page should have exactly one h1').toBe(1);

      // h1 should be first heading
      expect(headings[0].level, 'First heading should be h1').toBe(1);

      // Check that heading levels don't skip (e.g., h1 -> h3 without h2)
      for (let i = 1; i < headings.length; i++) {
        const currentLevel = headings[i].level;
        const previousLevel = headings[i - 1].level;

        // Can go to same level, one level deeper, or any level higher
        const isValidTransition =
          currentLevel === previousLevel || // Same level
          currentLevel === previousLevel + 1 || // One level deeper
          currentLevel < previousLevel; // Going back up

        expect(
          isValidTransition,
          `Invalid heading transition from h${previousLevel} to h${currentLevel} (${headings[i-1].text} -> ${headings[i].text})`
        ).toBe(true);
      }
    });

    test('Main heading contains product name', async ({ page }) => {
      const h1 = page.locator('h1').first();
      await expect(h1).toContainText(/MirDB/i);
    });

    test('Section headings are present and meaningful', async ({ page }) => {
      // Check for expected section headings
      const expectedSections = ['Features', 'Quick Start', 'Architecture', 'Configuration', 'Status'];

      for (const section of expectedSections) {
        const heading = page.locator('h2, h3').filter({ hasText: new RegExp(section, 'i') });
        const count = await heading.count();
        expect(count, `Should have heading for "${section}" section`).toBeGreaterThan(0);
      }
    });
  });

  test.describe('Semantic HTML Structure', () => {
    test('TC7: Check semantic HTML structure - Page uses semantic elements (header, main, nav, footer, section)', async ({ page }) => {
      // Check for required semantic elements
      const semanticElements = [
        { selector: 'header', name: 'header', minCount: 1 },
        { selector: 'main', name: 'main', minCount: 1 },
        { selector: 'nav', name: 'nav', minCount: 1 },
        { selector: 'footer', name: 'footer', minCount: 1 },
        { selector: 'section', name: 'section', minCount: 3 } // Features, Quick Start, Architecture, Config, Status
      ];

      for (const element of semanticElements) {
        const count = await page.locator(element.selector).count();
        expect(count,
          `Page should have at least ${element.minCount} <${element.name}> element(s), found ${count}`
        ).toBeGreaterThanOrEqual(element.minCount);
      }
    });

    test('Header has correct landmark role', async ({ page }) => {
      const header = page.locator('header').first();
      const role = await header.getAttribute('role');

      // HTML5 header element is implicitly role="banner" at page level
      // If explicit role is set, it should be "banner"
      if (role) {
        expect(role).toBe('banner');
      }

      // Header should exist
      await expect(header).toBeVisible();
    });

    test('Navigation has correct role and label', async ({ page }) => {
      const nav = page.locator('nav').first();
      await expect(nav).toHaveAttribute('role', 'navigation');

      // Navigation should have aria-label for screen readers
      const ariaLabel = await nav.getAttribute('aria-label');
      expect(ariaLabel).not.toBeNull();
      expect(ariaLabel?.length).toBeGreaterThan(0);
    });

    test('Main content area exists and is properly labeled', async ({ page }) => {
      const main = page.locator('main');
      await expect(main).toHaveCount(1);

      // Main should have an id for skip link
      const id = await main.getAttribute('id');
      expect(id).not.toBeNull();
    });

    test('Footer has correct landmark role', async ({ page }) => {
      const footer = page.locator('footer').first();
      const role = await footer.getAttribute('role');

      // Footer element is implicitly role="contentinfo" at page level
      // If explicit role is set, it should be "contentinfo"
      if (role) {
        expect(role).toBe('contentinfo');
      }

      await expect(footer).toBeVisible();
    });

    test('Sections have proper identification', async ({ page }) => {
      const sections = page.locator('main section');
      const sectionCount = await sections.count();

      for (let i = 0; i < sectionCount; i++) {
        const section = sections.nth(i);

        // Each section should have an id or aria-labelledby
        const id = await section.getAttribute('id');
        const ariaLabelledby = await section.getAttribute('aria-labelledby');

        const hasIdentification = id !== null || ariaLabelledby !== null;
        expect(hasIdentification, `Section ${i} should have id or aria-labelledby`).toBe(true);
      }
    });

    test('Lists are properly structured', async ({ page }) => {
      // Check that feature cards are in a container that makes semantic sense
      const featureGrid = page.locator('.features-grid');
      await expect(featureGrid).toBeVisible();

      // Status lists should use ul/li
      const statusLists = page.locator('.status-list');
      const listCount = await statusLists.count();

      for (let i = 0; i < listCount; i++) {
        const list = statusLists.nth(i);
        const tagName = await list.evaluate(el => el.tagName.toLowerCase());
        expect(tagName, 'Status list should be ul or ol').toMatch(/^(ul|ol)$/);

        // List should have aria-label
        const ariaLabel = await list.getAttribute('aria-label');
        expect(ariaLabel, 'Status list should have aria-label').not.toBeNull();
      }
    });
  });

  test.describe('Additional Accessibility Checks', () => {
    test('Language is declared on html element', async ({ page }) => {
      const html = page.locator('html');
      const lang = await html.getAttribute('lang');
      expect(lang).not.toBeNull();
      expect(lang?.length).toBeGreaterThan(0);
    });

    test('Page has descriptive title', async ({ page }) => {
      const title = await page.title();
      expect(title.length).toBeGreaterThan(0);
      expect(title.toLowerCase()).toContain('mirdb');
    });

    test('Buttons have accessible names', async ({ page }) => {
      const buttons = page.locator('button');
      const buttonCount = await buttons.count();

      for (let i = 0; i < buttonCount; i++) {
        const button = buttons.nth(i);

        // Button should have text content or aria-label
        const text = await button.textContent();
        const ariaLabel = await button.getAttribute('aria-label');
        const title = await button.getAttribute('title');

        const hasAccessibleName =
          (text && text.trim().length > 0) ||
          (ariaLabel && ariaLabel.length > 0) ||
          (title && title.length > 0);

        expect(hasAccessibleName, `Button ${i} should have accessible name`).toBe(true);
      }
    });

    test('Links have descriptive text', async ({ page }) => {
      const links = page.locator('a[href]');
      const linkCount = await links.count();

      for (let i = 0; i < linkCount; i++) {
        const link = links.nth(i);

        // Link should have text content or aria-label
        const text = await link.textContent();
        const ariaLabel = await link.getAttribute('aria-label');

        const hasDescriptiveText =
          (text && text.trim().length > 0) ||
          (ariaLabel && ariaLabel.length > 0);

        expect(hasDescriptiveText, `Link ${i} should have descriptive text`).toBe(true);

        // Should not be just "click here" or "read more"
        const textLower = (text || '').toLowerCase().trim();
        expect(
          textLower !== 'click here' && textLower !== 'read more' && textLower !== 'here',
          `Link ${i} should not use generic text like "${textLower}"`
        ).toBe(true);
      }
    });

    test('Decorative images are hidden from screen readers', async ({ page }) => {
      // SVG icons should have aria-hidden
      const decorativeIcons = page.locator('svg[aria-hidden="true"]');
      const iconCount = await decorativeIcons.count();

      // Should have at least some decorative icons marked properly
      expect(iconCount).toBeGreaterThan(0);
    });

    test('External links have appropriate attributes', async ({ page }) => {
      const externalLinks = page.locator('a[target="_blank"]');
      const count = await externalLinks.count();

      for (let i = 0; i < count; i++) {
        const link = externalLinks.nth(i);

        // External links should have rel="noopener noreferrer"
        const rel = await link.getAttribute('rel');
        expect(rel, `External link ${i} should have rel attribute`).not.toBeNull();
        expect(rel).toContain('noopener');
      }
    });
  });
});
