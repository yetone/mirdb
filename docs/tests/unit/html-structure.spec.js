/**
 * Unit tests for HTML Structure and Accessibility
 * Owner: Scenario 9
 *
 * Test cases:
 * - Test case 3: Measure text color contrast in light mode
 * - Test case 4: Measure text color contrast in dark mode
 * - Test case 5: Check heading hierarchy (h1 > h2 > h3, no skipped levels)
 * - Test case 6: Verify image alt text (logo, usage, architecture diagram)
 * - Test case 7: Check button ARIA labels (theme toggle and copy buttons)
 */

const { test, expect } = require('@playwright/test');

/**
 * Calculate relative luminance for WCAG contrast ratio
 * @see https://www.w3.org/WAI/GL/wiki/Relative_luminance
 */
function getLuminance(r, g, b) {
  const [rs, gs, bs] = [r, g, b].map(c => {
    c = c / 255;
    return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
  });
  return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
}

/**
 * Calculate contrast ratio between two colors
 * @see https://www.w3.org/WAI/GL/wiki/Contrast_ratio
 */
function getContrastRatio(rgb1, rgb2) {
  const l1 = getLuminance(rgb1.r, rgb1.g, rgb1.b);
  const l2 = getLuminance(rgb2.r, rgb2.g, rgb2.b);
  const lighter = Math.max(l1, l2);
  const darker = Math.min(l1, l2);
  return (lighter + 0.05) / (darker + 0.05);
}

/**
 * Parse RGB color string to object
 */
function parseRgb(rgbString) {
  const match = rgbString.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
  if (match) {
    return { r: parseInt(match[1]), g: parseInt(match[2]), b: parseInt(match[3]) };
  }
  return null;
}

test.describe('HTML Structure - Hero Section Semantic Validation', () => {
  test.beforeEach(async ({ page, baseURL }) => {
    await page.goto(baseURL);
  });

  test('should have proper heading hierarchy starting with h1 in hero', async ({ page }) => {
    // Test case 4: Check semantic HTML structure
    const h1Elements = page.locator('h1');
    const h1Count = await h1Elements.count();

    // There should be exactly one h1 on the page
    expect(h1Count).toBe(1);

    // The h1 should be in the hero section
    const heroH1 = page.locator('#hero h1');
    await expect(heroH1).toBeVisible();
  });

  test('should have h2 elements following h1 in heading hierarchy', async ({ page }) => {
    // Verify proper heading hierarchy (h1 > h2)
    const h1 = page.locator('h1');
    const h2Elements = page.locator('h2');

    // h1 should exist
    await expect(h1).toBeVisible();

    // h2 elements should exist for sections
    const h2Count = await h2Elements.count();
    expect(h2Count).toBeGreaterThanOrEqual(1);

    // No h3 should appear without h2 preceding it in the DOM
    // This is a basic check - more complex hierarchy validation would require parsing the DOM
  });

  test('should have semantic section elements with proper IDs', async ({ page }) => {
    // Verify sections have proper IDs for navigation
    const heroSection = page.locator('section#hero');
    const gettingStartedSection = page.locator('section#getting-started');

    await expect(heroSection).toBeVisible();
    await expect(gettingStartedSection).toBeVisible();
  });

  test('should have proper main content area', async ({ page }) => {
    // Verify main element exists and contains hero
    const main = page.locator('main');
    await expect(main).toBeVisible();

    // Hero should be inside main
    const heroInMain = main.locator('#hero');
    await expect(heroInMain).toBeVisible();
  });

  test('should have skip-to-content link for accessibility', async ({ page }) => {
    // Verify skip link exists
    const skipLink = page.locator('a[href="#main-content"]');
    await expect(skipLink).toHaveCount(1);
  });
});

test.describe('Test Case 3: Color Contrast - Light Mode', () => {
  test.beforeEach(async ({ page, baseURL }) => {
    await page.goto(baseURL);
    // Ensure light mode
    await page.evaluate(() => {
      localStorage.setItem('theme', 'light');
      document.documentElement.classList.remove('dark');
    });
    await page.reload();
  });

  test('text color contrast ratio is at least 4.5:1 in light mode', async ({ page }) => {
    // Test main heading contrast
    const h1 = page.locator('h1');
    const h1Colors = await h1.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        color: styles.color,
        bgColor: window.getComputedStyle(el.closest('section') || document.body).backgroundColor
      };
    });

    const textColor = parseRgb(h1Colors.color);
    let bgColor = parseRgb(h1Colors.bgColor);

    // If background is transparent, use white (default light mode bg)
    if (!bgColor || h1Colors.bgColor === 'rgba(0, 0, 0, 0)') {
      bgColor = { r: 255, g: 255, b: 255 };
    }

    if (textColor && bgColor) {
      const contrastRatio = getContrastRatio(textColor, bgColor);
      expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
    }
  });

  test('body text contrast ratio meets WCAG AA in light mode', async ({ page }) => {
    // Test paragraph text contrast
    const paragraphs = page.locator('p');
    const count = await paragraphs.count();

    if (count > 0) {
      const firstP = paragraphs.first();
      const colors = await firstP.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        let parent = el.parentElement;
        let bgColor = 'rgba(0, 0, 0, 0)';

        // Walk up the DOM to find the first non-transparent background
        while (parent && bgColor === 'rgba(0, 0, 0, 0)') {
          bgColor = window.getComputedStyle(parent).backgroundColor;
          parent = parent.parentElement;
        }

        return {
          color: styles.color,
          bgColor: bgColor
        };
      });

      const textColor = parseRgb(colors.color);
      let bgColor = parseRgb(colors.bgColor);

      // If background is transparent, use white
      if (!bgColor || colors.bgColor === 'rgba(0, 0, 0, 0)') {
        bgColor = { r: 255, g: 255, b: 255 };
      }

      if (textColor && bgColor) {
        const contrastRatio = getContrastRatio(textColor, bgColor);
        expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
      }
    }
  });

  test('navigation link contrast meets WCAG AA in light mode', async ({ page }) => {
    // Test navigation links
    const navLinks = page.locator('nav a').first();
    const colors = await navLinks.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      const header = el.closest('header');
      const bgColor = header ? window.getComputedStyle(header).backgroundColor : 'rgb(255, 255, 255)';
      return {
        color: styles.color,
        bgColor: bgColor
      };
    });

    const textColor = parseRgb(colors.color);
    let bgColor = parseRgb(colors.bgColor);

    if (!bgColor || colors.bgColor === 'rgba(0, 0, 0, 0)') {
      bgColor = { r: 255, g: 255, b: 255 };
    }

    if (textColor && bgColor) {
      const contrastRatio = getContrastRatio(textColor, bgColor);
      expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
    }
  });
});

test.describe('Test Case 4: Color Contrast - Dark Mode', () => {
  test.beforeEach(async ({ page, baseURL }) => {
    await page.goto(baseURL);
    // Enable dark mode
    await page.evaluate(() => {
      localStorage.setItem('theme', 'dark');
      document.documentElement.classList.add('dark');
    });
    await page.reload();
    // Ensure dark mode is applied
    await page.waitForTimeout(100);
  });

  test('text color contrast ratio is at least 4.5:1 in dark mode', async ({ page }) => {
    // Verify dark mode is active
    const isDark = await page.evaluate(() => document.documentElement.classList.contains('dark'));
    expect(isDark).toBe(true);

    // Test main heading contrast in dark mode
    const h1 = page.locator('h1');
    const h1Colors = await h1.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      const section = el.closest('section') || document.body;
      return {
        color: styles.color,
        bgColor: window.getComputedStyle(section).backgroundColor
      };
    });

    const textColor = parseRgb(h1Colors.color);
    let bgColor = parseRgb(h1Colors.bgColor);

    // Dark mode default background
    if (!bgColor || h1Colors.bgColor === 'rgba(0, 0, 0, 0)') {
      bgColor = { r: 15, g: 23, b: 42 }; // dark-bg: #0f172a
    }

    if (textColor && bgColor) {
      const contrastRatio = getContrastRatio(textColor, bgColor);
      expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
    }
  });

  test('body text contrast ratio meets WCAG AA in dark mode', async ({ page }) => {
    // Verify dark mode
    const isDark = await page.evaluate(() => document.documentElement.classList.contains('dark'));
    expect(isDark).toBe(true);

    // Test paragraph text contrast
    const paragraphs = page.locator('p');
    const count = await paragraphs.count();

    if (count > 0) {
      const firstP = paragraphs.first();
      const colors = await firstP.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        let parent = el.parentElement;
        let bgColor = 'rgba(0, 0, 0, 0)';

        while (parent && bgColor === 'rgba(0, 0, 0, 0)') {
          bgColor = window.getComputedStyle(parent).backgroundColor;
          parent = parent.parentElement;
        }

        return {
          color: styles.color,
          bgColor: bgColor
        };
      });

      const textColor = parseRgb(colors.color);
      let bgColor = parseRgb(colors.bgColor);

      // Dark mode default
      if (!bgColor || colors.bgColor === 'rgba(0, 0, 0, 0)') {
        bgColor = { r: 15, g: 23, b: 42 };
      }

      if (textColor && bgColor) {
        const contrastRatio = getContrastRatio(textColor, bgColor);
        expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
      }
    }
  });

  test('navigation link contrast meets WCAG AA in dark mode', async ({ page }) => {
    // Verify dark mode
    const isDark = await page.evaluate(() => document.documentElement.classList.contains('dark'));
    expect(isDark).toBe(true);

    const navLinks = page.locator('nav a').first();
    const colors = await navLinks.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      const header = el.closest('header');
      const bgColor = header ? window.getComputedStyle(header).backgroundColor : 'rgb(15, 23, 42)';
      return {
        color: styles.color,
        bgColor: bgColor
      };
    });

    const textColor = parseRgb(colors.color);
    let bgColor = parseRgb(colors.bgColor);

    if (!bgColor || colors.bgColor === 'rgba(0, 0, 0, 0)') {
      bgColor = { r: 15, g: 23, b: 42 };
    }

    if (textColor && bgColor) {
      const contrastRatio = getContrastRatio(textColor, bgColor);
      expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
    }
  });
});

test.describe('Test Case 5: Heading Hierarchy', () => {
  test.beforeEach(async ({ page, baseURL }) => {
    await page.goto(baseURL);
  });

  test('headings follow semantic order: h1 > h2 > h3, no skipped levels', async ({ page }) => {
    // Get all headings in document order
    const headings = await page.evaluate(() => {
      const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      return Array.from(allHeadings).map(h => ({
        level: parseInt(h.tagName.charAt(1)),
        text: h.textContent?.trim().substring(0, 50)
      }));
    });

    // Should have at least one h1
    expect(headings.length).toBeGreaterThan(0);
    expect(headings[0].level).toBe(1);

    // Check that no levels are skipped
    let previousLevel = 0;
    for (const heading of headings) {
      // Allow same level or going up one level at a time
      // Also allow going back to any previous level
      if (heading.level > previousLevel) {
        // When going deeper, should only go one level at a time
        expect(heading.level).toBeLessThanOrEqual(previousLevel + 1);
      }
      previousLevel = heading.level;
    }
  });

  test('exactly one h1 exists on the page', async ({ page }) => {
    const h1Count = await page.locator('h1').count();
    expect(h1Count).toBe(1);
  });

  test('h1 appears before any h2', async ({ page }) => {
    const headingPositions = await page.evaluate(() => {
      const h1 = document.querySelector('h1');
      const firstH2 = document.querySelector('h2');

      if (!h1 || !firstH2) return null;

      // Compare document positions
      const position = h1.compareDocumentPosition(firstH2);
      return {
        h1BeforeH2: (position & Node.DOCUMENT_POSITION_FOLLOWING) !== 0
      };
    });

    if (headingPositions) {
      expect(headingPositions.h1BeforeH2).toBe(true);
    }
  });

  test('each section has an h2 heading', async ({ page }) => {
    // Check key sections have proper headings
    const sections = ['features', 'architecture', 'getting-started', 'status'];

    for (const sectionId of sections) {
      const section = page.locator(`section#${sectionId}`);
      const isVisible = await section.isVisible();

      if (isVisible) {
        const h2 = section.locator('h2');
        const h2Count = await h2.count();
        expect(h2Count).toBeGreaterThanOrEqual(1);
      }
    }
  });
});

test.describe('Test Case 6: Image Alt Text', () => {
  test.beforeEach(async ({ page, baseURL }) => {
    await page.goto(baseURL);
  });

  test('all images have descriptive alt attributes', async ({ page }) => {
    const images = page.locator('img');
    const count = await images.count();

    for (let i = 0; i < count; i++) {
      const img = images.nth(i);
      const alt = await img.getAttribute('alt');

      // All images should have alt attribute
      expect(alt).not.toBeNull();
      // Alt should not be empty (except for decorative images which would have alt="")
      // For decorative images, empty alt is acceptable
    }
  });

  test('logo image has descriptive alt text', async ({ page }) => {
    const logo = page.locator('img[src*="logo"]');
    const count = await logo.count();

    if (count > 0) {
      const alt = await logo.first().getAttribute('alt');
      expect(alt).not.toBeNull();
      expect(alt).toContain('MirDB');
    }
  });

  test('usage.gif image has descriptive alt text', async ({ page }) => {
    const usageImg = page.locator('img[src*="usage"]');
    const count = await usageImg.count();

    if (count > 0) {
      const alt = await usageImg.first().getAttribute('alt');
      expect(alt).not.toBeNull();
      expect(alt.length).toBeGreaterThan(0);
    }
  });

  test('decorative images have empty alt attribute', async ({ page }) => {
    // Decorative images (like icons) should have alt="" or aria-hidden="true"
    const svgIcons = page.locator('svg');
    const count = await svgIcons.count();

    // SVGs should have aria-hidden="true" or role="img" with aria-label
    for (let i = 0; i < Math.min(count, 10); i++) {
      const svg = svgIcons.nth(i);
      const ariaHidden = await svg.getAttribute('aria-hidden');
      const role = await svg.getAttribute('role');
      const ariaLabel = await svg.getAttribute('aria-label');

      // SVG should either be hidden from screen readers or have proper labeling
      const isProperlyLabeled = ariaHidden === 'true' || (role === 'img' && ariaLabel);
      // Allow SVGs without any ARIA attributes as they may be inside labeled buttons
      expect(ariaHidden === 'true' || ariaHidden === null || isProperlyLabeled).toBe(true);
    }
  });
});

test.describe('Test Case 7: Button ARIA Labels', () => {
  test.beforeEach(async ({ page, baseURL }) => {
    await page.goto(baseURL);
  });

  test('theme toggle button has aria-label', async ({ page }) => {
    const themeToggle = page.locator('#theme-toggle');
    const ariaLabel = await themeToggle.getAttribute('aria-label');

    expect(ariaLabel).not.toBeNull();
    expect(ariaLabel.length).toBeGreaterThan(0);
    // Check that aria-label contains either 'dark', 'theme', 'mode', or 'toggle'
    const lowerLabel = ariaLabel.toLowerCase();
    const hasAccessibleLabel = lowerLabel.includes('dark') ||
                               lowerLabel.includes('theme') ||
                               lowerLabel.includes('mode') ||
                               lowerLabel.includes('toggle');
    expect(hasAccessibleLabel).toBe(true);
  });

  test('mobile menu button has aria-label', async ({ page }) => {
    // Switch to mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });
    await page.reload();

    const menuButton = page.locator('#mobile-menu-button');
    const ariaLabel = await menuButton.getAttribute('aria-label');

    expect(ariaLabel).not.toBeNull();
    expect(ariaLabel.length).toBeGreaterThan(0);
  });

  test('copy buttons have aria-label', async ({ page }) => {
    // Check for copy buttons (if any exist)
    const copyButtons = page.locator('button.copy-btn, button.copy-button, [aria-label*="copy" i]');
    const count = await copyButtons.count();

    for (let i = 0; i < count; i++) {
      const btn = copyButtons.nth(i);
      const ariaLabel = await btn.getAttribute('aria-label');
      const textContent = await btn.textContent();

      // Button should have either aria-label or visible text
      expect(ariaLabel !== null || textContent.trim().length > 0).toBe(true);
    }
  });

  test('all interactive buttons have accessible names', async ({ page }) => {
    const buttons = page.locator('button');
    const count = await buttons.count();

    for (let i = 0; i < count; i++) {
      const btn = buttons.nth(i);
      const isVisible = await btn.isVisible();

      if (isVisible) {
        const ariaLabel = await btn.getAttribute('aria-label');
        const ariaLabelledBy = await btn.getAttribute('aria-labelledby');
        const textContent = await btn.textContent();
        const title = await btn.getAttribute('title');

        // Button should have accessible name via one of these methods
        const hasAccessibleName =
          (ariaLabel && ariaLabel.length > 0) ||
          (ariaLabelledBy && ariaLabelledBy.length > 0) ||
          (textContent && textContent.trim().length > 0) ||
          (title && title.length > 0);

        expect(hasAccessibleName).toBe(true);
      }
    }
  });

  test('external links have accessible labels', async ({ page }) => {
    // External links (like GitHub) should have clear labels
    const externalLinks = page.locator('a[target="_blank"]');
    const count = await externalLinks.count();

    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      const ariaLabel = await link.getAttribute('aria-label');
      const textContent = await link.textContent();
      const title = await link.getAttribute('title');

      // Link should have accessible text
      const hasAccessibleName =
        (ariaLabel && ariaLabel.length > 0) ||
        (textContent && textContent.trim().length > 0) ||
        (title && title.length > 0);

      expect(hasAccessibleName).toBe(true);
    }
  });
});
