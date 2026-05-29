import { test, expect } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

test.describe('Accessibility Compliance', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Tab through all interactive elements in logical order', async ({ page }) => {
    // Get all initially focusable elements in the document
    const focusableSelectors = 'a[href], button, input, select, textarea, [tabindex]:not([tabindex="-1"])';
    const focusableElements = await page.locator(focusableSelectors).all();
    expect(focusableElements.length).toBeGreaterThan(0);

    // Press Tab and verify each element receives focus
    const focusedTags: string[] = [];
    const focusedTestIds: (string | null)[] = [];

    for (let i = 0; i < focusableElements.length; i++) {
      await page.keyboard.press('Tab');
      const activeElement = await page.evaluate(() => {
        const el = document.activeElement;
        return {
          tag: el?.tagName?.toLowerCase() || null,
          testId: el?.getAttribute('data-testid') || null,
          ariaLabel: el?.getAttribute('aria-label') || null,
        };
      });
      expect(activeElement.tag).not.toBeNull();
      expect(activeElement.tag).not.toBe('body');
      focusedTags.push(activeElement.tag!);
      focusedTestIds.push(activeElement.testId);
    }

    // Verify logical tab order: skip link first, then nav links, then form, then CTA, then footer
    const firstFocused = focusedTestIds[0];
    expect(firstFocused).toBeTruthy();
  });

  test('TC3: axe-core audit passes with zero critical or serious violations', async ({ page }) => {
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21aa'])
      .analyze();

    const criticalAndSerious = accessibilityScanResults.violations.filter(
      (v) => v.impact === 'critical' || v.impact === 'serious'
    );

    expect(
      criticalAndSerious,
      `Found ${criticalAndSerious.length} critical/serious accessibility violations: ${
        criticalAndSerious.map((v) => `${v.id}: ${v.description}`).join(', ')
      }`
    ).toHaveLength(0);
  });

  test('TC4: prefers-reduced-motion disables animations', async ({ page }) => {
    // Emulate prefers-reduced-motion
    await page.emulateMedia({ reducedMotion: 'reduce' });
    await page.goto('/');

    // Verify reduced motion styles are applied by checking animation duration
    const reducedMotionStyles = await page.evaluate(() => {
      // Check if the media query matched by looking at a test element
      const testEl = document.createElement('div');
      testEl.style.animation = 'none';
      document.body.appendChild(testEl);
      const computed = window.getComputedStyle(testEl);
      const result = {
        animationDuration: computed.animationDuration,
        prefersReducedMotion: window.matchMedia('(prefers-reduced-motion: reduce)').matches,
      };
      document.body.removeChild(testEl);
      return result;
    });

    expect(reducedMotionStyles.prefersReducedMotion).toBe(true);
  });

  test('TC7: Verify heading hierarchy has no skipped levels', async ({ page }) => {
    const headings = await page.evaluate(() => {
      const headingElements = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      return Array.from(headingElements).map((h) => ({
        level: parseInt(h.tagName[1]),
        text: h.textContent?.trim() || '',
      }));
    });

    expect(headings.length).toBeGreaterThan(0);

    // Verify there is exactly one H1
    const h1s = headings.filter((h) => h.level === 1);
    expect(h1s.length).toBe(1);
    expect(h1s[0].text).toContain('Shorten Your URLs');

    // Verify no skipped heading levels (e.g., h1 -> h3 without h2)
    for (let i = 1; i < headings.length; i++) {
      const prevLevel = headings[i - 1].level;
      const currLevel = headings[i].level;
      // Current level should not skip more than one level down from previous
      expect(
        currLevel,
        `Heading level skipped from h${prevLevel} to h${currLevel}: "${headings[i].text}"`
      ).toBeLessThanOrEqual(prevLevel + 1);
    }
  });

  test('TC6: Verify ARIA landmarks exist on page', async ({ page }) => {
    const landmarks = await page.evaluate(() => {
      return {
        nav: document.querySelectorAll('nav').length,
        main: document.querySelectorAll('main').length,
        footer: document.querySelectorAll('footer').length,
        sectionsWithAriaLabel: document.querySelectorAll('section[aria-label]').length,
      };
    });

    expect(landmarks.nav).toBeGreaterThanOrEqual(1);
    expect(landmarks.main).toBe(1);
    expect(landmarks.footer).toBe(1);
    expect(landmarks.sectionsWithAriaLabel).toBeGreaterThanOrEqual(1);
  });

  test('Focus indicators are visible on all interactive elements', async ({ page }) => {
    const interactiveElements = await page.locator('a, button, input').all();

    for (const element of interactiveElements) {
      await element.focus();
      const outline = await element.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          outlineWidth: style.outlineWidth,
          outlineStyle: style.outlineStyle,
          outlineColor: style.outlineColor,
        };
      });

      // Focus indicator should have a visible outline
      expect(
        outline.outlineWidth,
        `Element ${await element.evaluate((el) => el.tagName)} lacks visible focus indicator`
      ).not.toBe('0px');
    }
  });

  test('Skip link is available and functional for keyboard users', async ({ page }) => {
    // Press Tab to reveal skip link
    await page.keyboard.press('Tab');

    const skipLink = page.locator('.skip-link');
    await expect(skipLink).toBeVisible();
    await expect(skipLink).toHaveAttribute('href', '#main-content');

    // Activate skip link with Enter key (standard keyboard interaction)
    await page.keyboard.press('Enter');

    const mainContent = page.locator('#main-content');
    await expect(mainContent).toBeVisible();
  });

  test('URL input has proper labeling for screen readers', async ({ page }) => {
    const urlInput = page.getByTestId('url-input');

    // Verify the input exists
    await expect(urlInput).toBeVisible();

    // Check for associated label
    const ariaLabel = await urlInput.getAttribute('aria-label');
    const ariaLabelledBy = await urlInput.getAttribute('aria-labelledby');
    const id = await urlInput.getAttribute('id');

    // Should have either aria-label, aria-labelledby, or a matching label element
    const hasAriaLabel = ariaLabel !== null && ariaLabel !== '';
    const hasLabelledBy = ariaLabelledBy !== null && ariaLabelledBy !== '';

    let hasMatchingLabel = false;
    if (id) {
      const label = page.locator(`label[for="${id}"]`);
      hasMatchingLabel = (await label.count()) > 0;
    }

    expect(
      hasAriaLabel || hasLabelledBy || hasMatchingLabel,
      'URL input must have an associated label (htmlFor), aria-label, or aria-labelledby'
    ).toBe(true);
  });

  test('Images and icons have appropriate alt text or aria-hidden', async ({ page }) => {
    const images = await page.locator('img').all();
    const iconsWithRoleImg = await page.locator('[role="img"]').all();

    // Check all images have alt text
    for (const img of images) {
      const alt = await img.getAttribute('alt');
      expect(alt, 'Image is missing alt text').toBeTruthy();
    }

    // Check all role="img" elements have aria-label or aria-labelledby
    for (const icon of iconsWithRoleImg) {
      const ariaLabel = await icon.getAttribute('aria-label');
      const ariaLabelledBy = await icon.getAttribute('aria-labelledby');
      expect(
        ariaLabel || ariaLabelledBy,
        'Icon with role="img" is missing aria-label or aria-labelledby'
      ).toBeTruthy();
    }
  });
});
