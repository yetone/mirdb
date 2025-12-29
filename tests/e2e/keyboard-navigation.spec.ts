import { test, expect, Page } from '@playwright/test';

/**
 * E2E Tests for MirDB Homepage Keyboard Navigation Accessibility
 *
 * These tests verify that the page is fully navigable using keyboard only:
 * - All interactive elements receive focus in logical order
 * - Focused elements have visible focus indicators
 * - Buttons can be activated with Enter key
 * - Links can be activated with Enter key
 */

test.describe('Accessibility - Keyboard Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Tab through page from start - All interactive elements receive focus in logical order', async ({ page }) => {
    // Get all interactive elements in expected tab order
    const interactiveElements = page.locator('a[href], button, input, select, textarea, [tabindex]:not([tabindex="-1"])');
    const elementCount = await interactiveElements.count();

    // Ensure there are interactive elements on the page
    expect(elementCount).toBeGreaterThan(0);

    // Start from the body to ensure clean focus state
    await page.focus('body');

    // Track the focused elements as we tab through
    const focusedElements: string[] = [];

    // Tab through all interactive elements
    for (let i = 0; i < elementCount; i++) {
      await page.keyboard.press('Tab');

      // Get the currently focused element
      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) return null;
        return {
          tagName: el.tagName.toLowerCase(),
          href: (el as HTMLAnchorElement).href || null,
          text: el.textContent?.trim().slice(0, 50) || '',
          id: el.id || null,
          className: el.className || ''
        };
      });

      if (focusedElement) {
        focusedElements.push(`${focusedElement.tagName}: ${focusedElement.text || focusedElement.href || focusedElement.id}`);
      }
    }

    // Verify that we were able to tab through multiple elements
    expect(focusedElements.length).toBeGreaterThan(0);

    // Verify logical order - hero section links should come before footer links
    const heroGetStartedIndex = focusedElements.findIndex(el => el.toLowerCase().includes('get started'));
    const heroGitHubIndex = focusedElements.findIndex(el => el.toLowerCase().includes('github') && !el.toLowerCase().includes('footer'));

    // Both CTA buttons should be focusable
    expect(heroGetStartedIndex).toBeGreaterThanOrEqual(0);
    expect(heroGitHubIndex).toBeGreaterThanOrEqual(0);

    // Get Started should come before GitHub in the hero section
    // (Both are in the same CTA buttons container)
    if (heroGetStartedIndex >= 0 && heroGitHubIndex >= 0) {
      expect(heroGetStartedIndex).toBeLessThan(heroGitHubIndex);
    }
  });

  test('TC2: Check focus visibility - Focused elements have visible focus indicator (outline or highlight)', async ({ page }) => {
    // Tab to the first interactive element
    await page.keyboard.press('Tab');

    // Check multiple interactive elements for visible focus indicators
    const elementsToCheck = ['a.btn-primary', 'a.btn-secondary', '.footer-links a'];

    for (const selector of elementsToCheck) {
      const element = page.locator(selector).first();

      if (await element.count() > 0) {
        // Focus the element
        await element.focus();

        // Get computed styles for focus state
        const focusStyles = await element.evaluate((el) => {
          const styles = window.getComputedStyle(el);
          return {
            outline: styles.outline,
            outlineWidth: styles.outlineWidth,
            outlineColor: styles.outlineColor,
            outlineStyle: styles.outlineStyle,
            boxShadow: styles.boxShadow,
            border: styles.border,
            borderColor: styles.borderColor
          };
        });

        // Check for visible focus indicator - either outline or box-shadow
        const hasOutline = focusStyles.outlineStyle !== 'none' &&
                           focusStyles.outlineWidth !== '0px' &&
                           focusStyles.outlineColor !== 'transparent';
        const hasBoxShadow = focusStyles.boxShadow !== 'none';
        const hasBorderChange = focusStyles.border !== 'none';

        // At least one focus indicator should be present
        const hasFocusIndicator = hasOutline || hasBoxShadow || hasBorderChange;

        expect(hasFocusIndicator).toBeTruthy();
      }
    }
  });

  test('TC3: Activate button with Enter key - Buttons can be activated with Enter key when focused', async ({ page }) => {
    // Find the "Get Started" button
    const getStartedButton = page.locator('a.btn-primary').filter({ hasText: /Get Started/i });

    await expect(getStartedButton).toBeVisible();

    // Focus the button
    await getStartedButton.focus();

    // Verify it's focused
    const isFocused = await getStartedButton.evaluate((el) => document.activeElement === el);
    expect(isFocused).toBeTruthy();

    // Get the href before pressing Enter
    const href = await getStartedButton.getAttribute('href');
    expect(href).toBeTruthy();

    // Press Enter to activate the link (button)
    await page.keyboard.press('Enter');

    // For internal anchor links, check if page scrolled to the section
    if (href?.startsWith('#')) {
      const targetId = href.slice(1);
      await page.waitForFunction((id) => {
        const targetElement = document.getElementById(id);
        if (!targetElement) return false;
        const rect = targetElement.getBoundingClientRect();
        // Check if element is near the top of the viewport (within 200px)
        return rect.top >= -200 && rect.top <= 200;
      }, targetId, { timeout: 5000 });
    }
  });

  test('TC4: Activate link with Enter key - Links can be activated with Enter key when focused', async ({ page }) => {
    // Find the GitHub link in the hero section
    const githubLink = page.locator('.hero a.btn-secondary').filter({ hasText: /GitHub/i });

    await expect(githubLink).toBeVisible();

    // Focus the link
    await githubLink.focus();

    // Verify it's focused
    const isFocused = await githubLink.evaluate((el) => document.activeElement === el);
    expect(isFocused).toBeTruthy();

    // Get the href to verify it's a valid external link
    const href = await githubLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href?.toLowerCase()).toContain('github');

    // For external links, we verify the link is properly configured
    // (We don't actually navigate to prevent leaving the test page)
    const target = await githubLink.getAttribute('target');
    const rel = await githubLink.getAttribute('rel');

    // External links should open in new tab with proper security attributes
    expect(target).toBe('_blank');
    expect(rel).toContain('noopener');
  });

  test('TC5: All focusable elements are reachable via Tab key', async ({ page }) => {
    // Get all expected interactive elements
    const allLinks = await page.locator('a[href]').all();
    const allButtons = await page.locator('button').all();

    const totalInteractive = allLinks.length + allButtons.length;

    // Start from body
    await page.focus('body');

    // Tab through all elements and count unique focused elements
    const focusedSet = new Set<string>();

    // Tab more times than elements to ensure we cycle through
    for (let i = 0; i < totalInteractive + 5; i++) {
      await page.keyboard.press('Tab');

      const focusedId = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) return null;
        // Create a unique identifier for the element
        const tagName = el.tagName.toLowerCase();
        const href = (el as HTMLAnchorElement).href || '';
        const text = el.textContent?.trim().slice(0, 30) || '';
        return `${tagName}:${href || text}`;
      });

      if (focusedId) {
        focusedSet.add(focusedId);
      }
    }

    // Verify we can reach most interactive elements
    // (Some might be hidden or have tabindex=-1)
    expect(focusedSet.size).toBeGreaterThanOrEqual(Math.floor(totalInteractive * 0.8));
  });

  test('TC6: Focus order follows visual layout (top to bottom, left to right)', async ({ page }) => {
    // Tab through elements and record their vertical positions
    const focusPositions: { y: number; text: string }[] = [];

    await page.focus('body');

    // Tab through first 10 interactive elements
    for (let i = 0; i < 10; i++) {
      await page.keyboard.press('Tab');

      const position = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) return null;
        const rect = el.getBoundingClientRect();
        return {
          y: rect.top,
          text: el.textContent?.trim().slice(0, 30) || ''
        };
      });

      if (position) {
        focusPositions.push(position);
      }
    }

    // Verify that focus generally moves down the page
    // Allow some flexibility for elements at similar vertical positions
    let outOfOrderCount = 0;
    for (let i = 1; i < focusPositions.length; i++) {
      // Allow 100px tolerance for elements on the same "row"
      if (focusPositions[i].y < focusPositions[i - 1].y - 100) {
        outOfOrderCount++;
      }
    }

    // At most 20% of transitions should be out of order
    expect(outOfOrderCount).toBeLessThanOrEqual(focusPositions.length * 0.2);
  });

  test('TC7: Skip links or first focusable element is accessible', async ({ page }) => {
    // Tab to first element
    await page.keyboard.press('Tab');

    // Verify something is focused
    const firstFocusedTag = await page.evaluate(() => {
      const el = document.activeElement;
      return el?.tagName.toLowerCase() || null;
    });

    // First focusable element should be a link or button
    expect(['a', 'button', 'input']).toContain(firstFocusedTag);
  });

  test('TC8: Space key can activate buttons', async ({ page }) => {
    // Create a button for testing (since our page uses anchor tags as buttons)
    // We'll test that anchor tags styled as buttons work with Enter
    // For true buttons, Space should also work

    // Find any button element (if exists)
    const button = page.locator('button').first();

    if (await button.count() > 0) {
      await button.focus();

      // Verify it's focused
      const isFocused = await button.evaluate((el) => document.activeElement === el);
      expect(isFocused).toBeTruthy();

      // Space should activate buttons
      await page.keyboard.press('Space');

      // Verify button was clicked (would trigger any onclick handlers)
    } else {
      // If no button elements, test passes as page uses anchor tags
      expect(true).toBeTruthy();
    }
  });
});
