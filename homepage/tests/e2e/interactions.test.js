/**
 * Hover States and Visual Feedback E2E Tests
 * Owner: Scenario 16 - Hover States and Visual Feedback
 *
 * Tests:
 * - Navigation links hover state
 * - Theme toggle button hover state
 * - Feature cards hover effect
 * - Copy buttons hover state
 * - GitHub link hover state
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = path.join(__dirname, '..', '..', 'index.html');

test.describe('Hover States and Visual Feedback E2E Tests (Scenario 16)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(`file://${indexPath}`);
    // Wait for CSS to fully load
    await page.waitForLoadState('networkidle');
  });

  /**
   * Test Case 1: Navigation links hover state
   * Input: Hover over navigation links
   * Expected: Links show hover state (color change or underline)
   */
  test('Test Case 1: Navigation links show hover state (color change)', async ({ page }) => {
    // Get first navigation link
    const navLink = page.locator('.nav__link').first();
    await expect(navLink).toBeVisible();

    // Get initial color before hover
    const initialColor = await navLink.evaluate(el => {
      return getComputedStyle(el).color;
    });

    // Hover over the link
    await navLink.hover();

    // Wait for transition to complete
    await page.waitForTimeout(300);

    // Get color after hover
    const hoverColor = await navLink.evaluate(el => {
      return getComputedStyle(el).color;
    });

    // Verify color changed on hover
    expect(hoverColor).not.toBe(initialColor);
  });

  /**
   * Test Case 1 (Extended): All navigation links have hover state
   */
  test('Test Case 1 (Extended): All navigation links have hover transitions', async ({ page }) => {
    const navLinks = page.locator('.nav__link');
    const count = await navLinks.count();

    expect(count).toBeGreaterThan(0);

    // Check each navigation link has transition property
    for (let i = 0; i < count; i++) {
      const link = navLinks.nth(i);
      const transition = await link.evaluate(el => {
        return getComputedStyle(el).transition;
      });

      // Should have color transition
      expect(transition).toContain('color');
    }
  });

  /**
   * Test Case 2: Theme toggle button hover state
   * Input: Hover over theme toggle button
   * Expected: Button shows hover state
   */
  test('Test Case 2: Theme toggle button shows hover state', async ({ page }) => {
    const themeToggle = page.locator('.theme-toggle');
    await expect(themeToggle).toBeVisible();

    // Get initial background color before hover
    const initialBgColor = await themeToggle.evaluate(el => {
      return getComputedStyle(el).backgroundColor;
    });

    // Get initial color before hover
    const initialColor = await themeToggle.evaluate(el => {
      return getComputedStyle(el).color;
    });

    // Hover over the button
    await themeToggle.hover();

    // Wait for transition to complete
    await page.waitForTimeout(300);

    // Get background color after hover
    const hoverBgColor = await themeToggle.evaluate(el => {
      return getComputedStyle(el).backgroundColor;
    });

    // Get color after hover
    const hoverColor = await themeToggle.evaluate(el => {
      return getComputedStyle(el).color;
    });

    // Verify either background or text color changed
    const hasVisualChange = (hoverBgColor !== initialBgColor) || (hoverColor !== initialColor);
    expect(hasVisualChange).toBe(true);
  });

  /**
   * Test Case 2 (Extended): Theme toggle has transition property
   */
  test('Test Case 2 (Extended): Theme toggle has transition property', async ({ page }) => {
    const themeToggle = page.locator('.theme-toggle');
    const transition = await themeToggle.evaluate(el => {
      return getComputedStyle(el).transition;
    });

    // Should have transition for smooth hover effect
    expect(transition).not.toBe('none');
    expect(transition.length).toBeGreaterThan(0);
  });

  /**
   * Test Case 3: Feature cards hover effect
   * Input: Hover over feature cards
   * Expected: Cards show subtle hover effect (shadow or scale)
   */
  test('Test Case 3: Feature cards show subtle hover effect (shadow and transform)', async ({ page }) => {
    const featureCard = page.locator('.feature-card').first();
    await expect(featureCard).toBeVisible();

    // Scroll feature card into view
    await featureCard.scrollIntoViewIfNeeded();

    // Get initial transform before hover
    const initialTransform = await featureCard.evaluate(el => {
      return getComputedStyle(el).transform;
    });

    // Get initial box-shadow before hover
    const initialBoxShadow = await featureCard.evaluate(el => {
      return getComputedStyle(el).boxShadow;
    });

    // Hover over the card
    await featureCard.hover();

    // Wait for transition to complete
    await page.waitForTimeout(300);

    // Get transform after hover
    const hoverTransform = await featureCard.evaluate(el => {
      return getComputedStyle(el).transform;
    });

    // Get box-shadow after hover
    const hoverBoxShadow = await featureCard.evaluate(el => {
      return getComputedStyle(el).boxShadow;
    });

    // Verify transform changed (translateY effect)
    const transformChanged = hoverTransform !== initialTransform;

    // Verify box-shadow changed
    const shadowChanged = hoverBoxShadow !== initialBoxShadow;

    // At least one should change
    expect(transformChanged || shadowChanged).toBe(true);
  });

  /**
   * Test Case 3 (Extended): All feature cards have hover transitions
   */
  test('Test Case 3 (Extended): All feature cards have hover transitions', async ({ page }) => {
    const featureCards = page.locator('.feature-card');
    const count = await featureCards.count();

    expect(count).toBe(4); // 4 feature cards expected

    for (let i = 0; i < count; i++) {
      const card = featureCards.nth(i);
      const transition = await card.evaluate(el => {
        return getComputedStyle(el).transition;
      });

      // Should have transition for smooth hover effect
      expect(transition).not.toBe('none');
    }
  });

  /**
   * Test Case 3 (Extended): Feature card border changes on hover
   */
  test('Test Case 3 (Extended): Feature card border color changes on hover', async ({ page }) => {
    const featureCard = page.locator('.feature-card').first();
    await expect(featureCard).toBeVisible();
    await featureCard.scrollIntoViewIfNeeded();

    // Get initial border color
    const initialBorderColor = await featureCard.evaluate(el => {
      return getComputedStyle(el).borderColor;
    });

    // Hover over the card
    await featureCard.hover();
    await page.waitForTimeout(300);

    // Get border color after hover
    const hoverBorderColor = await featureCard.evaluate(el => {
      return getComputedStyle(el).borderColor;
    });

    // Border color should change to primary color on hover
    expect(hoverBorderColor).not.toBe(initialBorderColor);
  });

  /**
   * Test Case 4: Copy buttons hover state
   * Input: Hover over copy buttons
   * Expected: Copy buttons show hover state
   */
  test('Test Case 4: Copy buttons show hover state', async ({ page }) => {
    const copyButton = page.locator('.code-block__copy-btn').first();
    await expect(copyButton).toBeVisible();

    // Scroll copy button into view
    await copyButton.scrollIntoViewIfNeeded();

    // Get initial background color before hover
    const initialBgColor = await copyButton.evaluate(el => {
      return getComputedStyle(el).backgroundColor;
    });

    // Get initial text color before hover
    const initialColor = await copyButton.evaluate(el => {
      return getComputedStyle(el).color;
    });

    // Hover over the button
    await copyButton.hover();

    // Wait for transition to complete
    await page.waitForTimeout(300);

    // Get background color after hover
    const hoverBgColor = await copyButton.evaluate(el => {
      return getComputedStyle(el).backgroundColor;
    });

    // Get text color after hover
    const hoverColor = await copyButton.evaluate(el => {
      return getComputedStyle(el).color;
    });

    // Verify visual change on hover (background or text color)
    const hasVisualChange = (hoverBgColor !== initialBgColor) || (hoverColor !== initialColor);
    expect(hasVisualChange).toBe(true);
  });

  /**
   * Test Case 4 (Extended): All copy buttons have hover transitions
   */
  test('Test Case 4 (Extended): All copy buttons have hover transitions', async ({ page }) => {
    const copyButtons = page.locator('.code-block__copy-btn');
    const count = await copyButtons.count();

    expect(count).toBeGreaterThan(0);

    for (let i = 0; i < count; i++) {
      const button = copyButtons.nth(i);
      const transition = await button.evaluate(el => {
        return getComputedStyle(el).transition;
      });

      // Should have transition for smooth hover effect
      expect(transition).not.toBe('none');
    }
  });

  /**
   * Test Case 5: GitHub link hover state
   * Input: Hover over GitHub link/button
   * Expected: GitHub link shows hover state
   */
  test('Test Case 5: GitHub link shows hover state', async ({ page }) => {
    const githubLink = page.locator('.github-link');
    await expect(githubLink).toBeVisible();

    // Get initial background color before hover
    const initialBgColor = await githubLink.evaluate(el => {
      return getComputedStyle(el).backgroundColor;
    });

    // Get initial color before hover
    const initialColor = await githubLink.evaluate(el => {
      return getComputedStyle(el).color;
    });

    // Hover over the link
    await githubLink.hover();

    // Wait for transition to complete
    await page.waitForTimeout(300);

    // Get background color after hover
    const hoverBgColor = await githubLink.evaluate(el => {
      return getComputedStyle(el).backgroundColor;
    });

    // Get color after hover
    const hoverColor = await githubLink.evaluate(el => {
      return getComputedStyle(el).color;
    });

    // Verify visual change on hover (background or text color)
    const hasVisualChange = (hoverBgColor !== initialBgColor) || (hoverColor !== initialColor);
    expect(hasVisualChange).toBe(true);
  });

  /**
   * Test Case 5 (Extended): GitHub link has transition property
   */
  test('Test Case 5 (Extended): GitHub link has transition property', async ({ page }) => {
    const githubLink = page.locator('.github-link');
    const transition = await githubLink.evaluate(el => {
      return getComputedStyle(el).transition;
    });

    // Should have transition for smooth hover effect
    expect(transition).not.toBe('none');
    expect(transition.length).toBeGreaterThan(0);
  });

  /**
   * Additional test: Footer GitHub link has hover state
   */
  test('Footer link has hover state', async ({ page }) => {
    const footerLink = page.locator('.footer a').first();
    await expect(footerLink).toBeVisible();

    // Scroll footer into view
    await footerLink.scrollIntoViewIfNeeded();

    // Get initial styles before hover
    const initialStyles = await footerLink.evaluate(el => {
      const styles = getComputedStyle(el);
      return {
        color: styles.color,
        textDecoration: styles.textDecoration
      };
    });

    // Hover over the link
    await footerLink.hover();
    await page.waitForTimeout(300);

    // Get styles after hover
    const hoverStyles = await footerLink.evaluate(el => {
      const styles = getComputedStyle(el);
      return {
        color: styles.color,
        textDecoration: styles.textDecoration
      };
    });

    // Verify some visual change occurred on hover
    const hasChange = (hoverStyles.color !== initialStyles.color) ||
                      (hoverStyles.textDecoration !== initialStyles.textDecoration);
    expect(hasChange).toBe(true);
  });

  /**
   * Additional test: Focus states work alongside hover
   */
  test('Interactive elements have focus states', async ({ page }) => {
    // Test navigation link focus
    const navLink = page.locator('.nav__link').first();
    await navLink.focus();

    const navLinkOutline = await navLink.evaluate(el => {
      return getComputedStyle(el).outline;
    });
    // Focus should show some indicator (browser default or custom)

    // Test theme toggle focus
    const themeToggle = page.locator('.theme-toggle');
    await themeToggle.focus();

    // Test copy button focus
    const copyButton = page.locator('.code-block__copy-btn').first();
    await copyButton.scrollIntoViewIfNeeded();
    await copyButton.focus();

    const copyButtonOutline = await copyButton.evaluate(el => {
      return getComputedStyle(el).outline;
    });
    // Should have focus indicator
    expect(copyButtonOutline).not.toBe('0px none rgb(0, 0, 0)');
  });

  /**
   * Additional test: Cursor changes to pointer on interactive elements
   */
  test('Interactive elements show pointer cursor on hover', async ({ page }) => {
    // Check navigation link cursor
    const navLink = page.locator('.nav__link').first();
    const navLinkCursor = await navLink.evaluate(el => {
      return getComputedStyle(el).cursor;
    });
    expect(navLinkCursor).toBe('pointer');

    // Check theme toggle cursor
    const themeToggle = page.locator('.theme-toggle');
    const themeToggleCursor = await themeToggle.evaluate(el => {
      return getComputedStyle(el).cursor;
    });
    expect(themeToggleCursor).toBe('pointer');

    // Check copy button cursor
    const copyButton = page.locator('.code-block__copy-btn').first();
    await copyButton.scrollIntoViewIfNeeded();
    const copyButtonCursor = await copyButton.evaluate(el => {
      return getComputedStyle(el).cursor;
    });
    expect(copyButtonCursor).toBe('pointer');

    // Check GitHub link cursor
    const githubLink = page.locator('.github-link');
    const githubLinkCursor = await githubLink.evaluate(el => {
      return getComputedStyle(el).cursor;
    });
    expect(githubLinkCursor).toBe('pointer');
  });
});
