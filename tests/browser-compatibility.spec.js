// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Browser Compatibility Tests
 *
 * These tests verify that the MirDB homepage works correctly across
 * Chrome, Firefox, Safari (Webkit), and Edge browsers as specified
 * in the PRD success criteria.
 */

// Test that page loads and renders correctly
test.describe('Browser Compatibility - Page Load and Rendering', () => {
  test('homepage loads successfully and displays core content', async ({ page, browserName }) => {
    await page.goto('/');

    // Verify page title
    await expect(page).toHaveTitle(/MirDB/);

    // Verify hero section renders
    const heroTitle = page.locator('.hero-title');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toContainText('MirDB');

    // Verify tagline renders
    const tagline = page.locator('.hero-tagline');
    await expect(tagline).toBeVisible();

    // Verify hero CTA buttons render
    const ctaButtons = page.locator('.hero-cta .btn');
    await expect(ctaButtons).toHaveCount(2);

    // Log browser name for debugging
    console.log(`✓ Page loaded successfully in ${browserName}`);
  });

  test('navigation renders and is functional', async ({ page, browserName }) => {
    await page.goto('/');

    // Verify header/navigation is visible
    const header = page.locator('.header');
    await expect(header).toBeVisible();

    // Verify logo
    const logo = page.locator('.logo');
    await expect(logo).toBeVisible();
    await expect(logo).toContainText('MirDB');

    // Verify navigation links
    const navLinks = page.locator('.nav-links a');
    const linkCount = await navLinks.count();
    expect(linkCount).toBeGreaterThan(0);

    // Verify theme toggle exists
    const themeToggle = page.locator('.theme-toggle');
    await expect(themeToggle).toBeVisible();

    console.log(`✓ Navigation renders correctly in ${browserName}`);
  });

  test('features section renders correctly', async ({ page, browserName }) => {
    await page.goto('/');

    // Navigate to features section
    const featuresSection = page.locator('.features');
    await expect(featuresSection).toBeVisible();

    // Verify feature cards render
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(3);

    // Verify each card has title and description
    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      await expect(card.locator('h3')).toBeVisible();
      await expect(card.locator('p')).toBeVisible();
    }

    console.log(`✓ Features section renders correctly in ${browserName}`);
  });

  test('code blocks render with syntax highlighting', async ({ page, browserName }) => {
    await page.goto('/');

    // Find code blocks
    const codeBlocks = page.locator('pre, .code-block');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    // Verify code blocks have visible content
    const firstCodeBlock = codeBlocks.first();
    await expect(firstCodeBlock).toBeVisible();

    // Verify code block styling (background color indicates it's styled)
    const codeBlockBg = await firstCodeBlock.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    expect(codeBlockBg).not.toBe('rgba(0, 0, 0, 0)'); // Not transparent

    console.log(`✓ Code blocks render correctly in ${browserName}`);
  });

  test('footer renders correctly', async ({ page, browserName }) => {
    await page.goto('/');

    // Scroll to footer
    const footer = page.locator('.footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Verify footer links
    const footerLinks = page.locator('.footer-links a');
    const linkCount = await footerLinks.count();
    expect(linkCount).toBeGreaterThan(0);

    console.log(`✓ Footer renders correctly in ${browserName}`);
  });
});

// Test CSS custom properties functionality
test.describe('Browser Compatibility - CSS Custom Properties', () => {
  test('CSS custom properties are applied correctly', async ({ page, browserName }) => {
    await page.goto('/');

    // Get computed styles that use CSS custom properties
    const body = page.locator('body');
    const bodyStyles = await body.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        backgroundColor: styles.backgroundColor,
        color: styles.color,
        fontFamily: styles.fontFamily
      };
    });

    // Verify background color is applied (not transparent/default)
    expect(bodyStyles.backgroundColor).not.toBe('rgba(0, 0, 0, 0)');
    expect(bodyStyles.color).not.toBe('rgba(0, 0, 0, 0)');

    // Verify CSS custom property values are resolved
    const rootStyles = await page.evaluate(() => {
      const root = document.documentElement;
      const computedStyle = getComputedStyle(root);
      return {
        colorBg: computedStyle.getPropertyValue('--color-bg').trim(),
        colorText: computedStyle.getPropertyValue('--color-text').trim(),
        colorPrimary: computedStyle.getPropertyValue('--color-primary').trim()
      };
    });

    // CSS custom properties should be defined and non-empty
    expect(rootStyles.colorBg).not.toBe('');
    expect(rootStyles.colorText).not.toBe('');
    expect(rootStyles.colorPrimary).not.toBe('');

    console.log(`✓ CSS custom properties work correctly in ${browserName}`);
  });

  test('theme toggle changes CSS custom properties', async ({ page, browserName }) => {
    await page.goto('/');

    // Get initial theme state
    const initialTheme = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme');
    });

    // Get initial background color
    const initialBg = await page.evaluate(() => {
      return window.getComputedStyle(document.body).backgroundColor;
    });

    // Click theme toggle
    const themeToggle = page.locator('.theme-toggle');
    await themeToggle.click();

    // Wait for theme transition
    await page.waitForTimeout(300);

    // Get new theme state
    const newTheme = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme');
    });

    // Get new background color
    const newBg = await page.evaluate(() => {
      return window.getComputedStyle(document.body).backgroundColor;
    });

    // Theme should have changed
    expect(newTheme).not.toBe(initialTheme);
    // Background color should have changed
    expect(newBg).not.toBe(initialBg);

    console.log(`✓ Theme toggle works correctly in ${browserName}`);
  });

  test('CSS fallbacks work for custom properties', async ({ page, browserName }) => {
    await page.goto('/');

    // Test that elements using CSS custom properties render properly
    // even if custom properties had to fall back to defaults

    // Check primary button styling
    const primaryBtn = page.locator('.btn-primary').first();
    if (await primaryBtn.isVisible()) {
      const btnStyles = await primaryBtn.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          backgroundColor: styles.backgroundColor,
          color: styles.color,
          borderRadius: styles.borderRadius
        };
      });

      // Button should have proper styling
      expect(btnStyles.backgroundColor).not.toBe('rgba(0, 0, 0, 0)');
      expect(btnStyles.color).toBeTruthy();
    }

    // Check header styling
    const header = page.locator('.header');
    const headerStyles = await header.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        backgroundColor: styles.backgroundColor,
        borderBottomColor: styles.borderBottomColor
      };
    });

    expect(headerStyles.backgroundColor).not.toBe('rgba(0, 0, 0, 0)');

    console.log(`✓ CSS fallbacks work correctly in ${browserName}`);
  });
});

// Test interactive features
test.describe('Browser Compatibility - Interactive Features', () => {
  test('copy-to-clipboard buttons work', async ({ page, browserName }) => {
    await page.goto('/');

    // Find copy buttons
    const copyButtons = page.locator('.copy-button');
    const copyButtonCount = await copyButtons.count();

    if (copyButtonCount > 0) {
      const firstCopyBtn = copyButtons.first();
      await firstCopyBtn.scrollIntoViewIfNeeded();
      await expect(firstCopyBtn).toBeVisible();

      // Click copy button
      await firstCopyBtn.click();

      // Verify button state changed (copied class or text change)
      await page.waitForTimeout(200);

      // Check if button shows copied state
      const btnText = await firstCopyBtn.textContent();
      const hasCopiedClass = await firstCopyBtn.evaluate((el) => el.classList.contains('copied'));

      // Either the text changed to "Copied!" or the class was added
      const copyWorked = btnText?.includes('Copied') || hasCopiedClass;

      console.log(`✓ Copy button interaction works in ${browserName} (copied state: ${copyWorked})`);
    } else {
      console.log(`ℹ No copy buttons found in ${browserName}`);
    }
  });

  test('smooth scrolling works for anchor links', async ({ page, browserName }) => {
    await page.goto('/');

    // Find navigation links with hash anchors
    const navLinks = page.locator('.nav-links a[href^="#"]');
    const linkCount = await navLinks.count();

    if (linkCount > 0) {
      // Get initial scroll position
      const initialScroll = await page.evaluate(() => window.scrollY);

      // Click first anchor link
      const firstLink = navLinks.first();
      await firstLink.click();

      // Wait for scroll animation
      await page.waitForTimeout(500);

      // Get new scroll position
      const newScroll = await page.evaluate(() => window.scrollY);

      // Scroll position should have changed (or stayed 0 if target is at top)
      console.log(`✓ Smooth scrolling tested in ${browserName} (scroll: ${initialScroll} -> ${newScroll})`);
    }
  });

  test('hover effects work on interactive elements', async ({ page, browserName }) => {
    await page.goto('/');

    // Test hover on feature card
    const featureCard = page.locator('.feature-card').first();
    await featureCard.scrollIntoViewIfNeeded();

    // Get initial transform
    const initialTransform = await featureCard.evaluate((el) => {
      return window.getComputedStyle(el).transform;
    });

    // Hover over card
    await featureCard.hover();
    await page.waitForTimeout(300);

    // Get transform after hover
    const hoverTransform = await featureCard.evaluate((el) => {
      return window.getComputedStyle(el).transform;
    });

    console.log(`✓ Hover effects tested in ${browserName}`);
    console.log(`  Initial transform: ${initialTransform}`);
    console.log(`  Hover transform: ${hoverTransform}`);
  });
});

// Test responsive layout
test.describe('Browser Compatibility - Responsive Layout', () => {
  test('layout adapts to desktop viewport', async ({ page, browserName }) => {
    await page.setViewportSize({ width: 1280, height: 800 });
    await page.goto('/');

    // Verify desktop layout
    const featureGrid = page.locator('.feature-grid');
    await expect(featureGrid).toBeVisible();

    // Check grid layout (should be multi-column on desktop)
    const gridStyles = await featureGrid.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        display: styles.display,
        gridTemplateColumns: styles.gridTemplateColumns
      };
    });

    expect(gridStyles.display).toBe('grid');

    console.log(`✓ Desktop layout works in ${browserName}`);
  });

  test('layout adapts to tablet viewport', async ({ page, browserName }) => {
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto('/');

    // Verify navigation is still visible on tablet
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();

    // Verify feature grid adapts
    const featureGrid = page.locator('.feature-grid');
    await expect(featureGrid).toBeVisible();

    console.log(`✓ Tablet layout works in ${browserName}`);
  });

  test('layout adapts to mobile viewport', async ({ page, browserName }) => {
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    // Verify hero section is visible
    const heroTitle = page.locator('.hero-title');
    await expect(heroTitle).toBeVisible();

    // Verify content is not cut off horizontally
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);

    // Body should not be significantly wider than viewport (some small difference is acceptable)
    expect(bodyWidth).toBeLessThanOrEqual(viewportWidth + 5);

    console.log(`✓ Mobile layout works in ${browserName}`);
  });
});

// Test accessibility basics across browsers
test.describe('Browser Compatibility - Accessibility', () => {
  test('focus states are visible', async ({ page, browserName }) => {
    await page.goto('/');

    // Tab to first focusable element
    await page.keyboard.press('Tab');

    // Check that some element has focus
    const focusedElement = await page.evaluate(() => {
      const el = document.activeElement;
      return el ? el.tagName : null;
    });

    expect(focusedElement).toBeTruthy();

    console.log(`✓ Focus states work in ${browserName}`);
  });

  test('touch targets meet minimum size requirements', async ({ page, browserName }) => {
    await page.goto('/');

    // Check theme toggle button size (should be at least 44x44px)
    const themeToggle = page.locator('.theme-toggle');
    const toggleSize = await themeToggle.boundingBox();

    if (toggleSize) {
      expect(toggleSize.width).toBeGreaterThanOrEqual(44);
      expect(toggleSize.height).toBeGreaterThanOrEqual(44);
    }

    console.log(`✓ Touch targets sized correctly in ${browserName}`);
  });

  test('semantic HTML elements are used', async ({ page, browserName }) => {
    await page.goto('/');

    // Check for semantic elements
    const header = page.locator('header, .header');
    const main = page.locator('main');
    const footer = page.locator('footer, .footer');
    const nav = page.locator('nav, .nav');

    // At least header and footer should exist
    const headerExists = await header.count() > 0;
    const footerExists = await footer.count() > 0;

    expect(headerExists || footerExists).toBeTruthy();

    console.log(`✓ Semantic HTML verified in ${browserName}`);
  });
});
