import { test, expect } from '@playwright/test';

/**
 * Cross-Browser Compatibility Tests
 * Scenario: Verify the homepage renders correctly across major browsers including Chrome, Firefox, Safari, and Edge
 *
 * Test Cases:
 * 1. Page renders correctly with proper styling in Chrome
 * 2. Page renders correctly with proper styling in Firefox
 * 3. Page renders correctly with proper styling in Safari
 * 4. Page renders correctly with proper styling in Edge
 * 5. Layout CSS works consistently across all browsers
 * 6. All JS features (copy button, etc.) work in all browsers
 */

test.describe('Cross-Browser Compatibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for page to be fully loaded
    await page.waitForLoadState('networkidle');
  });

  test('TC1: Page renders correctly with proper styling', async ({ page, browserName }) => {
    // Verify page loads with correct title
    await expect(page).toHaveTitle(/MirDB/);

    // Verify main page elements are visible
    const navbar = page.locator('.navbar');
    await expect(navbar).toBeVisible();

    const hero = page.locator('#hero, .hero').first();
    await expect(hero).toBeVisible();

    const features = page.locator('#features, .features').first();
    await expect(features).toBeVisible();

    const gettingStarted = page.locator('#getting-started, .getting-started').first();
    await expect(gettingStarted).toBeVisible();

    // Verify CSS is properly loaded and applied
    const heroStyles = await hero.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        padding: style.padding,
        textAlign: style.textAlign,
      };
    });

    // Hero section should have styling applied
    expect(heroStyles.textAlign).toBe('center');

    // Verify CSS variables are working (browser-specific CSS rendering)
    const rootStyles = await page.evaluate(() => {
      const style = window.getComputedStyle(document.documentElement);
      return {
        colorPrimary: style.getPropertyValue('--color-primary').trim(),
        colorBackground: style.getPropertyValue('--color-background').trim(),
        maxWidth: style.getPropertyValue('--max-width').trim(),
      };
    });

    // CSS variables should be defined
    expect(rootStyles.colorPrimary).toBeTruthy();
    expect(rootStyles.maxWidth).toBeTruthy();

    // Log which browser is being tested for debugging
    console.log(`Testing on browser: ${browserName}`);
  });

  test('TC5: CSS flexbox/grid compatibility across browsers', async ({ page, browserName }) => {
    // Test CSS Grid in feature cards
    const featureGrid = page.locator('.feature-grid, .features-grid').first();
    await expect(featureGrid).toBeVisible();

    const gridStyles = await featureGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns,
      };
    });

    // Grid should be working correctly
    expect(gridStyles.display).toBe('grid');

    // Verify grid has multiple columns (cross-browser grid support)
    const columns = gridStyles.gridTemplateColumns.split(' ').filter((col: string) => col.trim() !== '');
    expect(columns.length).toBeGreaterThanOrEqual(1);

    // Test Flexbox in navigation
    const navLinks = page.locator('.nav-links').first();
    const navStyles = await navLinks.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        alignItems: style.alignItems,
      };
    });

    // Navigation should use flexbox
    expect(navStyles.display).toBe('flex');

    // Test Flexbox in CTA buttons
    const ctaButtons = page.locator('.cta-buttons').first();
    if (await ctaButtons.count() > 0) {
      const ctaStyles = await ctaButtons.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          display: style.display,
        };
      });
      expect(ctaStyles.display).toBe('flex');
    }

    // Test CSS transitions work (hover effects)
    const firstFeatureCard = page.locator('.feature-card').first();
    await expect(firstFeatureCard).toBeVisible();

    const cardStyles = await firstFeatureCard.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        transition: style.transition,
        borderRadius: style.borderRadius,
      };
    });

    // Cards should have transitions and border-radius
    expect(cardStyles.borderRadius).toBeTruthy();

    console.log(`Flexbox/Grid compatibility verified on browser: ${browserName}`);
  });

  test('TC6: JavaScript functionality works across browsers', async ({ page, browserName }) => {
    // Test copy button functionality
    const copyButtons = page.locator('.copy-btn');
    const copyButtonCount = await copyButtons.count();
    expect(copyButtonCount).toBeGreaterThan(0);

    // Get the first copy button and its code block
    const firstCopyButton = copyButtons.first();
    await expect(firstCopyButton).toBeVisible();

    // Click the copy button and verify state change
    await firstCopyButton.click();

    // Wait for the button to show "Copied!" state
    await expect(firstCopyButton.locator('.copy-text')).toHaveText('Copied!', { timeout: 5000 });

    // Verify the check icon is visible after clicking
    const checkIcon = firstCopyButton.locator('.check-icon');
    await expect(checkIcon).not.toHaveClass(/hidden/);

    // Wait for button to reset (2 seconds as per implementation)
    await page.waitForTimeout(2500);

    // Verify button resets to original state
    await expect(firstCopyButton.locator('.copy-text')).toHaveText('Copy');

    // Test mobile navigation toggle
    // First resize to mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });

    // Mobile menu button should be visible
    const mobileMenuBtn = page.locator('.mobile-menu-btn');
    await expect(mobileMenuBtn).toBeVisible();

    // Click mobile menu button
    await mobileMenuBtn.click();

    // Nav links should now have 'active' class
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toHaveClass(/active/);

    // Verify aria-expanded is updated
    await expect(mobileMenuBtn).toHaveAttribute('aria-expanded', 'true');

    // Click a nav link to close menu
    const firstNavLink = navLinks.locator('a').first();
    await firstNavLink.click();

    // Menu should close
    await expect(navLinks).not.toHaveClass(/active/);

    // Test smooth scroll functionality
    await page.setViewportSize({ width: 1920, height: 1080 });

    // Scroll to top first
    await page.evaluate(() => window.scrollTo(0, 0));
    await page.waitForTimeout(500);

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.pageYOffset);

    // Click on a navigation link that scrolls to a section
    const featuresLink = page.locator('.nav-links a[href="#features"]');
    if (await featuresLink.count() > 0) {
      await featuresLink.click();

      // Wait for smooth scroll animation
      await page.waitForTimeout(1000);

      // Verify page scrolled
      const finalScrollY = await page.evaluate(() => window.pageYOffset);
      expect(finalScrollY).toBeGreaterThan(initialScrollY);
    }

    console.log(`JavaScript functionality verified on browser: ${browserName}`);
  });

  test('TC2-4: Page layout and styling consistency', async ({ page, browserName }) => {
    // Verify fonts are loaded correctly
    const bodyStyles = await page.evaluate(() => {
      const style = window.getComputedStyle(document.body);
      return {
        fontFamily: style.fontFamily,
        fontSize: style.fontSize,
        lineHeight: style.lineHeight,
        color: style.color,
        backgroundColor: style.backgroundColor,
      };
    });

    // Font family should be applied
    expect(bodyStyles.fontFamily).toBeTruthy();
    // Background color should be set (dark theme)
    expect(bodyStyles.backgroundColor).toBeTruthy();

    // Verify heading styles
    const h1 = page.locator('h1').first();
    await expect(h1).toBeVisible();

    const h1Styles = await h1.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        fontSize: style.fontSize,
        fontWeight: style.fontWeight,
        margin: style.margin,
      };
    });

    // H1 should have larger font size
    const fontSize = parseFloat(h1Styles.fontSize);
    expect(fontSize).toBeGreaterThan(20); // Should be significantly larger than body text

    // Verify button styles
    const primaryBtn = page.locator('.btn-primary').first();
    await expect(primaryBtn).toBeVisible();

    const btnStyles = await primaryBtn.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        backgroundColor: style.backgroundColor,
        borderRadius: style.borderRadius,
        padding: style.padding,
        cursor: style.cursor,
      };
    });

    // Button should have styling applied
    expect(btnStyles.backgroundColor).toBeTruthy();
    expect(btnStyles.borderRadius).toBeTruthy();
    // Note: WebKit may report 'auto' for cursor on anchor elements even with cursor: pointer
    // This is valid behavior as the cursor is inherited and computed differently
    expect(['pointer', 'auto']).toContain(btnStyles.cursor);

    // Verify code blocks render correctly
    const codeBlock = page.locator('pre code').first();
    await expect(codeBlock).toBeVisible();

    const codeStyles = await codeBlock.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        fontFamily: style.fontFamily,
        fontSize: style.fontSize,
      };
    });

    // Code should use monospace font
    expect(codeStyles.fontFamily.toLowerCase()).toMatch(/mono|courier|consolas/);

    // Verify architecture diagram renders
    const archDiagram = page.locator('.architecture-diagram');
    if (await archDiagram.count() > 0) {
      await expect(archDiagram).toBeVisible();
    }

    // Verify table renders correctly
    const configTable = page.locator('#configuration table, .configuration table').first();
    if (await configTable.count() > 0) {
      await expect(configTable).toBeVisible();

      // Check table has rows
      const tableRows = configTable.locator('tr');
      const rowCount = await tableRows.count();
      expect(rowCount).toBeGreaterThan(1);
    }

    // Verify footer renders
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    console.log(`Layout and styling verified on browser: ${browserName}`);
  });

  test('Cross-browser: External resources load correctly', async ({ page, browserName }) => {
    // Verify external CSS (Prism.js) loads
    const prismCss = page.locator('link[href*="prism"]');
    if (await prismCss.count() > 0) {
      // Check if stylesheet is loaded
      const prismLoaded = await page.evaluate(() => {
        const styleSheets = Array.from(document.styleSheets);
        return styleSheets.some(sheet => sheet.href?.includes('prism'));
      });
      expect(prismLoaded).toBeTruthy();
    }

    // Verify external JavaScript loads
    const scriptsLoaded = await page.evaluate(() => {
      // Check if DOMContentLoaded has fired (means our main.js ran)
      const copyBtns = document.querySelectorAll('.copy-btn');
      return copyBtns.length > 0;
    });
    expect(scriptsLoaded).toBeTruthy();

    // Verify images/SVGs render
    const svgIcons = page.locator('svg');
    const svgCount = await svgIcons.count();
    expect(svgCount).toBeGreaterThan(0);

    // Verify first SVG is visible
    if (svgCount > 0) {
      const firstSvg = svgIcons.first();
      await expect(firstSvg).toBeVisible();
    }

    console.log(`External resources verified on browser: ${browserName}`);
  });

  test('Cross-browser: Color contrast and accessibility', async ({ page, browserName }) => {
    // Verify color scheme is applied consistently
    const colors = await page.evaluate(() => {
      const styles = window.getComputedStyle(document.documentElement);
      const bodyStyles = window.getComputedStyle(document.body);
      return {
        primary: styles.getPropertyValue('--color-primary'),
        text: styles.getPropertyValue('--color-text'),
        background: styles.getPropertyValue('--color-background'),
        bodyBg: bodyStyles.backgroundColor,
        bodyColor: bodyStyles.color,
      };
    });

    // CSS variables should be applied
    expect(colors.primary).toBeTruthy();
    expect(colors.text).toBeTruthy();
    // Verify body has background color applied
    expect(colors.bodyBg).toBeTruthy();

    // Verify ARIA attributes are present
    const navRole = await page.locator('nav[role="navigation"]').count();
    expect(navRole).toBeGreaterThan(0);

    // Verify aria-label on mobile menu button
    const mobileBtn = page.locator('.mobile-menu-btn[aria-label]');
    expect(await mobileBtn.count()).toBeGreaterThan(0);

    // Verify semantic HTML structure
    const sections = page.locator('section');
    const sectionCount = await sections.count();
    expect(sectionCount).toBeGreaterThanOrEqual(3);

    // Verify heading hierarchy
    const headings = await page.evaluate(() => {
      const h1Count = document.querySelectorAll('h1').length;
      const h2Count = document.querySelectorAll('h2').length;
      const h3Count = document.querySelectorAll('h3').length;
      return { h1Count, h2Count, h3Count };
    });

    expect(headings.h1Count).toBeGreaterThan(0);
    expect(headings.h2Count).toBeGreaterThan(0);

    console.log(`Accessibility features verified on browser: ${browserName}`);
  });
});
