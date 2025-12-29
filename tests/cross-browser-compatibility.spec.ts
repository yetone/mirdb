import { test, expect } from '@playwright/test';

test.describe('Cross-Browser Compatibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('page renders correctly with proper layout structure', async ({ page, browserName }) => {
    // Visual regression test - verify page renders correctly across browsers
    // Expected: Page renders correctly with no layout issues

    // Check that the main document is rendered
    const html = page.locator('html');
    await expect(html).toBeVisible();

    // Verify viewport is set correctly
    const viewport = page.viewportSize();
    expect(viewport).not.toBeNull();

    // Check all main sections are visible and rendered
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeVisible();

    const configSection = page.locator('[data-testid="configuration-section"]');
    await expect(configSection).toBeVisible();

    const techSpecsSection = page.locator('[data-testid="technical-specs-section"]');
    await expect(techSpecsSection).toBeVisible();

    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await expect(architectureSection).toBeVisible();

    const footer = page.locator('[data-testid="footer"]');
    await expect(footer).toBeVisible();
  });

  test('CSS styles are applied correctly', async ({ page, browserName }) => {
    // Verify CSS is rendered correctly across browsers

    // Check hero section styling
    const heroH1 = page.locator('[data-testid="hero-product-name"]');
    await expect(heroH1).toBeVisible();

    // Verify font styling is applied
    const h1Style = await heroH1.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        fontWeight: style.fontWeight,
        color: style.color,
        fontSize: parseFloat(style.fontSize)
      };
    });

    // Font weight should be bold (700 or 800)
    expect(parseInt(h1Style.fontWeight)).toBeGreaterThanOrEqual(700);

    // Font size should be reasonably large for a headline
    expect(h1Style.fontSize).toBeGreaterThan(20);

    // Check button styling
    const ctaButton = page.locator('[data-testid="hero-cta-primary"]');
    await expect(ctaButton).toBeVisible();

    const buttonStyle = await ctaButton.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        backgroundColor: style.backgroundColor,
        borderRadius: style.borderRadius,
        padding: style.padding
      };
    });

    // Button should have background color
    expect(buttonStyle.backgroundColor).not.toBe('rgba(0, 0, 0, 0)');
    expect(buttonStyle.backgroundColor).not.toBe('transparent');
  });

  test('flexbox and grid layouts render correctly', async ({ page, browserName }) => {
    // Test CSS Grid and Flexbox compatibility

    // Check features grid layout
    const featuresGrid = page.locator('[data-testid="features-grid"]');
    await expect(featuresGrid).toBeVisible();

    const gridStyle = await featuresGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns
      };
    });

    // Should use grid layout
    expect(gridStyle.display).toBe('grid');

    // Check feature cards are visible
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(3);

    // Check flexbox on CTA buttons
    const ctaButtons = page.locator('.cta-buttons');
    const flexStyle = await ctaButtons.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        gap: style.gap
      };
    });

    expect(flexStyle.display).toBe('flex');
  });

  test('SVG graphics render correctly', async ({ page, browserName }) => {
    // Test SVG rendering across browsers

    // Check architecture diagram SVG
    const diagramSvg = page.locator('[data-testid="architecture-diagram"] svg');
    await expect(diagramSvg).toBeVisible();

    // Verify SVG has proper dimensions
    const svgBox = await diagramSvg.boundingBox();
    expect(svgBox).not.toBeNull();
    if (svgBox) {
      expect(svgBox.width).toBeGreaterThan(0);
      expect(svgBox.height).toBeGreaterThan(0);
    }

    // Check that SVG elements are rendered
    const walBox = page.locator('[data-component="wal"]');
    await expect(walBox).toBeVisible();

    const memtableBox = page.locator('[data-component="memtable"]');
    await expect(memtableBox).toBeVisible();

    const sstableBox = page.locator('[data-component="sstable"]');
    await expect(sstableBox).toBeVisible();
  });

  test('fonts render correctly', async ({ page, browserName }) => {
    // Test font rendering across browsers

    const heroTitle = page.locator('[data-testid="hero-product-name"]');
    await expect(heroTitle).toBeVisible();

    const fontInfo = await heroTitle.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        fontFamily: style.fontFamily,
        fontWeight: style.fontWeight,
        lineHeight: style.lineHeight
      };
    });

    // Should have a font family defined
    expect(fontInfo.fontFamily).toBeTruthy();
    expect(fontInfo.fontFamily.length).toBeGreaterThan(0);
  });

  test('code blocks render with proper styling', async ({ page, browserName }) => {
    // Test code/pre blocks rendering

    const configCode = page.locator('[data-testid="config-toml"]');
    await expect(configCode).toBeVisible();

    const codeStyle = await configCode.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        backgroundColor: style.backgroundColor,
        fontFamily: style.fontFamily,
        overflow: style.overflow
      };
    });

    // Should have a dark background
    expect(codeStyle.backgroundColor).not.toBe('rgba(0, 0, 0, 0)');
    expect(codeStyle.backgroundColor).not.toBe('transparent');

    // Should use monospace font
    expect(codeStyle.fontFamily.toLowerCase()).toMatch(/mono|consolas|courier/);
  });

  test('links and navigation work correctly', async ({ page, browserName }) => {
    // Test link rendering and interaction

    // Check GitHub link in hero
    const githubLink = page.locator('[data-testid="hero-cta-github"]');
    await expect(githubLink).toBeVisible();

    const href = await githubLink.getAttribute('href');
    expect(href).toContain('github.com');

    // Check footer GitHub link
    const footerGithubLink = page.locator('[data-testid="footer-github-link"]');
    await expect(footerGithubLink).toBeVisible();

    const footerHref = await footerGithubLink.getAttribute('href');
    expect(footerHref).toContain('github.com');
  });

  test('scrolling and smooth behavior works', async ({ page, browserName }) => {
    // Test scrolling functionality

    // Get initial scroll position
    const initialScroll = await page.evaluate(() => window.scrollY);
    expect(initialScroll).toBe(0);

    // Scroll to features section
    await page.locator('[data-testid="features-section"]').scrollIntoViewIfNeeded();

    // Wait for scroll to complete
    await page.waitForTimeout(500);

    // Verify scroll happened
    const newScroll = await page.evaluate(() => window.scrollY);
    expect(newScroll).toBeGreaterThan(0);
  });

  test('responsive design elements are present', async ({ page, browserName }) => {
    // Test that responsive design CSS is working

    // Check that viewport meta tag exists
    const viewportMeta = page.locator('meta[name="viewport"]');
    await expect(viewportMeta).toHaveAttribute('content', /width=device-width/);

    // Test at desktop size
    await page.setViewportSize({ width: 1920, height: 1080 });

    const heroDesktop = page.locator('[data-testid="hero-section"]');
    await expect(heroDesktop).toBeVisible();

    const desktopH1Size = await page.locator('[data-testid="hero-product-name"]').evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });

    // Test at mobile size
    await page.setViewportSize({ width: 375, height: 667 });

    const heroMobile = page.locator('[data-testid="hero-section"]');
    await expect(heroMobile).toBeVisible();

    const mobileH1Size = await page.locator('[data-testid="hero-product-name"]').evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });

    // Font size should be smaller on mobile
    expect(mobileH1Size).toBeLessThan(desktopH1Size);
  });

  test('tables render correctly', async ({ page, browserName }) => {
    // Test table rendering

    const configTable = page.locator('.config-table');
    await expect(configTable).toBeVisible();

    // Check table has headers
    const tableHeaders = page.locator('.config-table th');
    const headerCount = await tableHeaders.count();
    expect(headerCount).toBeGreaterThanOrEqual(3);

    // Check table has rows
    const tableRows = page.locator('.config-table tbody tr');
    const rowCount = await tableRows.count();
    expect(rowCount).toBeGreaterThanOrEqual(5);
  });

  test('CSS custom properties (variables) work correctly', async ({ page, browserName }) => {
    // Test CSS custom properties compatibility

    const computedPrimary = await page.evaluate(() => {
      const root = document.documentElement;
      return getComputedStyle(root).getPropertyValue('--primary-color').trim();
    });

    // Should have the primary color variable defined
    expect(computedPrimary).toBeTruthy();
    expect(computedPrimary).toMatch(/#[0-9a-fA-F]{6}|rgb/);
  });

  test('no JavaScript errors on page load', async ({ page, browserName }) => {
    // Capture any JavaScript errors
    const errors: string[] = [];

    page.on('pageerror', (error) => {
      errors.push(error.message);
    });

    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Should have no JS errors
    expect(errors).toHaveLength(0);
  });

  test('images and assets load correctly', async ({ page, browserName }) => {
    // Wait for all images to load
    await page.waitForLoadState('networkidle');

    // Check that feature icons (SVGs) are rendered
    const featureIcons = page.locator('.feature-icon svg');
    const iconCount = await featureIcons.count();
    expect(iconCount).toBeGreaterThanOrEqual(4);

    // Verify each icon is visible
    for (let i = 0; i < iconCount; i++) {
      await expect(featureIcons.nth(i)).toBeVisible();
    }
  });

  test('accessibility structure is maintained', async ({ page, browserName }) => {
    // Test basic accessibility structure across browsers

    // Check for main heading
    const h1 = page.locator('h1');
    await expect(h1).toBeVisible();

    // Check for proper heading hierarchy
    const h2s = page.locator('h2');
    const h2Count = await h2s.count();
    expect(h2Count).toBeGreaterThanOrEqual(4);

    // Check for semantic sections
    const sections = page.locator('section');
    const sectionCount = await sections.count();
    expect(sectionCount).toBeGreaterThanOrEqual(5);

    // Check for footer
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();
  });
});
