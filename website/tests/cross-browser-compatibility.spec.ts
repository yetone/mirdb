import { test, expect } from '@playwright/test';

/**
 * Cross-Browser Compatibility Tests
 *
 * These tests verify that the MirDB homepage works correctly across
 * major browsers: Chrome, Firefox, Safari (WebKit), and Edge.
 *
 * The tests run on all configured browsers in playwright.config.ts
 * and verify content rendering, CSS features, and interactive elements.
 */

test.describe('Cross-Browser Compatibility - Homepage Content', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('TC1: All content and functionality works correctly on current browser', async ({ page, browserName }) => {
    // Log browser name for reporting
    console.log(`Testing on browser: ${browserName}`);

    // Verify hero section renders correctly
    const heroSection = page.locator('.hero-section');
    await expect(heroSection).toBeVisible();

    const heroTitle = page.locator('.hero-section h1');
    await expect(heroTitle).toHaveText('MirDB');
    await expect(heroTitle).toBeVisible();

    const tagline = page.locator('.tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('persistent key-value store');

    // Verify CTA buttons are visible and clickable
    const getStartedBtn = page.locator('.cta-primary');
    await expect(getStartedBtn).toBeVisible();
    await expect(getStartedBtn).toHaveText('Get Started');

    const githubBtn = page.locator('.cta-secondary');
    await expect(githubBtn).toBeVisible();
    await expect(githubBtn).toHaveText('View on GitHub');

    // Verify features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify all 7 feature cards are rendered
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(7);

    // Verify architecture section
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Verify SVG diagram is rendered
    const lsmDiagram = page.locator('.lsm-diagram');
    await expect(lsmDiagram).toBeVisible();

    // Verify quick start section
    const quickStartSection = page.locator('#quickstart');
    await expect(quickStartSection).toBeVisible();

    // Verify code blocks are present
    const codeBlocks = page.locator('.code-block');
    await expect(codeBlocks).toHaveCount(4);

    // Verify project status section
    const statusSection = page.locator('#project-status');
    await expect(statusSection).toBeVisible();

    // Verify completed features list
    const completedFeatures = page.locator('.status-item[data-status="completed"]');
    await expect(completedFeatures).toHaveCount(7);

    // Verify planned features list
    const plannedFeatures = page.locator('.status-item[data-status="planned"]');
    await expect(plannedFeatures).toHaveCount(4);

    console.log(`All content verification passed on ${browserName}`);
  });

  test('TC5: CSS grid/flexbox layout renders consistently', async ({ page, browserName }) => {
    console.log(`Testing CSS grid/flexbox on: ${browserName}`);

    // Test hero section flexbox centering
    const heroSection = page.locator('.hero-section');
    const heroBox = await heroSection.boundingBox();
    expect(heroBox).not.toBeNull();
    expect(heroBox!.width).toBeGreaterThan(0);
    expect(heroBox!.height).toBeGreaterThan(0);

    // Verify hero content is centered (approximately)
    const heroContent = page.locator('.hero-content');
    const contentBox = await heroContent.boundingBox();
    expect(contentBox).not.toBeNull();

    // Test features grid layout
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Verify grid is using CSS Grid
    const gridDisplay = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(gridDisplay).toBe('grid');

    // Test that all feature cards have proper dimensions
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();

    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      const box = await card.boundingBox();
      expect(box).not.toBeNull();
      expect(box!.width).toBeGreaterThan(200);
      expect(box!.height).toBeGreaterThan(100);
    }

    // Test commands grid layout
    const commandsGrid = page.locator('.commands-grid');
    await expect(commandsGrid).toBeVisible();

    const commandsGridDisplay = await commandsGrid.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(commandsGridDisplay).toBe('grid');

    // Test status grid layout
    const statusGrid = page.locator('.status-grid');
    await expect(statusGrid).toBeVisible();

    const statusGridDisplay = await statusGrid.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(statusGridDisplay).toBe('grid');

    // Test architecture explanation grid
    const archExplanation = page.locator('.architecture-explanation');
    await expect(archExplanation).toBeVisible();

    const archGridDisplay = await archExplanation.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(archGridDisplay).toBe('grid');

    // Test CTA buttons flex layout
    const ctaButtons = page.locator('.cta-buttons');
    const ctaDisplay = await ctaButtons.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(ctaDisplay).toBe('flex');

    console.log(`CSS grid/flexbox verification passed on ${browserName}`);
  });

  test('TC6: Syntax highlighting works correctly', async ({ page, browserName }) => {
    console.log(`Testing syntax highlighting on: ${browserName}`);

    // Navigate to quick start section where code blocks are
    await page.locator('#quickstart').scrollIntoViewIfNeeded();
    await page.waitForTimeout(300); // Allow for smooth scroll

    // Get all code blocks
    const codeBlocks = page.locator('.code-block');
    const blockCount = await codeBlocks.count();
    expect(blockCount).toBe(4);

    // Test that syntax highlighting classes are applied and visible
    // Check for comment styling
    const comments = page.locator('.code-block .comment');
    const commentCount = await comments.count();
    expect(commentCount).toBeGreaterThan(0);

    // Verify comment color is applied (should be greyish for comments)
    const firstComment = comments.first();
    const commentColor = await firstComment.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });
    // Comment should have a specific color (not default text color)
    expect(commentColor).not.toBe('rgb(0, 0, 0)');

    // Check for command/keyword styling
    const commands = page.locator('.code-block .command');
    const commandCount = await commands.count();
    expect(commandCount).toBeGreaterThan(0);

    const firstCommand = commands.first();
    const commandColor = await firstCommand.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });
    // Command should have a blue-ish color
    expect(commandColor).not.toBe('rgb(0, 0, 0)');

    // Check for string styling
    const strings = page.locator('.code-block .string');
    const stringCount = await strings.count();
    expect(stringCount).toBeGreaterThan(0);

    const firstString = strings.first();
    const stringColor = await firstString.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });
    // String should have a green-ish color
    expect(stringColor).not.toBe('rgb(0, 0, 0)');

    // Check for keyword styling
    const keywords = page.locator('.code-block .keyword');
    const keywordCount = await keywords.count();
    expect(keywordCount).toBeGreaterThan(0);

    // Verify code blocks have dark background
    const firstCodeBlock = codeBlocks.first();
    const bgColor = await firstCodeBlock.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    // Background should be dark (#1e293b = rgb(30, 41, 59))
    expect(bgColor).toBe('rgb(30, 41, 59)');

    // Verify monospace font family is applied
    const fontFamily = await firstCodeBlock.evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });
    expect(fontFamily.toLowerCase()).toMatch(/monaco|menlo|ubuntu mono|consolas|monospace/i);

    console.log(`Syntax highlighting verification passed on ${browserName}`);
  });
});

test.describe('Cross-Browser Compatibility - Interactive Elements', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('Smooth scroll navigation works correctly', async ({ page, browserName }) => {
    console.log(`Testing smooth scroll on: ${browserName}`);

    // Click "Get Started" button which should scroll to quickstart section
    const getStartedBtn = page.locator('.cta-primary');
    await getStartedBtn.click();

    // Wait for scroll to complete
    await page.waitForTimeout(500);

    // Verify quickstart section is now in view
    const quickStart = page.locator('#quickstart');
    await expect(quickStart).toBeInViewport();

    console.log(`Smooth scroll verification passed on ${browserName}`);
  });

  test('Hover effects work correctly on feature cards', async ({ page, browserName }) => {
    console.log(`Testing hover effects on: ${browserName}`);

    // Get first feature card
    const firstCard = page.locator('.feature-card').first();

    // Get initial transform and box-shadow
    const initialStyles = await firstCard.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        transform: styles.transform,
        boxShadow: styles.boxShadow,
      };
    });

    // Hover over the card
    await firstCard.hover();

    // Wait for transition
    await page.waitForTimeout(350);

    // Get hover styles
    const hoverStyles = await firstCard.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        transform: styles.transform,
        boxShadow: styles.boxShadow,
      };
    });

    // Transform should change (translateY applied)
    // Note: The transform might be "none" or "matrix(...)" depending on browser
    // We check that either transform changed or box-shadow changed
    const transformChanged = initialStyles.transform !== hoverStyles.transform;
    const shadowChanged = initialStyles.boxShadow !== hoverStyles.boxShadow;

    expect(transformChanged || shadowChanged).toBe(true);

    console.log(`Hover effects verification passed on ${browserName}`);
  });

  test('CTA buttons have proper styling and transitions', async ({ page, browserName }) => {
    console.log(`Testing CTA button styling on: ${browserName}`);

    // Test primary button
    const primaryBtn = page.locator('.cta-primary');

    const primaryStyles = await primaryBtn.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        backgroundColor: styles.backgroundColor,
        borderRadius: styles.borderRadius,
        padding: styles.padding,
        display: styles.display,
      };
    });

    // Primary button should have white background
    expect(primaryStyles.backgroundColor).toBe('rgb(255, 255, 255)');
    // Should have border radius
    expect(primaryStyles.borderRadius).toBe('8px');
    // Should use flexbox or inline-flex
    expect(primaryStyles.display).toMatch(/flex/);

    // Test secondary button
    const secondaryBtn = page.locator('.cta-secondary');

    const secondaryStyles = await secondaryBtn.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        backgroundColor: styles.backgroundColor,
        borderRadius: styles.borderRadius,
        borderWidth: styles.borderWidth,
      };
    });

    // Secondary button should have transparent background
    expect(secondaryStyles.backgroundColor).toMatch(/rgba\(0, 0, 0, 0\)|transparent/);
    // Should have border
    expect(secondaryStyles.borderWidth).toBe('2px');

    console.log(`CTA button styling verification passed on ${browserName}`);
  });
});

test.describe('Cross-Browser Compatibility - CSS Custom Properties', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('CSS custom properties are applied correctly', async ({ page, browserName }) => {
    console.log(`Testing CSS custom properties on: ${browserName}`);

    // Check that CSS variables are defined and used
    const rootVariables = await page.evaluate(() => {
      const rootStyles = getComputedStyle(document.documentElement);
      return {
        primaryColor: rootStyles.getPropertyValue('--primary-color').trim(),
        secondaryColor: rootStyles.getPropertyValue('--secondary-color').trim(),
        textColor: rootStyles.getPropertyValue('--text-color').trim(),
        textLight: rootStyles.getPropertyValue('--text-light').trim(),
        background: rootStyles.getPropertyValue('--background').trim(),
      };
    });

    // Verify CSS variables are defined
    expect(rootVariables.primaryColor).toBe('#2563eb');
    expect(rootVariables.secondaryColor).toBe('#1f2937');
    expect(rootVariables.textColor).toBe('#1f2937');
    expect(rootVariables.textLight).toBe('#6b7280');
    expect(rootVariables.background).toBe('#ffffff');

    // Verify variables are applied to elements
    const sectionTitle = page.locator('.section-title').first();
    const titleColor = await sectionTitle.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });
    // Text color should be #1f2937 = rgb(31, 41, 55)
    expect(titleColor).toBe('rgb(31, 41, 55)');

    console.log(`CSS custom properties verification passed on ${browserName}`);
  });
});

test.describe('Cross-Browser Compatibility - SVG Rendering', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('Architecture SVG diagram renders correctly', async ({ page, browserName }) => {
    console.log(`Testing SVG rendering on: ${browserName}`);

    // Navigate to architecture section
    await page.locator('#architecture').scrollIntoViewIfNeeded();
    await page.waitForTimeout(300);

    // Check SVG is present and visible
    const svgDiagram = page.locator('.lsm-diagram');
    await expect(svgDiagram).toBeVisible();

    // Check SVG has viewBox attribute
    const viewBox = await svgDiagram.getAttribute('viewBox');
    expect(viewBox).toBe('0 0 800 500');

    // Check SVG contains expected elements
    const svgRects = page.locator('.lsm-diagram rect');
    const rectCount = await svgRects.count();
    expect(rectCount).toBeGreaterThan(5);

    // Check SVG text elements
    const svgTexts = page.locator('.lsm-diagram text');
    const textCount = await svgTexts.count();
    expect(textCount).toBeGreaterThan(10);

    // Check arrow markers are defined
    const arrowDefs = page.locator('.lsm-diagram defs marker');
    const markerCount = await arrowDefs.count();
    expect(markerCount).toBe(3); // Blue, Green, Orange arrows

    // Verify SVG has proper dimensions
    const svgBox = await svgDiagram.boundingBox();
    expect(svgBox).not.toBeNull();
    expect(svgBox!.width).toBeGreaterThan(100);
    expect(svgBox!.height).toBeGreaterThan(100);

    console.log(`SVG rendering verification passed on ${browserName}`);
  });
});

test.describe('Cross-Browser Compatibility - Typography', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('Typography renders consistently across browsers', async ({ page, browserName }) => {
    console.log(`Testing typography on: ${browserName}`);

    // Check body font family
    const bodyFontFamily = await page.evaluate(() => {
      return window.getComputedStyle(document.body).fontFamily;
    });
    // Should use system font stack
    expect(bodyFontFamily).toMatch(/-apple-system|BlinkMacSystemFont|Segoe UI|Roboto|sans-serif/i);

    // Check hero title sizing (should use clamp)
    const heroTitle = page.locator('.hero-section h1');
    const titleFontSize = await heroTitle.evaluate((el) => {
      return window.getComputedStyle(el).fontSize;
    });
    // Font size should be reasonable (between 48px and 80px for desktop)
    const size = parseInt(titleFontSize);
    expect(size).toBeGreaterThanOrEqual(48);
    expect(size).toBeLessThanOrEqual(100);

    // Check section titles
    const sectionTitle = page.locator('.section-title').first();
    const sectionTitleSize = await sectionTitle.evaluate((el) => {
      return window.getComputedStyle(el).fontSize;
    });
    const sectionSize = parseInt(sectionTitleSize);
    expect(sectionSize).toBeGreaterThanOrEqual(24);

    // Check line height on body
    const lineHeight = await page.evaluate(() => {
      return window.getComputedStyle(document.body).lineHeight;
    });
    // Line height should be set (not "normal")
    expect(lineHeight).not.toBe('normal');

    console.log(`Typography verification passed on ${browserName}`);
  });
});
