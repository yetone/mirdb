import { test, expect } from '@playwright/test';

test.describe('Cross-Browser Compatibility - Firefox', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Page loads without console errors in Firefox', async ({ page }) => {
    const consoleErrors: string[] = [];

    // Collect console errors
    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        consoleErrors.push(msg.text());
      }
    });

    // Reload page to capture any load-time errors
    await page.reload();
    await page.waitForLoadState('networkidle');

    // Verify page title is correct
    await expect(page).toHaveTitle('MirDB - Persistent Key-Value Store with Memcached Compatibility');

    // Verify no JavaScript errors occurred
    expect(consoleErrors.length).toBe(0);
  });

  test('TC2: All sections display with correct layout in Firefox', async ({ page }) => {
    // Check hero section renders correctly
    const hero = page.locator('.hero');
    await expect(hero).toBeVisible();

    // Verify hero has proper background (gradient)
    const heroStyles = await hero.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        backgroundImage: style.backgroundImage,
        color: style.color,
        padding: style.padding,
      };
    });
    expect(heroStyles.backgroundImage).toContain('linear-gradient');

    // Check features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify features grid layout
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(4);

    // Check getting started section
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Check commands section
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Verify commands grid layout
    const commandsGrid = page.locator('.commands-grid');
    await expect(commandsGrid).toBeVisible();
    const commandCategories = page.locator('.command-category');
    await expect(commandCategories).toHaveCount(4);

    // Check architecture section
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Verify SVG diagram is visible
    const diagram = page.locator('.architecture-diagram');
    await expect(diagram).toBeVisible();

    // Check footer
    const footer = page.locator('.footer');
    await expect(footer).toBeVisible();
  });

  test('TC3: Smooth scroll navigation works in Firefox', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);
    expect(initialScrollY).toBe(0);

    // Click on Features navigation link
    await page.click('.nav-links a[href="#features"]');

    // Wait for smooth scroll animation
    await page.waitForTimeout(600);

    // Verify scroll position changed
    const afterFeaturesScrollY = await page.evaluate(() => window.scrollY);
    expect(afterFeaturesScrollY).toBeGreaterThan(0);

    // Verify features section is in viewport
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();

    // Click on Getting Started navigation link
    await page.click('.nav-links a[href="#getting-started"]');
    await page.waitForTimeout(600);

    // Verify getting started section is in viewport
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeInViewport();

    // Click on Commands navigation link
    await page.click('.nav-links a[href="#commands"]');
    await page.waitForTimeout(600);

    // Verify commands section is in viewport
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeInViewport();

    // Click on Architecture navigation link
    await page.click('.nav-links a[href="#architecture"]');
    await page.waitForTimeout(600);

    // Verify architecture section is in viewport
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeInViewport();
  });

  test('CSS flexbox and grid layouts render correctly', async ({ page }) => {
    // Verify navigation uses flexbox correctly
    const navContainer = page.locator('.nav-container');
    const navContainerDisplay = await navContainer.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(navContainerDisplay).toBe('flex');

    // Verify features grid uses CSS grid correctly
    const featuresGrid = page.locator('.features-grid');
    const featuresGridDisplay = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(featuresGridDisplay).toBe('grid');

    // Verify commands grid uses CSS grid correctly
    const commandsGrid = page.locator('.commands-grid');
    const commandsGridDisplay = await commandsGrid.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(commandsGridDisplay).toBe('grid');

    // Verify footer uses grid correctly
    const footerContent = page.locator('.footer-content');
    const footerContentDisplay = await footerContent.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(footerContentDisplay).toBe('grid');
  });

  test('CSS custom properties (variables) work correctly', async ({ page }) => {
    // Verify CSS custom properties are applied
    const body = page.locator('body');
    const bodyStyles = await body.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        fontFamily: style.fontFamily,
        color: style.color,
        backgroundColor: style.backgroundColor,
      };
    });

    // Verify body has proper styling from CSS variables
    expect(bodyStyles.fontFamily).toBeTruthy();
    expect(bodyStyles.color).toBeTruthy();
    expect(bodyStyles.backgroundColor).toBeTruthy();

    // Verify primary button uses correct color
    const primaryButton = page.locator('.btn-primary').first();
    const buttonBgColor = await primaryButton.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    // Primary color is #2563eb which is rgb(37, 99, 235)
    expect(buttonBgColor).toContain('37');
    expect(buttonBgColor).toContain('99');
    expect(buttonBgColor).toContain('235');
  });

  test('SVG diagram renders correctly', async ({ page }) => {
    // Verify SVG architecture diagram is visible and properly sized
    const svgDiagram = page.locator('.architecture-diagram');
    await expect(svgDiagram).toBeVisible();

    // Check SVG has proper viewBox
    const viewBox = await svgDiagram.getAttribute('viewBox');
    expect(viewBox).toBe('0 0 600 400');

    // Verify SVG elements are rendered
    const svgRects = page.locator('.architecture-diagram rect');
    const rectsCount = await svgRects.count();
    expect(rectsCount).toBeGreaterThan(0);

    // Verify SVG text elements
    const svgTexts = page.locator('.architecture-diagram text');
    const textsCount = await svgTexts.count();
    expect(textsCount).toBeGreaterThan(0);
  });

  test('External links have correct attributes', async ({ page }) => {
    // Check GitHub link in navigation
    const navGithubLink = page.locator('.nav-links a:has-text("GitHub")');
    await expect(navGithubLink).toHaveAttribute('target', '_blank');
    await expect(navGithubLink).toHaveAttribute('rel', 'noopener noreferrer');
    await expect(navGithubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

    // Check GitHub link in hero section
    const heroGithubLink = page.locator('.hero-buttons a:has-text("GitHub")');
    await expect(heroGithubLink).toHaveAttribute('target', '_blank');
    await expect(heroGithubLink).toHaveAttribute('rel', 'noopener noreferrer');
  });

  test('Fixed navigation stays at top when scrolling', async ({ page }) => {
    const navbar = page.locator('.navbar');

    // Verify navbar is visible initially
    await expect(navbar).toBeVisible();

    // Get initial position
    const initialBox = await navbar.boundingBox();
    expect(initialBox).not.toBeNull();
    expect(initialBox!.y).toBeLessThanOrEqual(10);

    // Scroll down the page
    await page.evaluate(() => window.scrollTo(0, 500));
    await page.waitForTimeout(300);

    // Verify navbar is still at top (fixed position)
    const afterScrollBox = await navbar.boundingBox();
    expect(afterScrollBox).not.toBeNull();
    expect(afterScrollBox!.y).toBeLessThanOrEqual(10);
  });

  test('Code blocks render with syntax highlighting', async ({ page }) => {
    // Navigate to getting started section
    await page.click('.nav-links a[href="#getting-started"]');
    await page.waitForTimeout(500);

    // Verify code blocks are present
    const codeBlocks = page.locator('pre code');
    const codeBlocksCount = await codeBlocks.count();
    expect(codeBlocksCount).toBeGreaterThan(0);

    // Verify Prism.js classes are applied for syntax highlighting
    const firstCodeBlock = codeBlocks.first();
    const classAttr = await firstCodeBlock.getAttribute('class');
    expect(classAttr).toContain('language-');
  });
});
