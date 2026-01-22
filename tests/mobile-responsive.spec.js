// @ts-check
const { test, expect } = require('@playwright/test');

// Mobile viewport configuration (iPhone SE)
const MOBILE_VIEWPORT = { width: 375, height: 667 };

test.describe('Responsive Design - Mobile', () => {
  test.use({ viewport: MOBILE_VIEWPORT });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: All content fits within viewport without horizontal scrolling at 375px width', async ({ page }) => {
    // Wait for page to fully load
    await page.waitForLoadState('domcontentloaded');

    // Check that the document body width does not exceed viewport width
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = MOBILE_VIEWPORT.width;

    // The body width should not exceed the viewport width (no horizontal scrolling)
    expect(bodyWidth).toBeLessThanOrEqual(viewportWidth);

    // Additionally check the html element
    const htmlWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    expect(htmlWidth).toBeLessThanOrEqual(viewportWidth);

    // Verify there's no horizontal overflow on the page
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });

  test('TC2: Hero section stacks vertically with readable text and accessible CTAs on mobile', async ({ page }) => {
    // Verify hero section is visible
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Check hero content container
    const heroContent = page.locator('.hero-content');
    await expect(heroContent).toBeVisible();

    // Verify logo is visible
    const heroLogo = page.locator('.hero-logo');
    await expect(heroLogo).toBeVisible();

    // Verify title is visible and readable
    const heroTitle = page.locator('.hero h1');
    await expect(heroTitle).toBeVisible();

    // Check title font size is appropriate for mobile (at least 1.5rem = 24px)
    const titleFontSize = await heroTitle.evaluate(el =>
      parseFloat(window.getComputedStyle(el).fontSize)
    );
    expect(titleFontSize).toBeGreaterThanOrEqual(24);

    // Verify tagline is visible
    const heroTagline = page.locator('.hero-tagline');
    await expect(heroTagline).toBeVisible();

    // Verify subtitle is visible
    const heroSubtitle = page.locator('.hero-subtitle');
    await expect(heroSubtitle).toBeVisible();

    // Verify CTAs are visible and properly styled
    const heroCtas = page.locator('.hero-ctas');
    await expect(heroCtas).toBeVisible();

    // Check that CTAs stack vertically (flex-direction: column)
    const ctasFlexDirection = await heroCtas.evaluate(el =>
      window.getComputedStyle(el).flexDirection
    );
    expect(ctasFlexDirection).toBe('column');

    // Verify Get Started button is visible and clickable
    const getStartedBtn = page.locator('.hero-ctas .btn-primary');
    await expect(getStartedBtn).toBeVisible();

    // Verify GitHub button is visible and clickable
    const githubBtn = page.locator('.hero-ctas .btn-secondary');
    await expect(githubBtn).toBeVisible();
  });

  test('TC3: Feature cards stack vertically in single column layout on mobile', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Check the features grid
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Verify grid has single column layout on mobile
    const gridColumns = await featuresGrid.evaluate(el =>
      window.getComputedStyle(el).gridTemplateColumns
    );

    // On mobile, grid should have a single column (the computed value will be a single pixel value)
    // The value should be equivalent to a single column (no multiple column values)
    const columnCount = gridColumns.split(' ').filter(v => v.trim() !== '').length;
    expect(columnCount).toBe(1);

    // Verify all feature cards are visible
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBe(6); // 6 feature cards

    // Check each card is visible and fits within viewport
    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      await card.scrollIntoViewIfNeeded();
      await expect(card).toBeVisible();

      // Verify card width doesn't exceed viewport
      const cardBox = await card.boundingBox();
      expect(cardBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
    }
  });

  test('TC4: All buttons and links have minimum 44x44px touch target', async ({ page }) => {
    // WCAG 2.1 AAA recommends 44x44px minimum touch target
    const MIN_TOUCH_TARGET = 44;

    // Test hero CTA buttons
    const heroPrimaryBtn = page.locator('.hero-ctas .btn-primary');
    await expect(heroPrimaryBtn).toBeVisible();
    const primaryBtnBox = await heroPrimaryBtn.boundingBox();
    expect(primaryBtnBox.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
    expect(primaryBtnBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);

    const heroSecondaryBtn = page.locator('.hero-ctas .btn-secondary');
    await expect(heroSecondaryBtn).toBeVisible();
    const secondaryBtnBox = await heroSecondaryBtn.boundingBox();
    expect(secondaryBtnBox.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
    expect(secondaryBtnBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);

    // Test navigation links
    const navLinks = page.locator('.nav-links a');
    const navLinkCount = await navLinks.count();

    for (let i = 0; i < navLinkCount; i++) {
      const link = navLinks.nth(i);
      await link.scrollIntoViewIfNeeded();
      const linkBox = await link.boundingBox();

      // For inline links, we check the effective touch target area
      // Links should have enough padding or size for easy tapping
      expect(linkBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
    }

    // Test copy buttons in quick start section
    const quickStartSection = page.locator('#quick-start');
    await quickStartSection.scrollIntoViewIfNeeded();

    const copyButtons = page.locator('.copy-btn');
    const copyBtnCount = await copyButtons.count();

    for (let i = 0; i < copyBtnCount; i++) {
      const btn = copyButtons.nth(i);
      await btn.scrollIntoViewIfNeeded();
      const btnBox = await btn.boundingBox();
      expect(btnBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
    }

    // Test footer links
    const footerLinks = page.locator('.footer-links a');
    const footerLinkCount = await footerLinks.count();

    for (let i = 0; i < footerLinkCount; i++) {
      const link = footerLinks.nth(i);
      await link.scrollIntoViewIfNeeded();
      const linkBox = await link.boundingBox();
      expect(linkBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
    }
  });

  test('TC5: Code blocks are scrollable horizontally if needed and text is readable on mobile', async ({ page }) => {
    // Navigate to quick start section
    const quickStartSection = page.locator('#quick-start');
    await quickStartSection.scrollIntoViewIfNeeded();
    await expect(quickStartSection).toBeVisible();

    // Check code blocks
    const codeBlocks = page.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      await codeBlock.scrollIntoViewIfNeeded();
      await expect(codeBlock).toBeVisible();

      // Check that code block doesn't exceed viewport width
      const codeBlockBox = await codeBlock.boundingBox();
      expect(codeBlockBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);

      // Check the pre element has overflow-x: auto for horizontal scrolling
      const preElement = codeBlock.locator('pre');
      const overflowX = await preElement.evaluate(el =>
        window.getComputedStyle(el).overflowX
      );
      expect(['auto', 'scroll']).toContain(overflowX);

      // Check code text is readable (font-size should be at least 12px)
      const codeElement = codeBlock.locator('code');
      const codeFontSize = await codeElement.evaluate(el =>
        parseFloat(window.getComputedStyle(el).fontSize)
      );
      expect(codeFontSize).toBeGreaterThanOrEqual(12);

      // Verify the code content is present
      const codeText = await codeElement.textContent();
      expect(codeText.length).toBeGreaterThan(0);
    }
  });

  test('Navigation is accessible on mobile', async ({ page }) => {
    // Check navigation is visible
    const nav = page.locator('.nav');
    await expect(nav).toBeVisible();

    // On mobile, navigation should stack vertically (flex-direction: column)
    const navFlexDirection = await nav.evaluate(el =>
      window.getComputedStyle(el).flexDirection
    );
    expect(navFlexDirection).toBe('column');

    // Check nav links are accessible
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();

    // Nav links should wrap on mobile
    const navLinksFlexWrap = await navLinks.evaluate(el =>
      window.getComputedStyle(el).flexWrap
    );
    expect(navLinksFlexWrap).toBe('wrap');
  });

  test('Footer content displays correctly on mobile', async ({ page }) => {
    // Scroll to footer
    const footer = page.locator('.footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Check footer content stacks vertically on mobile
    const footerContent = page.locator('.footer-content');
    const flexDirection = await footerContent.evaluate(el =>
      window.getComputedStyle(el).flexDirection
    );
    expect(flexDirection).toBe('column');

    // Verify footer author text is visible
    const footerAuthor = page.locator('.footer-author');
    await expect(footerAuthor).toBeVisible();

    // Verify footer links are visible
    const footerLinks = page.locator('.footer-links');
    await expect(footerLinks).toBeVisible();
  });

  test('Roadmap section displays correctly on mobile', async ({ page }) => {
    // Navigate to roadmap section
    const roadmapSection = page.locator('#roadmap');
    await roadmapSection.scrollIntoViewIfNeeded();
    await expect(roadmapSection).toBeVisible();

    // Check roadmap content grid adapts to mobile
    const roadmapContent = page.locator('.roadmap-content');
    await expect(roadmapContent).toBeVisible();

    // Verify completed and planned sections are visible
    const completedSection = page.locator('.roadmap-list.completed');
    await expect(completedSection).toBeVisible();

    const plannedSection = page.locator('.roadmap-list.planned');
    await expect(plannedSection).toBeVisible();
  });

  test('Demo section image is responsive on mobile', async ({ page }) => {
    // Navigate to demo section
    const demoSection = page.locator('#demo');
    await demoSection.scrollIntoViewIfNeeded();
    await expect(demoSection).toBeVisible();

    // Check demo gif
    const demoGif = page.locator('.demo-gif');
    await expect(demoGif).toBeVisible();

    // Verify the image width doesn't exceed container
    const demoContent = page.locator('.demo-content');
    const demoContentBox = await demoContent.boundingBox();
    const demoGifBox = await demoGif.boundingBox();

    expect(demoGifBox.width).toBeLessThanOrEqual(demoContentBox.width);
    expect(demoGifBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
  });
});
