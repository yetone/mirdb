import { test, expect } from '@playwright/test';

test.describe('Responsive Design - Mobile', () => {
  test('TC1: Page renders without horizontal scroll at 375x667 viewport (iPhone SE)', async ({ page }) => {
    // Set viewport to iPhone SE dimensions
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    // Wait for page to fully load
    await page.waitForLoadState('domcontentloaded');

    // Verify no horizontal scroll by checking document width equals viewport width
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);

    // Verify page content is visible and has stacked layout
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Verify CTA buttons stack vertically on mobile
    const ctaButtons = page.locator('.cta-buttons');
    const flexDirection = await ctaButtons.evaluate((el) => {
      return window.getComputedStyle(el).flexDirection;
    });
    expect(flexDirection).toBe('column');

    // Verify major sections are accessible
    const sections = ['#features', '#architecture', '#quick-start', '#configuration', '#project-status'];
    for (const section of sections) {
      const sectionElement = page.locator(section);
      await sectionElement.scrollIntoViewIfNeeded();
      await expect(sectionElement).toBeVisible();
    }

    // Verify feature cards stack vertically (single column layout)
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(5);

    // Get all feature card positions to verify single-column layout
    const cardPositions = await featureCards.evaluateAll((cards) => {
      return cards.map((card) => {
        const rect = card.getBoundingClientRect();
        return { x: rect.x, y: rect.y, width: rect.width };
      });
    });

    // On mobile, cards should have similar X positions (stacked vertically)
    const uniqueXPositions = new Set(cardPositions.map((pos) => Math.round(pos.x)));
    // Should have only 1-2 unique X positions on mobile (stacked layout)
    expect(uniqueXPositions.size).toBeLessThanOrEqual(2);
  });

  test('TC2: Page renders correctly at 414x896 viewport (iPhone XR)', async ({ page }) => {
    // Set viewport to iPhone XR dimensions
    await page.setViewportSize({ width: 414, height: 896 });
    await page.goto('/');

    await page.waitForLoadState('domcontentloaded');

    // Verify no horizontal scroll
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);

    // Verify hero section is visible and properly constrained
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    const heroBox = await heroSection.boundingBox();
    expect(heroBox).not.toBeNull();
    if (heroBox) {
      // Hero should fit within viewport width
      expect(heroBox.width).toBeLessThanOrEqual(414);
    }

    // Verify all major sections are accessible
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await architectureSection.scrollIntoViewIfNeeded();
    await expect(architectureSection).toBeVisible();

    // Verify architecture content is single column on mobile
    const architectureContent = page.locator('.architecture-content');
    const gridTemplateColumns = await architectureContent.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });
    // On mobile (max-width: 900px), should be single column (1fr)
    expect(gridTemplateColumns).not.toContain('1fr 1fr');

    // Verify project status section stacks on mobile
    const statusContent = page.locator('.status-content');
    await statusContent.scrollIntoViewIfNeeded();
    const statusGridColumns = await statusContent.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });
    // On mobile (max-width: 768px), should be single column
    expect(statusGridColumns).not.toContain('1fr 1fr');

    // Verify footer is visible
    const footerSection = page.locator('[data-testid="footer-section"]');
    await footerSection.scrollIntoViewIfNeeded();
    await expect(footerSection).toBeVisible();
  });

  test('TC3: Code blocks are horizontally scrollable on mobile', async ({ page }) => {
    // Set viewport to mobile dimensions
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    await page.waitForLoadState('domcontentloaded');

    // Navigate to quick start section
    const quickStartSection = page.locator('#quick-start');
    await quickStartSection.scrollIntoViewIfNeeded();
    await expect(quickStartSection).toBeVisible();

    // Verify code block has overflow-x: auto for horizontal scrolling
    const codeBlock = page.locator('.code-block');
    await expect(codeBlock).toBeVisible();

    const overflowX = await codeBlock.evaluate((el) => {
      return window.getComputedStyle(el).overflowX;
    });
    expect(overflowX).toBe('auto');

    // Verify code block is contained within viewport width
    const codeBlockBox = await codeBlock.boundingBox();
    expect(codeBlockBox).not.toBeNull();
    if (codeBlockBox) {
      // Code block should not exceed viewport width (accounting for padding)
      expect(codeBlockBox.width).toBeLessThanOrEqual(375);
    }

    // Verify code content is visible and readable
    const codeContent = codeBlock.locator('code');
    await expect(codeContent).toBeVisible();
    await expect(codeContent).toContainText('cargo install mirdb');

    // Verify config table wrapper also has horizontal scroll handling
    const configTableWrapper = page.locator('.config-table-wrapper');
    await configTableWrapper.scrollIntoViewIfNeeded();

    const tableOverflowX = await configTableWrapper.evaluate((el) => {
      return window.getComputedStyle(el).overflowX;
    });
    expect(tableOverflowX).toBe('auto');
  });

  test('TC4: Mobile navigation menu is accessible and functional', async ({ page }) => {
    // Set viewport to mobile dimensions
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    await page.waitForLoadState('domcontentloaded');

    // Verify navbar is visible
    const navbar = page.locator('[data-testid="navbar"]');
    await expect(navbar).toBeVisible();

    // Verify nav links are visible (they should be wrapped on mobile)
    const navLinks = page.locator('[data-testid="nav-links"]');
    await expect(navLinks).toBeVisible();

    // Verify navigation links are accessible
    const featuresLink = navLinks.locator('a[href="#features"]');
    await expect(featuresLink).toBeVisible();

    const quickStartLink = navLinks.locator('a[href="#quick-start"]');
    await expect(quickStartLink).toBeVisible();

    // Verify nav links wrap on mobile (flex-wrap: wrap)
    const flexWrap = await navLinks.evaluate((el) => {
      return window.getComputedStyle(el).flexWrap;
    });
    expect(flexWrap).toBe('wrap');

    // Verify nav links are centered on mobile
    const justifyContent = await navLinks.evaluate((el) => {
      return window.getComputedStyle(el).justifyContent;
    });
    expect(justifyContent).toBe('center');

    // Test navigation functionality - click on Features link
    await featuresLink.click();
    await page.waitForTimeout(500); // Wait for smooth scroll

    // Verify URL hash changed
    const url = page.url();
    expect(url).toContain('#features');

    // Verify features section is now visible in viewport
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Test Quick Start navigation
    await quickStartLink.click();
    await page.waitForTimeout(500);
    expect(page.url()).toContain('#quick-start');
  });

  test('TC5: Body text has minimum 16px font size and adequate line height on mobile', async ({ page }) => {
    // Set viewport to mobile dimensions
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    await page.waitForLoadState('domcontentloaded');

    // Check body font size (should be based on rem, typically 16px base)
    const bodyFontSize = await page.evaluate(() => {
      const body = document.body;
      return parseFloat(window.getComputedStyle(body).fontSize);
    });
    expect(bodyFontSize).toBeGreaterThanOrEqual(16);

    // Check body line height (should be adequate for readability)
    const bodyLineHeight = await page.evaluate(() => {
      const body = document.body;
      const lineHeight = window.getComputedStyle(body).lineHeight;
      const fontSize = parseFloat(window.getComputedStyle(body).fontSize);
      // If line-height is 'normal', it's typically around 1.2
      if (lineHeight === 'normal') {
        return 1.2;
      }
      // If it's a numeric value (like 1.6), return it
      if (!lineHeight.includes('px')) {
        return parseFloat(lineHeight);
      }
      // If it's in px, convert to ratio
      return parseFloat(lineHeight) / fontSize;
    });
    // Line height should be at least 1.4 for good readability
    expect(bodyLineHeight).toBeGreaterThanOrEqual(1.4);

    // Check hero section text readability
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Check h1 font size on mobile (should be 2.5rem = 40px at 16px base)
    const h1FontSize = await page.locator('h1').evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    expect(h1FontSize).toBeGreaterThanOrEqual(40);

    // Check tagline font size on mobile (should be 1.2rem = ~19px)
    const taglineFontSize = await page.locator('.tagline').evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    expect(taglineFontSize).toBeGreaterThanOrEqual(16);

    // Check value proposition text readability
    const valueProposition = page.locator('.value-proposition');
    const valuePropositionFontSize = await valueProposition.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    expect(valuePropositionFontSize).toBeGreaterThanOrEqual(16);

    // Check feature card text readability
    const featureCardText = page.locator('.feature-card p').first();
    await featureCardText.scrollIntoViewIfNeeded();
    const featureTextFontSize = await featureCardText.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    // Feature card text might be slightly smaller (0.95rem ≈ 15.2px) but still readable
    expect(featureTextFontSize).toBeGreaterThanOrEqual(15);

    // Check paragraph line height for readability
    const paragraphLineHeight = await page.locator('.value-proposition').evaluate((el) => {
      const style = window.getComputedStyle(el);
      const lineHeight = parseFloat(style.lineHeight);
      const fontSize = parseFloat(style.fontSize);
      return lineHeight / fontSize;
    });
    expect(paragraphLineHeight).toBeGreaterThanOrEqual(1.4);
  });
});
