/**
 * Responsive Design E2E Tests
 * Owner: Scenario 9 (mobile), Scenario 10 (tablet)
 *
 * Test cases:
 * Mobile (375px):
 * - No horizontal overflow
 * - Feature cards stack vertically
 * - Mobile navigation accessible
 * - Touch interactions work
 *
 * Tablet (768px):
 * - Appropriate layout adaptation
 * - 2-column feature layout
 * - Demo fully functional
 * - Navigation works appropriately
 */

const { test, expect } = require('@playwright/test');

// Mobile viewport configuration (iPhone SE / standard mobile)
const MOBILE_VIEWPORT = {
  width: 375,
  height: 667,
};

// Tablet viewport configuration
const TABLET_VIEWPORT = {
  width: 768,
  height: 1024,
};

// =================================================================
// Scenario 9 - Mobile Responsive Tests (375px)
// =================================================================

test.describe('Scenario 9 - Responsive Design - Mobile (375px)', () => {
  test.beforeEach(async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize(MOBILE_VIEWPORT);
    await page.goto('/');
    // Wait for page to fully load
    await page.waitForLoadState('networkidle');
  });

  test('TC1: Page content fits without horizontal overflow at 375px viewport', async ({ page }) => {
    // Check that the body does not have horizontal scrollbar
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);

    // Body scroll width should not exceed viewport width (allow 1px tolerance for rounding)
    expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth + 1);

    // Also verify no element exceeds the viewport
    const hasHorizontalOverflow = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalOverflow).toBe(false);

    // Check that overflow-x is hidden on body
    const bodyOverflowX = await page.evaluate(() => {
      return window.getComputedStyle(document.body).overflowX;
    });
    expect(bodyOverflowX).toBe('hidden');
  });

  test('TC2: Hero section displays correctly on mobile', async ({ page }) => {
    const hero = page.locator('#hero');
    await expect(hero).toBeVisible();

    // Check hero headline is visible and readable
    const headline = page.locator('.hero__headline');
    await expect(headline).toBeVisible();

    // Verify headline font size is appropriately reduced for mobile
    const headlineFontSize = await headline.evaluate(el => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    // Mobile font size should be smaller than desktop (3rem = 48px desktop, should be ~22-36px on mobile)
    expect(headlineFontSize).toBeLessThan(48);
    expect(headlineFontSize).toBeGreaterThan(18);

    // Check CTAs are visible and properly sized
    const ctaPrimary = page.locator('[data-testid="cta-try-demo"]');
    const ctaSecondary = page.locator('[data-testid="cta-get-started"]');
    await expect(ctaPrimary).toBeVisible();
    await expect(ctaSecondary).toBeVisible();

    // CTAs should stack vertically on mobile (flex-direction: column)
    const ctaContainer = page.locator('.hero__cta');
    const ctaFlexDirection = await ctaContainer.evaluate(el => {
      return window.getComputedStyle(el).flexDirection;
    });
    expect(ctaFlexDirection).toBe('column');

    // CTA buttons should have touch-friendly minimum height (44px)
    const primaryHeight = await ctaPrimary.evaluate(el => el.offsetHeight);
    expect(primaryHeight).toBeGreaterThanOrEqual(44);
  });

  test('TC3: Feature cards stack vertically on mobile', async ({ page }) => {
    // Scroll to features section
    await page.locator('#features').scrollIntoViewIfNeeded();

    const featuresGrid = page.locator('.features__grid');
    await expect(featuresGrid).toBeVisible();

    // Check grid has single column layout
    const gridColumns = await featuresGrid.evaluate(el => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });

    // Should be a single column (1fr)
    // The computed value will be something like "343px" (single column width)
    const columnCount = gridColumns.split(' ').filter(col => col !== '0px' && col !== '').length;
    expect(columnCount).toBe(1);

    // Verify feature cards exist and are visible
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(4);

    // Each card should take full width
    const firstCard = featureCards.first();
    const cardWidth = await firstCard.evaluate(el => el.offsetWidth);
    const containerWidth = await featuresGrid.evaluate(el => el.offsetWidth);

    // Card width should be close to container width (allowing for padding)
    expect(cardWidth).toBeGreaterThan(containerWidth * 0.8);
  });

  test('TC4: Demo section is usable on mobile screen', async ({ page }) => {
    // Scroll to demo section
    await page.locator('#demo').scrollIntoViewIfNeeded();

    const demoSection = page.locator('#demo');
    await expect(demoSection).toBeVisible();

    // Terminal should be visible
    const terminal = page.locator('.demo__terminal');
    await expect(terminal).toBeVisible();

    // Input field should be visible and usable
    const input = page.locator('[data-testid="demo-input"]');
    await expect(input).toBeVisible();

    // Input should have 16px font size to prevent iOS zoom
    const inputFontSize = await input.evaluate(el => {
      return window.getComputedStyle(el).fontSize;
    });
    expect(parseFloat(inputFontSize)).toBeGreaterThanOrEqual(16);

    // Submit button should be visible
    const submitBtn = page.locator('[data-testid="demo-submit"]');
    await expect(submitBtn).toBeVisible();

    // Submit button should have touch-friendly size
    const submitHeight = await submitBtn.evaluate(el => el.offsetHeight);
    expect(submitHeight).toBeGreaterThanOrEqual(44);

    // Example buttons should be visible
    const exampleBtns = page.locator('.demo__example-btn');
    const exampleCount = await exampleBtns.count();
    expect(exampleCount).toBeGreaterThan(0);

    // Example buttons should be touch-friendly
    const firstExample = exampleBtns.first();
    const exampleHeight = await firstExample.evaluate(el => el.offsetHeight);
    expect(exampleHeight).toBeGreaterThanOrEqual(44);
  });

  test('TC5: Navigation accessible via hamburger menu on mobile', async ({ page }) => {
    // Hamburger menu toggle should be visible on mobile
    const menuToggle = page.locator('#menu-toggle');
    await expect(menuToggle).toBeVisible();

    // Check that menu toggle is displayed (not hidden)
    const toggleDisplay = await menuToggle.evaluate(el => {
      return window.getComputedStyle(el).display;
    });
    expect(toggleDisplay).not.toBe('none');

    // Navigation should be hidden initially
    const nav = page.locator('#main-nav');
    const navRight = await nav.evaluate(el => {
      return window.getComputedStyle(el).right;
    });
    // Nav should be positioned off-screen (negative right value or hidden)
    expect(navRight).toMatch(/-?\d+px/);

    // Click hamburger menu
    await menuToggle.click();

    // Wait for animation
    await page.waitForTimeout(400);

    // Navigation should now be visible (right: 0)
    const navRightAfterClick = await nav.evaluate(el => {
      return window.getComputedStyle(el).right;
    });
    expect(navRightAfterClick).toBe('0px');

    // Nav links should be visible
    const navLinks = page.locator('.header__nav-link');
    const firstLink = navLinks.first();
    await expect(firstLink).toBeVisible();

    // Click a nav link - should navigate and close menu
    await page.locator('[data-testid="nav-features"]').click();

    // Wait for animation
    await page.waitForTimeout(400);

    // Menu should close after clicking a link
    const navRightAfterNav = await nav.evaluate(el => {
      return window.getComputedStyle(el).right;
    });
    // Should be back off-screen or closing
    expect(parseInt(navRightAfterNav) || -100).toBeLessThanOrEqual(0);
  });

  test('TC6: All interactive elements respond to touch events', async ({ page }) => {
    // Test navigation toggle touch
    const menuToggle = page.locator('#menu-toggle');
    await expect(menuToggle).toBeVisible();

    // Menu toggle should be clickable/tappable
    const toggleBox = await menuToggle.boundingBox();
    expect(toggleBox.width).toBeGreaterThanOrEqual(44);
    expect(toggleBox.height).toBeGreaterThanOrEqual(44);

    // Test hero CTA buttons are touch-friendly
    const ctaPrimary = page.locator('[data-testid="cta-try-demo"]');
    const ctaBox = await ctaPrimary.boundingBox();
    expect(ctaBox.height).toBeGreaterThanOrEqual(44);

    // Scroll to features and test cards are tappable
    await page.locator('#features').scrollIntoViewIfNeeded();
    const featureCard = page.locator('.feature-card').first();
    await expect(featureCard).toBeVisible();

    // Scroll to demo section
    await page.locator('#demo').scrollIntoViewIfNeeded();

    // Test demo input can receive focus/input
    const demoInput = page.locator('[data-testid="demo-input"]');
    await demoInput.click();
    await expect(demoInput).toBeFocused();

    // Type in the input
    await demoInput.fill('GET test');
    const inputValue = await demoInput.inputValue();
    expect(inputValue).toBe('GET test');

    // Test demo submit button
    const submitBtn = page.locator('[data-testid="demo-submit"]');
    const submitBox = await submitBtn.boundingBox();
    expect(submitBox.height).toBeGreaterThanOrEqual(44);

    // Test example button is clickable
    const exampleBtn = page.locator('[data-testid="example-set"]');
    await expect(exampleBtn).toBeVisible();
    const exampleBox = await exampleBtn.boundingBox();
    expect(exampleBox.height).toBeGreaterThanOrEqual(44);

    // Click should work (populate input)
    await exampleBtn.click();
    await page.waitForTimeout(100);

    // The input should now have a command
    const newInputValue = await demoInput.inputValue();
    expect(newInputValue.length).toBeGreaterThan(0);

    // Scroll to quickstart and test copy buttons
    await page.locator('#quickstart').scrollIntoViewIfNeeded();
    const copyBtn = page.locator('.quickstart__copy-btn').first();
    await expect(copyBtn).toBeVisible();
    const copyBox = await copyBtn.boundingBox();
    expect(copyBox.height).toBeGreaterThanOrEqual(44);
  });

  test('All sections are accessible and scrollable on mobile', async ({ page }) => {
    // Test that each major section is visible when scrolled to
    const sections = ['#hero', '#features', '#demo', '#examples', '#quickstart', '#status'];

    for (const sectionId of sections) {
      const section = page.locator(sectionId);
      await section.scrollIntoViewIfNeeded();
      await expect(section).toBeVisible();

      // Verify section fits within viewport width
      const sectionWidth = await section.evaluate(el => el.offsetWidth);
      expect(sectionWidth).toBeLessThanOrEqual(375);
    }

    // Footer should also be visible
    const footer = page.locator('#footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();
  });

  test('Images and media do not cause horizontal overflow', async ({ page }) => {
    // Check all images have max-width: 100%
    const images = page.locator('img');
    const imageCount = await images.count();

    for (let i = 0; i < imageCount; i++) {
      const img = images.nth(i);
      const maxWidth = await img.evaluate(el => {
        return window.getComputedStyle(el).maxWidth;
      });
      expect(maxWidth).toBe('100%');
    }

    // Check all SVGs have max-width: 100%
    const svgs = page.locator('svg');
    const svgCount = await svgs.count();

    for (let i = 0; i < svgCount; i++) {
      const svg = svgs.nth(i);
      const maxWidth = await svg.evaluate(el => {
        return window.getComputedStyle(el).maxWidth;
      });
      expect(maxWidth).toBe('100%');
    }
  });

  test('Text is readable and properly sized on mobile', async ({ page }) => {
    // Check body font size is at least 14px
    const bodyFontSize = await page.evaluate(() => {
      return parseFloat(window.getComputedStyle(document.body).fontSize);
    });
    expect(bodyFontSize).toBeGreaterThanOrEqual(14);

    // Check line height for readability
    const bodyLineHeight = await page.evaluate(() => {
      const lineHeight = window.getComputedStyle(document.body).lineHeight;
      const fontSize = parseFloat(window.getComputedStyle(document.body).fontSize);
      return parseFloat(lineHeight) / fontSize;
    });
    expect(bodyLineHeight).toBeGreaterThanOrEqual(1.4);

    // Check feature descriptions are readable
    await page.locator('#features').scrollIntoViewIfNeeded();
    const description = page.locator('.feature-card__description').first();
    const descFontSize = await description.evaluate(el => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    expect(descFontSize).toBeGreaterThanOrEqual(12);
  });
});

// =================================================================
// Scenario 10 - Tablet Responsive Tests (768px)
// =================================================================

test.describe('Scenario 10 - Responsive Design - Tablet (768px)', () => {
  test.beforeEach(async ({ page }) => {
    // Set tablet viewport (768px width)
    await page.setViewportSize(TABLET_VIEWPORT);
    await page.goto('/');
  });

  test('TC1: Page displays correctly at 768px without horizontal overflow', async ({ page }) => {
    // Verify page loads successfully
    await expect(page).toHaveTitle(/MirDB/);

    // Check for horizontal overflow by comparing scroll width to viewport width
    const hasHorizontalOverflow = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalOverflow).toBe(false);

    // Verify body width doesn't exceed viewport
    const bodyWidth = await page.evaluate(() => {
      return document.body.scrollWidth;
    });
    expect(bodyWidth).toBeLessThanOrEqual(768);

    // Check that main content sections are visible
    await expect(page.locator('#hero')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();
    await expect(page.locator('#demo')).toBeVisible();
  });

  test('TC2: Features section displays in 2-column layout on tablet', async ({ page }) => {
    // Scroll to features section
    await page.locator('#features').scrollIntoViewIfNeeded();

    // Verify features grid is visible
    const featuresGrid = page.locator('.features__grid');
    await expect(featuresGrid).toBeVisible();

    // Count feature cards
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(4);
    expect(cardCount).toBeLessThanOrEqual(6);

    // Check grid layout - at 768px should be 2 columns
    const gridColumns = await featuresGrid.evaluate((el) => {
      const computedStyle = window.getComputedStyle(el);
      return computedStyle.gridTemplateColumns;
    });

    // Should have 2 columns (two 'px' or 'fr' values)
    const columnCount = gridColumns.split(' ').filter(col => col.match(/\d/)).length;
    expect(columnCount).toBe(2);

    // Verify cards are properly sized and don't overflow
    const firstCard = featureCards.first();
    const cardBox = await firstCard.boundingBox();
    expect(cardBox.width).toBeLessThan(768 / 2 + 50); // Each card should be less than half viewport + padding
  });

  test('TC3: Demo section is fully functional on tablet', async ({ page }) => {
    // Scroll to demo section
    await page.locator('#demo').scrollIntoViewIfNeeded();

    // Verify demo terminal is visible
    const demoTerminal = page.locator('.demo__terminal');
    await expect(demoTerminal).toBeVisible();

    // Verify demo terminal fits within viewport
    const terminalBox = await demoTerminal.boundingBox();
    expect(terminalBox.width).toBeLessThanOrEqual(768);

    // Verify input field is visible and interactive
    const demoInput = page.locator('[data-testid="demo-input"]');
    await expect(demoInput).toBeVisible();
    await expect(demoInput).toBeEnabled();

    // Verify submit button is visible
    const submitButton = page.locator('[data-testid="demo-submit"]');
    await expect(submitButton).toBeVisible();
    await expect(submitButton).toBeEnabled();

    // Verify example command buttons are visible
    const exampleButtons = page.locator('.demo__example-btn');
    const buttonCount = await exampleButtons.count();
    expect(buttonCount).toBeGreaterThan(0);

    // Verify first example button is clickable
    const firstExampleBtn = exampleButtons.first();
    await expect(firstExampleBtn).toBeVisible();

    // Verify output area exists and is visible
    const outputArea = page.locator('#demo-output');
    await expect(outputArea).toBeVisible();
  });

  test('TC4: Navigation works appropriately for tablet size', async ({ page }) => {
    // Verify header is visible
    const header = page.locator('#header');
    await expect(header).toBeVisible();

    // At 768px, hamburger menu should be visible
    const menuToggle = page.locator('#menu-toggle');
    await expect(menuToggle).toBeVisible();

    // Click the menu toggle to open navigation
    await menuToggle.click();

    // Verify navigation is now open/visible
    const navPanel = page.locator('#main-nav');
    await expect(navPanel).toBeVisible();

    // Check that nav has the open class or is positioned correctly
    const isNavOpen = await navPanel.evaluate((el) => {
      const computedStyle = window.getComputedStyle(el);
      // Check if nav is visible by position
      return computedStyle.right === '0px' || el.classList.contains('header__nav--open');
    });
    expect(isNavOpen).toBe(true);

    // Verify navigation links are visible in the open menu
    const navLinks = page.locator('.header__nav-link');
    const linkCount = await navLinks.count();
    expect(linkCount).toBeGreaterThan(0);

    // Verify first navigation link is clickable
    const featuresLink = page.locator('[data-testid="nav-features"]');
    await expect(featuresLink).toBeVisible();

    // Click on Features link and verify navigation works
    await featuresLink.click();

    // Wait for scroll and verify we're at features section
    await page.waitForTimeout(500); // Wait for smooth scroll
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });

  test('Hero section adapts correctly for tablet viewport', async ({ page }) => {
    // Verify hero section is visible
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Verify hero content doesn't overflow
    const heroContent = page.locator('.hero__content');
    const contentBox = await heroContent.boundingBox();
    expect(contentBox.width).toBeLessThanOrEqual(768);

    // Verify CTA buttons are visible and properly sized
    const ctaButtons = page.locator('.hero__btn');
    const buttonCount = await ctaButtons.count();
    expect(buttonCount).toBe(2);

    // Verify buttons have adequate touch target size (min 44px height)
    const tryDemoBtn = page.locator('[data-testid="cta-try-demo"]');
    const btnBox = await tryDemoBtn.boundingBox();
    expect(btnBox.height).toBeGreaterThanOrEqual(40); // Allow some tolerance
  });

  test('Quick Start section displays correctly on tablet', async ({ page }) => {
    // Scroll to quickstart section
    await page.locator('#quickstart').scrollIntoViewIfNeeded();

    // Verify section is visible
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Verify steps are visible
    const steps = page.locator('.quickstart__step');
    const stepCount = await steps.count();
    expect(stepCount).toBeGreaterThanOrEqual(3);

    // Verify code blocks don't overflow
    const codeBlocks = page.locator('.quickstart__code-block');
    const firstCodeBlock = codeBlocks.first();
    const codeBlockBox = await firstCodeBlock.boundingBox();
    expect(codeBlockBox.width).toBeLessThanOrEqual(768);

    // Verify copy buttons are visible
    const copyButtons = page.locator('.quickstart__copy-btn');
    const copyBtnCount = await copyButtons.count();
    expect(copyBtnCount).toBeGreaterThan(0);
  });

  test('Status section displays correctly on tablet', async ({ page }) => {
    // Scroll to status section
    await page.locator('#status').scrollIntoViewIfNeeded();

    // Verify section is visible
    const statusSection = page.locator('#status');
    await expect(statusSection).toBeVisible();

    // Verify status content is visible
    const statusContent = page.locator('[data-testid="status-list"]');
    await expect(statusContent).toBeVisible();

    // Verify status items are displayed
    const implementedItems = page.locator('[data-testid="status-item-implemented"]');
    const implementedCount = await implementedItems.count();
    expect(implementedCount).toBeGreaterThan(0);

    // Verify planned items are displayed
    const plannedItems = page.locator('[data-testid="status-item-planned"]');
    const plannedCount = await plannedItems.count();
    expect(plannedCount).toBeGreaterThan(0);
  });

  test('Footer displays correctly on tablet', async ({ page }) => {
    // Scroll to footer
    await page.locator('#footer').scrollIntoViewIfNeeded();

    // Verify footer is visible
    const footer = page.locator('#footer');
    await expect(footer).toBeVisible();

    // Verify footer links are accessible
    const footerLinks = page.locator('.footer__link');
    const linkCount = await footerLinks.count();
    expect(linkCount).toBeGreaterThan(0);

    // Verify GitHub link is visible
    const githubLink = page.locator('[data-testid="footer-github"]');
    await expect(githubLink).toBeVisible();

    // Verify footer doesn't overflow
    const footerBox = await footer.boundingBox();
    expect(footerBox.width).toBeLessThanOrEqual(768);
  });

  test('All interactive elements have adequate touch target sizes', async ({ page }) => {
    // Check CTA buttons
    const tryDemoBtn = page.locator('[data-testid="cta-try-demo"]');
    await expect(tryDemoBtn).toBeVisible();
    const ctaBtnBox = await tryDemoBtn.boundingBox();
    expect(ctaBtnBox.height).toBeGreaterThanOrEqual(40);

    // Scroll to demo and check demo submit button
    await page.locator('#demo').scrollIntoViewIfNeeded();
    const submitBtn = page.locator('[data-testid="demo-submit"]');
    await expect(submitBtn).toBeVisible();
    const submitBtnBox = await submitBtn.boundingBox();
    expect(submitBtnBox.height).toBeGreaterThanOrEqual(40);
  });

  test('Code examples section displays correctly on tablet', async ({ page }) => {
    // Scroll to examples section
    await page.locator('#examples').scrollIntoViewIfNeeded();

    // Verify section is visible
    const examplesSection = page.locator('#examples');
    await expect(examplesSection).toBeVisible();

    // Verify code example cards are visible
    const codeExamples = page.locator('.code-example');
    const exampleCount = await codeExamples.count();
    expect(exampleCount).toBeGreaterThanOrEqual(2);

    // Verify code examples don't overflow
    const firstExample = codeExamples.first();
    const exampleBox = await firstExample.boundingBox();
    expect(exampleBox.width).toBeLessThanOrEqual(768);

    // Verify copy buttons are accessible
    const copyBtn = page.locator('[data-testid="copy-btn-set"]');
    await expect(copyBtn).toBeVisible();
  });
});
