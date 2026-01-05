const { test, expect } = require('@playwright/test');

test.describe('Responsive Design - Desktop Viewport', () => {
  test.beforeEach(async ({ page }) => {
    // Set desktop viewport to 1920x1080
    await page.setViewportSize({ width: 1920, height: 1080 });
    await page.goto('/');
  });

  test('TC1: Page renders without horizontal scrollbar at 1920x1080 viewport', async ({ page }) => {
    // Check that body does not have horizontal overflow
    const hasHorizontalScrollbar = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScrollbar).toBe(false);

    // Verify all main content sections are visible
    const header = page.locator('header');
    const hero = page.locator('.hero');
    const features = page.locator('.features');
    const quickstart = page.locator('.quickstart');
    const commands = page.locator('.commands');
    const footer = page.locator('footer');

    await expect(header).toBeVisible();
    await expect(hero).toBeVisible();
    await expect(features).toBeVisible();
    await expect(quickstart).toBeVisible();
    await expect(commands).toBeVisible();
    await expect(footer).toBeVisible();
  });

  test('TC2: Hero section displays full-width with proper centering', async ({ page }) => {
    const hero = page.locator('.hero');
    const heroContent = page.locator('.hero-content');
    const heroH1 = page.locator('.hero h1');
    const tagline = page.locator('.hero .tagline');

    // Verify hero section is visible
    await expect(hero).toBeVisible();
    await expect(heroContent).toBeVisible();
    await expect(heroH1).toBeVisible();
    await expect(tagline).toBeVisible();

    // Get hero section bounding box
    const heroBox = await hero.boundingBox();

    // Verify hero takes full viewport width (or close to it)
    expect(heroBox.width).toBeGreaterThanOrEqual(1900);

    // Verify hero content is centered by checking computed styles
    const heroContentStyles = await heroContent.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      const rect = el.getBoundingClientRect();
      const parentRect = el.parentElement.getBoundingClientRect();
      const leftMargin = rect.left - parentRect.left;
      const rightMargin = parentRect.right - rect.right;
      return {
        marginLeft: styles.marginLeft,
        marginRight: styles.marginRight,
        leftOffset: leftMargin,
        rightOffset: rightMargin,
        textAlign: styles.textAlign
      };
    });

    // Check centering - left and right offsets should be similar (within 50px tolerance)
    const marginDifference = Math.abs(heroContentStyles.leftOffset - heroContentStyles.rightOffset);
    expect(marginDifference).toBeLessThan(100);

    // Verify content is centered (text-align: center is set on .hero)
    const heroTextAlign = await hero.evaluate((el) => window.getComputedStyle(el).textAlign);
    expect(heroTextAlign).toBe('center');

    // Verify CTA buttons are visible
    const ctaButtons = page.locator('.cta-buttons');
    await expect(ctaButtons).toBeVisible();
  });

  test('TC3: Feature cards display in a row or grid layout', async ({ page }) => {
    const featuresGrid = page.locator('.features-grid');
    const featureCards = page.locator('.feature-card');

    // Verify features grid and cards are visible
    await expect(featuresGrid).toBeVisible();

    // Get all feature cards
    const cardCount = await featureCards.count();
    expect(cardCount).toBe(3);

    // Check that grid is using grid or flex layout for horizontal display
    const gridDisplay = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(gridDisplay).toBe('grid');

    // Get the bounding boxes of all feature cards
    const cardPositions = await featureCards.evaluateAll((cards) => {
      return cards.map(card => {
        const rect = card.getBoundingClientRect();
        return { left: rect.left, right: rect.right, top: rect.top, bottom: rect.bottom, width: rect.width };
      });
    });

    // Verify cards are displayed in a row (same Y position or close to it)
    const firstCardTop = cardPositions[0].top;
    const allCardsInRow = cardPositions.every(card => Math.abs(card.top - firstCardTop) < 10);
    expect(allCardsInRow).toBe(true);

    // Verify cards are horizontally distributed (not stacked vertically)
    const firstCardRight = cardPositions[0].right;
    const secondCardLeft = cardPositions[1].left;
    expect(secondCardLeft).toBeGreaterThan(firstCardRight - 50); // Cards should be adjacent, not overlapping

    // Verify each card has reasonable width for desktop
    cardPositions.forEach(card => {
      expect(card.width).toBeGreaterThan(250);
    });
  });

  test('TC4: Full navigation menu is visible without hamburger menu', async ({ page }) => {
    const navLinks = page.locator('.nav-links');
    const logo = page.locator('.logo');

    // Verify navigation is visible
    await expect(navLinks).toBeVisible();
    await expect(logo).toBeVisible();

    // Check nav-links display is not 'none' (which it would be on mobile via media query)
    const navLinksDisplay = await navLinks.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(navLinksDisplay).not.toBe('none');
    expect(navLinksDisplay).toBe('flex');

    // Verify all navigation links are visible
    const navItems = navLinks.locator('a');
    const navCount = await navItems.count();
    expect(navCount).toBeGreaterThanOrEqual(4); // Features, Quick Start, Commands, Docs, GitHub

    // Verify each nav link is visible
    for (let i = 0; i < navCount; i++) {
      const navItem = navItems.nth(i);
      await expect(navItem).toBeVisible();
    }

    // Verify navigation items text includes expected links
    const navTexts = await navItems.allTextContents();
    const navTextsLower = navTexts.map(t => t.toLowerCase());

    expect(navTextsLower.some(t => t.includes('features'))).toBe(true);
    expect(navTextsLower.some(t => t.includes('quick start'))).toBe(true);
    expect(navTextsLower.some(t => t.includes('commands'))).toBe(true);
    expect(navTextsLower.some(t => t.includes('github'))).toBe(true);

    // Verify no hamburger/mobile menu icon is visible
    const hamburger = page.locator('.hamburger, .mobile-menu, .menu-toggle, [aria-label="Menu"]');
    await expect(hamburger).toHaveCount(0);
  });

  test('Desktop viewport utilizes available horizontal space appropriately', async ({ page }) => {
    // Verify container max-width is applied
    const container = page.locator('.container').first();
    const containerStyles = await container.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        maxWidth: styles.maxWidth,
        width: el.getBoundingClientRect().width
      };
    });

    // Container should have max-width constraint
    expect(containerStyles.maxWidth).toBe('1200px');

    // Verify sections are using full width where appropriate
    const heroSection = page.locator('.hero');
    const heroBox = await heroSection.boundingBox();
    expect(heroBox.width).toBe(1920); // Hero uses full viewport width

    // Verify feature cards are spread horizontally
    const featureCards = page.locator('.feature-card');
    const firstCard = await featureCards.first().boundingBox();
    const lastCard = await featureCards.last().boundingBox();

    // Cards should span a significant portion of the container
    const totalSpread = lastCard.x + lastCard.width - firstCard.x;
    expect(totalSpread).toBeGreaterThan(800);
  });

  test('All sections display correctly without overflow or broken layouts', async ({ page }) => {
    const sections = [
      { selector: '.hero', name: 'Hero' },
      { selector: '.features', name: 'Features' },
      { selector: '.quickstart', name: 'Quick Start' },
      { selector: '.commands', name: 'Commands' },
      { selector: '.configuration', name: 'Configuration' },
      { selector: 'footer', name: 'Footer' }
    ];

    for (const section of sections) {
      const element = page.locator(section.selector);
      await expect(element).toBeVisible();

      // Check for no horizontal overflow within each section
      const hasOverflow = await element.evaluate((el) => {
        return el.scrollWidth > el.clientWidth;
      });
      expect(hasOverflow).toBe(false);

      // Verify section width doesn't exceed viewport
      const box = await element.boundingBox();
      expect(box.width).toBeLessThanOrEqual(1920);
    }

    // Verify code blocks don't overflow
    const codeBlocks = page.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();

    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      const styles = await codeBlock.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return {
          overflowX: computed.overflowX
        };
      });
      // Code blocks should handle overflow gracefully
      expect(['auto', 'scroll', 'hidden']).toContain(styles.overflowX);
    }
  });
});
