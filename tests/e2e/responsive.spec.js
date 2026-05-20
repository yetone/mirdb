/**
 * Responsive Design E2E Tests
 * Owner: Scenario 8 - Responsive Design
 *
 * Tests:
 * - Full page render at desktop (1280px)
 * - Full page render at tablet (900px)
 * - Full page render at mobile (375px)
 * - No horizontal scrolling at key breakpoints
 * - Layout adaptations per breakpoint
 * - Hamburger menu visibility on mobile
 * - Touch target sizes on mobile
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = path.join(__dirname, '../../index.html');
const fileUrl = 'file://' + indexPath;

const BREAKPOINTS = {
  mobile: { width: 375, height: 812, name: 'mobile' },
  tablet: { width: 900, height: 1024, name: 'tablet' },
  desktop: { width: 1280, height: 800, name: 'desktop' },
  wide: { width: 1920, height: 1080, name: 'wide' },
  small: { width: 320, height: 568, name: 'small' },
};

test.describe('Responsive Design - Desktop Viewport (1280px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(BREAKPOINTS.desktop);
    await page.goto(fileUrl);
  });

  test('full desktop layout renders correctly', async ({ page }) => {
    const hero = page.locator('section#hero');
    const features = page.locator('section#features');
    const overview = page.locator('section#overview');
    const quickstart = page.locator('section#quickstart');
    const roadmap = page.locator('section#roadmap');
    const footer = page.locator('footer');

    await expect(hero).toBeVisible();
    await expect(features).toBeVisible();
    await expect(overview).toBeVisible();
    await expect(quickstart).toBeVisible();
    await expect(roadmap).toBeVisible();
    await expect(footer).toBeVisible();
  });

  test('hero section has large typography on desktop', async ({ page }) => {
    const tagline = page.locator('section#hero h1.hero-tagline');
    await expect(tagline).toBeVisible();

    const fontSize = await tagline.evaluate((el) => {
      return window.getComputedStyle(el).fontSize;
    });
    const fontSizeRem = parseFloat(fontSize) / 16;
    expect(fontSizeRem).toBeGreaterThanOrEqual(2.5);
  });

  test('horizontal navigation is visible on desktop', async ({ page }) => {
    const desktopNav = page.locator('nav.desktop-nav');
    await expect(desktopNav).toBeVisible();
  });

  test('hamburger menu is hidden on desktop', async ({ page }) => {
    const hamburger = page.locator('.hamburger');
    const isVisible = await hamburger.isVisible();
    expect(isVisible).toBe(false);
  });

  test('hero CTA buttons are horizontal on desktop', async ({ page }) => {
    const ctaGroup = page.locator('.hero-cta-group');
    const flexDirection = await ctaGroup.evaluate((el) => {
      return window.getComputedStyle(el).flexDirection;
    });
    expect(flexDirection).toBe('row');
  });

  test('feature grid has 4 columns on desktop', async ({ page }) => {
    const featuresGrid = page.locator('.features-grid');
    const gridTemplateColumns = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });
    const columns = gridTemplateColumns.split(' ').length;
    expect(columns).toBe(4);
  });

  test('footer content is horizontal on desktop', async ({ page }) => {
    const footerContent = page.locator('.footer-content');
    const flexDirection = await footerContent.evaluate((el) => {
      return window.getComputedStyle(el).flexDirection;
    });
    expect(flexDirection).toBe('row');
  });
});

test.describe('Responsive Design - Tablet Viewport (900px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(BREAKPOINTS.tablet);
    await page.goto(fileUrl);
  });

  test('tablet layout renders correctly', async ({ page }) => {
    const hero = page.locator('section#hero');
    const features = page.locator('section#features');
    const overview = page.locator('section#overview');

    await expect(hero).toBeVisible();
    await expect(features).toBeVisible();
    await expect(overview).toBeVisible();
  });

  test('horizontal navigation is visible on tablet', async ({ page }) => {
    const desktopNav = page.locator('nav.desktop-nav');
    await expect(desktopNav).toBeVisible();
  });

  test('hamburger menu is hidden on tablet', async ({ page }) => {
    const hamburger = page.locator('.hamburger');
    const isVisible = await hamburger.isVisible();
    expect(isVisible).toBe(false);
  });

  test('hero tagline has medium font size on tablet', async ({ page }) => {
    const tagline = page.locator('section#hero h1.hero-tagline');
    const fontSize = await tagline.evaluate((el) => {
      return window.getComputedStyle(el).fontSize;
    });
    const fontSizeRem = parseFloat(fontSize) / 16;
    expect(fontSizeRem).toBeGreaterThanOrEqual(1.8);
    expect(fontSizeRem).toBeLessThanOrEqual(3);
  });

  test('hero CTA buttons are horizontal on tablet', async ({ page }) => {
    const ctaGroup = page.locator('.hero-cta-group');
    const flexDirection = await ctaGroup.evaluate((el) => {
      return window.getComputedStyle(el).flexDirection;
    });
    expect(flexDirection).toBe('row');
  });

  test('feature grid has 2 columns on tablet', async ({ page }) => {
    const featuresGrid = page.locator('.features-grid');
    const gridTemplateColumns = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });
    const columns = gridTemplateColumns.split(' ').length;
    expect(columns).toBe(2);
  });

  test('footer content is horizontal on tablet', async ({ page }) => {
    const footerContent = page.locator('.footer-content');
    const flexDirection = await footerContent.evaluate((el) => {
      return window.getComputedStyle(el).flexDirection;
    });
    expect(flexDirection).toBe('row');
  });
});

test.describe('Responsive Design - Mobile Viewport (375px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(BREAKPOINTS.mobile);
    await page.goto(fileUrl);
  });

  test('mobile layout renders correctly', async ({ page }) => {
    const hero = page.locator('section#hero');
    const features = page.locator('section#features');
    const overview = page.locator('section#overview');

    await expect(hero).toBeVisible();
    await expect(features).toBeVisible();
    await expect(overview).toBeVisible();
  });

  test('hamburger menu is visible on mobile', async ({ page }) => {
    const hamburger = page.locator('.hamburger');
    await expect(hamburger).toBeVisible();
  });

  test('horizontal navigation is hidden on mobile', async ({ page }) => {
    const desktopNav = page.locator('nav.desktop-nav');
    const isVisible = await desktopNav.isVisible();
    expect(isVisible).toBe(false);
  });

  test('hero tagline has smaller font size on mobile', async ({ page }) => {
    const tagline = page.locator('section#hero h1.hero-tagline');
    const fontSize = await tagline.evaluate((el) => {
      return window.getComputedStyle(el).fontSize;
    });
    const fontSizeRem = parseFloat(fontSize) / 16;
    expect(fontSizeRem).toBeLessThanOrEqual(2.5);
  });

  test('hero CTA buttons stack vertically on mobile', async ({ page }) => {
    const ctaGroup = page.locator('.hero-cta-group');
    const flexDirection = await ctaGroup.evaluate((el) => {
      return window.getComputedStyle(el).flexDirection;
    });
    expect(flexDirection).toBe('column');
  });

  test('feature grid has 1 column on mobile', async ({ page }) => {
    const featuresGrid = page.locator('.features-grid');
    const gridTemplateColumns = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });
    const columns = gridTemplateColumns.split(' ').length;
    expect(columns).toBe(1);
  });

  test('roadmap lists stack to 1 column on mobile', async ({ page }) => {
    const roadmapLists = page.locator('.roadmap-lists');
    const gridTemplateColumns = await roadmapLists.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });
    const columns = gridTemplateColumns.split(' ').length;
    expect(columns).toBe(1);
  });

  test('footer content stacks vertically on mobile', async ({ page }) => {
    const footerContent = page.locator('.footer-content');
    const flexDirection = await footerContent.evaluate((el) => {
      return window.getComputedStyle(el).flexDirection;
    });
    expect(flexDirection).toBe('column');
  });

  test('mobile nav menu can be opened via hamburger', async ({ page }) => {
    const hamburger = page.locator('.hamburger');
    const mobileNav = page.locator('#mobile-nav');

    await hamburger.click();
    await expect(mobileNav).toHaveClass(/open/);

    const ariaExpanded = await hamburger.getAttribute('aria-expanded');
    expect(ariaExpanded).toBe('true');
  });

  test('container padding is reduced on mobile', async ({ page }) => {
    const container = page.locator('.container').first();
    const paddingLeft = await container.evaluate((el) => {
      return window.getComputedStyle(el).paddingLeft;
    });
    const paddingLeftPx = parseFloat(paddingLeft);
    expect(paddingLeftPx).toBeLessThanOrEqual(16);
  });
});

test.describe('Responsive Design - No Horizontal Scrolling', () => {
  test('no horizontal overflow at 320px width', async ({ page }) => {
    await page.setViewportSize(BREAKPOINTS.small);
    await page.goto(fileUrl);

    const hasHorizontalOverflow = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalOverflow).toBe(false);
  });

  test('no horizontal overflow at 375px width', async ({ page }) => {
    await page.setViewportSize(BREAKPOINTS.mobile);
    await page.goto(fileUrl);

    const hasHorizontalOverflow = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalOverflow).toBe(false);
  });

  test('no horizontal overflow at 768px width', async ({ page }) => {
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto(fileUrl);

    const hasHorizontalOverflow = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalOverflow).toBe(false);
  });

  test('no horizontal overflow at 1024px width', async ({ page }) => {
    await page.setViewportSize({ width: 1024, height: 768 });
    await page.goto(fileUrl);

    const hasHorizontalOverflow = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalOverflow).toBe(false);
  });

  test('no horizontal overflow at 1920px width', async ({ page }) => {
    await page.setViewportSize(BREAKPOINTS.wide);
    await page.goto(fileUrl);

    const hasHorizontalOverflow = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalOverflow).toBe(false);
  });
});

test.describe('Responsive Design - Touch Target Sizes', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(BREAKPOINTS.mobile);
    await page.goto(fileUrl);
  });

  test('hamburger button has minimum 44x44px touch target', async ({ page }) => {
    const hamburger = page.locator('.hamburger');
    const box = await hamburger.boundingBox();
    expect(box.width).toBeGreaterThanOrEqual(44);
    expect(box.height).toBeGreaterThanOrEqual(44);
  });

  test('CTA buttons have minimum 44px height on mobile', async ({ page }) => {
    const ctaButtons = page.locator('.hero-cta');
    const count = await ctaButtons.count();
    for (let i = 0; i < count; i++) {
      const box = await ctaButtons.nth(i).boundingBox();
      expect(box.height).toBeGreaterThanOrEqual(44);
      expect(box.width).toBeGreaterThanOrEqual(44);
    }
  });

  test('copy button has minimum 44x44px touch target', async ({ page }) => {
    const copyButton = page.locator('.copy-button');
    const box = await copyButton.boundingBox();
    expect(box.width).toBeGreaterThanOrEqual(44);
    expect(box.height).toBeGreaterThanOrEqual(44);
  });

  test('feature cards have adequate padding for touch', async ({ page }) => {
    const cards = page.locator('.feature-card');
    const count = await cards.count();
    for (let i = 0; i < count; i++) {
      const box = await cards.nth(i).boundingBox();
      expect(box.width).toBeGreaterThanOrEqual(44);
      expect(box.height).toBeGreaterThanOrEqual(44);
    }
  });

  test('roadmap items have adequate touch target size', async ({ page }) => {
    const items = page.locator('.roadmap-item');
    const count = await items.count();
    for (let i = 0; i < count; i++) {
      const box = await items.nth(i).boundingBox();
      expect(box.height).toBeGreaterThanOrEqual(44);
    }
  });
});

test.describe('Responsive Design - Wide Screens', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(BREAKPOINTS.wide);
    await page.goto(fileUrl);
  });

  test('page renders correctly at 1920px width', async ({ page }) => {
    const hero = page.locator('section#hero');
    const features = page.locator('section#features');

    await expect(hero).toBeVisible();
    await expect(features).toBeVisible();
  });

  test('container has max-width constraint on wide screens', async ({ page }) => {
    const container = page.locator('.container').first();
    const maxWidth = await container.evaluate((el) => {
      return window.getComputedStyle(el).maxWidth;
    });
    expect(maxWidth).not.toBe('none');
  });
});
