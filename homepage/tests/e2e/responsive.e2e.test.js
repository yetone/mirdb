/**
 * Responsive Design E2E Tests for MirDB Homepage
 * Owner: Scenario 10 - Responsive Design
 *
 * Tests the homepage renders correctly across all required viewports
 * from 320px mobile to 2560px desktop.
 */

const { chromium } = require('playwright');
const path = require('path');

// Helper: count number of grid columns from computed style
function countGridColumns(gridTemplateColumns) {
  if (!gridTemplateColumns || gridTemplateColumns === 'none') return 0;
  return gridTemplateColumns.split(/\s+/).filter(s => s && s !== '0px').length;
}

describe('Responsive Design - E2E Tests', () => {
  let browser;
  let page;
  const htmlPath = 'file://' + path.join(__dirname, '..', '..', 'public', 'index.html');

  const VIEWPORTS = {
    mobileSE: { width: 320, height: 568 },
    mobile8: { width: 375, height: 667 },
    tablet: { width: 768, height: 1024 },
    smallDesktop: { width: 1024, height: 768 },
    standardDesktop: { width: 1920, height: 1080 },
    ultrawide: { width: 2560, height: 1440 },
  };

  beforeAll(async () => {
    browser = await chromium.launch({ headless: true });
  });

  afterAll(async () => {
    if (browser) await browser.close();
  });

  beforeEach(async () => {
    page = await browser.newPage();
    await page.goto(htmlPath);
  });

  afterEach(async () => {
    if (page) await page.close();
  });

  // ========================================
  // Test 1: iPhone SE (320px)
  // ========================================
  describe('Test 1: Mobile viewport 320px (iPhone SE)', () => {
    beforeEach(async () => {
      await page.setViewportSize(VIEWPORTS.mobileSE);
      await page.reload();
    });

    it('features grid shows single column', async () => {
      const gridComputed = await page.evaluate(() => {
        const el = document.querySelector('.features-grid');
        return el ? window.getComputedStyle(el).gridTemplateColumns : '';
      });
      expect(countGridColumns(gridComputed)).toBe(1);
    });

    it('hamburger menu is visible', async () => {
      const toggle = await page.locator('.mobile-menu-toggle');
      const box = await toggle.boundingBox();
      expect(box).not.toBeNull();
      expect(box.width).toBeGreaterThan(0);
      expect(box.height).toBeGreaterThan(0);
    });

    it('navigation links are hidden initially', async () => {
      const display = await page.evaluate(() => {
        const el = document.querySelector('.nav-links');
        return el ? window.getComputedStyle(el).display : '';
      });
      expect(display).toBe('none');
    });

    it('no horizontal scroll', async () => {
      const overflow = await page.evaluate(() => {
        return document.documentElement.scrollWidth > window.innerWidth;
      });
      expect(overflow).toBe(false);
    });

    it('tap targets are at least 44x44px', async () => {
      const tapTargets = await page.locator('button, a, .btn').all();
      const failures = [];
      for (const target of tapTargets) {
        const box = await target.boundingBox();
        if (box && box.width > 0 && box.height > 0) {
          if (box.width < 44 || box.height < 44) {
            failures.push(`${box.width}x${box.height}`);
          }
        }
      }
      expect(failures).toHaveLength(0);
    });
  });

  // ========================================
  // Test 2: iPhone 8 (375px)
  // ========================================
  describe('Test 2: Mobile viewport 375px (iPhone 8)', () => {
    beforeEach(async () => {
      await page.setViewportSize(VIEWPORTS.mobile8);
      await page.reload();
    });

    it('base font is readable at 16px', async () => {
      const fontSize = await page.evaluate(() => {
        return window.getComputedStyle(document.body).fontSize;
      });
      expect(fontSize).toBe('16px');
    });

    it('hero section fits within viewport height', async () => {
      const hero = await page.locator('.hero');
      const box = await hero.boundingBox();
      expect(box).not.toBeNull();
      expect(box.height).toBeLessThanOrEqual(VIEWPORTS.mobile8.height + 100);
    });

    it('CTA button is full-width or prominent', async () => {
      const cta = await page.locator('.hero-actions .btn-primary');
      const box = await cta.boundingBox();
      expect(box).not.toBeNull();
      expect(box.width).toBeGreaterThanOrEqual(200);
    });
  });

  // ========================================
  // Test 3: iPad (768px)
  // ========================================
  describe('Test 3: Tablet viewport 768px (iPad)', () => {
    beforeEach(async () => {
      await page.setViewportSize(VIEWPORTS.tablet);
      await page.reload();
    });

    it('features grid shows 2 columns', async () => {
      const gridComputed = await page.evaluate(() => {
        const el = document.querySelector('.features-grid');
        return el ? window.getComputedStyle(el).gridTemplateColumns : '';
      });
      expect(countGridColumns(gridComputed)).toBe(2);
    });

    it('navigation shows text links, not hamburger', async () => {
      const toggle = await page.locator('.mobile-menu-toggle');
      const box = await toggle.boundingBox();
      // On tablet, hamburger should be hidden (null, width/height 0, or not in layout)
      const isHidden = !box || box.width === 0 || box.height === 0;
      expect(isHidden).toBe(true);

      const display = await page.evaluate(() => {
        const el = document.querySelector('.nav-links');
        return el ? window.getComputedStyle(el).display : '';
      });
      expect(display).toBe('flex');
    });

    it('footer shows 3 columns', async () => {
      const gridComputed = await page.evaluate(() => {
        const el = document.querySelector('.footer-grid');
        return el ? window.getComputedStyle(el).gridTemplateColumns : '';
      });
      expect(countGridColumns(gridComputed)).toBe(3);
    });
  });

  // ========================================
  // Test 4: Small Desktop (1024px)
  // ========================================
  describe('Test 4: Desktop viewport 1024px', () => {
    beforeEach(async () => {
      await page.setViewportSize(VIEWPORTS.smallDesktop);
      await page.reload();
    });

    it('features grid shows 4 columns', async () => {
      const gridComputed = await page.evaluate(() => {
        const el = document.querySelector('.features-grid');
        return el ? window.getComputedStyle(el).gridTemplateColumns : '';
      });
      expect(countGridColumns(gridComputed)).toBe(4);
    });

    it('full horizontal navigation is visible', async () => {
      const toggle = await page.locator('.mobile-menu-toggle');
      const box = await toggle.boundingBox();
      const isHidden = !box || box.width === 0 || box.height === 0;
      expect(isHidden).toBe(true);

      const display = await page.evaluate(() => {
        const el = document.querySelector('.nav-links');
        return el ? window.getComputedStyle(el).display : '';
      });
      expect(display).toBe('flex');

      const menuItems = await page.locator('.nav-links a').all();
      expect(menuItems.length).toBeGreaterThanOrEqual(4);
    });
  });

  // ========================================
  // Test 5: Standard Desktop (1920px)
  // ========================================
  describe('Test 5: Standard Desktop viewport 1920px', () => {
    beforeEach(async () => {
      await page.setViewportSize(VIEWPORTS.standardDesktop);
      await page.reload();
    });

    it('content is centered with comfortable max-width', async () => {
      const container = await page.locator('.container').first();
      const box = await container.boundingBox();
      const viewportWidth = VIEWPORTS.standardDesktop.width;

      expect(box.x).toBeGreaterThan(0);
      expect(box.x + box.width).toBeLessThan(viewportWidth);
      expect(box.width).toBeLessThanOrEqual(1200);
    });

    it('all sections are well-proportioned', async () => {
      const hero = await page.locator('.hero');
      const features = await page.locator('.features');
      const heroBox = await hero.boundingBox();
      const featuresBox = await features.boundingBox();

      expect(heroBox.width).toBeGreaterThan(0);
      expect(featuresBox.width).toBeGreaterThan(0);
    });
  });

  // ========================================
  // Test 6: Ultrawide (2560px)
  // ========================================
  describe('Test 6: Ultrawide viewport 2560px', () => {
    beforeEach(async () => {
      await page.setViewportSize(VIEWPORTS.ultrawide);
      await page.reload();
    });

    it('content is constrained to max-width container and centered', async () => {
      const container = await page.locator('.container').first();
      const box = await container.boundingBox();
      const viewportWidth = VIEWPORTS.ultrawide.width;

      expect(box.width).toBeLessThan(viewportWidth - 100);

      const leftMargin = box.x;
      const rightMargin = viewportWidth - box.x - box.width;
      const marginDiff = Math.abs(leftMargin - rightMargin);
      expect(marginDiff).toBeLessThanOrEqual(50);
    });

    it('hero content is centered', async () => {
      const heroContent = await page.locator('.hero-content');
      const box = await heroContent.boundingBox();
      const viewportWidth = VIEWPORTS.ultrawide.width;

      const leftMargin = box.x;
      const rightMargin = viewportWidth - box.x - box.width;
      const marginDiff = Math.abs(leftMargin - rightMargin);
      expect(marginDiff).toBeLessThanOrEqual(100);
    });
  });

  // ========================================
  // Test 7: Orientation Changes
  // ========================================
  describe('Test 7: Orientation changes', () => {
    it('layout adapts correctly between portrait and landscape on mobile', async () => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.reload();

      const portraitBody = await page.locator('body').boundingBox();
      expect(portraitBody.width).toBe(375);

      await page.setViewportSize({ width: 667, height: 375 });
      await page.waitForTimeout(500);

      const landscapeBody = await page.locator('body').boundingBox();
      expect(landscapeBody.width).toBe(667);

      const scrollWidth = await page.evaluate(() => document.body.scrollWidth);
      expect(scrollWidth).toBeLessThanOrEqual(667 + 1);

      const gridComputed = await page.evaluate(() => {
        const el = document.querySelector('.features-grid');
        return el ? window.getComputedStyle(el).gridTemplateColumns : '';
      });
      expect(countGridColumns(gridComputed)).toBe(2);
    });

    it('no content overflow or clipping on orientation change', async () => {
      await page.setViewportSize({ width: 320, height: 568 });
      await page.reload();

      const portraitOverflow = await page.evaluate(() => {
        return document.documentElement.scrollWidth > window.innerWidth;
      });
      expect(portraitOverflow).toBe(false);

      await page.setViewportSize({ width: 568, height: 320 });
      await page.waitForTimeout(500);

      const landscapeOverflow = await page.evaluate(() => {
        return document.documentElement.scrollWidth > window.innerWidth;
      });
      expect(landscapeOverflow).toBe(false);
    });
  });
});
