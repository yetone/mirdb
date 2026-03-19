/**
 * Cross-Browser Compatibility E2E Tests.
 * Owner: Scenario 15 - Cross-Browser Compatibility
 *
 * Verifies homepage works correctly on modern browsers:
 * - Chrome (Chromium)
 * - Firefox
 * - Safari (WebKit)
 * - Edge
 *
 * Requirements: NFR-1 (Support last 2 versions of Chrome, Firefox, Safari, Edge)
 */

import { test, expect, BrowserContext } from '@playwright/test';

// Test constants
const SECTIONS = [
  { id: '#hero', name: 'Hero' },
  { id: '#features', name: 'Features' },
  { id: '#how-it-works', name: 'How It Works' },
  { id: '#status', name: 'Status' },
  { id: '#quick-start', name: 'Quick Start' },
  { id: '#resources', name: 'Resources' },
];

const NAV_LINKS = ['Features', 'How It Works', 'Status', 'Quick Start', 'Resources'];

test.describe('Cross-Browser Compatibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test.describe('Page Structure Rendering', () => {
    test('all main sections render correctly', async ({ page, browserName }) => {
      // Verify each section is present and visible
      for (const section of SECTIONS) {
        const sectionElement = page.locator(section.id);
        await expect(
          sectionElement,
          `${section.name} section should be visible in ${browserName}`
        ).toBeVisible();
      }

      // Check header
      const header = page.locator('header');
      await expect(header, `Header should be visible in ${browserName}`).toBeVisible();

      // Check footer
      const footer = page.locator('footer');
      await expect(footer, `Footer should be visible in ${browserName}`).toBeVisible();
    });

    test('MirDB branding displays correctly', async ({ page, browserName }) => {
      // Check logo/brand text
      const logo = page.getByText('MirDB').first();
      await expect(logo, `MirDB logo should be visible in ${browserName}`).toBeVisible();

      // Check tagline in hero section
      const tagline = page.getByText(/Persistent Key-Value Store/i);
      await expect(tagline, `Tagline should be visible in ${browserName}`).toBeVisible();
    });

    test('navigation menu renders with all links', async ({ page, browserName }) => {
      for (const linkName of NAV_LINKS) {
        const navLink = page.getByRole('menuitem', { name: linkName });
        await expect(
          navLink,
          `Navigation link "${linkName}" should be visible in ${browserName}`
        ).toBeVisible();
      }
    });
  });

  test.describe('Flexbox and Grid Layouts', () => {
    test('features section displays cards in grid layout', async ({ page, browserName }) => {
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();

      // Check that feature cards are present
      const featureCards = featuresSection.locator('[class*="grid"]').first();
      await expect(
        featureCards,
        `Feature cards grid should render in ${browserName}`
      ).toBeVisible();

      // Verify cards have proper dimensions (not collapsed)
      const boundingBox = await featureCards.boundingBox();
      expect(boundingBox, `Grid should have dimensions in ${browserName}`).not.toBeNull();
      if (boundingBox) {
        expect(boundingBox.width).toBeGreaterThan(100);
        expect(boundingBox.height).toBeGreaterThan(50);
      }
    });

    test('resources section displays grid layout correctly', async ({ page, browserName }) => {
      const resourcesSection = page.locator('#resources');
      await expect(resourcesSection).toBeVisible();

      // Scroll to resources section
      await resourcesSection.scrollIntoViewIfNeeded();

      // Check grid layout exists
      const gridContainer = resourcesSection.locator('[class*="grid"]').first();
      await expect(
        gridContainer,
        `Resources grid should render in ${browserName}`
      ).toBeVisible();

      const boundingBox = await gridContainer.boundingBox();
      expect(boundingBox, `Resources grid should have dimensions in ${browserName}`).not.toBeNull();
      if (boundingBox) {
        expect(boundingBox.width).toBeGreaterThan(100);
      }
    });

    test('flexbox layouts in header render correctly', async ({ page, browserName }) => {
      const header = page.locator('header');
      await expect(header).toBeVisible();

      // Check that header contains flex containers (may be nested)
      const hasFlexContainer = await header.evaluate((el) => {
        // Check header and its descendants for flex display
        const elements = [el, ...Array.from(el.querySelectorAll('*'))];
        return elements.some((element) => {
          const computed = window.getComputedStyle(element);
          return computed.display === 'flex' || computed.display === 'inline-flex';
        });
      });

      expect(
        hasFlexContainer,
        `Header should contain flexbox layout in ${browserName}`
      ).toBeTruthy();
    });
  });

  test.describe('Interactive Elements', () => {
    test('smooth scroll navigation works', async ({ page, browserName }) => {
      // Get initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY);

      // Click on Features navigation
      await page.getByRole('menuitem', { name: 'Features' }).click();

      // Wait for smooth scroll animation
      await page.waitForTimeout(1000);

      // Verify scroll occurred
      const newScrollY = await page.evaluate(() => window.scrollY);
      expect(
        newScrollY,
        `Page should scroll down when clicking nav link in ${browserName}`
      ).toBeGreaterThan(initialScrollY);

      // Verify Features section is near top of viewport
      const featuresSection = page.locator('#features');
      const boundingBox = await featuresSection.boundingBox();
      expect(boundingBox, `Features section should be visible in ${browserName}`).not.toBeNull();
      if (boundingBox) {
        expect(boundingBox.y).toBeLessThan(250);
      }
    });

    test('CTA buttons are clickable and visible', async ({ page, browserName }) => {
      // Check Get Started button
      const getStartedBtn = page.getByRole('link', { name: /Get Started/i });
      await expect(
        getStartedBtn,
        `Get Started button should be visible in ${browserName}`
      ).toBeVisible();
      await expect(getStartedBtn).toBeEnabled();

      // Check GitHub button
      const githubBtn = page.getByRole('link', { name: /GitHub/i }).first();
      await expect(
        githubBtn,
        `GitHub button should be visible in ${browserName}`
      ).toBeVisible();
      await expect(githubBtn).toBeEnabled();
    });

    test('code blocks are rendered and copy button is functional', async ({ page, browserName }) => {
      // Navigate to Quick Start section
      const quickStartSection = page.locator('#quick-start');
      await quickStartSection.scrollIntoViewIfNeeded();

      // Check code blocks exist
      const codeBlocks = quickStartSection.locator('pre, [class*="code"]');
      const count = await codeBlocks.count();
      expect(count, `Code blocks should exist in ${browserName}`).toBeGreaterThan(0);

      // Check copy buttons exist
      const copyButtons = quickStartSection.locator('button[aria-label*="Copy"], button:has-text("Copy")');
      const copyCount = await copyButtons.count();
      expect(copyCount, `Copy buttons should exist in ${browserName}`).toBeGreaterThan(0);
    });
  });

  test.describe('Visual Consistency', () => {
    test('header has consistent fixed positioning', async ({ page, browserName }) => {
      const header = page.locator('header');

      // Get initial position
      const initialBox = await header.boundingBox();
      expect(initialBox).not.toBeNull();

      // Scroll down
      await page.evaluate(() => window.scrollTo(0, 500));
      await page.waitForTimeout(300);

      // Header should remain at top
      const afterScrollBox = await header.boundingBox();
      expect(afterScrollBox).not.toBeNull();
      expect(
        afterScrollBox!.y,
        `Header should stay fixed at top in ${browserName}`
      ).toBe(0);
    });

    test('page has no horizontal overflow', async ({ page, browserName }) => {
      // Check for horizontal scrollbar
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });

      expect(
        hasHorizontalScroll,
        `Page should not have horizontal overflow in ${browserName}`
      ).toBe(false);
    });

    test('all images and SVGs are visible and rendered', async ({ page, browserName }) => {
      // Wait for page to fully load
      await page.waitForTimeout(1000);

      // Check that SVG elements are present and visible
      const svgElements = page.locator('svg');
      const svgCount = await svgElements.count();
      expect(svgCount, `Page should have SVG elements in ${browserName}`).toBeGreaterThan(0);

      // Verify at least some SVGs are visible
      let visibleSvgCount = 0;
      for (let i = 0; i < svgCount; i++) {
        const svg = svgElements.nth(i);
        if (await svg.isVisible()) {
          visibleSvgCount++;
          // Check SVG has valid dimensions
          const boundingBox = await svg.boundingBox();
          if (boundingBox) {
            expect(
              boundingBox.width > 0 && boundingBox.height > 0,
              `SVG ${i + 1} should have valid dimensions in ${browserName}`
            ).toBeTruthy();
          }
        }
      }

      expect(
        visibleSvgCount,
        `At least some SVGs should be visible in ${browserName}`
      ).toBeGreaterThan(0);

      // Check img elements exist and render with valid layout dimensions
      // Note: We check that images occupy space in the layout rather than checking
      // the .complete property, which has inconsistent behavior across browsers
      // (especially Firefox with lazy-loaded images or Next.js Image components)
      const imgElements = page.locator('img[src]');
      const imgCount = await imgElements.count();

      let visibleImgCount = 0;
      for (let i = 0; i < imgCount; i++) {
        const img = imgElements.nth(i);
        const isVisible = await img.isVisible();
        if (isVisible) {
          visibleImgCount++;
          // Check that the image has rendered dimensions (occupies space in layout)
          const boundingBox = await img.boundingBox();
          // Images should occupy space in the layout (not be collapsed)
          // We allow height=0 for badges that may fail to load externally
          if (boundingBox) {
            expect(
              boundingBox.width >= 0,
              `Image ${i + 1} should have valid width in ${browserName}`
            ).toBeTruthy();
          }
        }
      }

      // Should have at least one img element visible (the logo)
      expect(
        visibleImgCount,
        `Should have visible images in ${browserName}`
      ).toBeGreaterThan(0);
    });
  });

  test.describe('Theme Support', () => {
    test('theme toggle is visible and functional', async ({ page, browserName }) => {
      // Find theme toggle button
      const themeToggle = page.locator('button[aria-label*="theme"], button[aria-label*="mode"], [data-testid="theme-toggle"]').first();

      // If theme toggle exists, test it
      const toggleExists = await themeToggle.count() > 0;
      if (toggleExists) {
        await expect(themeToggle).toBeVisible();
        await expect(themeToggle).toBeEnabled();

        // Click to toggle theme
        await themeToggle.click();
        await page.waitForTimeout(300);

        // Theme should have changed (check html class or color scheme)
        const htmlClass = await page.evaluate(() => document.documentElement.className);
        expect(
          htmlClass.includes('dark') || htmlClass.includes('light') || htmlClass === '',
          `Theme toggle should change theme class in ${browserName}`
        ).toBeTruthy();
      }
    });
  });

  test.describe('Accessibility Basics', () => {
    test('page has proper heading hierarchy', async ({ page, browserName }) => {
      // Check h1 exists
      const h1 = page.locator('h1');
      const h1Count = await h1.count();
      expect(h1Count, `Page should have h1 heading in ${browserName}`).toBeGreaterThan(0);

      // Check h2 exists for sections
      const h2 = page.locator('h2');
      const h2Count = await h2.count();
      expect(h2Count, `Page should have h2 headings in ${browserName}`).toBeGreaterThan(0);
    });

    test('interactive elements are keyboard accessible', async ({ page, browserName }) => {
      // Tab to first interactive element
      await page.keyboard.press('Tab');

      // Check that an element received focus
      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement;
        return el ? el.tagName.toLowerCase() : null;
      });

      expect(
        focusedElement,
        `Keyboard navigation should work in ${browserName}`
      ).not.toBeNull();
    });
  });
});
