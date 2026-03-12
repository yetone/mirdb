/**
 * Responsive Design E2E Tests
 * Owner: Scenario 5 - Responsive Design
 *
 * Tests:
 * - Desktop (1920x1080): Full layout displays correctly
 * - Tablet (768x1024): Layout adapts appropriately
 * - Mobile (375x667): Content readable, no horizontal scroll
 * - Images scale appropriately at all viewports
 * - Navigation remains accessible on all devices
 *
 * Traceability: REQ-10, NFR-1
 */

import { test, expect } from '@playwright/test';
import { BASE_URL, VIEWPORTS, waitForPageLoad, getByTestId } from './test-utils';

test.describe('Responsive Design', () => {
  test.describe('Desktop Viewport (1920x1080)', () => {
    test.use({ viewport: VIEWPORTS.desktop });

    test('TC1: All content displays correctly with proper spacing and alignment', async ({ page }) => {
      await page.goto('/');
      await waitForPageLoad(page);

      // Verify hero section is visible and properly laid out
      const heroSection = page.locator(getByTestId('hero-section'));
      await expect(heroSection).toBeVisible();

      // Verify main title is visible
      const heroTitle = page.locator('h1');
      await expect(heroTitle).toBeVisible();
      await expect(heroTitle).toContainText('MirDB');

      // Verify all major sections are visible
      await expect(page.locator(getByTestId('about-section'))).toBeVisible();
      await expect(page.locator(getByTestId('features-section'))).toBeVisible();
      await expect(page.locator(getByTestId('quickstart-section'))).toBeVisible();
      await expect(page.locator(getByTestId('status-section'))).toBeVisible();
      await expect(page.locator(getByTestId('footer-section'))).toBeVisible();

      // Verify CTA buttons are side by side on desktop (flex row)
      const ctaContainer = page.locator('.hero-cta');
      const ctaWidth = await ctaContainer.evaluate(el => {
        const style = window.getComputedStyle(el);
        return style.flexDirection;
      });
      expect(ctaWidth).toBe('row');

      // No horizontal scrollbar
      const hasHorizontalScrollbar = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScrollbar).toBe(false);
    });
  });

  test.describe('Tablet Viewport (768x1024)', () => {
    test.use({ viewport: VIEWPORTS.tablet });

    test('TC2: Layout adapts to tablet size, all content remains accessible', async ({ page }) => {
      await page.goto('/');
      await waitForPageLoad(page);

      // Verify all sections are visible
      await expect(page.locator(getByTestId('hero-section'))).toBeVisible();
      await expect(page.locator(getByTestId('about-section'))).toBeVisible();
      await expect(page.locator(getByTestId('features-section'))).toBeVisible();
      await expect(page.locator(getByTestId('quickstart-section'))).toBeVisible();
      await expect(page.locator(getByTestId('footer-section'))).toBeVisible();

      // Verify navigation is accessible
      await expect(page.locator(getByTestId('docs-link'))).toBeVisible();
      await expect(page.locator(getByTestId('github-link'))).toBeVisible();

      // Verify CTA buttons are visible
      await expect(page.locator(getByTestId('get-started-btn'))).toBeVisible();
      await expect(page.locator(getByTestId('view-source-btn'))).toBeVisible();

      // No horizontal scrollbar
      const hasHorizontalScrollbar = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScrollbar).toBe(false);
    });
  });

  test.describe('Mobile Viewport (375x667)', () => {
    test.use({ viewport: VIEWPORTS.mobile });

    test('TC3: Content displays in single column, no horizontal scrollbar appears', async ({ page }) => {
      await page.goto('/');
      await waitForPageLoad(page);

      // Verify all sections are visible
      await expect(page.locator(getByTestId('hero-section'))).toBeVisible();
      await expect(page.locator(getByTestId('about-section'))).toBeVisible();
      await expect(page.locator(getByTestId('features-section'))).toBeVisible();
      await expect(page.locator(getByTestId('quickstart-section'))).toBeVisible();
      await expect(page.locator(getByTestId('footer-section'))).toBeVisible();

      // Verify CTA buttons stack vertically (column layout) on mobile
      const ctaContainer = page.locator('.hero-cta');
      const flexDirection = await ctaContainer.evaluate(el => {
        const style = window.getComputedStyle(el);
        return style.flexDirection;
      });
      expect(flexDirection).toBe('column');

      // Feature list should be single column
      const featureList = page.locator('.feature-list');
      const featureColumns = await featureList.evaluate(el => {
        const style = window.getComputedStyle(el);
        return style.gridTemplateColumns;
      });
      // Single column means a single value or 1fr pattern
      expect(featureColumns).toMatch(/^(\d+px|1fr)$/);
    });

    test('TC4: No horizontal scrolling required to view content', async ({ page }) => {
      await page.goto('/');
      await waitForPageLoad(page);

      // Check that the document doesn't have horizontal overflow
      const hasHorizontalScrollbar = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScrollbar).toBe(false);

      // Check that body doesn't overflow
      const bodyOverflows = await page.evaluate(() => {
        const body = document.body;
        return body.scrollWidth > body.clientWidth;
      });
      expect(bodyOverflows).toBe(false);
    });

    test('TC5: Logo image scales to fit mobile viewport width', async ({ page }) => {
      await page.goto('/');
      await waitForPageLoad(page);

      const logo = page.locator('.hero-logo');
      await expect(logo).toBeVisible();

      // Logo should not exceed viewport width
      const logoBox = await logo.boundingBox();
      const viewportWidth = 375;

      expect(logoBox).not.toBeNull();
      if (logoBox) {
        expect(logoBox.width).toBeLessThanOrEqual(viewportWidth);
        // Logo should not overflow horizontally
        expect(logoBox.x + logoBox.width).toBeLessThanOrEqual(viewportWidth);
      }
    });

    test('TC6: Usage demo GIF scales appropriately for mobile viewport', async ({ page }) => {
      await page.goto('/');
      await waitForPageLoad(page);

      const demoImage = page.locator('.hero-demo-image');
      await expect(demoImage).toBeVisible();

      // Demo image should not exceed viewport width
      const imageBox = await demoImage.boundingBox();
      const viewportWidth = 375;

      expect(imageBox).not.toBeNull();
      if (imageBox) {
        expect(imageBox.width).toBeLessThanOrEqual(viewportWidth);
      }
    });

    test('TC7: Body text font size is at least 16px', async ({ page }) => {
      await page.goto('/');
      await waitForPageLoad(page);

      // Check the computed font size of body
      const bodyFontSize = await page.evaluate(() => {
        const body = document.body;
        const style = window.getComputedStyle(body);
        return parseFloat(style.fontSize);
      });
      expect(bodyFontSize).toBeGreaterThanOrEqual(16);

      // Check about section paragraph font size
      const aboutParagraph = page.locator('.about p');
      const aboutFontSize = await aboutParagraph.evaluate(el => {
        const style = window.getComputedStyle(el);
        return parseFloat(style.fontSize);
      });
      expect(aboutFontSize).toBeGreaterThanOrEqual(16);

      // Check hero tagline font size
      const tagline = page.locator('.hero-tagline');
      const taglineFontSize = await tagline.evaluate(el => {
        const style = window.getComputedStyle(el);
        return parseFloat(style.fontSize);
      });
      expect(taglineFontSize).toBeGreaterThanOrEqual(16);
    });

    test('TC8: CTA buttons are easily tappable (minimum 44x44px touch target)', async ({ page }) => {
      await page.goto('/');
      await waitForPageLoad(page);

      // Check Get Started button
      const getStartedBtn = page.locator(getByTestId('get-started-btn'));
      const getStartedBox = await getStartedBtn.boundingBox();
      expect(getStartedBox).not.toBeNull();
      if (getStartedBox) {
        expect(getStartedBox.height).toBeGreaterThanOrEqual(44);
        expect(getStartedBox.width).toBeGreaterThanOrEqual(44);
      }

      // Check View Source button
      const viewSourceBtn = page.locator(getByTestId('view-source-btn'));
      const viewSourceBox = await viewSourceBtn.boundingBox();
      expect(viewSourceBox).not.toBeNull();
      if (viewSourceBox) {
        expect(viewSourceBox.height).toBeGreaterThanOrEqual(44);
        expect(viewSourceBox.width).toBeGreaterThanOrEqual(44);
      }

      // Check navigation links (should also be tappable)
      const docsLink = page.locator(getByTestId('docs-link'));
      const docsLinkBox = await docsLink.boundingBox();
      expect(docsLinkBox).not.toBeNull();
      if (docsLinkBox) {
        expect(docsLinkBox.height).toBeGreaterThanOrEqual(44);
      }
    });
  });

  test.describe('Cross-viewport Image Scaling', () => {
    const viewports = [
      { name: 'desktop', ...VIEWPORTS.desktop },
      { name: 'tablet', ...VIEWPORTS.tablet },
      { name: 'mobile', ...VIEWPORTS.mobile },
    ];

    for (const viewport of viewports) {
      test(`Images scale correctly at ${viewport.name} viewport`, async ({ page }) => {
        await page.setViewportSize({ width: viewport.width, height: viewport.height });
        await page.goto('/');
        await waitForPageLoad(page);

        // Check logo
        const logo = page.locator('.hero-logo');
        await expect(logo).toBeVisible();
        const logoBox = await logo.boundingBox();
        expect(logoBox).not.toBeNull();
        if (logoBox) {
          expect(logoBox.width).toBeLessThanOrEqual(viewport.width);
        }

        // Check usage demo image
        const demoImage = page.locator('.hero-demo-image');
        await expect(demoImage).toBeVisible();
        const demoBox = await demoImage.boundingBox();
        expect(demoBox).not.toBeNull();
        if (demoBox) {
          expect(demoBox.width).toBeLessThanOrEqual(viewport.width);
        }
      });
    }
  });
});
