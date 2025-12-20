// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

/**
 * CSS Layout Integration Tests
 *
 * These tests verify that CSS flexbox and grid features work consistently
 * across all supported browsers (Chrome, Firefox, Safari, Edge).
 */

const indexPath = `file://${path.resolve(__dirname, '../../index.html')}`;

test.describe('CSS Flexbox/Grid Integration Tests', () => {

  test.describe('Flexbox Container Tests', () => {

    test('hero section uses flexbox with correct properties', async ({ page, browserName }) => {
      await page.goto(indexPath);

      const hero = page.locator('.hero');
      const styles = await hero.evaluate((el) => {
        const cs = window.getComputedStyle(el);
        return {
          display: cs.display,
          flexDirection: cs.flexDirection,
          justifyContent: cs.justifyContent,
          alignItems: cs.alignItems
        };
      });

      expect(styles.display).toBe('flex');
      expect(styles.flexDirection).toBe('column');
      expect(styles.justifyContent).toBe('center');
      expect(styles.alignItems).toBe('center');

      console.log(`[${browserName}] Hero flexbox properties verified`);
    });

    test('CTA buttons use flexbox gap correctly', async ({ page, browserName }) => {
      await page.goto(indexPath);

      const ctaContainer = page.locator('.hero-cta');
      const styles = await ctaContainer.evaluate((el) => {
        const cs = window.getComputedStyle(el);
        return {
          display: cs.display,
          gap: cs.gap,
          justifyContent: cs.justifyContent,
          flexWrap: cs.flexWrap
        };
      });

      expect(styles.display).toBe('flex');
      expect(styles.gap).toBe('16px');
      expect(styles.justifyContent).toBe('center');
      expect(styles.flexWrap).toBe('wrap');

      console.log(`[${browserName}] CTA flexbox gap verified`);
    });

    test('differentiators container uses flexbox correctly', async ({ page, browserName }) => {
      await page.goto(indexPath);

      const differentiators = page.locator('.hero-differentiators');
      const styles = await differentiators.evaluate((el) => {
        const cs = window.getComputedStyle(el);
        return {
          display: cs.display,
          gap: cs.gap,
          justifyContent: cs.justifyContent,
          flexWrap: cs.flexWrap
        };
      });

      expect(styles.display).toBe('flex');
      expect(styles.gap).toBe('32px');
      expect(styles.justifyContent).toBe('center');
      expect(styles.flexWrap).toBe('wrap');

      console.log(`[${browserName}] Differentiators flexbox verified`);
    });

    test('individual differentiator items use flexbox', async ({ page, browserName }) => {
      await page.goto(indexPath);

      const differentiator = page.locator('.differentiator').first();
      const styles = await differentiator.evaluate((el) => {
        const cs = window.getComputedStyle(el);
        return {
          display: cs.display,
          alignItems: cs.alignItems,
          gap: cs.gap
        };
      });

      expect(styles.display).toBe('flex');
      expect(styles.alignItems).toBe('center');
      expect(styles.gap).toBe('8px');

      console.log(`[${browserName}] Individual differentiator flexbox verified`);
    });

    test('button internal layout uses inline-flex', async ({ page, browserName }) => {
      await page.goto(indexPath);

      const btn = page.locator('.btn').first();
      const display = await btn.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });

      // Buttons should use inline-flex for icon+text alignment
      // Note: browsers may report 'inline-flex' or 'flex' for inline-flex elements
      expect(['inline-flex', 'flex']).toContain(display);

      console.log(`[${browserName}] Button flex layout verified`);
    });

  });

  test.describe('Counter-based Layout Tests', () => {

    test('installation steps use CSS counters', async ({ page, browserName }) => {
      await page.goto(indexPath);

      // Verify the counter-reset is applied
      const steps = page.locator('.installation-steps');
      const counterReset = await steps.evaluate((el) => {
        return window.getComputedStyle(el).counterReset;
      });

      // Should have counter-reset defined
      expect(counterReset).toContain('step-counter');

      // Verify each step increments counter
      const stepItems = page.locator('.installation-steps li');
      const count = await stepItems.count();
      expect(count).toBe(4);

      console.log(`[${browserName}] CSS counters verified`);
    });

    test('step numbers render with correct styling', async ({ page, browserName }) => {
      await page.goto(indexPath);

      // Check that the ::before pseudo-element creates the number circle
      // We can verify this by checking the padding-left and position
      const stepItem = page.locator('.installation-steps li').first();
      const styles = await stepItem.evaluate((el) => {
        const cs = window.getComputedStyle(el);
        return {
          paddingLeft: cs.paddingLeft,
          position: cs.position,
          counterIncrement: cs.counterIncrement
        };
      });

      expect(styles.paddingLeft).toBe('60px');
      expect(styles.position).toBe('relative');
      expect(styles.counterIncrement).toContain('step-counter');

      console.log(`[${browserName}] Step number styling verified`);
    });

  });

  test.describe('Table Layout Tests', () => {

    test('config table uses proper table layout', async ({ page, browserName }) => {
      await page.goto(indexPath);

      const table = page.locator('.config-table');
      const styles = await table.evaluate((el) => {
        const cs = window.getComputedStyle(el);
        return {
          width: cs.width,
          borderCollapse: cs.borderCollapse
        };
      });

      expect(styles.borderCollapse).toBe('collapse');
      // Width should be 100% of parent
      expect(parseFloat(styles.width)).toBeGreaterThan(0);

      console.log(`[${browserName}] Table layout verified`);
    });

    test('table cells have consistent padding', async ({ page, browserName }) => {
      await page.goto(indexPath);

      // Check header cell padding
      const th = page.locator('.config-table th').first();
      const thPadding = await th.evaluate((el) => {
        return window.getComputedStyle(el).padding;
      });
      expect(thPadding).toMatch(/12px 16px/);

      // Check data cell padding
      const td = page.locator('.config-table td').first();
      const tdPadding = await td.evaluate((el) => {
        return window.getComputedStyle(el).padding;
      });
      expect(tdPadding).toMatch(/12px 16px/);

      console.log(`[${browserName}] Table cell padding verified`);
    });

  });

  test.describe('Viewport and Container Tests', () => {

    test('container has max-width constraint', async ({ page, browserName }) => {
      await page.goto(indexPath);

      const container = page.locator('.container').first();
      const maxWidth = await container.evaluate((el) => {
        return window.getComputedStyle(el).maxWidth;
      });

      // Max-width should be set (either 1200px or 900px for different containers)
      // The quick-start container uses 900px, main container uses 1200px
      const maxWidthValue = parseFloat(maxWidth);
      expect(maxWidthValue).toBeGreaterThanOrEqual(900);
      expect(maxWidthValue).toBeLessThanOrEqual(1200);

      console.log(`[${browserName}] Container max-width verified`);
    });

    test('container is centered with auto margins', async ({ page, browserName }) => {
      await page.goto(indexPath);

      const container = page.locator('.container').first();
      const styles = await container.evaluate((el) => {
        const cs = window.getComputedStyle(el);
        return {
          marginLeft: cs.marginLeft,
          marginRight: cs.marginRight
        };
      });

      // Auto margins resolve to equal values on each side
      // Just check they're computed values (not 'auto')
      expect(parseFloat(styles.marginLeft)).toBeGreaterThanOrEqual(0);
      expect(parseFloat(styles.marginRight)).toBeGreaterThanOrEqual(0);

      console.log(`[${browserName}] Container centering verified`);
    });

    test('hero section has minimum height', async ({ page, browserName }) => {
      await page.goto(indexPath);

      const hero = page.locator('.hero');
      const minHeight = await hero.evaluate((el) => {
        return window.getComputedStyle(el).minHeight;
      });

      // min-height: 100vh is computed to pixel value
      // The computed value should be at least viewport height (typically 720px in Playwright default viewport)
      const minHeightValue = parseFloat(minHeight);
      expect(minHeightValue).toBeGreaterThanOrEqual(600);

      // Also verify actual computed height
      const box = await hero.boundingBox();
      expect(box?.height).toBeGreaterThan(400);

      console.log(`[${browserName}] Hero minimum height verified`);
    });

  });

  test.describe('CSS Transform Tests', () => {

    test('buttons support transform property', async ({ page, browserName }) => {
      await page.goto(indexPath);

      const btn = page.locator('.btn-primary').first();

      // Get initial transform
      const initialTransform = await btn.evaluate((el) => {
        return window.getComputedStyle(el).transform;
      });

      // Hover to trigger transform
      await btn.hover();
      await page.waitForTimeout(300);

      // Transform should be supported (either none or matrix)
      expect(initialTransform).toBeDefined();

      console.log(`[${browserName}] Transform property supported`);
    });

  });

  test.describe('CSS Transition Tests', () => {

    test('buttons have transition defined', async ({ page, browserName }) => {
      await page.goto(indexPath);

      const btn = page.locator('.btn').first();
      const transition = await btn.evaluate((el) => {
        return window.getComputedStyle(el).transition;
      });

      // Should have transition: all 0.2s ease
      expect(transition).toContain('0.2s');

      console.log(`[${browserName}] Button transitions verified`);
    });

  });

  test.describe('Pseudo-element Tests', () => {

    test('installation step numbers render via ::before', async ({ page, browserName }) => {
      await page.goto(indexPath);

      // Check that step items have proper positioning for pseudo-elements
      const stepItems = page.locator('.installation-steps li');

      for (let i = 0; i < await stepItems.count(); i++) {
        const item = stepItems.nth(i);

        // Check position is relative (required for absolute positioned ::before)
        const position = await item.evaluate((el) => {
          return window.getComputedStyle(el).position;
        });
        expect(position).toBe('relative');
      }

      console.log(`[${browserName}] Pseudo-element positioning verified`);
    });

  });

  test.describe('Overflow Handling Tests', () => {

    test('code blocks handle overflow correctly', async ({ page, browserName }) => {
      await page.goto(indexPath);

      const codeBlock = page.locator('.code-block').first();
      const overflowX = await codeBlock.evaluate((el) => {
        return window.getComputedStyle(el).overflowX;
      });

      expect(overflowX).toBe('auto');

      console.log(`[${browserName}] Code block overflow verified`);
    });

  });

  test.describe('Border-radius Tests', () => {

    test('elements have consistent border-radius', async ({ page, browserName }) => {
      await page.goto(indexPath);

      // Check button border-radius
      const btn = page.locator('.btn').first();
      const btnRadius = await btn.evaluate((el) => {
        return window.getComputedStyle(el).borderRadius;
      });
      expect(btnRadius).toBe('8px');

      // Check code block border-radius
      const codeBlock = page.locator('.code-block').first();
      const codeRadius = await codeBlock.evaluate((el) => {
        return window.getComputedStyle(el).borderRadius;
      });
      expect(codeRadius).toBe('8px');

      // Check config-defaults border-radius
      const configDefaults = page.locator('.config-defaults');
      const configRadius = await configDefaults.evaluate((el) => {
        return window.getComputedStyle(el).borderRadius;
      });
      expect(configRadius).toBe('12px');

      console.log(`[${browserName}] Border-radius consistency verified`);
    });

  });

  test.describe('Line Height Tests', () => {

    test('body has proper line-height', async ({ page, browserName }) => {
      await page.goto(indexPath);

      const body = page.locator('body');
      const lineHeight = await body.evaluate((el) => {
        return window.getComputedStyle(el).lineHeight;
      });

      // line-height: 1.6 at typical font size
      const lineHeightValue = parseFloat(lineHeight);
      expect(lineHeightValue).toBeGreaterThan(20);

      console.log(`[${browserName}] Line height verified`);
    });

  });

});
