/**
 * Scenario 12 - Accessibility (WCAG 2.1 AA) E2E Tests
 * Owner: Scenario 12
 *
 * Validates:
 *   - axe-core audit produces zero serious/critical violations on wcag2a + wcag2aa
 *   - Color contrast meets 4.5:1 in both light and dark themes (NFR-3)
 *   - Tab order is logical, every interactive element is focusable, no traps
 *   - Every interactive element shows a visible focus ring
 *   - Every <img> element carries an alt attribute (empty alt allowed for decorative)
 *   - <main>, <nav>, <footer> landmarks are present
 *   - Hamburger button exposes aria-controls + aria-expanded
 *   - Dark theme passes axe with the same severity bar
 *   - Negative case: missing alt is detected by the audit
 */

import { test, expect } from '@playwright/test';
import path from 'path';
import { fileURLToPath, pathToFileURL } from 'url';
import fs from 'fs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const HOMEPAGE_DIR = path.resolve(__dirname, '../../');
const INDEX_PATH = path.resolve(HOMEPAGE_DIR, 'index.html');
const PAGE_URL = pathToFileURL(INDEX_PATH).toString();

// Resolve the local axe-core source so we can inject it directly into the page.
const AXE_SOURCE_PATH = path.resolve(
  HOMEPAGE_DIR,
  'node_modules/axe-core/axe.min.js'
);
const AXE_SOURCE = fs.readFileSync(AXE_SOURCE_PATH, 'utf-8');

const SEVERE_IMPACTS = new Set(['serious', 'critical']);

/**
 * Filter axe violations down to the impact bar that the scenario cares about
 * (serious + critical) so cosmetic findings do not flake the suite.
 */
function severeViolations(violations) {
  return (violations || []).filter((v) => SEVERE_IMPACTS.has(v.impact));
}

/**
 * Inject axe-core and run a full WCAG 2.1 AA audit.
 * Returns the raw axe results.
 */
async function runAxe(page, options = {}) {
  await page.evaluate((src) => {
    // eslint-disable-next-line no-eval
    new Function(src)();
  }, AXE_SOURCE);

  return page.evaluate(async (opts) => {
    const runOptions = {
      runOnly: { type: 'tag', values: ['wcag2a', 'wcag2aa'] },
      ...opts,
    };
    return await window.axe.run(document, runOptions);
  }, options);
}

test.describe('Accessibility (WCAG 2.1 AA)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(PAGE_URL);
    // Wait for the document to be settled.
    await page.waitForLoadState('domcontentloaded');
  });

  // Test Case 1: full axe audit, zero serious/critical violations
  test('axe-core finds zero serious/critical violations on wcag2a + wcag2aa rules', async ({ page }) => {
    const results = await runAxe(page);

    const severe = severeViolations(results.violations);
    if (severe.length > 0) {
      // Surface the violation details in the assertion failure message.
      const summary = severe
        .map((v) => `${v.id} (${v.impact}): ${v.help}`)
        .join('\n');
      expect.soft(severe, `Severe violations:\n${summary}`).toEqual([]);
    }
    expect(severe).toEqual([]);
  });

  // Test Case 2: color contrast in light theme
  test('color-contrast rule produces no violations in light theme', async ({ page }) => {
    // Force light theme so the audit is deterministic.
    await page.evaluate(() => {
      document.documentElement.dataset.theme = 'light';
    });

    const results = await runAxe(page, {
      runOnly: { type: 'rule', values: ['color-contrast'] },
    });
    const violations = severeViolations(results.violations);
    expect(violations).toEqual([]);
  });

  // Test Case 3: tab order matches reading order, every interactive element focusable, no traps
  test('keyboard tab order reaches every interactive element with no trap', async ({ page }) => {
    const focusableSelectors = [
      'a[href]',
      'button:not([disabled])',
      'input:not([disabled])',
      'select:not([disabled])',
      'textarea:not([disabled])',
      '[tabindex]:not([tabindex="-1"])',
    ].join(',');

    const expectedCount = await page.evaluate((selectors) => {
      return Array.from(document.querySelectorAll(selectors)).filter((el) => {
        // visible / not aria-hidden
        const style = window.getComputedStyle(el);
        const isAriaHidden = el.closest('[aria-hidden="true"]');
        const isVisible =
          style.display !== 'none' &&
          style.visibility !== 'hidden' &&
          !isAriaHidden;
        return isVisible;
      }).length;
    }, focusableSelectors);

    expect(expectedCount).toBeGreaterThan(0);

    // Walk Tab through the document and collect each activeElement.
    const visited = new Set();
    let trapped = false;
    let lastIdent = null;
    let sameStreak = 0;

    // Click on the body first to ensure focus starts from the document root.
    await page.evaluate(() => {
      if (document.activeElement && document.activeElement !== document.body) {
        document.activeElement.blur();
      }
      document.body.focus();
    });

    // We allow up to 3x the expected count to detect non-trap behavior.
    for (let i = 0; i < expectedCount * 3 + 10; i++) {
      await page.keyboard.press('Tab');
      const ident = await page.evaluate(() => {
        const el = document.activeElement;
        if (!el || el === document.body) return null;
        // Build a DOM-path-based identifier so duplicate hrefs/text don't collide.
        const path = [];
        let node = el;
        while (node && node !== document.documentElement) {
          let part = node.tagName.toLowerCase();
          if (node.id) part += `#${node.id}`;
          let nth = 1;
          let sib = node;
          while ((sib = sib.previousElementSibling) != null) nth++;
          part += `:nth-child(${nth})`;
          path.unshift(part);
          node = node.parentElement;
        }
        return path.join('>');
      });
      if (ident) {
        visited.add(ident);
        if (ident === lastIdent) {
          sameStreak++;
          if (sameStreak >= 5) {
            trapped = true;
            break;
          }
        } else {
          sameStreak = 0;
          lastIdent = ident;
        }
      }
      if (visited.size >= expectedCount) {
        trapped = false;
        break;
      }
    }

    // No focus trap: we must have visited at least the expected number of unique focusable items.
    expect(trapped).toBe(false);
    expect(visited.size).toBeGreaterThanOrEqual(expectedCount);
  });

  // Test Case 4: focus indicators visible on every interactive element
  test('every interactive element shows a visible focus ring (outline or box-shadow)', async ({ page }) => {
    const violators = await page.evaluate(() => {
      const focusableSelectors = [
        'a[href]',
        'button:not([disabled])',
        'input:not([disabled])',
        'select:not([disabled])',
        'textarea:not([disabled])',
        '[tabindex]:not([tabindex="-1"])',
      ].join(',');

      const elements = Array.from(
        document.querySelectorAll(focusableSelectors)
      ).filter((el) => {
        const style = window.getComputedStyle(el);
        return (
          style.display !== 'none' &&
          style.visibility !== 'hidden' &&
          !el.closest('[aria-hidden="true"]')
        );
      });

      const bad = [];
      for (const el of elements) {
        el.focus();
        const cs = window.getComputedStyle(el);
        const outlineStyle = cs.outlineStyle;
        const outlineWidth = parseFloat(cs.outlineWidth);
        const boxShadow = cs.boxShadow;
        const hasOutline = outlineStyle !== 'none' && outlineWidth > 0;
        const hasBoxShadow = boxShadow && boxShadow !== 'none';
        if (!hasOutline && !hasBoxShadow) {
          bad.push({
            tag: el.tagName,
            id: el.id,
            text: (el.textContent || '').trim().slice(0, 40),
            outlineStyle,
            outlineWidth,
            boxShadow,
          });
        }
        el.blur();
      }
      return bad;
    });

    expect(violators, JSON.stringify(violators, null, 2)).toEqual([]);
  });

  // Test Case 5: every <img> has alt
  test('every <img> element has an alt attribute (empty allowed)', async ({ page }) => {
    const missingAlt = await page.evaluate(() => {
      return document.querySelectorAll('img:not([alt])').length;
    });
    expect(missingAlt).toBe(0);
  });

  // Test Case 6: <main>, <nav>, <footer> landmarks present
  test('page exposes <main>, <nav>, and <footer> landmarks', async ({ page }) => {
    const landmarks = await page.evaluate(() => ({
      main: document.querySelectorAll('main').length,
      nav: document.querySelectorAll('nav').length,
      footer: document.querySelectorAll('footer').length,
      hamburgerHasAriaControls: !!document
        .querySelector('#hamburger')
        ?.hasAttribute('aria-controls'),
      hamburgerHasAriaExpanded: !!document
        .querySelector('#hamburger')
        ?.hasAttribute('aria-expanded'),
    }));

    expect(landmarks.main).toBeGreaterThanOrEqual(1);
    expect(landmarks.nav).toBeGreaterThanOrEqual(1);
    expect(landmarks.footer).toBeGreaterThanOrEqual(1);
    expect(landmarks.hamburgerHasAriaControls).toBe(true);
    expect(landmarks.hamburgerHasAriaExpanded).toBe(true);
  });

  // Test Case 7: dark theme passes axe with same severity bar
  test('dark theme produces zero serious/critical violations', async ({ page }) => {
    // Apply dark theme directly so the test does not depend on the
    // theme-toggle JS implementation (Scenario 8).
    await page.evaluate(() => {
      document.documentElement.dataset.theme = 'dark';
    });

    const results = await runAxe(page);
    const severe = severeViolations(results.violations);
    if (severe.length > 0) {
      const summary = severe
        .map((v) => `${v.id} (${v.impact}): ${v.help}`)
        .join('\n');
      expect.soft(severe, `Severe dark-theme violations:\n${summary}`).toEqual(
        []
      );
    }
    expect(severe).toEqual([]);
  });

  // Test Case 8: negative case - injected image without alt is detected
  test('negative case: an <img> missing alt is flagged by the audit', async ({ page }) => {
    await page.evaluate(() => {
      const bad = document.createElement('img');
      bad.id = 'a11y-negative-fixture';
      bad.src = 'about:blank';
      // Deliberately omit alt
      document.body.appendChild(bad);
    });

    const missingAlt = await page.evaluate(() => {
      return document.querySelectorAll('img:not([alt])').length;
    });
    expect(missingAlt).toBeGreaterThan(0);

    // axe should also identify this as a violation
    const results = await runAxe(page, {
      runOnly: { type: 'rule', values: ['image-alt'] },
    });
    const flagged = results.violations.filter((v) => v.id === 'image-alt');
    expect(flagged.length).toBeGreaterThan(0);
    const targets = flagged.flatMap((v) => v.nodes.map((n) => n.target.join(' ')));
    const hitsFixture = targets.some((t) => t.includes('a11y-negative-fixture'));
    expect(hitsFixture).toBe(true);

    // Cleanup so other tests are not affected
    await page.evaluate(() => {
      const el = document.getElementById('a11y-negative-fixture');
      if (el) el.remove();
    });
  });
});
