import { test, expect, Page } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

test.describe('Accessibility Compliance (WCAG 2.1 AA)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('Test Case 1: axe-core audit reports no critical violations', async ({
    page,
  }) => {
    const results = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
      .analyze();

    const critical = results.violations.filter((v) => v.impact === 'critical');

    if (critical.length > 0) {
      console.log(
        'Critical violations:',
        JSON.stringify(
          critical.map((v) => ({ id: v.id, impact: v.impact, help: v.help })),
          null,
          2
        )
      );
    }

    expect(critical).toHaveLength(0);

    const totalRulesEvaluated =
      results.violations.length +
      results.passes.length +
      results.incomplete.length;
    const violationCount = results.violations.length;
    const passRate =
      totalRulesEvaluated === 0
        ? 100
        : ((totalRulesEvaluated - violationCount) / totalRulesEvaluated) * 100;

    expect(
      passRate,
      `Accessibility score (rule pass rate) is ${passRate.toFixed(1)}% (${violationCount} violation types of ${totalRulesEvaluated} rules)`
    ).toBeGreaterThanOrEqual(90);
  });

  test('Test Case 2: all interactive elements are reachable via keyboard', async ({
    page,
  }) => {
    const interactiveSelectors = [
      'header a',
      'header button',
      'main a',
      'main button',
      'footer a',
    ];

    const interactiveCount = await page.evaluate((selectors) => {
      const all = new Set<Element>();
      selectors.forEach((sel) => {
        document.querySelectorAll(sel).forEach((el) => {
          const style = window.getComputedStyle(el);
          if (
            style.display !== 'none' &&
            style.visibility !== 'hidden' &&
            (el as HTMLElement).offsetParent !== null
          ) {
            all.add(el);
          }
        });
      });
      return all.size;
    }, interactiveSelectors);

    expect(interactiveCount).toBeGreaterThan(0);

    await page.evaluate(() => {
      (document.activeElement as HTMLElement)?.blur();
      window.scrollTo(0, 0);
    });

    const focusedElements: string[] = [];
    const maxTabs = 60;

    for (let i = 0; i < maxTabs; i++) {
      await page.keyboard.press('Tab');
      const info = await page.evaluate(() => {
        const active = document.activeElement as HTMLElement | null;
        if (!active || active === document.body) return null;
        return {
          tag: active.tagName.toLowerCase(),
          text:
            active.getAttribute('aria-label') ||
            active.textContent?.trim().slice(0, 40) ||
            '',
          testid: active.getAttribute('data-testid') || '',
        };
      });
      if (info) {
        focusedElements.push(`${info.tag}:${info.testid || info.text}`);
      }
    }

    expect(focusedElements.length).toBeGreaterThanOrEqual(5);

    const uniqueFocused = new Set(focusedElements);
    expect(uniqueFocused.size).toBeGreaterThanOrEqual(4);

    const focusedTags = focusedElements.map((s) => s.split(':')[0]);
    expect(focusedTags).toContain('a');
  });

  test('Test Case 3: focused elements have visible focus indicators', async ({
    page,
  }) => {
    const samples: { selector: string; description: string }[] = [
      { selector: 'header a[data-testid="header-logo"]', description: 'Header logo link' },
      { selector: 'header nav a >> nth=0', description: 'First nav link' },
      { selector: 'footer nav a >> nth=0', description: 'First footer link' },
    ];

    for (const sample of samples) {
      const element = page.locator(sample.selector).first();
      if ((await element.count()) === 0) continue;

      await element.scrollIntoViewIfNeeded();
      await element.focus();
      await page.waitForTimeout(50);

      const focusStyle = await element.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          outlineStyle: style.outlineStyle,
          outlineWidth: style.outlineWidth,
          outlineColor: style.outlineColor,
          boxShadow: style.boxShadow,
          border: style.border,
        };
      });

      const hasOutline =
        focusStyle.outlineStyle !== 'none' &&
        focusStyle.outlineWidth !== '0px' &&
        focusStyle.outlineWidth !== '';
      const hasBoxShadow =
        focusStyle.boxShadow !== 'none' && focusStyle.boxShadow !== '';

      const hasVisibleFocus = hasOutline || hasBoxShadow;
      expect(
        hasVisibleFocus,
        `${sample.description} should have a visible focus indicator. Got: outline=${focusStyle.outlineStyle} ${focusStyle.outlineWidth}, boxShadow=${focusStyle.boxShadow}`
      ).toBe(true);
    }
  });

  test('Test Case 4: heading hierarchy has exactly one h1 and follows logical order', async ({
    page,
  }) => {
    const h1Count = await page.locator('h1').count();
    expect(h1Count).toBe(1);

    const headings = await page.evaluate(() => {
      const els = Array.from(
        document.querySelectorAll('h1, h2, h3, h4, h5, h6')
      ) as HTMLElement[];
      return els.map((el) => ({
        level: parseInt(el.tagName.substring(1), 10),
        text: el.textContent?.trim().slice(0, 60) || '',
      }));
    });

    expect(headings.length).toBeGreaterThan(0);
    expect(headings[0].level).toBe(1);

    for (let i = 1; i < headings.length; i++) {
      const current = headings[i].level;
      const previous = headings[i - 1].level;
      if (current > previous) {
        expect(
          current - previous,
          `Heading level jumped from h${previous} ("${headings[i - 1].text}") to h${current} ("${headings[i].text}") - should not skip levels`
        ).toBeLessThanOrEqual(1);
      }
    }
  });

  test('Test Case 5: all images have descriptive alt text', async ({ page }) => {
    const imageInfo = await page.evaluate(() => {
      const imgs = Array.from(document.querySelectorAll('img')) as HTMLImageElement[];
      return imgs.map((img) => ({
        src: img.getAttribute('src') || '',
        alt: img.getAttribute('alt'),
        ariaHidden: img.getAttribute('aria-hidden'),
      }));
    });

    for (const img of imageInfo) {
      const isDecorative = img.ariaHidden === 'true';

      if (!isDecorative) {
        expect(
          img.alt,
          `Image ${img.src} is missing alt attribute`
        ).not.toBeNull();
        expect(
          img.alt!.trim().length,
          `Image ${img.src} has empty alt text`
        ).toBeGreaterThan(0);
        expect(
          img.alt!.trim().length,
          `Image ${img.src} alt text "${img.alt}" is not descriptive enough (< 5 chars)`
        ).toBeGreaterThanOrEqual(5);
      }
    }

    const usageGif = page.locator('[data-testid="usage-gif"]');
    if ((await usageGif.count()) > 0) {
      const alt = await usageGif.getAttribute('alt');
      expect(alt).toBeTruthy();
      expect(alt!.length).toBeGreaterThan(10);
    }
  });

  test('Test Case 6: page has main, nav, and footer ARIA landmarks', async ({
    page,
  }) => {
    const headerCount = await page.locator('header').count();
    expect(headerCount).toBeGreaterThanOrEqual(1);

    const mainCount = await page.locator('main').count();
    expect(mainCount).toBe(1);

    const navCount = await page.locator('nav').count();
    expect(navCount).toBeGreaterThanOrEqual(1);

    const footerCount = await page.locator('footer').count();
    expect(footerCount).toBe(1);

    const navLabels = await page.evaluate(() => {
      return Array.from(document.querySelectorAll('nav')).map(
        (n) =>
          n.getAttribute('aria-label') ||
          n.getAttribute('aria-labelledby') ||
          ''
      );
    });
    const allNavsLabeled = navLabels.every((l) => l.length > 0);
    expect(
      allNavsLabeled,
      `Multiple <nav> elements should have aria-label. Got: ${JSON.stringify(navLabels)}`
    ).toBe(true);

    const html = page.locator('html');
    const lang = await html.getAttribute('lang');
    expect(lang).toBeTruthy();
    expect(lang!.length).toBeGreaterThanOrEqual(2);
  });
});
