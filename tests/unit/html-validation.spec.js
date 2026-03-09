/**
 * HTML Validation Unit Tests
 * Owner: Scenario 15 - HTML Validation and Standards
 *
 * Tests:
 * - DOCTYPE declaration
 * - charset UTF-8
 * - viewport meta
 * - HTML validation passes
 * - Exactly one h1
 * - lang attribute present
 */

const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');
const { HtmlValidate } = require('html-validate');

// Path to the HTML file
const htmlFilePath = path.join(__dirname, '../../docs/index.html');

test.describe('HTML Validation and Standards', () => {
  let htmlContent;

  test.beforeAll(() => {
    htmlContent = fs.readFileSync(htmlFilePath, 'utf-8');
  });

  test('TC1: Document starts with <!DOCTYPE html>', async () => {
    // Check that the document starts with the HTML5 doctype
    const trimmedContent = htmlContent.trim();
    expect(trimmedContent.startsWith('<!DOCTYPE html>')).toBe(true);
  });

  test('TC2: meta charset="UTF-8" present in head', async ({ page }) => {
    await page.goto('/');

    // Check for charset meta tag in head
    const charsetMeta = await page.locator('head meta[charset]');
    await expect(charsetMeta).toHaveCount(1);

    const charsetValue = await charsetMeta.getAttribute('charset');
    expect(charsetValue.toUpperCase()).toBe('UTF-8');
  });

  test('TC3: meta viewport with width=device-width, initial-scale=1 present', async ({ page }) => {
    await page.goto('/');

    // Check for viewport meta tag
    const viewportMeta = await page.locator('head meta[name="viewport"]');
    await expect(viewportMeta).toHaveCount(1);

    const contentValue = await viewportMeta.getAttribute('content');
    expect(contentValue).toContain('width=device-width');
    expect(contentValue).toContain('initial-scale=1');
  });

  test('TC4: HTML validation passes (no errors, warnings acceptable)', async () => {
    // Configure html-validate for HTML5 standard validation
    // Focus on structural HTML5 validity, not strict accessibility rules
    const htmlValidate = new HtmlValidate({
      extends: ['html-validate:standard'],
      rules: {
        // Disable rules that are best practices but not HTML5 standard errors
        'no-trailing-whitespace': 'off',
        'prefer-native-element': 'off',
        'no-inline-style': 'off',
        'require-sri': 'off',
        'unique-landmark': 'off', // Accessibility best practice
        'no-implicit-button-type': 'off', // Best practice, not required by HTML5
        'aria-label-misuse': 'off', // ARIA best practice
        'wcag/h71': 'off', // WCAG guideline
      },
    });

    const report = await htmlValidate.validateString(htmlContent);

    // Filter only errors (not warnings)
    const errors = report.results.flatMap(r =>
      r.messages.filter(m => m.severity === 2) // severity 2 = error
    );

    // Log any errors for debugging
    if (errors.length > 0) {
      console.log('HTML Validation Errors:', JSON.stringify(errors, null, 2));
    }

    expect(errors.length).toBe(0);
  });

  test('TC5: Page has exactly one h1 element (MirDB title)', async ({ page }) => {
    await page.goto('/');

    // Count h1 elements
    const h1Elements = await page.locator('h1');
    await expect(h1Elements).toHaveCount(1);

    // Verify it contains MirDB
    const h1Text = await h1Elements.textContent();
    expect(h1Text).toContain('MirDB');
  });

  test('TC6: html element has lang="en" attribute', async ({ page }) => {
    await page.goto('/');

    // Check lang attribute on html element
    const htmlElement = await page.locator('html');
    const langValue = await htmlElement.getAttribute('lang');

    expect(langValue).toBe('en');
  });
});

// Additional semantic structure tests
test.describe('Semantic HTML5 Structure', () => {
  test('Document uses semantic HTML5 elements', async ({ page }) => {
    await page.goto('/');

    // Verify presence of semantic elements
    await expect(page.locator('header')).toHaveCount(1);
    await expect(page.locator('main')).toHaveCount(1);
    await expect(page.locator('footer')).toHaveCount(1);

    // Verify sections exist
    const sections = await page.locator('main section');
    const sectionCount = await sections.count();
    expect(sectionCount).toBeGreaterThan(0);

    // Verify articles exist (for features, etc.)
    const articles = await page.locator('article');
    const articleCount = await articles.count();
    expect(articleCount).toBeGreaterThan(0);
  });

  test('Document has proper heading hierarchy', async ({ page }) => {
    await page.goto('/');

    // Exactly one h1
    const h1Count = await page.locator('h1').count();
    expect(h1Count).toBe(1);

    // h2 elements exist for sections
    const h2Elements = await page.locator('h2');
    const h2Count = await h2Elements.count();
    expect(h2Count).toBeGreaterThan(0);
  });

  test('Required meta tags are present', async ({ page }) => {
    await page.goto('/');

    // Title tag exists
    const title = await page.title();
    expect(title.length).toBeGreaterThan(0);
    expect(title).toContain('MirDB');

    // Description meta exists
    const descMeta = await page.locator('meta[name="description"]');
    await expect(descMeta).toHaveCount(1);
  });
});
