/**
 * HTML Validation Tests
 *
 * This test suite verifies that the MirDB landing page HTML is valid and follows best practices.
 *
 * Scenario: HTML Validation
 * - Verify the HTML is valid and follows best practices
 *
 * Test Cases:
 * 1. Validate HTML through W3C validator - No critical HTML validation errors
 * 2. Check for DOCTYPE declaration - Page starts with valid HTML5 DOCTYPE (<!DOCTYPE html>)
 * 3. Verify lang attribute on html element - HTML element has lang='en' attribute
 * 4. Check for charset declaration - Page includes <meta charset='UTF-8'> in head
 */

const { test, expect } = require('@playwright/test');
const { HtmlValidate } = require('html-validate');
const fs = require('fs');
const path = require('path');

test.describe('HTML Validation', () => {
  let htmlContent;

  test.beforeAll(async () => {
    // Read the HTML file directly for validation
    const htmlPath = path.join(__dirname, '..', 'index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');
  });

  /**
   * Test Case 1: Validate HTML through W3C validator
   * Input: Validate HTML through W3C validator
   * Expected: No critical HTML validation errors
   */
  test('TC1: No critical HTML validation errors', async () => {
    // Configure html-validate with standard rules
    // Focus on critical W3C validation errors, not stylistic preferences
    const htmlvalidate = new HtmlValidate({
      extends: ['html-validate:recommended'],
      rules: {
        // Customize rules as needed - disable rules that are too strict for this context
        'no-inline-style': 'off',
        'require-sri': 'off',
        'no-trailing-whitespace': 'off',
        'prefer-native-element': 'warn',
        // Redundant roles are valid HTML, just not necessary - not a critical error
        'no-redundant-role': 'warn',
        // Unique landmarks are an accessibility best practice, not HTML validity error
        'unique-landmark': 'warn',
      },
    });

    // Validate the HTML content
    const report = await htmlvalidate.validateString(htmlContent);

    // Log all messages for debugging
    if (report.results.length > 0 && report.results[0].messages.length > 0) {
      console.log('HTML Validation Messages:');
      for (const msg of report.results[0].messages) {
        console.log(`  [${msg.severity === 2 ? 'ERROR' : 'WARN'}] Line ${msg.line}: ${msg.message} (${msg.ruleId})`);
      }
    }

    // Filter for critical errors only (severity 2 = error)
    const criticalErrors = report.results.length > 0
      ? report.results[0].messages.filter(msg => msg.severity === 2)
      : [];

    // Log critical errors
    if (criticalErrors.length > 0) {
      console.log('\nCritical Errors:');
      for (const error of criticalErrors) {
        console.log(`  Line ${error.line}: ${error.message} (${error.ruleId})`);
      }
    }

    // Assert no critical validation errors
    expect(criticalErrors.length).toBe(0);
  });

  /**
   * Test Case 2: Check for DOCTYPE declaration
   * Input: Check for DOCTYPE declaration
   * Expected: Page starts with valid HTML5 DOCTYPE (<!DOCTYPE html>)
   */
  test('TC2: Page starts with valid HTML5 DOCTYPE', async () => {
    // Trim leading whitespace and check for DOCTYPE
    const trimmedHtml = htmlContent.trim();

    // Check that HTML starts with DOCTYPE declaration
    const startsWithDoctype = trimmedHtml.toLowerCase().startsWith('<!doctype html>');
    expect(startsWithDoctype).toBe(true);

    // Extract the actual DOCTYPE line for verification
    const firstLine = trimmedHtml.split('\n')[0].trim();
    console.log(`DOCTYPE declaration: ${firstLine}`);

    // Verify it's the correct HTML5 DOCTYPE format
    expect(firstLine.toLowerCase()).toBe('<!doctype html>');
  });

  /**
   * Test Case 3: Verify lang attribute on html element
   * Input: Verify lang attribute on html element
   * Expected: HTML element has lang='en' attribute
   */
  test('TC3: HTML element has lang attribute set to en', async ({ page }) => {
    await page.goto('/');

    // Get the lang attribute from the html element
    const htmlLang = await page.locator('html').getAttribute('lang');

    // Log the lang attribute
    console.log(`HTML lang attribute: ${htmlLang}`);

    // Verify lang attribute exists
    expect(htmlLang).toBeTruthy();

    // Verify lang attribute is set to 'en'
    expect(htmlLang).toBe('en');

    // Also verify in raw HTML content using regex
    const langRegex = /<html[^>]+lang\s*=\s*["']([^"']+)["']/i;
    const langMatch = htmlContent.match(langRegex);

    expect(langMatch).toBeTruthy();
    expect(langMatch[1]).toBe('en');
  });

  /**
   * Test Case 4: Check for charset declaration
   * Input: Check for charset declaration
   * Expected: Page includes <meta charset='UTF-8'> in head
   */
  test('TC4: Page includes meta charset UTF-8 in head', async ({ page }) => {
    await page.goto('/');

    // Check for charset meta tag
    const charsetMeta = page.locator('meta[charset]');
    await expect(charsetMeta).toHaveCount(1);

    // Get the charset value
    const charsetValue = await charsetMeta.getAttribute('charset');

    // Log the charset
    console.log(`Charset declaration: ${charsetValue}`);

    // Verify charset is UTF-8 (case-insensitive)
    expect(charsetValue.toLowerCase()).toBe('utf-8');

    // Verify charset meta tag is in the head section
    const headContent = await page.evaluate(() => {
      const head = document.head;
      const charsetMeta = head.querySelector('meta[charset]');
      return charsetMeta !== null;
    });
    expect(headContent).toBe(true);

    // Also verify charset appears early in the head (should be first or second element)
    const charsetPosition = await page.evaluate(() => {
      const head = document.head;
      const children = Array.from(head.children);
      const charsetIndex = children.findIndex(el =>
        el.tagName === 'META' && el.hasAttribute('charset')
      );
      return charsetIndex;
    });

    // Charset should be within first 3 elements of head (recommended best practice)
    console.log(`Charset meta tag position in head: ${charsetPosition}`);
    expect(charsetPosition).toBeLessThan(3);

    // Verify raw HTML contains proper charset declaration
    const charsetRegex = /<meta\s+charset\s*=\s*["']UTF-8["']/i;
    expect(htmlContent).toMatch(charsetRegex);
  });

  /**
   * Additional Test: Verify no deprecated HTML elements
   */
  test('TC-Additional: No deprecated HTML elements are used', async () => {
    // List of deprecated HTML elements
    const deprecatedElements = [
      'acronym', 'applet', 'basefont', 'big', 'blink', 'center',
      'dir', 'font', 'frame', 'frameset', 'isindex', 'keygen',
      'listing', 'marquee', 'menuitem', 'multicol', 'nextid',
      'nobr', 'noembed', 'noframes', 'plaintext', 'spacer',
      'strike', 'tt', 'xmp'
    ];

    const foundDeprecated = [];

    for (const element of deprecatedElements) {
      // Check if deprecated element exists in HTML (as opening tag)
      const regex = new RegExp(`<${element}[\\s>]`, 'gi');
      if (regex.test(htmlContent)) {
        foundDeprecated.push(element);
      }
    }

    if (foundDeprecated.length > 0) {
      console.log('Deprecated elements found:', foundDeprecated);
    }

    // Assert no deprecated elements found
    expect(foundDeprecated).toEqual([]);
  });

  /**
   * Additional Test: Verify no deprecated HTML attributes
   */
  test('TC-Additional: No deprecated HTML attributes are used', async () => {
    // List of deprecated HTML attributes (common ones)
    const deprecatedAttributes = [
      { attr: 'align', elements: ['div', 'p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'table', 'tr', 'td', 'th'] },
      { attr: 'bgcolor', elements: ['body', 'table', 'tr', 'td', 'th'] },
      { attr: 'border', elements: ['img', 'object'] },
      { attr: 'cellpadding', elements: ['table'] },
      { attr: 'cellspacing', elements: ['table'] },
      { attr: 'valign', elements: ['tr', 'td', 'th'] },
      { attr: 'width', elements: ['hr', 'table', 'td', 'th', 'pre'] },
      { attr: 'height', elements: ['td', 'th'] },
    ];

    const foundDeprecated = [];

    for (const { attr, elements } of deprecatedAttributes) {
      for (const element of elements) {
        // Check if deprecated attribute is used on the element
        const regex = new RegExp(`<${element}[^>]+${attr}\\s*=`, 'gi');
        if (regex.test(htmlContent)) {
          foundDeprecated.push(`${attr} on <${element}>`);
        }
      }
    }

    if (foundDeprecated.length > 0) {
      console.log('Deprecated attributes found:', foundDeprecated);
    }

    // Assert no deprecated attributes found
    expect(foundDeprecated).toEqual([]);
  });

  /**
   * Additional Test: Verify proper HTML structure
   */
  test('TC-Additional: HTML has proper document structure', async ({ page }) => {
    await page.goto('/');

    // Verify basic document structure
    const structure = await page.evaluate(() => {
      const html = document.documentElement;
      const head = document.head;
      const body = document.body;

      return {
        hasHtml: html !== null,
        hasHead: head !== null,
        hasBody: body !== null,
        htmlHasLang: html.hasAttribute('lang'),
        headHasTitle: head.querySelector('title') !== null,
        bodyHasContent: body.children.length > 0,
      };
    });

    expect(structure.hasHtml).toBe(true);
    expect(structure.hasHead).toBe(true);
    expect(structure.hasBody).toBe(true);
    expect(structure.htmlHasLang).toBe(true);
    expect(structure.headHasTitle).toBe(true);
    expect(structure.bodyHasContent).toBe(true);
  });

  /**
   * Additional Test: Verify all required meta tags are present
   */
  test('TC-Additional: All required meta tags are present', async ({ page }) => {
    await page.goto('/');

    // Check for essential meta tags
    const metaTags = await page.evaluate(() => {
      const head = document.head;
      return {
        charset: head.querySelector('meta[charset]') !== null,
        viewport: head.querySelector('meta[name="viewport"]') !== null,
        description: head.querySelector('meta[name="description"]') !== null,
      };
    });

    console.log('Meta tags present:', metaTags);

    expect(metaTags.charset).toBe(true);
    expect(metaTags.viewport).toBe(true);
    expect(metaTags.description).toBe(true);
  });
});
