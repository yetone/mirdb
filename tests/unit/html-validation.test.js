/**
 * Unit tests for HTML validation - verifying HTML is valid and follows best practices
 * Tests: DOCTYPE declaration, lang attribute, charset declaration, and W3C validation rules
 */
const { JSDOM } = require('jsdom');
const fs = require('fs');
const path = require('path');

const indexPath = path.join(__dirname, '../../index.html');

function runTests() {
  console.log('Running HTML Validation Tests...\n');

  let passed = 0;
  let failed = 0;
  const results = [];

  // Read the HTML file
  let html;
  try {
    html = fs.readFileSync(indexPath, 'utf-8');
  } catch (error) {
    console.error('Failed to read index.html:', error.message);
    process.exit(1);
  }

  // Parse HTML with JSDOM
  const dom = new JSDOM(html);
  const document = dom.window.document;

  // Test 1: Verify proper HTML5 DOCTYPE declaration
  function testDoctypeDeclaration() {
    const doctypeRegex = /^<!DOCTYPE\s+html\s*>/i;
    const htmlContent = html.trim();

    if (doctypeRegex.test(htmlContent)) {
      console.log('✓ Page has proper HTML5 DOCTYPE declaration');
      passed++;
      results.push({ test: 'DOCTYPE declaration', status: 'pass' });
    } else {
      console.log('✗ Page should have proper HTML5 DOCTYPE declaration (<!DOCTYPE html>)');
      failed++;
      results.push({
        test: 'DOCTYPE declaration',
        status: 'fail',
        error: 'Missing or incorrect DOCTYPE declaration. Expected: <!DOCTYPE html>'
      });
    }
  }

  // Test 2: Verify HTML element has lang attribute
  function testLangAttribute() {
    const htmlElement = document.querySelector('html');

    if (!htmlElement) {
      console.log('✗ HTML element not found');
      failed++;
      results.push({ test: 'lang attribute', status: 'fail', error: 'HTML element not found' });
      return;
    }

    const langAttr = htmlElement.getAttribute('lang');

    if (langAttr && langAttr.trim().length > 0) {
      console.log(`✓ HTML element has lang attribute set (lang="${langAttr}")`);
      passed++;
      results.push({ test: 'lang attribute', status: 'pass' });
    } else {
      console.log('✗ HTML element should have lang attribute set (e.g., lang="en")');
      failed++;
      results.push({
        test: 'lang attribute',
        status: 'fail',
        error: 'HTML element is missing the lang attribute'
      });
    }
  }

  // Test 3: Verify meta charset UTF-8 is declared
  function testCharsetDeclaration() {
    // Check for <meta charset="UTF-8"> or <meta charset="utf-8">
    const charsetMeta = document.querySelector('meta[charset]');
    const contentTypeMeta = document.querySelector('meta[http-equiv="Content-Type"]');

    let hasValidCharset = false;
    let charsetValue = null;

    if (charsetMeta) {
      charsetValue = charsetMeta.getAttribute('charset');
      if (charsetValue && charsetValue.toLowerCase() === 'utf-8') {
        hasValidCharset = true;
      }
    }

    // Also check for content-type meta tag with charset
    if (!hasValidCharset && contentTypeMeta) {
      const content = contentTypeMeta.getAttribute('content');
      if (content && content.toLowerCase().includes('charset=utf-8')) {
        hasValidCharset = true;
        charsetValue = 'UTF-8 (via Content-Type)';
      }
    }

    if (hasValidCharset) {
      console.log(`✓ Meta charset UTF-8 is declared (charset="${charsetValue}")`);
      passed++;
      results.push({ test: 'charset declaration', status: 'pass' });
    } else {
      console.log('✗ Meta charset UTF-8 should be declared (e.g., <meta charset="UTF-8">)');
      failed++;
      results.push({
        test: 'charset declaration',
        status: 'fail',
        error: 'Missing or incorrect charset declaration. Expected: <meta charset="UTF-8">'
      });
    }
  }

  // Test 4: W3C HTML validation (structural checks)
  function testW3CValidation() {
    const errors = [];

    // Check 1: html element exists
    const htmlElement = document.querySelector('html');
    if (!htmlElement) {
      errors.push('Missing <html> element');
    }

    // Check 2: head element exists
    const headElement = document.querySelector('head');
    if (!headElement) {
      errors.push('Missing <head> element');
    }

    // Check 3: body element exists
    const bodyElement = document.querySelector('body');
    if (!bodyElement) {
      errors.push('Missing <body> element');
    }

    // Check 4: title element exists in head
    const titleElement = document.querySelector('head > title');
    if (!titleElement) {
      errors.push('Missing <title> element in <head>');
    } else if (!titleElement.textContent.trim()) {
      errors.push('<title> element is empty');
    }

    // Check 5: No duplicate IDs
    const allIds = document.querySelectorAll('[id]');
    const idSet = new Set();
    const duplicateIds = [];
    allIds.forEach(element => {
      const id = element.getAttribute('id');
      if (idSet.has(id)) {
        duplicateIds.push(id);
      } else {
        idSet.add(id);
      }
    });
    if (duplicateIds.length > 0) {
      errors.push(`Duplicate IDs found: ${duplicateIds.join(', ')}`);
    }

    // Check 6: Images have alt attributes
    const images = document.querySelectorAll('img');
    const imagesWithoutAlt = [];
    images.forEach((img, index) => {
      if (!img.hasAttribute('alt')) {
        const src = img.getAttribute('src') || `image-${index}`;
        imagesWithoutAlt.push(src);
      }
    });
    if (imagesWithoutAlt.length > 0) {
      errors.push(`Images missing alt attribute: ${imagesWithoutAlt.join(', ')}`);
    }

    // Check 7: Buttons and links are properly formed
    const emptyLinks = document.querySelectorAll('a:not([href])');
    if (emptyLinks.length > 0) {
      errors.push(`Found ${emptyLinks.length} anchor elements without href attribute`);
    }

    // Check 8: No deprecated elements
    const deprecatedElements = ['center', 'font', 'strike', 'big', 'marquee', 'blink'];
    const foundDeprecated = [];
    deprecatedElements.forEach(tagName => {
      const elements = document.querySelectorAll(tagName);
      if (elements.length > 0) {
        foundDeprecated.push(`<${tagName}> (${elements.length})`);
      }
    });
    if (foundDeprecated.length > 0) {
      errors.push(`Deprecated elements found: ${foundDeprecated.join(', ')}`);
    }

    // Check 9: Proper heading hierarchy (no skipped levels)
    const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
    let lastHeadingLevel = 0;
    let headingOrderValid = true;
    headings.forEach((heading) => {
      const level = parseInt(heading.tagName.charAt(1));
      if (lastHeadingLevel === 0 && level !== 1) {
        // First heading should be h1
        headingOrderValid = false;
      } else if (level > lastHeadingLevel + 1 && lastHeadingLevel !== 0) {
        // Skipped a heading level
        headingOrderValid = false;
      }
      lastHeadingLevel = level;
    });
    if (!headingOrderValid) {
      errors.push('Heading hierarchy has skipped levels or does not start with h1');
    }

    // Check 10: Form elements have labels (if any forms exist)
    const inputs = document.querySelectorAll('input:not([type="hidden"]):not([type="submit"]):not([type="button"]):not([type="reset"])');
    const inputsWithoutLabel = [];
    inputs.forEach((input) => {
      const id = input.getAttribute('id');
      const ariaLabel = input.getAttribute('aria-label');
      const ariaLabelledby = input.getAttribute('aria-labelledby');
      const title = input.getAttribute('title');

      if (id) {
        const label = document.querySelector(`label[for="${id}"]`);
        if (!label && !ariaLabel && !ariaLabelledby && !title) {
          inputsWithoutLabel.push(id || 'unnamed');
        }
      } else if (!ariaLabel && !ariaLabelledby && !title) {
        inputsWithoutLabel.push('input without id');
      }
    });
    if (inputsWithoutLabel.length > 0) {
      errors.push(`Form inputs missing labels: ${inputsWithoutLabel.join(', ')}`);
    }

    if (errors.length === 0) {
      console.log('✓ No HTML validation errors');
      passed++;
      results.push({ test: 'W3C HTML validation', status: 'pass' });
    } else {
      console.log(`✗ HTML validation errors found (${errors.length}):`);
      errors.forEach(error => console.log(`  - ${error}`));
      failed++;
      results.push({
        test: 'W3C HTML validation',
        status: 'fail',
        error: errors.join('; ')
      });
    }
  }

  // Run all tests
  testDoctypeDeclaration();
  testLangAttribute();
  testCharsetDeclaration();
  testW3CValidation();

  // Summary
  console.log(`\n${'='.repeat(50)}`);
  console.log(`Tests: ${passed} passed, ${failed} failed`);
  console.log(`${'='.repeat(50)}\n`);

  if (failed > 0) {
    process.exit(1);
  }
}

runTests();
