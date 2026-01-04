/**
 * Unit tests for HTML validation - verifying HTML is valid and follows best practices
 * Tests: DOCTYPE declaration, lang attribute, charset declaration, and W3C validation rules
 *
 * Test Cases:
 * 1. Run W3C HTML validation - No HTML validation errors
 * 2. Check DOCTYPE declaration - Page has proper HTML5 DOCTYPE declaration
 * 3. Check lang attribute - HTML element has lang attribute set
 * 4. Check charset declaration - Meta charset UTF-8 is declared
 */
const { JSDOM } = require('jsdom');
const { HtmlValidate } = require('html-validate');
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

  // Test 1: W3C HTML Validation using html-validate
  function testW3CValidation() {
    const htmlValidate = new HtmlValidate({
      extends: ['html-validate:recommended'],
      rules: {
        // Allow certain common patterns
        'no-inline-style': 'off',
        'require-sri': 'off',
        'no-trailing-whitespace': 'off',
        'attr-quotes': ['error', { style: 'double' }],
        'doctype-html': 'error',
        'element-required-attributes': 'error',
        'no-dup-attr': 'error',
        'no-dup-id': 'error',
        'void-style': 'off',
        'attribute-empty-style': 'off'
      }
    });

    const report = htmlValidate.validateStringSync(html);

    if (report.valid) {
      console.log('✓ W3C HTML validation passed - No HTML validation errors');
      passed++;
      results.push({ test: 'W3C HTML validation', status: 'pass' });
    } else {
      const errors = report.results[0]?.messages.filter(m => m.severity === 2) || [];
      const warnings = report.results[0]?.messages.filter(m => m.severity === 1) || [];

      if (errors.length === 0) {
        console.log('✓ W3C HTML validation passed - No HTML validation errors');
        if (warnings.length > 0) {
          console.log(`  (${warnings.length} warnings present but no errors)`);
        }
        passed++;
        results.push({ test: 'W3C HTML validation', status: 'pass' });
      } else {
        console.log('✗ W3C HTML validation failed');
        errors.forEach(err => {
          console.log(`  Line ${err.line}:${err.column} - ${err.message}`);
        });
        failed++;
        results.push({
          test: 'W3C HTML validation',
          status: 'fail',
          error: `${errors.length} validation errors found`,
          details: errors.map(e => `Line ${e.line}: ${e.message}`)
        });
      }
    }
  }

  // Test 2: Verify proper HTML5 DOCTYPE declaration
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

  // Test 3: Verify HTML element has lang attribute
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

  // Test 4: Verify meta charset UTF-8 is declared
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
      console.log('✓ Meta charset UTF-8 is declared');
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

  // Run all tests
  testW3CValidation();
  testDoctypeDeclaration();
  testLangAttribute();
  testCharsetDeclaration();

  // Summary
  console.log(`\n${'='.repeat(50)}`);
  console.log(`Tests: ${passed} passed, ${failed} failed`);
  console.log(`${'='.repeat(50)}\n`);

  if (failed > 0) {
    process.exit(1);
  }
}

runTests();
