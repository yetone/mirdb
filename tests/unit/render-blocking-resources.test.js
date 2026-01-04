/**
 * Unit test for verifying no render-blocking resources
 * CSS is minimal and JS is deferred or async where possible
 */
const { JSDOM } = require('jsdom');
const fs = require('fs');
const path = require('path');

// Test configuration
const indexPath = path.join(__dirname, '../../index.html');
const cssPath = path.join(__dirname, '../../styles.css');

function runTests() {
  console.log('Running Render-Blocking Resources Tests...\n');

  let passed = 0;
  let failed = 0;
  const results = [];

  // Read the HTML file
  let html;
  let cssContent;
  try {
    html = fs.readFileSync(indexPath, 'utf-8');
    cssContent = fs.readFileSync(cssPath, 'utf-8');
  } catch (error) {
    console.error('Failed to read files:', error.message);
    process.exit(1);
  }

  // Parse HTML with JSDOM
  const dom = new JSDOM(html);
  const document = dom.window.document;

  // Test 1: CSS is minimal (under 15KB unminified)
  function testCSSSize() {
    const cssSize = Buffer.byteLength(cssContent, 'utf-8');
    const cssSizeKB = cssSize / 1024;

    if (cssSize < 15 * 1024) {
      console.log(`✓ CSS is minimal: ${cssSizeKB.toFixed(2)}KB (under 15KB)`);
      passed++;
      results.push({ test: 'CSS size', status: 'pass' });
    } else {
      console.log(`✗ CSS is too large: ${cssSizeKB.toFixed(2)}KB (should be under 15KB)`);
      failed++;
      results.push({ test: 'CSS size', status: 'fail', error: `CSS is ${cssSizeKB.toFixed(2)}KB, should be under 15KB` });
    }
  }

  // Test 2: All external scripts use defer or async attribute
  function testScriptAttributes() {
    const scripts = document.querySelectorAll('script[src]');
    const blockingScripts = [];

    scripts.forEach(script => {
      const hasDefer = script.hasAttribute('defer');
      const hasAsync = script.hasAttribute('async');
      const isModule = script.getAttribute('type') === 'module';

      // Module scripts are deferred by default
      if (!hasDefer && !hasAsync && !isModule) {
        blockingScripts.push(script.getAttribute('src'));
      }
    });

    if (blockingScripts.length === 0) {
      console.log('✓ All external scripts use defer/async or are modules (or no external scripts)');
      passed++;
      results.push({ test: 'script attributes', status: 'pass' });
    } else {
      console.log(`✗ Found ${blockingScripts.length} blocking scripts: ${blockingScripts.join(', ')}`);
      failed++;
      results.push({ test: 'script attributes', status: 'fail', error: `Blocking scripts: ${blockingScripts.join(', ')}` });
    }
  }

  // Test 3: Inline scripts are minimal or non-existent
  function testInlineScripts() {
    const inlineScripts = document.querySelectorAll('script:not([src])');
    let totalInlineSize = 0;

    inlineScripts.forEach(script => {
      totalInlineSize += script.textContent.length;
    });

    if (totalInlineSize < 1024) {
      console.log(`✓ Inline scripts are minimal: ${totalInlineSize} bytes (under 1KB)`);
      passed++;
      results.push({ test: 'inline scripts', status: 'pass' });
    } else {
      console.log(`✗ Inline scripts too large: ${totalInlineSize} bytes (should be under 1KB)`);
      failed++;
      results.push({ test: 'inline scripts', status: 'fail', error: `Inline scripts are ${totalInlineSize} bytes` });
    }
  }

  // Test 4: No render-blocking stylesheets in body
  function testBodyStylesheets() {
    const bodyStylesheets = document.querySelectorAll('body link[rel="stylesheet"]');

    if (bodyStylesheets.length === 0) {
      console.log('✓ No render-blocking stylesheets in body');
      passed++;
      results.push({ test: 'body stylesheets', status: 'pass' });
    } else {
      console.log(`✗ Found ${bodyStylesheets.length} stylesheets in body (should be in head)`);
      failed++;
      results.push({ test: 'body stylesheets', status: 'fail', error: `${bodyStylesheets.length} stylesheets in body` });
    }
  }

  // Test 5: Reasonable number of stylesheets in head
  function testStylesheetCount() {
    const headStylesheets = document.querySelectorAll('head link[rel="stylesheet"]');

    if (headStylesheets.length <= 3) {
      console.log(`✓ Reasonable number of stylesheets: ${headStylesheets.length} (3 or fewer)`);
      passed++;
      results.push({ test: 'stylesheet count', status: 'pass' });
    } else {
      console.log(`✗ Too many stylesheets: ${headStylesheets.length} (should be 3 or fewer)`);
      failed++;
      results.push({ test: 'stylesheet count', status: 'fail', error: `${headStylesheets.length} stylesheets` });
    }
  }

  // Test 6: CSS does not use @import (render-blocking)
  function testCSSImport() {
    const importMatches = cssContent.match(/@import\s+/g);
    const importCount = importMatches ? importMatches.length : 0;

    if (importCount === 0) {
      console.log('✓ CSS does not use @import (no render-blocking imports)');
      passed++;
      results.push({ test: 'CSS import', status: 'pass' });
    } else {
      console.log(`✗ CSS uses ${importCount} @import statements (render-blocking)`);
      failed++;
      results.push({ test: 'CSS import', status: 'fail', error: `${importCount} @import statements` });
    }
  }

  // Test 7: HTML file is reasonable size (under 20KB)
  function testHTMLSize() {
    const htmlSize = Buffer.byteLength(html, 'utf-8');
    const htmlSizeKB = htmlSize / 1024;

    if (htmlSize < 20 * 1024) {
      console.log(`✓ HTML file is reasonable size: ${htmlSizeKB.toFixed(2)}KB (under 20KB)`);
      passed++;
      results.push({ test: 'HTML size', status: 'pass' });
    } else {
      console.log(`✗ HTML file is too large: ${htmlSizeKB.toFixed(2)}KB (should be under 20KB)`);
      failed++;
      results.push({ test: 'HTML size', status: 'fail', error: `HTML is ${htmlSizeKB.toFixed(2)}KB` });
    }
  }

  // Test 8: No blocking preload/prefetch misconfiguration
  function testPreloads() {
    const preloads = document.querySelectorAll('link[rel="preload"]');
    const misconfigured = [];

    preloads.forEach(link => {
      const as = link.getAttribute('as');
      const href = link.getAttribute('href');

      // Preloaded resources should have an 'as' attribute
      if (!as) {
        misconfigured.push(href);
      }
    });

    if (misconfigured.length === 0) {
      console.log('✓ No misconfigured preload links');
      passed++;
      results.push({ test: 'preload config', status: 'pass' });
    } else {
      console.log(`✗ Found ${misconfigured.length} misconfigured preloads: ${misconfigured.join(', ')}`);
      failed++;
      results.push({ test: 'preload config', status: 'fail', error: `Misconfigured: ${misconfigured.join(', ')}` });
    }
  }

  // Run all tests
  testCSSSize();
  testScriptAttributes();
  testInlineScripts();
  testBodyStylesheets();
  testStylesheetCount();
  testCSSImport();
  testHTMLSize();
  testPreloads();

  // Summary
  console.log(`\n${'='.repeat(50)}`);
  console.log(`Tests: ${passed} passed, ${failed} failed`);
  console.log(`${'='.repeat(50)}\n`);

  if (failed > 0) {
    process.exit(1);
  }
}

runTests();
