/**
 * CSS Validation Tests
 *
 * Test Case 1: W3C CSS validation - verifies CSS has no validation errors
 * Test Case 2: Browser prefix checking - ensures vendor prefixes are included
 */

const fs = require('fs');
const path = require('path');
const { validate } = require('csstree-validator');

const cssPath = path.join(__dirname, '../../styles.css');

function runTests() {
  console.log('Running CSS Validation Tests...\n');

  let passed = 0;
  let failed = 0;
  const results = [];

  // Read the CSS file
  let cssContent;
  try {
    cssContent = fs.readFileSync(cssPath, 'utf-8');
  } catch (error) {
    console.error('Failed to read styles.css:', error.message);
    process.exit(1);
  }

  // =========================================================================
  // TEST CASE 1: W3C CSS Validation
  // =========================================================================

  // Test 1.1: CSS file should have no syntax errors
  function testCssSyntaxErrors() {
    const result = validate(cssContent);

    // Filter out warnings about vendor prefixes (these are intentional)
    const errors = result.filter(issue => {
      const msg = issue.message || '';
      // Skip vendor prefix warnings (these are intentional for cross-browser support)
      if (msg.includes('-webkit-')) return false;
      if (msg.includes('-moz-')) return false;
      if (msg.includes('-ms-')) return false;
      if (msg.includes('-o-')) return false;
      return true;
    });

    if (errors.length === 0) {
      console.log('✓ CSS has no syntax errors');
      passed++;
      results.push({ test: 'CSS syntax validation', status: 'pass' });
    } else {
      const errorMessages = errors.map(e =>
        `Line ${e.line}:${e.column} - ${e.message}`
      ).join('\n  ');
      console.log(`✗ CSS validation errors found:\n  ${errorMessages}`);
      failed++;
      results.push({
        test: 'CSS syntax validation',
        status: 'fail',
        error: errorMessages
      });
    }
  }

  // Test 1.2: CSS file should exist and be readable
  function testCssFileExists() {
    if (fs.existsSync(cssPath) && cssContent.length > 0) {
      console.log('✓ CSS file exists and is readable');
      passed++;
      results.push({ test: 'CSS file exists', status: 'pass' });
    } else {
      console.log('✗ CSS file does not exist or is empty');
      failed++;
      results.push({
        test: 'CSS file exists',
        status: 'fail',
        error: 'CSS file not found or empty'
      });
    }
  }

  // Test 1.3: CSS should have valid structure with proper selectors
  function testValidSelectors() {
    const selectorPatterns = [
      { pattern: /\*[,\s]/, name: 'Universal selector (*)' },
      { pattern: /body\s*{/, name: 'Element selector (body)' },
      { pattern: /\.[\w-]+\s*{/, name: 'Class selectors' },
      { pattern: /:root\s*{/, name: ':root pseudo selector' },
      { pattern: /@media\s*\(/, name: 'Media queries' }
    ];

    const missing = selectorPatterns.filter(s => !s.pattern.test(cssContent));

    if (missing.length === 0) {
      console.log('✓ CSS has valid selectors structure');
      passed++;
      results.push({ test: 'Valid CSS selectors', status: 'pass' });
    } else {
      const missingNames = missing.map(m => m.name).join(', ');
      console.log(`✗ Missing CSS selectors: ${missingNames}`);
      failed++;
      results.push({
        test: 'Valid CSS selectors',
        status: 'fail',
        error: `Missing selectors: ${missingNames}`
      });
    }
  }

  // Test 1.4: CSS should have properly closed blocks
  function testBalancedBraces() {
    const openBraces = (cssContent.match(/{/g) || []).length;
    const closeBraces = (cssContent.match(/}/g) || []).length;

    if (openBraces === closeBraces) {
      console.log(`✓ CSS has balanced braces (${openBraces} opening, ${closeBraces} closing)`);
      passed++;
      results.push({ test: 'Balanced CSS braces', status: 'pass' });
    } else {
      console.log(`✗ CSS has unbalanced braces: ${openBraces} opening vs ${closeBraces} closing`);
      failed++;
      results.push({
        test: 'Balanced CSS braces',
        status: 'fail',
        error: `Unbalanced braces: ${openBraces} opening vs ${closeBraces} closing`
      });
    }
  }

  // Test 1.5: CSS should have valid property-value pairs
  function testValidProperties() {
    const propertyPatterns = [
      { pattern: /box-sizing\s*:\s*border-box/, name: 'box-sizing' },
      { pattern: /margin\s*:\s*0/, name: 'margin' },
      { pattern: /display\s*:\s*(flex|grid|block|inline-block)/, name: 'display' },
      { pattern: /color\s*:\s*(var\(|#)/, name: 'color' },
      { pattern: /background-color\s*:/, name: 'background-color' },
      { pattern: /font-family\s*:/, name: 'font-family' },
      { pattern: /font-size\s*:/, name: 'font-size' }
    ];

    const missing = propertyPatterns.filter(p => !p.pattern.test(cssContent));

    if (missing.length === 0) {
      console.log('✓ CSS has valid property-value pairs');
      passed++;
      results.push({ test: 'Valid CSS properties', status: 'pass' });
    } else {
      const missingNames = missing.map(m => m.name).join(', ');
      console.log(`✗ Missing CSS properties: ${missingNames}`);
      failed++;
      results.push({
        test: 'Valid CSS properties',
        status: 'fail',
        error: `Missing properties: ${missingNames}`
      });
    }
  }

  // Test 1.6: CSS custom properties should be properly defined
  function testCssVariables() {
    const hasVariables = /--[\w-]+\s*:\s*[^;]+;/.test(cssContent);
    const rootMatch = cssContent.match(/:root\s*{([^}]+)}/);

    const expectedVars = ['--primary-color', '--text-color', '--bg-color'];
    const missingVars = [];

    if (rootMatch) {
      expectedVars.forEach(varName => {
        if (!rootMatch[1].includes(varName)) {
          missingVars.push(varName);
        }
      });
    }

    if (hasVariables && rootMatch && missingVars.length === 0) {
      console.log('✓ CSS custom properties (variables) are properly defined');
      passed++;
      results.push({ test: 'CSS custom properties', status: 'pass' });
    } else {
      const error = missingVars.length > 0
        ? `Missing variables: ${missingVars.join(', ')}`
        : 'CSS variables not properly defined';
      console.log(`✗ ${error}`);
      failed++;
      results.push({
        test: 'CSS custom properties',
        status: 'fail',
        error: error
      });
    }
  }

  // =========================================================================
  // TEST CASE 2: Browser Vendor Prefixes
  // =========================================================================

  // Test 2.1: Should include -webkit- prefix for flexbox properties
  function testWebkitFlexbox() {
    const flexboxPrefixes = [
      { pattern: /-webkit-flex/, name: '-webkit-flex' },
      { pattern: /-webkit-justify-content/, name: '-webkit-justify-content' },
      { pattern: /-webkit-align-items/, name: '-webkit-align-items' }
    ];

    const missing = flexboxPrefixes.filter(p => !p.pattern.test(cssContent));

    if (missing.length === 0) {
      console.log('✓ Includes -webkit- prefix for flexbox properties');
      passed++;
      results.push({ test: 'Webkit flexbox prefixes', status: 'pass' });
    } else {
      const missingNames = missing.map(m => m.name).join(', ');
      console.log(`✗ Missing webkit flexbox prefixes: ${missingNames}`);
      failed++;
      results.push({
        test: 'Webkit flexbox prefixes',
        status: 'fail',
        error: `Missing: ${missingNames}`
      });
    }
  }

  // Test 2.2: Should include -webkit- prefix for sticky positioning
  function testWebkitSticky() {
    const hasWebkitSticky = /position\s*:\s*-webkit-sticky/.test(cssContent);
    const hasSticky = /position\s*:\s*sticky/.test(cssContent);

    if (hasWebkitSticky && hasSticky) {
      console.log('✓ Includes -webkit- prefix for sticky positioning');
      passed++;
      results.push({ test: 'Webkit sticky prefix', status: 'pass' });
    } else {
      const missing = [];
      if (!hasWebkitSticky) missing.push('-webkit-sticky');
      if (!hasSticky) missing.push('sticky');
      console.log(`✗ Missing sticky positioning: ${missing.join(', ')}`);
      failed++;
      results.push({
        test: 'Webkit sticky prefix',
        status: 'fail',
        error: `Missing: ${missing.join(', ')}`
      });
    }
  }

  // Test 2.3: Should include -webkit- prefix for scroll-behavior
  function testWebkitScrollBehavior() {
    const hasWebkitScroll = /-webkit-scroll-behavior\s*:\s*smooth/.test(cssContent);
    const hasScroll = /scroll-behavior\s*:\s*smooth/.test(cssContent);

    if (hasWebkitScroll && hasScroll) {
      console.log('✓ Includes -webkit- prefix for scroll-behavior');
      passed++;
      results.push({ test: 'Webkit scroll-behavior prefix', status: 'pass' });
    } else {
      const missing = [];
      if (!hasWebkitScroll) missing.push('-webkit-scroll-behavior');
      if (!hasScroll) missing.push('scroll-behavior');
      console.log(`✗ Missing scroll-behavior: ${missing.join(', ')}`);
      failed++;
      results.push({
        test: 'Webkit scroll-behavior prefix',
        status: 'fail',
        error: `Missing: ${missing.join(', ')}`
      });
    }
  }

  // Test 2.4: Should include -webkit- prefix for linear-gradient
  function testWebkitGradient() {
    const hasWebkitGradient = /-webkit-linear-gradient/.test(cssContent);
    const hasGradient = /linear-gradient/.test(cssContent);

    if (hasWebkitGradient && hasGradient) {
      console.log('✓ Includes -webkit- prefix for linear-gradient');
      passed++;
      results.push({ test: 'Webkit gradient prefix', status: 'pass' });
    } else {
      const missing = [];
      if (!hasWebkitGradient) missing.push('-webkit-linear-gradient');
      if (!hasGradient) missing.push('linear-gradient');
      console.log(`✗ Missing gradient: ${missing.join(', ')}`);
      failed++;
      results.push({
        test: 'Webkit gradient prefix',
        status: 'fail',
        error: `Missing: ${missing.join(', ')}`
      });
    }
  }

  // Test 2.5: Should include vendor prefixes for flex-direction
  function testFlexDirection() {
    const hasWebkitFlexDir = /-webkit-flex-direction/.test(cssContent);
    const hasFlexDir = /flex-direction/.test(cssContent);

    if (hasWebkitFlexDir && hasFlexDir) {
      console.log('✓ Includes vendor prefixes for flex-direction');
      passed++;
      results.push({ test: 'Flex-direction prefixes', status: 'pass' });
    } else {
      const missing = [];
      if (!hasWebkitFlexDir) missing.push('-webkit-flex-direction');
      if (!hasFlexDir) missing.push('flex-direction');
      console.log(`✗ Missing flex-direction: ${missing.join(', ')}`);
      failed++;
      results.push({
        test: 'Flex-direction prefixes',
        status: 'fail',
        error: `Missing: ${missing.join(', ')}`
      });
    }
  }

  // Test 2.6: Should include vendor prefixes for flex-wrap
  function testFlexWrap() {
    const hasWebkitFlexWrap = /-webkit-flex-wrap/.test(cssContent);
    const hasFlexWrap = /flex-wrap/.test(cssContent);

    if (hasWebkitFlexWrap && hasFlexWrap) {
      console.log('✓ Includes vendor prefixes for flex-wrap');
      passed++;
      results.push({ test: 'Flex-wrap prefixes', status: 'pass' });
    } else {
      const missing = [];
      if (!hasWebkitFlexWrap) missing.push('-webkit-flex-wrap');
      if (!hasFlexWrap) missing.push('flex-wrap');
      console.log(`✗ Missing flex-wrap: ${missing.join(', ')}`);
      failed++;
      results.push({
        test: 'Flex-wrap prefixes',
        status: 'fail',
        error: `Missing: ${missing.join(', ')}`
      });
    }
  }

  // Test 2.7: Standard property should follow vendor-prefixed version
  function testPrefixOrder() {
    // Check that display: flex follows display: -webkit-flex
    const displayFlexPattern = /display\s*:\s*-webkit-flex[\s\S]*?display\s*:\s*flex/;
    const hasCorrectOrder = displayFlexPattern.test(cssContent);

    if (hasCorrectOrder) {
      console.log('✓ Standard properties follow vendor-prefixed versions');
      passed++;
      results.push({ test: 'Prefix order', status: 'pass' });
    } else {
      console.log('✗ Standard property should follow vendor-prefixed version');
      failed++;
      results.push({
        test: 'Prefix order',
        status: 'fail',
        error: 'display: flex should follow display: -webkit-flex'
      });
    }
  }

  // Test 2.8: Comprehensive cross-browser flexbox support
  function testCrossBrowserFlexbox() {
    const requiredPrefixes = [
      'display: -webkit-flex',
      'display: flex',
      '-webkit-justify-content:',
      'justify-content:',
      '-webkit-align-items:',
      'align-items:'
    ];

    const missing = requiredPrefixes.filter(p => !cssContent.includes(p));

    if (missing.length === 0) {
      console.log('✓ Cross-browser flexbox layout is fully supported');
      passed++;
      results.push({ test: 'Cross-browser flexbox', status: 'pass' });
    } else {
      console.log(`✗ Missing cross-browser flexbox support: ${missing.join(', ')}`);
      failed++;
      results.push({
        test: 'Cross-browser flexbox',
        status: 'fail',
        error: `Missing: ${missing.join(', ')}`
      });
    }
  }

  // Test 2.9: Vendor prefixes in responsive media queries
  function testMediaQueryPrefixes() {
    const mediaQueryContent = cssContent.match(/@media[^{]+{[\s\S]*?}\s*}/g);

    if (mediaQueryContent) {
      const mediaQueryCSS = mediaQueryContent.join('');
      const hasWebkitInMedia = /-webkit-flex/.test(mediaQueryCSS);

      if (hasWebkitInMedia) {
        console.log('✓ Vendor prefixes are included in media queries');
        passed++;
        results.push({ test: 'Media query prefixes', status: 'pass' });
      } else {
        console.log('✗ Media queries should include vendor prefixes');
        failed++;
        results.push({
          test: 'Media query prefixes',
          status: 'fail',
          error: 'Missing -webkit- prefixes in media queries'
        });
      }
    } else {
      console.log('✗ No media queries found');
      failed++;
      results.push({
        test: 'Media query prefixes',
        status: 'fail',
        error: 'No media queries found in CSS'
      });
    }
  }

  // Run all tests
  console.log('=== Test Case 1: W3C CSS Validation ===\n');
  testCssFileExists();
  testCssSyntaxErrors();
  testValidSelectors();
  testBalancedBraces();
  testValidProperties();
  testCssVariables();

  console.log('\n=== Test Case 2: Browser Vendor Prefixes ===\n');
  testWebkitFlexbox();
  testWebkitSticky();
  testWebkitScrollBehavior();
  testWebkitGradient();
  testFlexDirection();
  testFlexWrap();
  testPrefixOrder();
  testCrossBrowserFlexbox();
  testMediaQueryPrefixes();

  // Summary
  console.log(`\n${'='.repeat(50)}`);
  console.log(`Tests: ${passed} passed, ${failed} failed`);
  console.log(`${'='.repeat(50)}\n`);

  if (failed > 0) {
    process.exit(1);
  }
}

runTests();
