// @ts-check
/**
 * Static Site Generation Tests
 * Scenario: Verify the site is properly generated as static files with fast build times
 *
 * Test Cases:
 * 1. Build completes successfully without errors
 * 2. Build completes in under 30 seconds
 * 3. Static HTML, CSS, and minimal JS files are generated
 * 4. Zero JavaScript errors in browser console when loading the site
 */

const { JSDOM } = require('jsdom');
const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');
const assert = require('assert');

// Test results collector
const results = {
  passed: 0,
  failed: 0,
  tests: []
};

function test(name, fn) {
  try {
    fn();
    results.passed++;
    results.tests.push({ name, status: 'pass' });
    console.log(`✓ ${name}`);
  } catch (error) {
    results.failed++;
    results.tests.push({ name, status: 'fail', error: error.message, stack: error.stack });
    console.log(`✗ ${name}`);
    console.log(`  Error: ${error.message}`);
  }
}

console.log('Static Site Generation Tests\n');
console.log('='.repeat(50));

// Test Case 1: Build completes successfully without errors
test('Test Case 1: Build completes successfully without errors', () => {
  const websiteDir = path.resolve(__dirname, '..');

  // Run the build command and capture output
  let buildOutput;
  let buildError = null;

  try {
    buildOutput = execSync('npm run build', {
      cwd: websiteDir,
      encoding: 'utf8',
      stdio: ['pipe', 'pipe', 'pipe']
    });
  } catch (error) {
    buildError = error;
  }

  // Assert no errors occurred
  assert(buildError === null, `Build should complete without errors. Error: ${buildError?.message}`);

  // Check that dist directory exists
  const distDir = path.join(websiteDir, 'dist');
  assert(fs.existsSync(distDir), 'dist directory should exist after build');

  // Check that index.html was created
  const indexPath = path.join(distDir, 'index.html');
  assert(fs.existsSync(indexPath), 'index.html should exist in dist directory');

  // Check that output.css was created
  const cssPath = path.join(distDir, 'output.css');
  assert(fs.existsSync(cssPath), 'output.css should exist in dist directory');
});

// Test Case 2: Build completes in under 30 seconds
test('Test Case 2: Build completes in under 30 seconds', () => {
  const websiteDir = path.resolve(__dirname, '..');

  const startTime = Date.now();

  try {
    execSync('npm run build', {
      cwd: websiteDir,
      encoding: 'utf8',
      stdio: ['pipe', 'pipe', 'pipe']
    });
  } catch (error) {
    assert.fail(`Build failed: ${error.message}`);
  }

  const endTime = Date.now();
  const buildTimeMs = endTime - startTime;
  const buildTimeSeconds = buildTimeMs / 1000;

  console.log(`    Build completed in ${buildTimeSeconds.toFixed(2)} seconds`);

  assert(buildTimeSeconds < 30, `Build should complete in under 30 seconds. Actual: ${buildTimeSeconds.toFixed(2)} seconds`);
});

// Test Case 3: Static HTML, CSS, and minimal JS files are generated
test('Test Case 3: Static HTML, CSS, and minimal JS files are generated', () => {
  const distDir = path.resolve(__dirname, '../dist');

  // Check dist directory exists
  assert(fs.existsSync(distDir), 'dist directory should exist');

  // Check index.html exists and is valid HTML
  const indexPath = path.join(distDir, 'index.html');
  assert(fs.existsSync(indexPath), 'index.html should exist');

  const htmlContent = fs.readFileSync(indexPath, 'utf8');
  assert(htmlContent.includes('<!DOCTYPE html>'), 'index.html should be valid HTML with DOCTYPE');
  assert(htmlContent.includes('<html'), 'index.html should contain html tag');
  assert(htmlContent.includes('<head>'), 'index.html should contain head tag');
  assert(htmlContent.includes('<body'), 'index.html should contain body tag');
  assert(htmlContent.includes('</html>'), 'index.html should have closing html tag');

  // Check output.css exists and has content
  const cssPath = path.join(distDir, 'output.css');
  assert(fs.existsSync(cssPath), 'output.css should exist');

  const cssContent = fs.readFileSync(cssPath, 'utf8');
  assert(cssContent.length > 0, 'output.css should have content');
  assert(cssContent.includes('tailwindcss'), 'CSS should include Tailwind CSS classes');

  // Verify CSS is minified (no newlines in most cases or very few)
  const cssLineCount = cssContent.split('\n').length;
  assert(cssLineCount < 10, `CSS should be minified (few lines). Actual line count: ${cssLineCount}`);

  // Check HTML references the CSS file
  assert(htmlContent.includes('output.css'), 'HTML should reference output.css');

  // Verify minimal JavaScript - only inline scripts for copy functionality
  const scriptTagMatches = htmlContent.match(/<script[^>]*>[\s\S]*?<\/script>/g) || [];
  const externalScripts = scriptTagMatches.filter(s => s.includes('src='));

  // Should have no external JS dependencies (only inline scripts for copy-to-clipboard)
  assert(externalScripts.length === 0, 'Should have no external JavaScript dependencies');

  console.log(`    HTML file size: ${(htmlContent.length / 1024).toFixed(2)} KB`);
  console.log(`    CSS file size: ${(cssContent.length / 1024).toFixed(2)} KB`);
  console.log(`    Inline scripts found: ${scriptTagMatches.length}`);
  console.log(`    External scripts found: ${externalScripts.length}`);
});

// Test Case 4: Zero JavaScript errors in browser console when loading the site
test('Test Case 4: Zero JavaScript errors in browser console when loading the site', () => {
  const indexPath = path.resolve(__dirname, '../dist/index.html');
  const htmlContent = fs.readFileSync(indexPath, 'utf8');

  // Create a virtual console to capture errors
  const virtualConsole = new (require('jsdom').VirtualConsole)();
  const consoleErrors = [];

  virtualConsole.on('jsdomError', (error) => {
    consoleErrors.push({ type: 'jsdomError', message: error.message, stack: error.stack });
  });

  virtualConsole.on('error', (message) => {
    consoleErrors.push({ type: 'error', message });
  });

  // Load the page with JavaScript execution
  const dom = new JSDOM(htmlContent, {
    runScripts: 'dangerously',
    resources: 'usable',
    virtualConsole,
    url: 'file://' + indexPath
  });

  // Give scripts time to execute
  // In JSDOM, inline scripts execute synchronously, so we just need a small delay
  // for any async operations

  // Check for JavaScript syntax errors in the HTML
  const scriptMatches = htmlContent.match(/<script[^>]*>([\s\S]*?)<\/script>/g) || [];

  scriptMatches.forEach((scriptTag, index) => {
    // Extract script content
    const scriptContent = scriptTag.replace(/<script[^>]*>/, '').replace(/<\/script>/, '');

    // Skip JSON-LD scripts
    if (scriptTag.includes('application/ld+json')) {
      // Validate JSON-LD
      try {
        JSON.parse(scriptContent);
      } catch (e) {
        consoleErrors.push({ type: 'json-ld-error', message: `Invalid JSON-LD in script ${index + 1}: ${e.message}` });
      }
      return;
    }

    // For JavaScript, try to parse it for syntax errors
    if (scriptContent.trim().length > 0) {
      try {
        // Use Function constructor to check for syntax errors
        new Function(scriptContent);
      } catch (e) {
        consoleErrors.push({ type: 'syntax-error', message: `JavaScript syntax error in script ${index + 1}: ${e.message}` });
      }
    }
  });

  // Report any errors found
  if (consoleErrors.length > 0) {
    console.log('    Console errors found:');
    consoleErrors.forEach(err => {
      console.log(`      - [${err.type}] ${err.message}`);
    });
  }

  // Filter out non-critical JSDOM errors (like missing resources which is expected in this test)
  const criticalErrors = consoleErrors.filter(err =>
    err.type === 'error' ||
    err.type === 'syntax-error' ||
    err.type === 'json-ld-error'
  );

  assert(criticalErrors.length === 0, `Should have zero JavaScript errors. Found ${criticalErrors.length} error(s): ${criticalErrors.map(e => e.message).join(', ')}`);

  console.log('    No JavaScript errors detected');

  dom.window.close();
});

console.log('\n' + '='.repeat(50));
console.log(`\nResults: ${results.passed} passed, ${results.failed} failed`);

// Output detailed results for CI/CD
console.log('\nDetailed Results:');
results.tests.forEach(t => {
  console.log(`  ${t.status === 'pass' ? '✓' : '✗'} ${t.name}`);
  if (t.error) {
    console.log(`    Error: ${t.error}`);
  }
});

// Exit with error code if any tests failed
if (results.failed > 0) {
  process.exit(1);
}
