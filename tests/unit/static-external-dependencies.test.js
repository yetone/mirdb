/**
 * Unit test for verifying the page has no external dependencies (CDNs/external JS)
 * This ensures the page can work offline and as a static file.
 */
const { JSDOM } = require('jsdom');
const fs = require('fs');
const path = require('path');

// Test configuration
const indexPath = path.join(__dirname, '../../index.html');
const stylesPath = path.join(__dirname, '../../styles.css');

function runTests() {
  console.log('Running Static External Dependencies Tests...\n');

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

  // Test 1: No external JavaScript files (CDN scripts)
  function testNoExternalScripts() {
    const scripts = document.querySelectorAll('script[src]');
    const externalScripts = [];

    scripts.forEach(script => {
      const src = script.getAttribute('src');
      if (src && (src.startsWith('http://') || src.startsWith('https://') || src.startsWith('//'))) {
        externalScripts.push(src);
      }
    });

    if (externalScripts.length === 0) {
      console.log('✓ No external JavaScript dependencies (CDNs) found');
      passed++;
      results.push({ test: 'no_external_scripts', status: 'pass' });
    } else {
      console.log(`✗ External JavaScript dependencies found: ${externalScripts.join(', ')}`);
      failed++;
      results.push({ test: 'no_external_scripts', status: 'fail', error: `External scripts: ${externalScripts.join(', ')}` });
    }
  }

  // Test 2: No external CSS files (except local styles.css)
  function testNoExternalStylesheets() {
    const stylesheets = document.querySelectorAll('link[rel="stylesheet"]');
    const externalStylesheets = [];

    stylesheets.forEach(link => {
      const href = link.getAttribute('href');
      if (href && (href.startsWith('http://') || href.startsWith('https://') || href.startsWith('//'))) {
        externalStylesheets.push(href);
      }
    });

    if (externalStylesheets.length === 0) {
      console.log('✓ No external CSS dependencies (CDNs) found');
      passed++;
      results.push({ test: 'no_external_stylesheets', status: 'pass' });
    } else {
      console.log(`✗ External CSS dependencies found: ${externalStylesheets.join(', ')}`);
      failed++;
      results.push({ test: 'no_external_stylesheets', status: 'fail', error: `External stylesheets: ${externalStylesheets.join(', ')}` });
    }
  }

  // Test 3: No external fonts (Google Fonts, etc.)
  function testNoExternalFonts() {
    const allLinks = document.querySelectorAll('link');
    const externalFonts = [];

    allLinks.forEach(link => {
      const href = link.getAttribute('href') || '';
      const rel = link.getAttribute('rel') || '';

      // Check for font preloads or Google Fonts
      if ((href.includes('fonts.googleapis.com') ||
           href.includes('fonts.gstatic.com') ||
           (rel.includes('preconnect') && href.includes('font'))) &&
          (href.startsWith('http://') || href.startsWith('https://') || href.startsWith('//'))) {
        externalFonts.push(href);
      }
    });

    // Also check for @import in inline styles
    const styles = document.querySelectorAll('style');
    styles.forEach(style => {
      const content = style.textContent || '';
      if (content.includes('@import') && content.includes('font')) {
        externalFonts.push('@import rule with font');
      }
    });

    if (externalFonts.length === 0) {
      console.log('✓ No external font dependencies found');
      passed++;
      results.push({ test: 'no_external_fonts', status: 'pass' });
    } else {
      console.log(`✗ External font dependencies found: ${externalFonts.join(', ')}`);
      failed++;
      results.push({ test: 'no_external_fonts', status: 'fail', error: `External fonts: ${externalFonts.join(', ')}` });
    }
  }

  // Test 4: Local stylesheet exists and is properly linked
  function testLocalStylesheet() {
    const localStylesheet = document.querySelector('link[href="styles.css"]');

    if (localStylesheet) {
      // Verify the file exists
      if (fs.existsSync(stylesPath)) {
        console.log('✓ Local stylesheet (styles.css) is properly linked and exists');
        passed++;
        results.push({ test: 'local_stylesheet', status: 'pass' });
      } else {
        console.log('✗ Local stylesheet is linked but file does not exist');
        failed++;
        results.push({ test: 'local_stylesheet', status: 'fail', error: 'styles.css file not found' });
      }
    } else {
      console.log('✗ No local stylesheet linked');
      failed++;
      results.push({ test: 'local_stylesheet', status: 'fail', error: 'No local stylesheet link found' });
    }
  }

  // Test 5: CSS uses system fonts (no external font loading required)
  function testSystemFonts() {
    try {
      const css = fs.readFileSync(stylesPath, 'utf-8');

      // Check if font-family uses system fonts
      const usesSystemFonts = css.includes('-apple-system') ||
                              css.includes('BlinkMacSystemFont') ||
                              css.includes('system-ui') ||
                              css.includes('Segoe UI');

      // Check for @font-face declarations (would require external or bundled fonts)
      const hasFontFace = css.includes('@font-face');

      // Check for external font imports
      const hasExternalImport = css.includes('@import') &&
                                (css.includes('fonts.googleapis') || css.includes('http'));

      if (usesSystemFonts && !hasFontFace && !hasExternalImport) {
        console.log('✓ CSS uses system fonts - no external font loading required');
        passed++;
        results.push({ test: 'system_fonts', status: 'pass' });
      } else {
        const issues = [];
        if (!usesSystemFonts) issues.push('No system fonts detected');
        if (hasFontFace) issues.push('@font-face found');
        if (hasExternalImport) issues.push('External font import found');
        console.log(`✗ Font dependency issues: ${issues.join(', ')}`);
        failed++;
        results.push({ test: 'system_fonts', status: 'fail', error: issues.join(', ') });
      }
    } catch (error) {
      console.log(`✗ Could not read styles.css: ${error.message}`);
      failed++;
      results.push({ test: 'system_fonts', status: 'fail', error: error.message });
    }
  }

  // Test 6: No external images required for basic functionality
  function testNoExternalImages() {
    const images = document.querySelectorAll('img');
    const externalImages = [];

    images.forEach(img => {
      const src = img.getAttribute('src');
      if (src && (src.startsWith('http://') || src.startsWith('https://') || src.startsWith('//'))) {
        externalImages.push(src);
      }
    });

    // Also check for CSS background images in inline styles
    const elementsWithStyle = document.querySelectorAll('[style*="background"]');
    elementsWithStyle.forEach(el => {
      const style = el.getAttribute('style') || '';
      if (style.includes('url(') && (style.includes('http://') || style.includes('https://'))) {
        externalImages.push('Inline style with external background');
      }
    });

    if (externalImages.length === 0) {
      console.log('✓ No external image dependencies required for basic functionality');
      passed++;
      results.push({ test: 'no_external_images', status: 'pass' });
    } else {
      console.log(`✗ External images found: ${externalImages.join(', ')}`);
      failed++;
      results.push({ test: 'no_external_images', status: 'fail', error: `External images: ${externalImages.join(', ')}` });
    }
  }

  // Run all tests
  testNoExternalScripts();
  testNoExternalStylesheets();
  testNoExternalFonts();
  testLocalStylesheet();
  testSystemFonts();
  testNoExternalImages();

  // Summary
  console.log(`\n${'='.repeat(50)}`);
  console.log(`Tests: ${passed} passed, ${failed} failed`);
  console.log(`${'='.repeat(50)}\n`);

  if (failed > 0) {
    process.exit(1);
  }
}

runTests();
