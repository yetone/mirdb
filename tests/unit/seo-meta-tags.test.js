/**
 * SEO Meta Tags Tests
 * Verifies that the page has proper meta tags for SEO (NFR-5)
 *
 * Test Cases:
 * 1. Check title tag - Page has descriptive title tag containing 'MirDB'
 * 2. Check meta description - Meta description tag is present with compelling, relevant content
 * 3. Check viewport meta tag - Viewport meta tag is set for responsive design
 * 4. Check Open Graph tags - OG tags (og:title, og:description, og:image) are present for social sharing
 * 5. Check canonical URL - Canonical link tag is present
 */
const { JSDOM } = require('jsdom');
const fs = require('fs');
const path = require('path');

const indexPath = path.join(__dirname, '../../index.html');

function runTests() {
  console.log('Running SEO Meta Tags Tests...\n');

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

  // Test 1: Check title tag contains 'MirDB'
  function testTitleTag() {
    const title = document.querySelector('title');

    if (!title) {
      console.log('✗ Page should have a title tag');
      failed++;
      results.push({
        test: 'Title tag',
        status: 'fail',
        error: 'Title tag is missing'
      });
      return;
    }

    const titleText = title.textContent;

    if (titleText && titleText.includes('MirDB')) {
      console.log(`✓ Page has descriptive title tag containing 'MirDB': "${titleText}"`);
      passed++;
      results.push({ test: 'Title tag', status: 'pass' });
    } else {
      console.log(`✗ Page title should contain 'MirDB'. Found: "${titleText}"`);
      failed++;
      results.push({
        test: 'Title tag',
        status: 'fail',
        error: `Title does not contain 'MirDB'. Found: "${titleText}"`
      });
    }
  }

  // Test 2: Check meta description
  function testMetaDescription() {
    const metaDescription = document.querySelector('meta[name="description"]');

    if (!metaDescription) {
      console.log('✗ Meta description tag is missing');
      failed++;
      results.push({
        test: 'Meta description',
        status: 'fail',
        error: 'Meta description tag is missing'
      });
      return;
    }

    const content = metaDescription.getAttribute('content');

    if (content && content.length >= 50 && content.length <= 160) {
      console.log(`✓ Meta description tag is present with compelling content (${content.length} chars)`);
      passed++;
      results.push({ test: 'Meta description', status: 'pass' });
    } else if (content && content.length > 0) {
      // Description present but might not be optimal length
      if (content.length < 50) {
        console.log(`⚠ Meta description is too short (${content.length} chars). Recommended: 50-160 chars`);
        console.log(`  Content: "${content}"`);
      } else if (content.length > 160) {
        console.log(`⚠ Meta description is too long (${content.length} chars). Recommended: 50-160 chars`);
      }
      // Still pass if content exists
      console.log(`✓ Meta description tag is present with relevant content`);
      passed++;
      results.push({ test: 'Meta description', status: 'pass' });
    } else {
      console.log('✗ Meta description content is empty or invalid');
      failed++;
      results.push({
        test: 'Meta description',
        status: 'fail',
        error: 'Meta description content is empty or invalid'
      });
    }
  }

  // Test 3: Check viewport meta tag
  function testViewportMeta() {
    const viewport = document.querySelector('meta[name="viewport"]');

    if (!viewport) {
      console.log('✗ Viewport meta tag is missing');
      failed++;
      results.push({
        test: 'Viewport meta tag',
        status: 'fail',
        error: 'Viewport meta tag is missing'
      });
      return;
    }

    const content = viewport.getAttribute('content');

    if (content && content.includes('width=device-width')) {
      console.log(`✓ Viewport meta tag is set for responsive design: "${content}"`);
      passed++;
      results.push({ test: 'Viewport meta tag', status: 'pass' });
    } else {
      console.log(`✗ Viewport meta tag should include 'width=device-width'. Found: "${content}"`);
      failed++;
      results.push({
        test: 'Viewport meta tag',
        status: 'fail',
        error: `Viewport meta tag missing 'width=device-width'. Found: "${content}"`
      });
    }
  }

  // Test 4: Check Open Graph tags
  function testOpenGraphTags() {
    const ogTitle = document.querySelector('meta[property="og:title"]');
    const ogDescription = document.querySelector('meta[property="og:description"]');
    const ogImage = document.querySelector('meta[property="og:image"]');

    const missingTags = [];
    const presentTags = [];

    if (ogTitle && ogTitle.getAttribute('content')) {
      presentTags.push('og:title');
    } else {
      missingTags.push('og:title');
    }

    if (ogDescription && ogDescription.getAttribute('content')) {
      presentTags.push('og:description');
    } else {
      missingTags.push('og:description');
    }

    if (ogImage && ogImage.getAttribute('content')) {
      presentTags.push('og:image');
    } else {
      missingTags.push('og:image');
    }

    if (missingTags.length === 0) {
      console.log(`✓ OG tags (og:title, og:description, og:image) are present for social sharing`);
      passed++;
      results.push({ test: 'Open Graph tags', status: 'pass' });
    } else {
      console.log(`✗ Missing OG tags: ${missingTags.join(', ')}`);
      if (presentTags.length > 0) {
        console.log(`  Present OG tags: ${presentTags.join(', ')}`);
      }
      failed++;
      results.push({
        test: 'Open Graph tags',
        status: 'fail',
        error: `Missing OG tags: ${missingTags.join(', ')}`
      });
    }
  }

  // Test 5: Check canonical URL
  function testCanonicalUrl() {
    const canonical = document.querySelector('link[rel="canonical"]');

    if (!canonical) {
      console.log('✗ Canonical link tag is missing');
      failed++;
      results.push({
        test: 'Canonical URL',
        status: 'fail',
        error: 'Canonical link tag is missing'
      });
      return;
    }

    const href = canonical.getAttribute('href');

    if (href && href.length > 0) {
      console.log(`✓ Canonical link tag is present: "${href}"`);
      passed++;
      results.push({ test: 'Canonical URL', status: 'pass' });
    } else {
      console.log('✗ Canonical link tag href is empty');
      failed++;
      results.push({
        test: 'Canonical URL',
        status: 'fail',
        error: 'Canonical link tag href is empty'
      });
    }
  }

  // Run all tests
  testTitleTag();
  testMetaDescription();
  testViewportMeta();
  testOpenGraphTags();
  testCanonicalUrl();

  // Summary
  console.log(`\n${'='.repeat(50)}`);
  console.log(`SEO Meta Tags Tests: ${passed} passed, ${failed} failed`);
  console.log(`${'='.repeat(50)}\n`);

  if (failed > 0) {
    process.exit(1);
  }
}

runTests();
