/**
 * Simple Node.js script to verify the usage.gif is correctly referenced in index.html
 * This runs without a browser for compatibility
 */

const fs = require('fs');
const path = require('path');

console.log('🔍 Verifying usage GIF display in homepage...\n');

try {
  // Read the HTML file
  const htmlPath = path.join(__dirname, '..', 'index.html');
  const htmlContent = fs.readFileSync(htmlPath, 'utf8');

  console.log('✅ Step 1: index.html file exists and is readable');

  // Check 1: Find usage.gif element
  const usageGifMatch = htmlContent.match(/<img[^>]*id="usage-gif"[^>]*src="([^"]*)"/);
  if (!usageGifMatch) {
    console.error('❌ FAIL: Could not find usage.gif element with id="usage-gif"');
    process.exit(1);
  }
  console.log('✅ Step 2: Found usage.gif element with id="usage-gif"');

  const src = usageGifMatch[1];
  console.log(`   Source path: ${src}`);

  // Check 2: Verify src attribute contains usage.gif
  if (!src.includes('usage.gif')) {
    console.error('❌ FAIL: Image src does not contain "usage.gif"');
    process.exit(1);
  }
  console.log('✅ Step 3: Image src correctly references usage.gif');

  // Check 3: Verify alt attribute
  const altMatch = htmlContent.match(/<img[^>]*id="usage-gif"[^>]*alt="([^"]*)"/);
  if (!altMatch) {
    console.error('❌ FAIL: Missing alt attribute');
    process.exit(1);
  }
  console.log(`✅ Step 4: Found alt attribute: "${altMatch[1]}"`);

  // Check 4: Verify it's in a Usage section
  if (!htmlContent.includes('<h2 class="section-title">Usage</h2>')) {
    console.error('❌ FAIL: Could not find Usage section');
    process.exit(1);
  }
  console.log('✅ Step 5: GIF is located in Usage section');

  // Check 5: Find CSS classes
  if (!htmlContent.includes('class="usage-gif"')) {
    console.error('❌ FAIL: Missing usage-gif CSS class');
    process.exit(1);
  }
  console.log('✅ Step 6: GIF has usage-gif CSS class for styling');

  // Check 6: Verify the GIF file actually exists
  const gifPath = path.join(__dirname, '..', 'assets', 'usage.gif');
  if (!fs.existsSync(gifPath)) {
    console.error('❌ FAIL: usage.gif file does not exist in assets/ directory');
    process.exit(1);
  }

  const stats = fs.statSync(gifPath);
  console.log(`✅ Step 7: usage.gif file exists (${(stats.size / 1024 / 1024).toFixed(2)} MB)`);

  // Check 7: Verify responsive styling exists
  if (!htmlContent.includes('max-width: 100%')) {
    console.error('❌ FAIL: Missing responsive styling (max-width: 100%)');
    process.exit(1);
  }
  console.log('✅ Step 8: Responsive CSS styling is present');

  // Check 8: Verify ALT text mentions MirDB usage
  if (!altMatch[1].includes('Usage') || !altMatch[1].includes('MirDB')) {
    console.error('❌ FAIL: ALT text does not describe MirDB usage properly');
    process.exit(1);
  }
  console.log(`✅ Step 9: ALT text properly describes MirDB usage`);

  // Check 9: Verify ALT text length is reasonable
  if (altMatch[1].length < 10 || altMatch[1].length > 80) {
    console.error('❌ FAIL: ALT text length ${altMatch[1].length} is not appropriate (expected 10-80 chars)');
    process.exit(1);
  }
  console.log(`✅ Step 10: ALT text length is appropriate (${altMatch[1].length} chars)`);

  // Check 10: Verify the GIF is within a proper context
  const usageSectionContent = htmlContent.match(/<section[^>]*class="section"[^>]*>[\s\S]*?<h2 class="section-title">Usage<\/h2>[\s\S]*?<img[^>]*id="usage-gif"[\s\S]*?<\/section>/);
  if (!usageSectionContent) {
    console.error('❌ FAIL: GIF is not properly wrapped in Usage section');
    process.exit(1);
  }
  console.log('✅ Step 11: GIF is properly wrapped in Usage section');

  console.log('\n✅ ✅ ✅  ALL CHECKS PASSED!  ✅ ✅ ✅');
  console.log('\nThe usage GIF is correctly implemented in the homepage:');
  console.log('  - HTML element exists with id="usage-gif"');
  console.log('  - Src path correctly references assets/usage.gif');
  console.log('  - ALT text is descriptive and accessible');
  console.log('  - GIF is located in Usage section');
  console.log('  - Responsive styling is applied');
  console.log('  - Asset file exists and is accessible');
  process.exit(0);

} catch (error) {
  console.error('❌ ERROR:', error.message);
  process.exit(1);
}
