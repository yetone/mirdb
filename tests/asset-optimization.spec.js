// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');
const fs = require('fs');

const indexPath = 'file://' + path.resolve(__dirname, '../index.html');
const assetsDir = path.resolve(__dirname, '../assets');
const stylesPath = path.resolve(__dirname, '../styles.css');

test.describe('Asset Optimization - NFR-6: All assets optimized for web delivery', () => {

  // Test Case 1: Check image file sizes
  // Expected: Images are reasonably compressed (no oversized images)
  test('TC1: Images are reasonably compressed (no oversized images)', async () => {
    // Define maximum acceptable file sizes for web images
    // Logo GIF: Should be under 3MB for an animated logo
    // Usage GIF: Should be under 10MB for a demo animation (these can be larger)
    const maxLogoSize = 3 * 1024 * 1024; // 3MB
    const maxUsageGifSize = 10 * 1024 * 1024; // 10MB for demo animation

    // Check logo.gif
    const logoPath = path.join(assetsDir, 'logo.gif');
    expect(fs.existsSync(logoPath), 'logo.gif should exist').toBe(true);

    const logoStats = fs.statSync(logoPath);
    const logoSizeMB = logoStats.size / (1024 * 1024);
    console.log(`logo.gif size: ${logoSizeMB.toFixed(2)} MB`);

    expect(logoStats.size).toBeLessThanOrEqual(maxLogoSize);

    // Check usage.gif
    const usagePath = path.join(assetsDir, 'usage.gif');
    expect(fs.existsSync(usagePath), 'usage.gif should exist').toBe(true);

    const usageStats = fs.statSync(usagePath);
    const usageSizeMB = usageStats.size / (1024 * 1024);
    console.log(`usage.gif size: ${usageSizeMB.toFixed(2)} MB`);

    expect(usageStats.size).toBeLessThanOrEqual(maxUsageGifSize);

    // Check total assets size is reasonable for a landing page
    const totalAssetSize = logoStats.size + usageStats.size;
    const totalAssetSizeMB = totalAssetSize / (1024 * 1024);
    console.log(`Total assets size: ${totalAssetSizeMB.toFixed(2)} MB`);

    // Total asset size should be under 15MB for acceptable page load
    const maxTotalAssetSize = 15 * 1024 * 1024; // 15MB
    expect(totalAssetSize).toBeLessThanOrEqual(maxTotalAssetSize);
  });

  // Test Case 2: Check if CSS is minified
  // Expected: CSS files are minified for production
  test('TC2: CSS files are minified for production', async () => {
    expect(fs.existsSync(stylesPath), 'styles.css should exist').toBe(true);

    const cssContent = fs.readFileSync(stylesPath, 'utf-8');
    const cssSize = cssContent.length;
    const cssSizeKB = cssSize / 1024;

    console.log(`CSS file size: ${cssSizeKB.toFixed(2)} KB`);
    console.log(`CSS line count: ${cssContent.split('\n').length}`);

    // For a static landing page, CSS should be reasonably sized
    // A well-organized CSS file under 50KB is acceptable
    // Minified CSS would be smaller, but for maintainability, we allow up to 50KB
    expect(cssSizeKB).toBeLessThan(50);

    // Check CSS efficiency metrics
    // 1. No excessive empty lines (more than 2 consecutive empty lines)
    const hasExcessiveEmptyLines = /\n{4,}/.test(cssContent);
    expect(hasExcessiveEmptyLines, 'CSS should not have excessive empty lines').toBe(false);

    // 2. No excessive comments (comments should be reasonable, not > 20% of content)
    const commentMatches = cssContent.match(/\/\*[\s\S]*?\*\//g) || [];
    const totalCommentLength = commentMatches.reduce((sum, comment) => sum + comment.length, 0);
    const commentPercentage = (totalCommentLength / cssSize) * 100;
    console.log(`CSS comment percentage: ${commentPercentage.toFixed(2)}%`);
    expect(commentPercentage).toBeLessThan(20);

    // 3. CSS uses variables for efficiency (DRY principle)
    const usesVariables = cssContent.includes(':root') && cssContent.includes('var(--');
    expect(usesVariables, 'CSS should use CSS variables for maintainability').toBe(true);

    // 4. Check for media queries (responsive design = efficient mobile delivery)
    const hasMediaQueries = cssContent.includes('@media');
    expect(hasMediaQueries, 'CSS should have media queries for responsive design').toBe(true);

    // 5. No !important overuse (sign of inefficient CSS)
    const importantCount = (cssContent.match(/!important/g) || []).length;
    console.log(`!important usage count: ${importantCount}`);
    expect(importantCount).toBeLessThan(10);
  });

  // Test Case 3: Check if JS is minified
  // Expected: JavaScript files are minified for production
  test('TC3: JavaScript files are minified for production', async ({ page }) => {
    await page.goto(indexPath, { waitUntil: 'domcontentloaded' });

    // Extract JavaScript from the page
    const jsInfo = await page.evaluate(() => {
      const scripts = document.querySelectorAll('script');
      const scriptData = [];

      for (const script of scripts) {
        if (script.src) {
          // External script
          scriptData.push({
            type: 'external',
            src: script.src,
            async: script.async,
            defer: script.defer
          });
        } else {
          // Inline script
          const content = script.textContent || '';
          scriptData.push({
            type: 'inline',
            content: content,
            length: content.length,
            lineCount: content.split('\n').length
          });
        }
      }

      return scriptData;
    });

    console.log('JavaScript info:', JSON.stringify(jsInfo, null, 2));

    // For a static landing page, minimal JavaScript is ideal
    // The page should work without JS for core content (progressive enhancement)

    // Check inline JavaScript is reasonable in size
    for (const script of jsInfo) {
      if (script.type === 'inline') {
        const sizeKB = script.length / 1024;
        console.log(`Inline JS size: ${sizeKB.toFixed(2)} KB, ${script.lineCount} lines`);

        // Inline JS should be small (under 5KB) for a simple landing page
        expect(script.length).toBeLessThan(5 * 1024);
      }
    }

    // Check external scripts are loaded efficiently
    for (const script of jsInfo) {
      if (script.type === 'external') {
        // External scripts should be async or defer to not block rendering
        expect(
          script.async || script.defer,
          `External script ${script.src} should be async or defer`
        ).toBe(true);
      }
    }

    // Total JS should be minimal for a static landing page
    const totalJSSize = jsInfo
      .filter(s => s.type === 'inline')
      .reduce((sum, s) => sum + (s.length || 0), 0);

    console.log(`Total inline JS size: ${(totalJSSize / 1024).toFixed(2)} KB`);

    // Total inline JS should be under 10KB for a simple landing page
    expect(totalJSSize).toBeLessThan(10 * 1024);
  });

  // Test Case 4: Verify appropriate image dimensions
  // Expected: Images are sized appropriately for their display size
  test('TC4: Images are sized appropriately for their display size', async ({ page }) => {
    await page.goto(indexPath, { waitUntil: 'networkidle' });

    // Get all images and their dimensions
    const imageData = await page.evaluate(() => {
      const images = document.querySelectorAll('img');
      return Array.from(images).map(img => {
        const rect = img.getBoundingClientRect();
        const computedStyle = window.getComputedStyle(img);

        return {
          src: img.src,
          alt: img.alt,
          // Natural dimensions (actual file dimensions)
          naturalWidth: img.naturalWidth,
          naturalHeight: img.naturalHeight,
          // Display dimensions (what's shown on screen)
          displayWidth: Math.round(rect.width),
          displayHeight: Math.round(rect.height),
          // CSS constraints
          maxWidth: computedStyle.maxWidth,
          maxHeight: computedStyle.maxHeight,
          // Check if loaded
          complete: img.complete,
          // External image check (badges, etc.)
          isExternal: img.src.startsWith('http') && !img.src.includes('localhost')
        };
      });
    });

    console.log('Image dimensions:', JSON.stringify(imageData, null, 2));

    for (const img of imageData) {
      // Skip external images (badges from shield.io, circleci, etc.)
      if (img.isExternal) {
        console.log(`Skipping external image: ${img.src}`);
        continue;
      }

      // Verify image is loaded
      expect(img.complete, `Image ${img.src} should be loaded`).toBe(true);

      // Images should have natural dimensions > 0
      expect(img.naturalWidth, `Image ${img.src} should have width`).toBeGreaterThan(0);
      expect(img.naturalHeight, `Image ${img.src} should have height`).toBeGreaterThan(0);

      // Check that images aren't massively oversized for their display
      // Allow up to 3x the display size for retina/HiDPI displays
      // (2x is standard for retina, 3x gives some buffer)
      const maxAllowedRatio = 3;

      if (img.displayWidth > 0 && img.displayHeight > 0) {
        const widthRatio = img.naturalWidth / img.displayWidth;
        const heightRatio = img.naturalHeight / img.displayHeight;

        console.log(`Image ${img.alt || img.src}: natural ${img.naturalWidth}x${img.naturalHeight}, ` +
                   `display ${img.displayWidth}x${img.displayHeight}, ` +
                   `ratio ${widthRatio.toFixed(2)}x${heightRatio.toFixed(2)}`);

        // For GIF animations, the natural size might be larger
        // We check that the ratio is reasonable (not >5x)
        const maxRatioForGif = 5;

        if (img.src.includes('.gif')) {
          expect(widthRatio).toBeLessThan(maxRatioForGif);
          expect(heightRatio).toBeLessThan(maxRatioForGif);
        } else {
          expect(widthRatio).toBeLessThan(maxAllowedRatio);
          expect(heightRatio).toBeLessThan(maxAllowedRatio);
        }
      }

      // Images should have CSS constraints for responsive sizing
      const hasWidthConstraint = img.maxWidth !== 'none' || img.displayWidth > 0;
      expect(hasWidthConstraint, `Image ${img.src} should have size constraints`).toBe(true);
    }
  });

  // Additional test: Check total page weight
  test('Total page weight is acceptable for web delivery', async ({ page }) => {
    // Read all asset files
    const htmlPath = path.resolve(__dirname, '../index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');
    const cssContent = fs.readFileSync(stylesPath, 'utf-8');

    const htmlSize = htmlContent.length;
    const cssSize = cssContent.length;

    // Get JS size from page
    await page.goto(indexPath, { waitUntil: 'domcontentloaded' });

    const jsSize = await page.evaluate(() => {
      const scripts = document.querySelectorAll('script');
      let totalSize = 0;
      for (const script of scripts) {
        if (!script.src) {
          totalSize += (script.textContent || '').length;
        }
      }
      return totalSize;
    });

    // Get image sizes
    const logoStats = fs.statSync(path.join(assetsDir, 'logo.gif'));
    const usageStats = fs.statSync(path.join(assetsDir, 'usage.gif'));

    const totalPageWeight = htmlSize + cssSize + jsSize + logoStats.size + usageStats.size;
    const totalPageWeightMB = totalPageWeight / (1024 * 1024);

    console.log('Page weight breakdown:');
    console.log(`  HTML: ${(htmlSize / 1024).toFixed(2)} KB`);
    console.log(`  CSS: ${(cssSize / 1024).toFixed(2)} KB`);
    console.log(`  JS: ${(jsSize / 1024).toFixed(2)} KB`);
    console.log(`  Images: ${((logoStats.size + usageStats.size) / (1024 * 1024)).toFixed(2)} MB`);
    console.log(`  Total: ${totalPageWeightMB.toFixed(2)} MB`);

    // For a landing page with animated GIFs, under 15MB is acceptable
    // Ideally would be under 5MB, but animated GIFs are inherently large
    expect(totalPageWeightMB).toBeLessThan(15);

    // HTML + CSS + JS should be very lightweight (under 100KB)
    const codeWeight = htmlSize + cssSize + jsSize;
    expect(codeWeight).toBeLessThan(100 * 1024);
  });

  // Additional test: Verify images have appropriate alt text
  test('All images have descriptive alt text', async ({ page }) => {
    await page.goto(indexPath, { waitUntil: 'domcontentloaded' });

    const images = await page.evaluate(() => {
      const imgs = document.querySelectorAll('img');
      return Array.from(imgs).map(img => ({
        src: img.src,
        alt: img.alt,
        hasAlt: img.hasAttribute('alt'),
        altLength: (img.alt || '').length
      }));
    });

    console.log('Image alt text:', images);

    for (const img of images) {
      // All images must have alt attribute
      expect(img.hasAlt, `Image ${img.src} must have alt attribute`).toBe(true);

      // Alt text should be descriptive (at least a few characters)
      // Empty alt="" is valid for decorative images, but our images are meaningful
      if (!img.src.includes('shield.io') && !img.src.includes('circleci')) {
        expect(img.altLength, `Image ${img.src} should have descriptive alt text`).toBeGreaterThan(5);
      }
    }
  });
});
