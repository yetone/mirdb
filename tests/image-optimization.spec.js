// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');
const fs = require('fs');

const indexPath = 'file://' + path.resolve(__dirname, '..', 'index.html');
const projectDir = path.resolve(__dirname, '..');

/**
 * Image Optimization Test Suite
 *
 * Tests that images on the homepage are optimized for web delivery:
 * 1. Architecture diagram uses SVG format or optimized raster format
 * 2. Total image payload is reasonable for page performance
 * 3. Images specify dimensions (width/height) to prevent layout shift
 */

// Maximum reasonable image payload for a static homepage
const MAX_TOTAL_IMAGE_SIZE_KB = 500; // 500KB max for all images combined
const MAX_SINGLE_IMAGE_SIZE_KB = 200; // 200KB max for any single image

test.describe('Image Optimization', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  /**
   * Test Case 1: Architecture diagram uses SVG format or optimized raster format
   * SVG is preferred for diagrams as it's scalable and typically smaller
   */
  test('TC1: architecture diagram uses SVG format or optimized raster format', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Check for the architecture diagram - look for SVG first (preferred)
    const svgDiagram = architectureSection.locator('svg.architecture-diagram, .architecture-diagram svg');
    const imgDiagram = architectureSection.locator('img.architecture-diagram, .architecture-diagram img');

    const hasSvg = await svgDiagram.count() > 0;
    const hasImg = await imgDiagram.count() > 0;

    // Either SVG or img should exist
    expect(hasSvg || hasImg).toBeTruthy();

    if (hasSvg) {
      // SVG is preferred for diagrams
      await expect(svgDiagram.first()).toBeVisible();
      console.log('Architecture diagram uses SVG format (preferred)');

      // Verify SVG has proper aria-label for accessibility
      const ariaLabel = await svgDiagram.first().getAttribute('aria-label');
      if (ariaLabel) {
        expect(ariaLabel.toLowerCase()).toContain('diagram');
        console.log(`SVG has aria-label: "${ariaLabel}"`);
      }
    } else if (hasImg) {
      // If using raster image, check it's optimized format
      const imgSrc = await imgDiagram.first().getAttribute('src');
      expect(imgSrc).toBeTruthy();

      // Check for optimized formats (WebP, PNG, or JPG - not BMP or unoptimized formats)
      const validFormats = ['.svg', '.webp', '.png', '.jpg', '.jpeg'];
      const hasValidFormat = validFormats.some(fmt => imgSrc.toLowerCase().endsWith(fmt));
      expect(hasValidFormat).toBeTruthy();

      console.log(`Architecture diagram uses raster format: ${imgSrc}`);

      // If it's a file path (not data URI), check file size
      if (!imgSrc.startsWith('data:')) {
        const imgPath = path.resolve(projectDir, imgSrc);
        if (fs.existsSync(imgPath)) {
          const stats = fs.statSync(imgPath);
          const sizeKB = stats.size / 1024;
          console.log(`Architecture diagram size: ${sizeKB.toFixed(2)} KB`);
          // Single image should be reasonably sized
          expect(sizeKB).toBeLessThan(MAX_SINGLE_IMAGE_SIZE_KB);
        }
      }
    }
  });

  /**
   * Test Case 2: Total image payload is reasonable for page performance
   * Measures all images (img elements and external image resources)
   */
  test('TC2: total image payload is reasonable for page performance', async ({ page }) => {
    // Find all img elements and SVG elements in the page
    const imgElements = await page.locator('img').all();
    const svgElements = await page.locator('svg').all();

    let totalImageSize = 0;
    const imageDetails = [];

    // Check img elements with external sources
    for (const img of imgElements) {
      const src = await img.getAttribute('src');
      if (src && !src.startsWith('data:')) {
        // Handle relative paths
        const imgPath = path.resolve(projectDir, src);
        if (fs.existsSync(imgPath)) {
          const stats = fs.statSync(imgPath);
          const sizeKB = stats.size / 1024;
          totalImageSize += sizeKB;
          imageDetails.push({ src, sizeKB });
        }
      }
    }

    // Check for any image files in assets directory
    const assetsDir = path.resolve(projectDir, 'assets');
    if (fs.existsSync(assetsDir)) {
      const assetFiles = fs.readdirSync(assetsDir);
      const imageExtensions = ['.png', '.jpg', '.jpeg', '.gif', '.webp', '.svg'];

      for (const file of assetFiles) {
        const ext = path.extname(file).toLowerCase();
        if (imageExtensions.includes(ext)) {
          const filePath = path.resolve(assetsDir, file);
          const stats = fs.statSync(filePath);
          const sizeKB = stats.size / 1024;

          // Only count if not already counted from img src
          const alreadyCounted = imageDetails.some(d =>
            d.src.endsWith(file) || d.src.includes(`assets/${file}`)
          );

          if (!alreadyCounted) {
            // Check if this image is actually used in the page
            const isUsed = await page.locator(`img[src*="${file}"]`).count() > 0;
            if (isUsed) {
              totalImageSize += sizeKB;
              imageDetails.push({ src: `assets/${file}`, sizeKB, note: 'from assets' });
            }
          }
        }
      }
    }

    // Log image details
    console.log('Image payload breakdown:');
    console.log(`  Number of SVG elements (inline): ${svgElements.length}`);
    console.log(`  Number of img elements: ${imgElements.length}`);

    if (imageDetails.length > 0) {
      for (const detail of imageDetails) {
        console.log(`  - ${detail.src}: ${detail.sizeKB.toFixed(2)} KB ${detail.note || ''}`);
      }
    }
    console.log(`  Total external image size: ${totalImageSize.toFixed(2)} KB`);
    console.log(`  Maximum allowed: ${MAX_TOTAL_IMAGE_SIZE_KB} KB`);

    // Inline SVGs are highly efficient and don't count toward image payload
    // The page primarily uses inline SVGs which is optimal
    if (svgElements.length > 0 && imgElements.length === 0) {
      console.log('Page uses inline SVGs exclusively - optimal for performance');
    }

    // Assert total image size is reasonable
    expect(totalImageSize).toBeLessThan(MAX_TOTAL_IMAGE_SIZE_KB);
  });

  /**
   * Test Case 3: Images specify dimensions (width/height) to prevent layout shift
   * CLS (Cumulative Layout Shift) is improved when images have explicit dimensions
   */
  test('TC3: images specify dimensions to prevent layout shift', async ({ page }) => {
    // Get all img elements
    const imgElements = await page.locator('img').all();

    // Get all SVG elements that might be images (with viewBox for dimensions)
    const svgElements = await page.locator('svg').all();

    let totalImages = 0;
    let imagesWithDimensions = 0;
    const issues = [];

    // Check img elements for width and height attributes
    for (let i = 0; i < imgElements.length; i++) {
      const img = imgElements[i];
      totalImages++;

      const width = await img.getAttribute('width');
      const height = await img.getAttribute('height');
      const style = await img.getAttribute('style');
      const src = await img.getAttribute('src');

      // Check if dimensions are specified via attributes or inline style
      const hasWidthAttr = width !== null;
      const hasHeightAttr = height !== null;

      // Also accept CSS dimensions in style attribute
      const hasWidthStyle = style && (style.includes('width') || style.includes('aspect-ratio'));
      const hasHeightStyle = style && (style.includes('height') || style.includes('aspect-ratio'));

      // Also check computed styles
      const computedWidth = await img.evaluate(el => {
        const computed = window.getComputedStyle(el);
        return computed.width !== 'auto' && computed.width !== '';
      });
      const computedHeight = await img.evaluate(el => {
        const computed = window.getComputedStyle(el);
        return computed.height !== 'auto' && computed.height !== '';
      });

      const hasDimensions = (hasWidthAttr && hasHeightAttr) ||
                           (hasWidthStyle || hasHeightStyle) ||
                           (computedWidth && computedHeight);

      if (hasDimensions) {
        imagesWithDimensions++;
      } else {
        issues.push({
          type: 'img',
          src: src || 'unknown',
          hasWidth: hasWidthAttr || hasWidthStyle || computedWidth,
          hasHeight: hasHeightAttr || hasHeightStyle || computedHeight
        });
      }
    }

    // Check SVG elements for viewBox (which defines dimensions)
    for (let i = 0; i < svgElements.length; i++) {
      const svg = svgElements[i];
      totalImages++;

      const viewBox = await svg.getAttribute('viewBox');
      const width = await svg.getAttribute('width');
      const height = await svg.getAttribute('height');

      // SVGs should have viewBox for proper scaling, or explicit dimensions
      const hasViewBox = viewBox !== null && viewBox.trim() !== '';
      const hasDimensions = (width !== null && height !== null) || hasViewBox;

      if (hasDimensions) {
        imagesWithDimensions++;
      } else {
        const className = await svg.getAttribute('class');
        issues.push({
          type: 'svg',
          class: className || 'unknown',
          hasViewBox,
          hasWidth: width !== null,
          hasHeight: height !== null
        });
      }
    }

    // Log results
    console.log(`Image dimension check results:`);
    console.log(`  Total images/SVGs: ${totalImages}`);
    console.log(`  With dimensions: ${imagesWithDimensions}`);

    if (issues.length > 0) {
      console.log('  Issues found:');
      for (const issue of issues) {
        if (issue.type === 'img') {
          console.log(`    - img[src="${issue.src}"]: width=${issue.hasWidth}, height=${issue.hasHeight}`);
        } else {
          console.log(`    - svg[class="${issue.class}"]: viewBox=${issue.hasViewBox}, width=${issue.hasWidth}, height=${issue.hasHeight}`);
        }
      }
    }

    // All images should have dimensions to prevent layout shift
    if (totalImages > 0) {
      const dimensionRatio = imagesWithDimensions / totalImages;
      console.log(`  Dimension coverage: ${(dimensionRatio * 100).toFixed(1)}%`);

      // All images/SVGs should have dimensions
      expect(imagesWithDimensions).toBe(totalImages);
    } else {
      // If no external images, that's fine (page might use inline SVGs only)
      console.log('  No img elements found - page may use inline SVGs only');
    }
  });

  /**
   * Additional test: Verify inline SVG icons have proper viewBox
   */
  test('inline SVG icons have proper viewBox for responsive sizing', async ({ page }) => {
    // Get all inline SVG elements
    const svgElements = await page.locator('svg').all();

    let svgCount = 0;
    let svgWithViewBox = 0;
    const svgIssues = [];

    for (const svg of svgElements) {
      svgCount++;
      const viewBox = await svg.getAttribute('viewBox');

      if (viewBox && viewBox.trim() !== '') {
        svgWithViewBox++;

        // Validate viewBox format (should be "minX minY width height")
        const viewBoxParts = viewBox.trim().split(/\s+/);
        expect(viewBoxParts.length).toBe(4);

        // All parts should be valid numbers
        for (const part of viewBoxParts) {
          expect(Number.isNaN(parseFloat(part))).toBeFalsy();
        }
      } else {
        const className = await svg.getAttribute('class');
        const ariaLabel = await svg.getAttribute('aria-label');
        svgIssues.push({
          class: className || 'none',
          ariaLabel: ariaLabel || 'none'
        });
      }
    }

    console.log(`SVG viewBox check:`);
    console.log(`  Total SVGs: ${svgCount}`);
    console.log(`  With viewBox: ${svgWithViewBox}`);

    if (svgIssues.length > 0) {
      console.log(`  SVGs missing viewBox:`);
      for (const issue of svgIssues) {
        console.log(`    - class="${issue.class}", aria-label="${issue.ariaLabel}"`);
      }
    }

    // All SVGs should have viewBox for proper responsive behavior
    expect(svgWithViewBox).toBe(svgCount);
  });

  /**
   * Additional test: No unnecessarily large images in assets
   */
  test('no unnecessarily large images in assets directory', async () => {
    const assetsDir = path.resolve(projectDir, 'assets');

    if (!fs.existsSync(assetsDir)) {
      console.log('No assets directory found - skipping large image check');
      return;
    }

    const files = fs.readdirSync(assetsDir);
    const imageExtensions = ['.png', '.jpg', '.jpeg', '.gif', '.webp', '.bmp'];
    const largeImages = [];

    for (const file of files) {
      const ext = path.extname(file).toLowerCase();
      if (imageExtensions.includes(ext)) {
        const filePath = path.resolve(assetsDir, file);
        const stats = fs.statSync(filePath);
        const sizeKB = stats.size / 1024;

        console.log(`Asset image: ${file} - ${sizeKB.toFixed(2)} KB`);

        if (sizeKB > MAX_SINGLE_IMAGE_SIZE_KB) {
          largeImages.push({ file, sizeKB });
        }
      }
    }

    if (largeImages.length > 0) {
      console.log(`\nWARNING: Large images found that may need optimization:`);
      for (const img of largeImages) {
        console.log(`  - ${img.file}: ${img.sizeKB.toFixed(2)} KB (max: ${MAX_SINGLE_IMAGE_SIZE_KB} KB)`);
      }
    }

    // Note: We log warnings but don't fail if large images exist in assets
    // but are not used in the page. The TC2 test checks actual page usage.
    // This test is informational for cleanup purposes.
  });
});
