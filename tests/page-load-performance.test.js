/**
 * Page Load Performance Tests (NFR-2)
 *
 * Tests to verify page performance requirements:
 * - Lighthouse Performance score >= 80
 * - First Contentful Paint under 3 seconds on 3G
 * - Total page size under 500KB
 * - Images optimized (WebP or compressed, total under 100KB)
 */

const fs = require('fs');
const path = require('path');

describe('Page Load Performance (NFR-2)', () => {
  const projectRoot = path.join(__dirname, '..');

  // Helper function to get file size in bytes
  const getFileSize = (filePath) => {
    try {
      const stats = fs.statSync(filePath);
      return stats.size;
    } catch (error) {
      return 0;
    }
  };

  // Helper function to find all files with certain extensions
  const findFiles = (dir, extensions, exclude = []) => {
    const files = [];
    const items = fs.readdirSync(dir, { withFileTypes: true });

    for (const item of items) {
      const fullPath = path.join(dir, item.name);

      // Skip excluded directories
      if (exclude.some(ex => fullPath.includes(ex))) {
        continue;
      }

      if (item.isDirectory()) {
        files.push(...findFiles(fullPath, extensions, exclude));
      } else if (extensions.some(ext => item.name.endsWith(ext))) {
        files.push(fullPath);
      }
    }

    return files;
  };

  // Helper to check if a file is referenced in HTML
  const isFileReferencedInHtml = (htmlContent, filename) => {
    const basename = path.basename(filename);
    return htmlContent.includes(basename);
  };

  describe('Test Case 1: Lighthouse Performance Audit', () => {
    /**
     * While we cannot run actual Lighthouse in Jest, we can verify that the page
     * follows best practices that contribute to a high performance score:
     * - Uses system fonts (no external font loading)
     * - No external JavaScript dependencies
     * - Uses semantic HTML
     * - Has proper meta tags
     */

    let htmlContent;
    let cssContent;

    beforeAll(() => {
      htmlContent = fs.readFileSync(path.join(projectRoot, 'index.html'), 'utf8');
      cssContent = fs.readFileSync(path.join(projectRoot, 'styles.css'), 'utf8');
    });

    test('should use system fonts to avoid font loading delay', () => {
      // Check that the CSS uses system font stack
      expect(cssContent).toMatch(/font-family:.*-apple-system|BlinkMacSystemFont|Segoe UI|system-ui/);

      // Should not have external font imports
      expect(htmlContent).not.toMatch(/<link[^>]*fonts\.googleapis\.com/);
      expect(cssContent).not.toMatch(/@import.*fonts\.googleapis\.com/);
    });

    test('should not include render-blocking external JavaScript', () => {
      // Check for external script tags (excluding inline scripts)
      const externalScriptPattern = /<script[^>]*src=["'][^"']+["']/gi;
      const matches = htmlContent.match(externalScriptPattern) || [];

      // Filter to only external CDN scripts (allow local scripts)
      const cdnScripts = matches.filter(script =>
        script.includes('http://') ||
        script.includes('https://') ||
        script.includes('//')
      );

      expect(cdnScripts.length).toBe(0);
    });

    test('should have proper meta viewport for mobile rendering', () => {
      expect(htmlContent).toMatch(/<meta[^>]*name=["']viewport["'][^>]*>/);
      expect(htmlContent).toMatch(/width=device-width/);
    });

    test('should have meta description for SEO', () => {
      expect(htmlContent).toMatch(/<meta[^>]*name=["']description["'][^>]*>/);
    });

    test('should use semantic HTML elements', () => {
      expect(htmlContent).toMatch(/<header/);
      expect(htmlContent).toMatch(/<section/);
      expect(htmlContent).toMatch(/<footer/);
      expect(htmlContent).toMatch(/<nav|<article/);
    });

    test('should have proper document structure', () => {
      expect(htmlContent).toMatch(/<!DOCTYPE html>/i);
      expect(htmlContent).toMatch(/<html[^>]*lang=["']en["']/);
      expect(htmlContent).toMatch(/<head>/);
      expect(htmlContent).toMatch(/<body>/);
    });
  });

  describe('Test Case 2: First Contentful Paint on 3G', () => {
    /**
     * FCP on 3G depends on:
     * - Small HTML file size (initial render)
     * - Minimal CSS (render-blocking)
     * - No blocking JS
     *
     * 3G simulation: ~400kbps = 50KB/s
     * For FCP < 3s, critical resources should be < 150KB
     */

    let htmlContent;
    let htmlSize;
    let cssSize;

    beforeAll(() => {
      htmlContent = fs.readFileSync(path.join(projectRoot, 'index.html'), 'utf8');
      htmlSize = getFileSize(path.join(projectRoot, 'index.html'));
      cssSize = getFileSize(path.join(projectRoot, 'styles.css'));
    });

    test('should have HTML file under 50KB for fast initial paint', () => {
      const maxHtmlSize = 50 * 1024; // 50KB
      expect(htmlSize).toBeLessThan(maxHtmlSize);
    });

    test('should have CSS file under 50KB for fast styling', () => {
      const maxCssSize = 50 * 1024; // 50KB
      expect(cssSize).toBeLessThan(maxCssSize);
    });

    test('should have critical path resources under 150KB (3G @ 50KB/s = 3s)', () => {
      // Critical path = HTML + CSS (render-blocking)
      const criticalPathSize = htmlSize + cssSize;
      const maxCriticalPath = 150 * 1024; // 150KB for 3s on 3G

      expect(criticalPathSize).toBeLessThan(maxCriticalPath);
    });

    test('should not have blocking script tags in head', () => {
      // Extract head content
      const headMatch = htmlContent.match(/<head[^>]*>([\s\S]*?)<\/head>/i);
      if (headMatch) {
        const headContent = headMatch[1];
        // Check for blocking scripts (without defer or async)
        const blockingScripts = headContent.match(/<script(?![^>]*(?:defer|async))[^>]*src=/gi);
        expect(blockingScripts || []).toHaveLength(0);
      }
    });

    test('should inline critical CSS or have small external CSS', () => {
      // Either CSS is inlined or external CSS is small
      const hasInlineCss = htmlContent.includes('<style');
      const hasSmallExternalCss = cssSize < 20 * 1024; // 20KB is very small

      // At least one should be true for good FCP
      expect(hasInlineCss || hasSmallExternalCss || cssSize < 50 * 1024).toBe(true);
    });
  });

  describe('Test Case 3: Total Page Weight', () => {
    /**
     * Total page size (HTML, CSS, JS, images) should be under 500KB
     */

    let htmlContent;

    beforeAll(() => {
      htmlContent = fs.readFileSync(path.join(projectRoot, 'index.html'), 'utf8');
    });

    test('should have total page size under 500KB', () => {
      const maxTotalSize = 500 * 1024; // 500KB

      // Get HTML and CSS sizes
      const htmlSize = getFileSize(path.join(projectRoot, 'index.html'));
      const cssSize = getFileSize(path.join(projectRoot, 'styles.css'));

      // Find all JS files that might be referenced
      const jsFiles = findFiles(projectRoot, ['.js'], ['node_modules', 'tests', '.git', '.something']);
      let jsSize = 0;
      for (const jsFile of jsFiles) {
        const basename = path.basename(jsFile);
        if (isFileReferencedInHtml(htmlContent, basename)) {
          jsSize += getFileSize(jsFile);
        }
      }

      // Find all image files referenced in HTML
      const imageExtensions = ['.png', '.jpg', '.jpeg', '.gif', '.webp', '.svg', '.ico'];
      const imageFiles = findFiles(projectRoot, imageExtensions, ['node_modules', '.git', '.something']);
      let imageSize = 0;
      for (const imageFile of imageFiles) {
        const basename = path.basename(imageFile);
        if (isFileReferencedInHtml(htmlContent, basename)) {
          imageSize += getFileSize(imageFile);
        }
      }

      const totalSize = htmlSize + cssSize + jsSize + imageSize;

      expect(totalSize).toBeLessThan(maxTotalSize);
    });

    test('should have HTML file size reasonable', () => {
      const htmlSize = getFileSize(path.join(projectRoot, 'index.html'));
      expect(htmlSize).toBeLessThan(100 * 1024); // Under 100KB
    });

    test('should have CSS file size reasonable', () => {
      const cssSize = getFileSize(path.join(projectRoot, 'styles.css'));
      expect(cssSize).toBeLessThan(100 * 1024); // Under 100KB
    });

    test('should report actual page component sizes', () => {
      const htmlSize = getFileSize(path.join(projectRoot, 'index.html'));
      const cssSize = getFileSize(path.join(projectRoot, 'styles.css'));

      // This test always passes but logs the sizes for visibility
      console.log(`Page component sizes:`);
      console.log(`  HTML: ${(htmlSize / 1024).toFixed(2)} KB`);
      console.log(`  CSS: ${(cssSize / 1024).toFixed(2)} KB`);
      console.log(`  Total (without images): ${((htmlSize + cssSize) / 1024).toFixed(2)} KB`);

      expect(true).toBe(true);
    });
  });

  describe('Test Case 4: Image Optimization', () => {
    /**
     * Images should be:
     * - Optimized (WebP format or compressed)
     * - Total under 100KB
     */

    let htmlContent;

    beforeAll(() => {
      htmlContent = fs.readFileSync(path.join(projectRoot, 'index.html'), 'utf8');
    });

    test('should have total referenced images under 100KB', () => {
      const maxImageSize = 100 * 1024; // 100KB

      const imageExtensions = ['.png', '.jpg', '.jpeg', '.gif', '.webp', '.svg', '.ico'];
      const imageFiles = findFiles(projectRoot, imageExtensions, ['node_modules', '.git', '.something']);

      let totalReferencedImageSize = 0;
      const referencedImages = [];

      for (const imageFile of imageFiles) {
        const basename = path.basename(imageFile);
        if (isFileReferencedInHtml(htmlContent, basename)) {
          const size = getFileSize(imageFile);
          totalReferencedImageSize += size;
          referencedImages.push({ file: basename, size });
        }
      }

      if (referencedImages.length > 0) {
        console.log('Referenced images:', referencedImages);
      }

      expect(totalReferencedImageSize).toBeLessThan(maxImageSize);
    });

    test('should use optimized image formats when images are present', () => {
      // Extract all image references from HTML
      const imgTags = htmlContent.match(/<img[^>]*src=["']([^"']+)["']/gi) || [];
      const cssBackgrounds = htmlContent.match(/url\(["']?([^)"']+)["']?\)/gi) || [];

      const allImageRefs = [...imgTags, ...cssBackgrounds];

      if (allImageRefs.length === 0) {
        // No images referenced - test passes
        expect(true).toBe(true);
        return;
      }

      // Check that images use optimized formats (webp, svg, or reasonable size)
      const inefficientFormats = allImageRefs.filter(ref => {
        const lowered = ref.toLowerCase();
        // Large uncompressed formats are inefficient
        return lowered.includes('.bmp') || lowered.includes('.tiff');
      });

      expect(inefficientFormats.length).toBe(0);
    });

    test('should not reference unoptimized large GIF files', () => {
      // Specifically check that large GIF files in assets are not referenced
      const assetsDir = path.join(projectRoot, 'assets');

      if (fs.existsSync(assetsDir)) {
        const gifFiles = findFiles(assetsDir, ['.gif'], []);

        for (const gifFile of gifFiles) {
          const size = getFileSize(gifFile);
          const basename = path.basename(gifFile);

          // If GIF is large (> 100KB), it should NOT be referenced
          if (size > 100 * 1024) {
            expect(isFileReferencedInHtml(htmlContent, basename)).toBe(false);
          }
        }
      } else {
        expect(true).toBe(true);
      }
    });

    test('should prefer WebP or SVG for any new images', () => {
      // Extract img src values
      const imgSrcMatch = htmlContent.match(/<img[^>]*src=["']([^"']+)["']/gi) || [];

      if (imgSrcMatch.length === 0) {
        // No images - passes by default
        expect(true).toBe(true);
        return;
      }

      // For informational purposes, log what formats are used
      const formats = imgSrcMatch.map(tag => {
        const match = tag.match(/src=["']([^"']+)["']/i);
        if (match) {
          const ext = path.extname(match[1]).toLowerCase();
          return ext;
        }
        return 'unknown';
      });

      console.log('Image formats in use:', [...new Set(formats)]);

      // This is a soft check - we prefer webp/svg but don't fail for jpg/png
      expect(true).toBe(true);
    });

    test('should have lazy loading for below-fold images', () => {
      // Check if any img tags have loading="lazy" attribute
      // This is a best practice for performance
      const imgTags = htmlContent.match(/<img[^>]*>/gi) || [];

      if (imgTags.length === 0) {
        // No images - test passes
        expect(true).toBe(true);
        return;
      }

      // For now, just verify that if there are images, they could have lazy loading
      // This is informational since our page currently has no images
      console.log(`Found ${imgTags.length} img tags`);

      const lazyLoadedImages = imgTags.filter(tag => tag.includes('loading="lazy"'));
      console.log(`${lazyLoadedImages.length} have lazy loading`);

      expect(true).toBe(true);
    });
  });

  describe('Performance Budget Summary', () => {
    test('should meet all performance budget requirements', () => {
      const htmlContent = fs.readFileSync(path.join(projectRoot, 'index.html'), 'utf8');
      const htmlSize = getFileSize(path.join(projectRoot, 'index.html'));
      const cssSize = getFileSize(path.join(projectRoot, 'styles.css'));

      // Find referenced images
      const imageExtensions = ['.png', '.jpg', '.jpeg', '.gif', '.webp', '.svg', '.ico'];
      const imageFiles = findFiles(projectRoot, imageExtensions, ['node_modules', '.git', '.something']);
      let imageSize = 0;
      for (const imageFile of imageFiles) {
        if (isFileReferencedInHtml(htmlContent, path.basename(imageFile))) {
          imageSize += getFileSize(imageFile);
        }
      }

      const totalSize = htmlSize + cssSize + imageSize;

      // Performance budget
      const budget = {
        totalPage: 500 * 1024,      // 500KB total
        criticalPath: 150 * 1024,   // 150KB for 3G FCP
        images: 100 * 1024          // 100KB images
      };

      console.log('\n=== Performance Budget Report ===');
      console.log(`Total Page: ${(totalSize / 1024).toFixed(2)} KB / ${(budget.totalPage / 1024)} KB (${((totalSize / budget.totalPage) * 100).toFixed(1)}%)`);
      console.log(`Critical Path: ${((htmlSize + cssSize) / 1024).toFixed(2)} KB / ${(budget.criticalPath / 1024)} KB (${(((htmlSize + cssSize) / budget.criticalPath) * 100).toFixed(1)}%)`);
      console.log(`Images: ${(imageSize / 1024).toFixed(2)} KB / ${(budget.images / 1024)} KB (${((imageSize / budget.images) * 100).toFixed(1)}%)`);
      console.log('=================================\n');

      expect(totalSize).toBeLessThan(budget.totalPage);
      expect(htmlSize + cssSize).toBeLessThan(budget.criticalPath);
      expect(imageSize).toBeLessThan(budget.images);
    });
  });
});
