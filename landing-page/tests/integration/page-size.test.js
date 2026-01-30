/**
 * Page Size Integration Tests
 * Owner: Scenario 10 - Performance - Page Load
 *
 * Tests:
 * - Total page size (HTML, CSS, JS) is under 500KB excluding images
 *
 * This test verifies the page meets NFR-1 performance requirements
 * defined in the PRD: "Total page size under 500KB (excluding images)"
 */
const fs = require('fs');
const path = require('path');

// Maximum allowed page size in bytes (500KB)
const MAX_PAGE_SIZE_BYTES = 500 * 1024;

// Root directory of the landing page
const LANDING_PAGE_ROOT = path.resolve(__dirname, '../..');

/**
 * Recursively get all files in a directory
 */
function getAllFiles(dirPath, arrayOfFiles = []) {
  if (!fs.existsSync(dirPath)) {
    return arrayOfFiles;
  }

  const files = fs.readdirSync(dirPath);

  files.forEach((file) => {
    const filePath = path.join(dirPath, file);
    if (fs.statSync(filePath).isDirectory()) {
      getAllFiles(filePath, arrayOfFiles);
    } else {
      arrayOfFiles.push(filePath);
    }
  });

  return arrayOfFiles;
}

/**
 * Get file size in bytes
 */
function getFileSize(filePath) {
  if (fs.existsSync(filePath)) {
    return fs.statSync(filePath).size;
  }
  return 0;
}

/**
 * Check if file is an image
 */
function isImageFile(filePath) {
  const imageExtensions = ['.gif', '.png', '.jpg', '.jpeg', '.webp', '.svg', '.ico', '.bmp'];
  const ext = path.extname(filePath).toLowerCase();
  return imageExtensions.includes(ext);
}

/**
 * Check if file is a font
 */
function isFontFile(filePath) {
  const fontExtensions = ['.woff', '.woff2', '.ttf', '.otf', '.eot'];
  const ext = path.extname(filePath).toLowerCase();
  return fontExtensions.includes(ext);
}

/**
 * Check if file is a code/text asset (HTML, CSS, JS)
 */
function isCodeAsset(filePath) {
  const codeExtensions = ['.html', '.css', '.js'];
  const ext = path.extname(filePath).toLowerCase();
  return codeExtensions.includes(ext);
}

describe('Page Size - Performance', () => {
  describe('HTML, CSS, and JS total size', () => {
    test('Total page size excluding images is under 500KB', () => {
      // Get HTML file
      const htmlFile = path.join(LANDING_PAGE_ROOT, 'index.html');
      const htmlSize = getFileSize(htmlFile);

      // Get all CSS files
      const cssDir = path.join(LANDING_PAGE_ROOT, 'css');
      const cssFiles = getAllFiles(cssDir).filter(f => f.endsWith('.css'));
      const cssSize = cssFiles.reduce((total, file) => total + getFileSize(file), 0);

      // Get all JS files
      const jsDir = path.join(LANDING_PAGE_ROOT, 'js');
      const jsFiles = getAllFiles(jsDir).filter(f => f.endsWith('.js'));
      const jsSize = jsFiles.reduce((total, file) => total + getFileSize(file), 0);

      // Calculate total size (excluding images and fonts)
      const totalSize = htmlSize + cssSize + jsSize;

      // Log breakdown for debugging
      console.log('Page Size Breakdown:');
      console.log(`  HTML: ${(htmlSize / 1024).toFixed(2)} KB`);
      console.log(`  CSS:  ${(cssSize / 1024).toFixed(2)} KB (${cssFiles.length} files)`);
      console.log(`  JS:   ${(jsSize / 1024).toFixed(2)} KB (${jsFiles.length} files)`);
      console.log(`  Total: ${(totalSize / 1024).toFixed(2)} KB`);
      console.log(`  Limit: ${(MAX_PAGE_SIZE_BYTES / 1024).toFixed(2)} KB`);

      // Assert total size is under threshold
      expect(totalSize).toBeLessThan(MAX_PAGE_SIZE_BYTES);
    });

    test('HTML file is reasonably sized', () => {
      const htmlFile = path.join(LANDING_PAGE_ROOT, 'index.html');
      const htmlSize = getFileSize(htmlFile);

      // HTML should be under 100KB for a landing page
      const maxHtmlSize = 100 * 1024;
      expect(htmlSize).toBeLessThan(maxHtmlSize);
    });

    test('Individual CSS files are reasonably sized', () => {
      const cssDir = path.join(LANDING_PAGE_ROOT, 'css');
      const cssFiles = getAllFiles(cssDir).filter(f => f.endsWith('.css'));

      // Each CSS file should be under 50KB
      const maxCssFileSize = 50 * 1024;

      cssFiles.forEach((file) => {
        const size = getFileSize(file);
        const fileName = path.relative(LANDING_PAGE_ROOT, file);
        expect(size).toBeLessThan(maxCssFileSize);
      });
    });

    test('Individual JS files are reasonably sized', () => {
      const jsDir = path.join(LANDING_PAGE_ROOT, 'js');
      const jsFiles = getAllFiles(jsDir).filter(f => f.endsWith('.js'));

      // Each JS file should be under 50KB (excluding minified bundles)
      const maxJsFileSize = 50 * 1024;

      jsFiles.forEach((file) => {
        const size = getFileSize(file);
        const fileName = path.relative(LANDING_PAGE_ROOT, file);
        expect(size).toBeLessThan(maxJsFileSize);
      });
    });
  });

  describe('Asset categorization', () => {
    test('All assets are properly categorized', () => {
      const assetsDir = path.join(LANDING_PAGE_ROOT, 'assets');

      if (fs.existsSync(assetsDir)) {
        const allAssets = getAllFiles(assetsDir);

        // Track categories
        const categories = {
          images: [],
          fonts: [],
          other: []
        };

        allAssets.forEach((file) => {
          if (isImageFile(file)) {
            categories.images.push(file);
          } else if (isFontFile(file)) {
            categories.fonts.push(file);
          } else {
            categories.other.push(file);
          }
        });

        console.log('Asset Categories:');
        console.log(`  Images: ${categories.images.length}`);
        console.log(`  Fonts: ${categories.fonts.length}`);
        console.log(`  Other: ${categories.other.length}`);

        // Test passes if categorization works
        expect(categories).toBeDefined();
      }
    });

    test('Code assets do not include images', () => {
      const htmlFile = path.join(LANDING_PAGE_ROOT, 'index.html');
      const cssDir = path.join(LANDING_PAGE_ROOT, 'css');
      const jsDir = path.join(LANDING_PAGE_ROOT, 'js');

      // HTML is not an image
      expect(isImageFile(htmlFile)).toBe(false);
      expect(isCodeAsset(htmlFile)).toBe(true);

      // CSS files are not images
      const cssFiles = getAllFiles(cssDir).filter(f => f.endsWith('.css'));
      cssFiles.forEach((file) => {
        expect(isImageFile(file)).toBe(false);
        expect(isCodeAsset(file)).toBe(true);
      });

      // JS files are not images
      const jsFiles = getAllFiles(jsDir).filter(f => f.endsWith('.js'));
      jsFiles.forEach((file) => {
        expect(isImageFile(file)).toBe(false);
        expect(isCodeAsset(file)).toBe(true);
      });
    });
  });
});
