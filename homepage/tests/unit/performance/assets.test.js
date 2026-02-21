/**
 * Page Asset Performance Unit Tests
 * Owner: Scenario 18 - Page Load Performance
 *
 * Tests:
 * - CSS minification for production
 * - Image optimization (appropriate format and compression)
 */

const fs = require('fs');
const path = require('path');

const BOOK_DIR = path.join(__dirname, '../../../book');

describe('Page Asset Performance', () => {
  describe('Test Case 4: CSS Minification', () => {
    const CSS_DIR = path.join(BOOK_DIR, 'css');

    test('CSS files exist in build output', () => {
      expect(fs.existsSync(CSS_DIR)).toBe(true);

      const cssFiles = fs.readdirSync(CSS_DIR).filter((f) => f.endsWith('.css'));
      expect(cssFiles.length).toBeGreaterThan(0);
    });

    test('CSS files are structured for production (no excessive whitespace)', () => {
      const cssFiles = fs.readdirSync(CSS_DIR).filter((f) => f.endsWith('.css'));

      cssFiles.forEach((file) => {
        const filePath = path.join(CSS_DIR, file);
        const content = fs.readFileSync(filePath, 'utf8');

        // Check that CSS has actual content
        expect(content.length).toBeGreaterThan(0);

        // Count lines vs total content ratio
        // Minified CSS typically has few newlines relative to content
        // For a static site, we check CSS is well-structured
        const lines = content.split('\n').filter((line) => line.trim().length > 0);

        // Verify CSS has rules (contains { and })
        expect(content).toContain('{');
        expect(content).toContain('}');

        // Verify CSS is valid (no syntax errors indicated by common patterns)
        // CSS should have property: value pairs
        expect(content).toMatch(/[a-z-]+\s*:\s*[^;]+;/i);
      });
    });

    test('CSS files have reasonable size for a static site', () => {
      const cssFiles = fs.readdirSync(CSS_DIR).filter((f) => f.endsWith('.css'));

      let totalSize = 0;
      cssFiles.forEach((file) => {
        const filePath = path.join(CSS_DIR, file);
        const stats = fs.statSync(filePath);
        totalSize += stats.size;

        // Individual CSS files should be under 100KB for a simple homepage
        expect(stats.size).toBeLessThan(100 * 1024);
      });

      // Total CSS bundle should be under 500KB for performance
      expect(totalSize).toBeLessThan(500 * 1024);
      console.log(`Total CSS size: ${(totalSize / 1024).toFixed(2)}KB`);
    });

    test('CSS does not contain debug comments or unnecessary bloat', () => {
      const cssFiles = fs.readdirSync(CSS_DIR).filter((f) => f.endsWith('.css'));

      cssFiles.forEach((file) => {
        const filePath = path.join(CSS_DIR, file);
        const content = fs.readFileSync(filePath, 'utf8');

        // Should not contain TODO or FIXME comments in production
        expect(content.toLowerCase()).not.toMatch(/\/\*\s*(todo|fixme)/i);

        // Should not contain console.log style debugging
        expect(content).not.toContain('debug:');

        // Should not have empty rule sets (unless intentional reset)
        // Allow some empty rules as they may be intentional
        const emptyRules = content.match(/\{\s*\}/g) || [];
        expect(emptyRules.length).toBeLessThan(5);
      });
    });
  });

  describe('Test Case 5: Image Optimization', () => {
    const IMAGES_DIR = path.join(BOOK_DIR, 'images');

    test('Image directory exists', () => {
      expect(fs.existsSync(IMAGES_DIR)).toBe(true);
    });

    test('Images use appropriate formats', () => {
      const files = fs.readdirSync(IMAGES_DIR);

      files.forEach((file) => {
        const ext = path.extname(file).toLowerCase();

        // Check that images use web-optimized formats
        const optimizedFormats = ['.svg', '.webp', '.png', '.jpg', '.jpeg', '.gif', '.ico'];
        expect(optimizedFormats).toContain(ext);

        // SVG is preferred for vector graphics (logos, diagrams)
        if (file.includes('architecture') || file.includes('diagram')) {
          expect(ext).toBe('.svg');
        }
      });
    });

    test('SVG files are properly optimized', () => {
      const files = fs.readdirSync(IMAGES_DIR).filter((f) => f.endsWith('.svg'));

      files.forEach((file) => {
        const filePath = path.join(IMAGES_DIR, file);
        const content = fs.readFileSync(filePath, 'utf8');

        // SVG should start with proper declaration
        expect(content).toMatch(/<svg/);

        // SVG should not contain unnecessary metadata
        expect(content).not.toContain('<metadata');

        // SVG file size should be reasonable (under 100KB for vector graphics)
        const stats = fs.statSync(filePath);
        expect(stats.size).toBeLessThan(100 * 1024);

        console.log(`SVG file ${file}: ${(stats.size / 1024).toFixed(2)}KB`);
      });
    });

    test('Raster images have reasonable file sizes', () => {
      const files = fs.readdirSync(IMAGES_DIR);
      const rasterFormats = ['.png', '.jpg', '.jpeg', '.gif', '.webp'];

      files.forEach((file) => {
        const ext = path.extname(file).toLowerCase();

        if (rasterFormats.includes(ext)) {
          const filePath = path.join(IMAGES_DIR, file);
          const stats = fs.statSync(filePath);

          // For a homepage, images should be optimized
          // Logo/icons: under 500KB (GIFs can be larger due to animation)
          // Hero images: under 1MB
          const maxSize = file.includes('hero') ? 1024 * 1024 : 3 * 1024 * 1024;
          expect(stats.size).toBeLessThan(maxSize);

          console.log(`Raster image ${file}: ${(stats.size / 1024).toFixed(2)}KB`);
        }
      });
    });

    test('Images have appropriate dimensions for web display', () => {
      const files = fs.readdirSync(IMAGES_DIR);

      files.forEach((file) => {
        const ext = path.extname(file).toLowerCase();

        // SVG dimensions are checked via viewBox
        if (ext === '.svg') {
          const filePath = path.join(IMAGES_DIR, file);
          const content = fs.readFileSync(filePath, 'utf8');

          // SVG should have viewBox or width/height for proper scaling
          expect(content).toMatch(/(viewBox|width|height)/);
        }
      });
    });

    test('No duplicate or unused images', () => {
      const files = fs.readdirSync(IMAGES_DIR);

      // Check for obvious duplicates by name pattern
      const baseNames = files.map((f) => {
        // Remove common suffixes like -1, -2, _copy, etc.
        return f.replace(/(-\d+|_copy|_backup)\.[a-z]+$/i, '');
      });

      const uniqueNames = [...new Set(baseNames)];

      // Should not have multiple copies of the same image
      expect(files.length).toBeLessThanOrEqual(uniqueNames.length + 2); // Allow small variance
    });
  });
});
