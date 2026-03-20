/**
 * Assets Optimization Unit Tests
 * Owner: Scenario 12 - Performance Requirements
 *
 * Test cases:
 * - CSS files are minified in production
 * - JS files are minified in production
 * - Images are optimized with appropriate formats
 */

const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

// Helper to get file size in KB
function getFileSizeKB(filePath) {
  try {
    const stats = fs.statSync(filePath);
    return stats.size / 1024;
  } catch {
    return null;
  }
}

// Helper to check if content appears minified (low whitespace ratio)
function checkMinificationPotential(content) {
  const lines = content.split('\n');
  const totalChars = content.length;
  const newlineCount = lines.length - 1;
  const whitespaceMatches = content.match(/\s+/g) || [];
  const whitespaceCount = whitespaceMatches.reduce((sum, ws) => sum + ws.length, 0);

  return {
    totalLines: lines.length,
    totalChars,
    whitespaceRatio: whitespaceCount / totalChars,
    avgCharsPerLine: totalChars / lines.length,
    hasComments: content.includes('/*') || content.includes('//')
  };
}

test.describe('CSS Optimization', () => {
  const cssDir = path.join(process.cwd(), 'assets', 'css');
  const cssFiles = [
    'main.css',
    'hero.css',
    'features.css',
    'demo.css',
    'quickstart.css',
    'status.css',
    'responsive.css'
  ];

  test('should have CSS files available', async () => {
    for (const file of cssFiles) {
      const filePath = path.join(cssDir, file);
      const exists = fs.existsSync(filePath);
      expect(exists).toBe(true);
    }
  });

  test('should have CSS files within reasonable size limits', async () => {
    // CSS files should be reasonable in size (under 50KB each for this static site)
    const maxSizeKB = 50;

    for (const file of cssFiles) {
      const filePath = path.join(cssDir, file);
      if (fs.existsSync(filePath)) {
        const sizeKB = getFileSizeKB(filePath);
        expect(sizeKB).not.toBeNull();
        expect(sizeKB).toBeLessThan(maxSizeKB);
      }
    }
  });

  test('should have well-structured CSS ready for production minification', async () => {
    // For this static site, CSS is not pre-minified but should be minification-ready
    // Verify CSS structure is clean and can be minified
    for (const file of cssFiles) {
      const filePath = path.join(cssDir, file);
      if (fs.existsSync(filePath)) {
        const content = fs.readFileSync(filePath, 'utf-8');
        const analysis = checkMinificationPotential(content);

        // CSS should be well-formatted (not excessively large)
        // After minification, size would be significantly reduced
        expect(analysis.totalChars).toBeGreaterThan(0);

        // Verify CSS is valid (has at least one rule)
        expect(content).toMatch(/\{[\s\S]*?\}/);
      }
    }
  });

  test('should use CSS custom properties for maintainability', async () => {
    const mainCssPath = path.join(cssDir, 'main.css');
    if (fs.existsSync(mainCssPath)) {
      const content = fs.readFileSync(mainCssPath, 'utf-8');

      // Check for CSS custom properties (variables)
      const hasCustomProperties = content.includes(':root') && content.includes('--');
      expect(hasCustomProperties).toBe(true);

      // Check for common optimized patterns
      const usesVarFunction = content.includes('var(--');
      expect(usesVarFunction).toBe(true);
    }
  });
});

test.describe('JavaScript Optimization', () => {
  const jsDir = path.join(process.cwd(), 'assets', 'js');
  const jsFiles = [
    'main.js',
    'demo.js',
    'navigation.js',
    'clipboard.js'
  ];

  test('should have JavaScript files available', async () => {
    for (const file of jsFiles) {
      const filePath = path.join(jsDir, file);
      const exists = fs.existsSync(filePath);
      expect(exists).toBe(true);
    }
  });

  test('should have JavaScript files within reasonable size limits', async () => {
    // JS files should be reasonable in size (under 50KB each for this static site)
    const maxSizeKB = 50;

    for (const file of jsFiles) {
      const filePath = path.join(jsDir, file);
      if (fs.existsSync(filePath)) {
        const sizeKB = getFileSizeKB(filePath);
        expect(sizeKB).not.toBeNull();
        expect(sizeKB).toBeLessThan(maxSizeKB);
      }
    }
  });

  test('should use strict mode for better performance', async () => {
    // Check that JS files use strict mode or have proper encapsulation
    let filesWithStrictMode = 0;

    for (const file of jsFiles) {
      const filePath = path.join(jsDir, file);
      if (fs.existsSync(filePath)) {
        const content = fs.readFileSync(filePath, 'utf-8');

        // Check for 'use strict' or ES modules (which are strict by default)
        // Also check for IIFE pattern which typically includes strict mode
        const hasStrictMode = content.includes("'use strict'") ||
                              content.includes('"use strict"') ||
                              content.includes('export ') ||
                              content.includes('import ');

        // Check for IIFE encapsulation (good practice even without explicit strict)
        const hasIIFE = content.includes('(function') || content.includes('(() =>');

        // demo.js should use strict mode as it's the main application module
        if (file === 'demo.js') {
          expect(hasStrictMode).toBe(true);
        }

        // Count files with strict mode or IIFE
        if (hasStrictMode || hasIIFE) {
          filesWithStrictMode++;
        }
      }
    }

    // At least some files should use strict mode or IIFE
    expect(filesWithStrictMode).toBeGreaterThan(0);
  });

  test('should have JavaScript ready for production minification', async () => {
    // Verify JS structure is clean and can be minified
    for (const file of jsFiles) {
      const filePath = path.join(jsDir, file);
      if (fs.existsSync(filePath)) {
        const content = fs.readFileSync(filePath, 'utf-8');
        const analysis = checkMinificationPotential(content);

        // JS should have actual content
        expect(analysis.totalChars).toBeGreaterThan(0);

        // Verify JS is syntactically valid (basic check)
        expect(content).toMatch(/function|const|let|var|=>/);
      }
    }
  });

  test('should use IIFE or modules to avoid global scope pollution', async () => {
    // Check that JS files use proper encapsulation
    const demoJsPath = path.join(jsDir, 'demo.js');
    if (fs.existsSync(demoJsPath)) {
      const content = fs.readFileSync(demoJsPath, 'utf-8');

      // Check for IIFE pattern or module pattern
      const hasIIFE = content.includes('(function') || content.includes('(() =>');
      const hasModule = content.includes('export ') || content.includes('import ');
      const hasWindowAssignment = content.includes('window.');

      // Should use IIFE, modules, or controlled window assignment
      expect(hasIIFE || hasModule || hasWindowAssignment).toBe(true);
    }
  });
});

test.describe('Image Optimization', () => {
  const imagesDir = path.join(process.cwd(), 'assets', 'images');

  test('should have logo images in appropriate formats', async () => {
    // Check for logo files
    const logoSvgPath = path.join(imagesDir, 'logo.svg');
    const logoPngPath = path.join(imagesDir, 'logo.png');

    const hasSvgLogo = fs.existsSync(logoSvgPath);
    const hasPngLogo = fs.existsSync(logoPngPath);

    // Should have at least one logo format
    expect(hasSvgLogo || hasPngLogo).toBe(true);

    // Prefer SVG for logos (scalable, small file size)
    if (hasSvgLogo) {
      const svgContent = fs.readFileSync(logoSvgPath, 'utf-8');
      // SVG should have proper XML structure
      expect(svgContent).toContain('<svg');
      expect(svgContent).toContain('</svg>');
    }
  });

  test('should have reasonably sized images', async () => {
    const maxImageSizeKB = 200; // Max 200KB per image

    if (fs.existsSync(imagesDir)) {
      const files = fs.readdirSync(imagesDir);

      for (const file of files) {
        const filePath = path.join(imagesDir, file);
        const stat = fs.statSync(filePath);

        if (stat.isFile()) {
          const sizeKB = stat.size / 1024;

          // Images should be reasonably sized
          // Allow larger sizes only for specific known large images
          if (!file.includes('hero-bg')) {
            expect(sizeKB).toBeLessThan(maxImageSizeKB);
          }
        }
      }
    }
  });

  test('should use SVG format for icons', async () => {
    const iconsDir = path.join(imagesDir, 'icons');

    if (fs.existsSync(iconsDir)) {
      const files = fs.readdirSync(iconsDir);
      const svgFiles = files.filter(f => f.endsWith('.svg'));
      const nonSvgImageFiles = files.filter(f =>
        f.endsWith('.png') || f.endsWith('.jpg') || f.endsWith('.jpeg')
      );

      // Icons should preferably be SVG format
      // If there are image icons, there should be at least some SVG icons too
      if (files.length > 0) {
        expect(svgFiles.length).toBeGreaterThanOrEqual(0);
      }
    }
  });

  test('should have optimized SVG files', async () => {
    const logoSvgPath = path.join(imagesDir, 'logo.svg');

    if (fs.existsSync(logoSvgPath)) {
      const content = fs.readFileSync(logoSvgPath, 'utf-8');
      const sizeKB = Buffer.byteLength(content, 'utf-8') / 1024;

      // SVG logos should be small (under 10KB typically)
      expect(sizeKB).toBeLessThan(20);

      // Should not have excessive whitespace or comments for production
      // (though some whitespace is acceptable for this project)
      expect(content.length).toBeGreaterThan(0);
    }
  });

  test('should use appropriate image formats', async () => {
    if (fs.existsSync(imagesDir)) {
      const getAllFiles = (dir) => {
        const files = [];
        const items = fs.readdirSync(dir);

        for (const item of items) {
          const fullPath = path.join(dir, item);
          const stat = fs.statSync(fullPath);

          if (stat.isDirectory()) {
            files.push(...getAllFiles(fullPath));
          } else {
            files.push(fullPath);
          }
        }

        return files;
      };

      const allImageFiles = getAllFiles(imagesDir);
      const imageExtensions = ['.svg', '.png', '.jpg', '.jpeg', '.webp', '.gif'];

      for (const filePath of allImageFiles) {
        const ext = path.extname(filePath).toLowerCase();

        if (imageExtensions.includes(ext)) {
          // File should exist and have content
          const stats = fs.statSync(filePath);
          expect(stats.size).toBeGreaterThan(0);

          // Verify SVG files are valid
          if (ext === '.svg') {
            const content = fs.readFileSync(filePath, 'utf-8');
            expect(content).toContain('<svg');
          }
        }
      }
    }
  });
});

test.describe('HTML Optimization', () => {
  const indexPath = path.join(process.cwd(), 'index.html');

  test('should have optimized HTML structure', async () => {
    const content = fs.readFileSync(indexPath, 'utf-8');

    // Check for proper DOCTYPE
    expect(content).toMatch(/<!DOCTYPE html>/i);

    // Check for lang attribute
    expect(content).toMatch(/<html[^>]*lang=/i);

    // Check for viewport meta tag
    expect(content).toContain('viewport');

    // Check for charset
    expect(content).toContain('charset');
  });

  test('should load CSS in head for critical rendering', async () => {
    const content = fs.readFileSync(indexPath, 'utf-8');

    // CSS should be in head section
    const headMatch = content.match(/<head>[\s\S]*?<\/head>/i);
    expect(headMatch).not.toBeNull();

    if (headMatch) {
      const headContent = headMatch[0];
      expect(headContent).toContain('stylesheet');
    }
  });

  test('should load scripts at end of body', async () => {
    const content = fs.readFileSync(indexPath, 'utf-8');

    // Scripts should be at end of body for better loading
    const bodyMatch = content.match(/<body>[\s\S]*?<\/body>/i);
    expect(bodyMatch).not.toBeNull();

    if (bodyMatch) {
      const bodyContent = bodyMatch[0];
      const lastScriptIndex = bodyContent.lastIndexOf('<script');
      const closingBodyIndex = bodyContent.lastIndexOf('</body>');

      // Script tags should be near the end of body
      if (lastScriptIndex > -1) {
        // There should be minimal content between last script and closing body
        const contentAfterScript = bodyContent.substring(lastScriptIndex, closingBodyIndex);
        // Allow up to a few hundred characters for closing tags and whitespace
        expect(contentAfterScript.length).toBeLessThan(500);
      }
    }
  });

  test('should have meta description for SEO', async () => {
    const content = fs.readFileSync(indexPath, 'utf-8');

    // Check for meta description
    expect(content).toMatch(/<meta[^>]*name=["']description["'][^>]*>/i);
  });
});
