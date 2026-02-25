/**
 * Performance Unit Tests
 * Owner: Scenario 13 - Performance and Page Load
 *
 * Test cases:
 * - Total page weight (HTML + CSS + JS + images) under 1MB
 * - Valid static HTML that works without server-side processing
 * - CSS and JS minification verification
 */

const fs = require('fs');
const path = require('path');

// Helper to get file size in bytes
const getFileSize = (filePath) => {
  try {
    const stats = fs.statSync(filePath);
    return stats.size;
  } catch {
    return 0;
  }
};

// Helper to recursively get directory size
const getDirectorySize = (dirPath) => {
  let totalSize = 0;

  if (!fs.existsSync(dirPath)) {
    return 0;
  }

  const files = fs.readdirSync(dirPath);

  for (const file of files) {
    const filePath = path.join(dirPath, file);
    const stats = fs.statSync(filePath);

    if (stats.isDirectory()) {
      totalSize += getDirectorySize(filePath);
    } else {
      totalSize += stats.size;
    }
  }

  return totalSize;
};

describe('Page Size Tests', () => {
  const siteDir = path.join(__dirname, '../../_site');

  beforeAll(() => {
    // Ensure the site is built
    if (!fs.existsSync(siteDir)) {
      const buildScript = path.join(__dirname, '../../build.js');
      require(buildScript);
    }
  });

  test('total page weight should be under 1MB', () => {
    const htmlSize = getFileSize(path.join(siteDir, 'index.html'));
    const cssSize = getDirectorySize(path.join(siteDir, 'assets', 'css'));
    const jsSize = getDirectorySize(path.join(siteDir, 'assets', 'js'));
    const imagesSize = getDirectorySize(path.join(siteDir, 'assets', 'images'));

    const totalSize = htmlSize + cssSize + jsSize + imagesSize;
    const totalMB = totalSize / (1024 * 1024);

    console.log('Asset sizes:');
    console.log(`  HTML: ${(htmlSize / 1024).toFixed(2)} KB`);
    console.log(`  CSS: ${(cssSize / 1024).toFixed(2)} KB`);
    console.log(`  JS: ${(jsSize / 1024).toFixed(2)} KB`);
    console.log(`  Images: ${(imagesSize / 1024).toFixed(2)} KB`);
    console.log(`  Total: ${totalMB.toFixed(2)} MB (${totalSize} bytes)`);

    // Total should be under 1MB (1,048,576 bytes)
    expect(totalSize).toBeLessThan(1048576);
  });

  test('HTML file should exist and be under 100KB', () => {
    const htmlPath = path.join(siteDir, 'index.html');
    expect(fs.existsSync(htmlPath)).toBe(true);

    const htmlSize = getFileSize(htmlPath);
    // HTML should be under 100KB for good performance
    expect(htmlSize).toBeLessThan(102400);
  });

  test('CSS file should exist and be under 50KB', () => {
    const cssPath = path.join(siteDir, 'assets', 'css', 'main.css');
    expect(fs.existsSync(cssPath)).toBe(true);

    const cssSize = getFileSize(cssPath);
    // CSS should be under 50KB for good performance
    expect(cssSize).toBeLessThan(51200);
  });

  test('JS files combined should be under 100KB', () => {
    const jsDir = path.join(siteDir, 'assets', 'js');
    const jsSize = getDirectorySize(jsDir);

    // JS should be under 100KB for good performance
    expect(jsSize).toBeLessThan(102400);
  });
});

describe('Static HTML Validation Tests', () => {
  const siteDir = path.join(__dirname, '../../_site');
  let htmlContent;

  beforeAll(() => {
    // Ensure the site is built
    if (!fs.existsSync(siteDir)) {
      const buildScript = path.join(__dirname, '../../build.js');
      require(buildScript);
    }

    const htmlPath = path.join(siteDir, 'index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');
  });

  test('HTML should have DOCTYPE declaration', () => {
    expect(htmlContent.trim().toLowerCase()).toMatch(/^<!doctype\s+html/i);
  });

  test('HTML should have html element with lang attribute', () => {
    expect(htmlContent).toMatch(/<html[^>]*\slang=["'][^"']+["']/i);
  });

  test('HTML should have head element with required meta tags', () => {
    // Should have charset
    expect(htmlContent).toMatch(/<meta[^>]*charset/i);

    // Should have viewport
    expect(htmlContent).toMatch(/<meta[^>]*viewport/i);

    // Should have title
    expect(htmlContent).toMatch(/<title[^>]*>/i);
  });

  test('HTML should have body element', () => {
    expect(htmlContent).toMatch(/<body[^>]*>/i);
  });

  test('HTML should contain MirDB content (not template variables)', () => {
    // Should have rendered content, not Liquid template variables
    expect(htmlContent).not.toMatch(/\{\{\s*site\./);
    expect(htmlContent).not.toMatch(/\{\{\s*page\./);
    expect(htmlContent).not.toMatch(/\{%\s*include/);

    // Should have actual MirDB content
    expect(htmlContent).toContain('MirDB');
  });

  test('HTML should be valid without server-side processing markers', () => {
    // Should not have PHP markers
    expect(htmlContent).not.toMatch(/<\?php/i);

    // Should not have ASP markers
    expect(htmlContent).not.toMatch(/<%/);

    // Should not have JSP markers
    expect(htmlContent).not.toMatch(/<%@/);

    // Should not have Node.js template markers (EJS)
    expect(htmlContent).not.toMatch(/<%=/);
  });

  test('HTML should have all essential page sections', () => {
    // Hero section
    expect(htmlContent).toMatch(/class="[^"]*hero/i);

    // Features section
    expect(htmlContent).toMatch(/id="features"/i);

    // Quickstart section
    expect(htmlContent).toMatch(/id="quickstart"/i);
  });

  test('HTML should have proper CSS link', () => {
    expect(htmlContent).toMatch(/<link[^>]*rel=["']stylesheet["'][^>]*href=["'][^"']*main\.css["']/i);
  });

  test('HTML should be minified (reduced whitespace)', () => {
    // Count consecutive whitespace patterns
    const extraWhitespace = htmlContent.match(/\n\s*\n/g);

    // Minified HTML should have minimal extra whitespace
    // Allow some whitespace for readability but should be reduced
    const whitespaceCount = extraWhitespace ? extraWhitespace.length : 0;
    expect(whitespaceCount).toBeLessThan(10);
  });
});

describe('Asset Minification Tests', () => {
  const siteDir = path.join(__dirname, '../../_site');

  beforeAll(() => {
    // Ensure the site is built
    if (!fs.existsSync(siteDir)) {
      const buildScript = path.join(__dirname, '../../build.js');
      require(buildScript);
    }
  });

  test('CSS should be compressed (minified)', () => {
    const cssPath = path.join(siteDir, 'assets', 'css', 'main.css');
    const cssContent = fs.readFileSync(cssPath, 'utf-8');

    // Count newlines - minified CSS has very few
    const newlineCount = (cssContent.match(/\n/g) || []).length;

    // Minified CSS typically has 0 or very few newlines
    expect(newlineCount).toBeLessThan(50);

    // CSS should have content
    expect(cssContent.length).toBeGreaterThan(100);
  });

  test('main.js should exist and be reasonable size', () => {
    const jsPath = path.join(siteDir, 'assets', 'js', 'main.js');
    expect(fs.existsSync(jsPath)).toBe(true);

    const jsContent = fs.readFileSync(jsPath, 'utf-8');

    // Should have actual JS content
    expect(jsContent.length).toBeGreaterThan(10);
  });
});

describe('Static Site Requirements (NFR-5)', () => {
  const siteDir = path.join(__dirname, '../../_site');

  beforeAll(() => {
    // Ensure the site is built
    if (!fs.existsSync(siteDir)) {
      const buildScript = path.join(__dirname, '../../build.js');
      require(buildScript);
    }
  });

  test('site should consist only of static files', () => {
    // Check that _site contains only static file types
    const checkStaticFiles = (dir) => {
      const allowedExtensions = [
        '.html', '.css', '.js', '.json',
        '.png', '.jpg', '.jpeg', '.gif', '.svg', '.ico', '.webp',
        '.woff', '.woff2', '.ttf', '.eot',
        '.txt', '.xml', '.map'
      ];

      if (!fs.existsSync(dir)) return true;

      const files = fs.readdirSync(dir);

      for (const file of files) {
        const filePath = path.join(dir, file);
        const stats = fs.statSync(filePath);

        if (stats.isDirectory()) {
          if (!checkStaticFiles(filePath)) return false;
        } else {
          const ext = path.extname(file).toLowerCase();
          if (ext && !allowedExtensions.includes(ext)) {
            console.log(`Found non-static file: ${filePath}`);
            return false;
          }
        }
      }

      return true;
    };

    expect(checkStaticFiles(siteDir)).toBe(true);
  });

  test('site should have no server-side configuration files', () => {
    const serverSideFiles = [
      'server.js', 'app.js', 'index.php',
      '.htaccess', 'web.config',
      'package.json', 'node_modules'
    ];

    for (const file of serverSideFiles) {
      const filePath = path.join(siteDir, file);
      expect(fs.existsSync(filePath)).toBe(false);
    }
  });

  test('HTML should be self-contained (no external API dependencies)', () => {
    const htmlPath = path.join(siteDir, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // Should not have fetch() calls that load external data for rendering
    // (Some external links are OK, but not data dependencies)
    expect(htmlContent).not.toMatch(/fetch\s*\(\s*['"][^'"]*api/i);

    // All essential content should be in the HTML
    expect(htmlContent).toContain('MirDB');
    expect(htmlContent).toContain('key-value');
  });
});
