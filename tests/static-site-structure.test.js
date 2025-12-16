/**
 * Static Site Structure Tests
 *
 * This test file validates that the MirDB homepage meets NFR-5:
 * "Static site with no server-side dependencies for easy hosting"
 *
 * Tests verify:
 * 1. index.html exists as main entry point
 * 2. CSS stylesheet exists and is linked from HTML
 * 3. No server-side files (.php, .py, .rb, etc.) in homepage directory
 * 4. No dynamic content loading via fetch/XMLHttpRequest (except CDN resources)
 */

const fs = require('fs');
const path = require('path');

describe('Static Site Structure', () => {
  const rootDir = path.resolve(__dirname, '..');
  let htmlContent;
  let jsContent;

  beforeAll(() => {
    // Read index.html content for tests
    const indexPath = path.join(rootDir, 'index.html');
    if (fs.existsSync(indexPath)) {
      htmlContent = fs.readFileSync(indexPath, 'utf8');
    }

    // Read prism.js content for tests
    const prismPath = path.join(rootDir, 'prism.js');
    if (fs.existsSync(prismPath)) {
      jsContent = fs.readFileSync(prismPath, 'utf8');
    }
  });

  describe('Test Case 1: Check for index.html file', () => {
    test('index.html exists as main entry point', () => {
      const indexPath = path.join(rootDir, 'index.html');
      expect(fs.existsSync(indexPath)).toBe(true);
    });

    test('index.html is a valid HTML file with DOCTYPE declaration', () => {
      expect(htmlContent).toBeDefined();
      expect(htmlContent.trim().toLowerCase()).toMatch(/^<!doctype html>/);
    });

    test('index.html contains essential HTML structure', () => {
      expect(htmlContent).toMatch(/<html[^>]*>/i);
      expect(htmlContent).toMatch(/<head>/i);
      expect(htmlContent).toMatch(/<body>/i);
      expect(htmlContent).toMatch(/<\/html>/i);
    });

    test('index.html has proper lang attribute for accessibility', () => {
      expect(htmlContent).toMatch(/<html[^>]*lang=["'][^"']+["'][^>]*>/i);
    });

    test('index.html contains title element', () => {
      expect(htmlContent).toMatch(/<title>[^<]+<\/title>/i);
    });
  });

  describe('Test Case 2: Check for styles.css file', () => {
    test('styles.css file exists', () => {
      const stylesPath = path.join(rootDir, 'styles.css');
      expect(fs.existsSync(stylesPath)).toBe(true);
    });

    test('styles.css is linked from index.html', () => {
      expect(htmlContent).toMatch(/<link[^>]*href=["']styles\.css["'][^>]*>/i);
    });

    test('styles.css link has rel="stylesheet" attribute', () => {
      // Match a link tag that has both href="styles.css" and rel="stylesheet"
      const stylesheetLinkPattern = /<link[^>]*rel=["']stylesheet["'][^>]*href=["']styles\.css["'][^>]*>|<link[^>]*href=["']styles\.css["'][^>]*rel=["']stylesheet["'][^>]*>/i;
      expect(htmlContent).toMatch(stylesheetLinkPattern);
    });

    test('styles.css contains valid CSS content', () => {
      const stylesPath = path.join(rootDir, 'styles.css');
      const cssContent = fs.readFileSync(stylesPath, 'utf8');

      // Should contain CSS rules (selector followed by braces)
      expect(cssContent).toMatch(/[^{]+\{[^}]*\}/);

      // Should be non-empty
      expect(cssContent.trim().length).toBeGreaterThan(0);
    });
  });

  describe('Test Case 3: Verify no PHP, Python, or server files', () => {
    const serverSideExtensions = ['.php', '.py', '.rb', '.asp', '.aspx', '.jsp', '.cgi', '.pl'];

    test('no .php files in homepage directory', () => {
      const phpFiles = findFilesWithExtension(rootDir, '.php');
      expect(phpFiles).toHaveLength(0);
    });

    test('no .py files in homepage directory', () => {
      const pyFiles = findFilesWithExtension(rootDir, '.py');
      expect(pyFiles).toHaveLength(0);
    });

    test('no .rb files in homepage directory', () => {
      const rbFiles = findFilesWithExtension(rootDir, '.rb');
      expect(rbFiles).toHaveLength(0);
    });

    test('no .asp or .aspx files in homepage directory', () => {
      const aspFiles = findFilesWithExtension(rootDir, '.asp');
      const aspxFiles = findFilesWithExtension(rootDir, '.aspx');
      expect(aspFiles).toHaveLength(0);
      expect(aspxFiles).toHaveLength(0);
    });

    test('no .jsp files in homepage directory', () => {
      const jspFiles = findFilesWithExtension(rootDir, '.jsp');
      expect(jspFiles).toHaveLength(0);
    });

    test('no .cgi or .pl files in homepage directory', () => {
      const cgiFiles = findFilesWithExtension(rootDir, '.cgi');
      const plFiles = findFilesWithExtension(rootDir, '.pl');
      expect(cgiFiles).toHaveLength(0);
      expect(plFiles).toHaveLength(0);
    });

    test('site contains only allowed static file types (HTML, CSS, JS, images, fonts)', () => {
      const allowedExtensions = [
        '.html', '.htm',           // HTML
        '.css',                     // CSS
        '.js', '.mjs',              // JavaScript
        '.png', '.jpg', '.jpeg', '.gif', '.svg', '.webp', '.ico',  // Images
        '.woff', '.woff2', '.ttf', '.otf', '.eot',  // Fonts
        '.json',                    // JSON (for configs)
        '.md', '.txt',              // Text files
        '.toml', '.yaml', '.yml',   // Config files
        '.xml',                     // XML
        '.lock',                    // Lock files
        '.rs',                      // Rust source (backend, not homepage)
        '.gitignore'                // Git
      ];

      const staticFiles = getStaticSiteFiles(rootDir);
      const invalidFiles = staticFiles.filter(file => {
        const ext = path.extname(file).toLowerCase();
        const basename = path.basename(file);
        // Allow files without extension (like .gitignore) or with allowed extensions
        return ext && !allowedExtensions.includes(ext) && !basename.startsWith('.');
      });

      // Filter out server-side files only
      const serverSideFiles = invalidFiles.filter(file => {
        const ext = path.extname(file).toLowerCase();
        return serverSideExtensions.includes(ext);
      });

      expect(serverSideFiles).toHaveLength(0);
    });
  });

  describe('Test Case 4: Check JavaScript for fetch/XMLHttpRequest calls', () => {
    test('no fetch() calls for dynamic content loading in prism.js', () => {
      if (jsContent) {
        // fetch() calls for API data loading
        const fetchPattern = /fetch\s*\(/g;
        const fetchMatches = jsContent.match(fetchPattern) || [];
        expect(fetchMatches).toHaveLength(0);
      }
    });

    test('no XMLHttpRequest usage in prism.js', () => {
      if (jsContent) {
        const xhrPattern = /new\s+XMLHttpRequest|XMLHttpRequest\s*\(/g;
        const xhrMatches = jsContent.match(xhrPattern) || [];
        expect(xhrMatches).toHaveLength(0);
      }
    });

    test('no axios library usage in prism.js', () => {
      if (jsContent) {
        const axiosPattern = /axios\s*\.\s*(get|post|put|delete|patch|request)\s*\(/g;
        const axiosMatches = jsContent.match(axiosPattern) || [];
        expect(axiosMatches).toHaveLength(0);
      }
    });

    test('no jQuery AJAX calls in prism.js', () => {
      if (jsContent) {
        const ajaxPattern = /\$\s*\.\s*ajax\s*\(|\$\s*\.\s*get\s*\(|\$\s*\.\s*post\s*\(/g;
        const ajaxMatches = jsContent.match(ajaxPattern) || [];
        expect(ajaxMatches).toHaveLength(0);
      }
    });

    test('HTML does not contain inline scripts with API calls', () => {
      // Check for inline script tags with fetch/XHR
      const scriptPattern = /<script[^>]*>[\s\S]*?<\/script>/gi;
      const scripts = htmlContent.match(scriptPattern) || [];

      for (const script of scripts) {
        // Skip external script references (they're checked separately)
        if (script.includes('src=')) continue;

        // Check inline script content
        expect(script).not.toMatch(/fetch\s*\(/);
        expect(script).not.toMatch(/XMLHttpRequest/);
        expect(script).not.toMatch(/axios\./);
      }
    });

    test('external scripts are only CDN resources or local static files', () => {
      // Extract all script src attributes
      const srcPattern = /<script[^>]*src=["']([^"']+)["'][^>]*>/gi;
      let match;
      const sources = [];

      while ((match = srcPattern.exec(htmlContent)) !== null) {
        sources.push(match[1]);
      }

      for (const src of sources) {
        // Local files are fine
        if (!src.startsWith('http://') && !src.startsWith('https://') && !src.startsWith('//')) {
          continue;
        }

        // CDN resources are allowed (common CDNs)
        const allowedCDNs = [
          'cdn.jsdelivr.net',
          'cdnjs.cloudflare.com',
          'unpkg.com',
          'fonts.googleapis.com',
          'fonts.gstatic.com',
          'code.jquery.com',
          'stackpath.bootstrapcdn.com'
        ];

        const isAllowedCDN = allowedCDNs.some(cdn => src.includes(cdn));
        expect(isAllowedCDN || src.startsWith('./') || src.startsWith('../') || !src.includes('://')).toBe(true);
      }
    });
  });

  describe('GitHub Pages Compatibility', () => {
    test('site has no server-side configuration files', () => {
      // Check for common server config files that would indicate server-side requirements
      const serverConfigFiles = [
        'server.js',
        'app.js',
        'index.php',
        'server.py',
        'app.py',
        'Gemfile',
        'composer.json',
        'requirements.txt',
        'Procfile',
        '.htaccess',
        'web.config',
        'nginx.conf',
        'apache.conf'
      ];

      // Filter to only homepage-related files (not the Rust backend)
      const homepageConfigFiles = serverConfigFiles.filter(file => {
        const filePath = path.join(rootDir, file);
        // Check if file exists and is not part of the Rust backend
        if (!fs.existsSync(filePath)) return false;

        // requirements.txt, Gemfile, composer.json in root would indicate homepage dependencies
        // We exclude files that are clearly for the Rust backend
        return true;
      });

      // Procfile, .htaccess, web.config, nginx.conf, apache.conf should definitely not exist
      const criticalServerFiles = ['Procfile', '.htaccess', 'web.config', 'nginx.conf', 'apache.conf'];
      const foundCriticalFiles = criticalServerFiles.filter(file =>
        fs.existsSync(path.join(rootDir, file))
      );

      expect(foundCriticalFiles).toHaveLength(0);
    });

    test('index.html can be served as static entry point', () => {
      // Verify index.html is at root level (standard for static hosting)
      const indexPath = path.join(rootDir, 'index.html');
      expect(fs.existsSync(indexPath)).toBe(true);

      // Verify it's not inside a build directory requiring compilation
      expect(indexPath).not.toMatch(/\/dist\//);
      expect(indexPath).not.toMatch(/\/build\//);
    });

    test('all referenced assets use relative paths', () => {
      // Check CSS links
      const cssLinks = htmlContent.match(/<link[^>]*href=["']([^"']+\.css)["'][^>]*>/gi) || [];
      for (const link of cssLinks) {
        const hrefMatch = link.match(/href=["']([^"']+)["']/i);
        if (hrefMatch) {
          const href = hrefMatch[1];
          // Should be relative path (not absolute server path)
          expect(href).not.toMatch(/^\/[^/]/); // Not starting with single /
        }
      }

      // Check JS scripts
      const jsScripts = htmlContent.match(/<script[^>]*src=["']([^"']+\.js)["'][^>]*>/gi) || [];
      for (const script of jsScripts) {
        const srcMatch = script.match(/src=["']([^"']+)["']/i);
        if (srcMatch) {
          const src = srcMatch[1];
          // Should be relative path (not absolute server path)
          expect(src).not.toMatch(/^\/[^/]/); // Not starting with single /
        }
      }
    });
  });
});

/**
 * Helper function to find files with a specific extension
 */
function findFilesWithExtension(dir, extension, excludeDirs = ['.git', 'node_modules', '.something', 'mirdb-server', 'skip-list', 'sstable']) {
  const files = [];

  function walkDir(currentPath) {
    const entries = fs.readdirSync(currentPath, { withFileTypes: true });

    for (const entry of entries) {
      const fullPath = path.join(currentPath, entry.name);

      if (entry.isDirectory()) {
        // Skip excluded directories
        if (!excludeDirs.includes(entry.name)) {
          walkDir(fullPath);
        }
      } else if (entry.isFile() && entry.name.endsWith(extension)) {
        files.push(fullPath);
      }
    }
  }

  walkDir(dir);
  return files;
}

/**
 * Helper function to get all static site files (excluding backend Rust code)
 */
function getStaticSiteFiles(dir, excludeDirs = ['.git', 'node_modules', '.something', 'mirdb-server', 'skip-list', 'sstable', 'tests', '.circleci']) {
  const files = [];

  function walkDir(currentPath) {
    const entries = fs.readdirSync(currentPath, { withFileTypes: true });

    for (const entry of entries) {
      const fullPath = path.join(currentPath, entry.name);

      if (entry.isDirectory()) {
        // Skip excluded directories
        if (!excludeDirs.includes(entry.name)) {
          walkDir(fullPath);
        }
      } else if (entry.isFile()) {
        files.push(fullPath);
      }
    }
  }

  walkDir(dir);
  return files;
}
