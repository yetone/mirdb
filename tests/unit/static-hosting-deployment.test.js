/**
 * Static Hosting Deployment Unit Tests
 * Owner: Scenario 19 - Static Hosting Deployment
 *
 * Tests:
 * - index.html exists in docs/ directory
 * - All asset paths are relative (no absolute paths to localhost or specific domains)
 * - No server-side requirements (no PHP, Node.js, or server-side code)
 * - 404 handling consideration (single page with all content)
 */
import { describe, it, expect, beforeAll } from 'vitest';
import { readFileSync, existsSync, readdirSync, statSync } from 'fs';
import { JSDOM } from 'jsdom';
import { resolve, join } from 'path';

describe('Static Hosting Deployment', () => {
  const docsDir = resolve(process.cwd(), 'docs');
  let htmlContent;
  let cssContent;
  let document;

  beforeAll(() => {
    // Load docs/index.html for testing
    const htmlPath = resolve(docsDir, 'index.html');
    if (existsSync(htmlPath)) {
      htmlContent = readFileSync(htmlPath, 'utf-8');
      // Use a proper URL to avoid localStorage/sessionStorage issues in JSDOM
      const dom = new JSDOM(htmlContent, { url: 'https://example.com/' });
      document = dom.window.document;
    }

    // Load docs/css/styles.css if exists
    const cssPath = resolve(docsDir, 'css/styles.css');
    if (existsSync(cssPath)) {
      cssContent = readFileSync(cssPath, 'utf-8');
    }
  });

  describe('TC1: index.html exists in docs/ or root directory', () => {
    it('index.html exists in docs/ directory', () => {
      const indexPath = resolve(docsDir, 'index.html');
      expect(existsSync(indexPath)).toBe(true);
    });

    it('docs/ directory exists for GitHub Pages deployment', () => {
      expect(existsSync(docsDir)).toBe(true);
      expect(statSync(docsDir).isDirectory()).toBe(true);
    });

    it('index.html is a valid HTML file with DOCTYPE', () => {
      expect(htmlContent).toBeDefined();
      expect(htmlContent.trim().toLowerCase()).toMatch(/^<!doctype html>/);
    });
  });

  describe('TC2: All asset paths are relative', () => {
    it('HTML has no absolute paths to localhost', () => {
      expect(htmlContent).not.toMatch(/http:\/\/localhost/i);
      expect(htmlContent).not.toMatch(/https:\/\/localhost/i);
    });

    it('HTML has no absolute paths to 127.0.0.1', () => {
      expect(htmlContent).not.toMatch(/http:\/\/127\.0\.0\.1/i);
      expect(htmlContent).not.toMatch(/https:\/\/127\.0\.0\.1/i);
    });

    it('CSS file references use relative paths', () => {
      const cssLinks = document.querySelectorAll('link[rel="stylesheet"]');
      cssLinks.forEach(link => {
        const href = link.getAttribute('href');
        // Should not start with http:// or https:// (except for CDN fonts which are acceptable)
        if (href && !href.includes('fonts.googleapis.com') && !href.includes('cdnjs.cloudflare.com')) {
          expect(href).not.toMatch(/^https?:\/\//);
        }
      });
    });

    it('Script file references use relative paths', () => {
      const scripts = document.querySelectorAll('script[src]');
      scripts.forEach(script => {
        const src = script.getAttribute('src');
        // Should not start with http:// or https:// (except for allowed CDNs)
        if (src && !src.includes('cdn.')) {
          expect(src).not.toMatch(/^https?:\/\//);
        }
      });
    });

    it('Image sources use relative paths or allowed external URLs', () => {
      const images = document.querySelectorAll('img[src]');
      images.forEach(img => {
        const src = img.getAttribute('src');
        // Allow external badge images (like CircleCI badges)
        if (src && !src.includes('circleci.com') && !src.includes('shields.io') && !src.includes('img.shields.io')) {
          // Internal images should be relative
          if (!src.startsWith('http')) {
            expect(src).not.toMatch(/^\/[^\/]/); // Not absolute path like /assets
            // Should be relative like assets/logo.gif or ./assets/logo.gif
          }
        }
      });
    });

    it('CSS content has no absolute localhost references', () => {
      if (cssContent) {
        expect(cssContent).not.toMatch(/url\(['"]?http:\/\/localhost/i);
        expect(cssContent).not.toMatch(/url\(['"]?https:\/\/localhost/i);
      }
    });
  });

  describe('TC3: No server-side requirements', () => {
    it('No PHP files in docs/ directory', () => {
      const phpFiles = findFiles(docsDir, '.php');
      expect(phpFiles.length).toBe(0);
    });

    it('No server-side template files in docs/ directory', () => {
      const templateExts = ['.php', '.asp', '.aspx', '.jsp', '.erb', '.ejs'];
      templateExts.forEach(ext => {
        const files = findFiles(docsDir, ext);
        expect(files.length).toBe(0);
      });
    });

    it('docs/ contains only static file types', () => {
      const allowedExtensions = ['.html', '.css', '.js', '.gif', '.png', '.jpg', '.jpeg', '.svg', '.ico', '.webp', '.woff', '.woff2', '.ttf', '.eot', '.json', '.xml', '.txt', '.map'];
      const allFiles = getAllFiles(docsDir);

      allFiles.forEach(file => {
        const ext = file.substring(file.lastIndexOf('.')).toLowerCase();
        expect(allowedExtensions).toContain(ext);
      });
    });

    it('No Node.js package.json in docs/ directory', () => {
      const packageJsonPath = resolve(docsDir, 'package.json');
      expect(existsSync(packageJsonPath)).toBe(false);
    });

    it('No server configuration files in docs/ directory', () => {
      const serverConfigs = ['server.js', 'app.js', 'index.js', 'server.py', 'app.py', 'main.go', 'Procfile', 'docker-compose.yml'];
      serverConfigs.forEach(config => {
        const configPath = resolve(docsDir, config);
        expect(existsSync(configPath)).toBe(false);
      });
    });
  });

  describe('TC4: 404 handling consideration', () => {
    it('Homepage is a single-page application OR has 404.html for SPA routing', () => {
      // Option 1: Single page with all content (check for main sections)
      const hasAllSections =
        document.querySelector('#hero') !== null &&
        document.querySelector('#features') !== null &&
        document.querySelector('#quick-start') !== null &&
        document.querySelector('#footer') !== null;

      // Option 2: 404.html exists for SPA routing
      const has404Page = existsSync(resolve(docsDir, '404.html'));

      // At least one of these should be true
      expect(hasAllSections || has404Page).toBe(true);
    });

    it('All navigation links point to same-page anchors or valid resources', () => {
      const navLinks = document.querySelectorAll('nav a[href^="#"]');
      navLinks.forEach(link => {
        const href = link.getAttribute('href');
        if (href && href.startsWith('#') && href !== '#') {
          const targetId = href.substring(1);
          const targetElement = document.getElementById(targetId);
          expect(targetElement).not.toBeNull();
        }
      });
    });

    it('Page contains all major content sections for single-page experience', () => {
      // Verify all major sections exist in a single page
      expect(document.querySelector('header')).not.toBeNull();
      expect(document.querySelector('main')).not.toBeNull();
      expect(document.querySelector('footer')).not.toBeNull();

      // Core content sections
      const sections = ['hero', 'features', 'quick-start'];
      sections.forEach(sectionId => {
        expect(document.getElementById(sectionId)).not.toBeNull();
      });
    });
  });

  describe('GitHub Pages deployment readiness', () => {
    it('docs/ folder structure is correct for GitHub Pages', () => {
      expect(existsSync(resolve(docsDir, 'index.html'))).toBe(true);
      expect(existsSync(resolve(docsDir, 'css'))).toBe(true);
      expect(existsSync(resolve(docsDir, 'js'))).toBe(true);
    });

    it('Required assets are present in docs/', () => {
      expect(existsSync(resolve(docsDir, 'css/styles.css'))).toBe(true);
      expect(existsSync(resolve(docsDir, 'js/main.js'))).toBe(true);
    });

    it('Asset references in HTML match actual file locations', () => {
      // Check CSS link
      const cssLink = document.querySelector('link[href*="styles.css"]');
      if (cssLink) {
        const href = cssLink.getAttribute('href');
        const cssPath = resolve(docsDir, href);
        expect(existsSync(cssPath)).toBe(true);
      }

      // Check JS script
      const jsScript = document.querySelector('script[src*="main.js"]');
      if (jsScript) {
        const src = jsScript.getAttribute('src');
        const jsPath = resolve(docsDir, src);
        expect(existsSync(jsPath)).toBe(true);
      }
    });
  });
});

/**
 * Helper function to find files with a specific extension in a directory
 */
function findFiles(dir, ext) {
  const files = [];
  if (!existsSync(dir)) return files;

  const items = readdirSync(dir);
  items.forEach(item => {
    const fullPath = join(dir, item);
    const stat = statSync(fullPath);
    if (stat.isDirectory()) {
      files.push(...findFiles(fullPath, ext));
    } else if (item.endsWith(ext)) {
      files.push(fullPath);
    }
  });
  return files;
}

/**
 * Helper function to get all files in a directory recursively
 */
function getAllFiles(dir) {
  const files = [];
  if (!existsSync(dir)) return files;

  const items = readdirSync(dir);
  items.forEach(item => {
    const fullPath = join(dir, item);
    const stat = statSync(fullPath);
    if (stat.isDirectory()) {
      files.push(...getAllFiles(fullPath));
    } else {
      files.push(fullPath);
    }
  });
  return files;
}
