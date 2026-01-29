/**
 * Build Verification Tests for Static Site Generation
 * Owner: Scenario 16 - Static Site Generation
 *
 * These tests verify:
 * - Build process completes successfully
 * - Build output contains static HTML/CSS/JS files
 * - HTML output is valid
 * - CSS output is valid
 * - No server-side dependencies at runtime
 */
import { describe, it, expect, beforeAll } from 'vitest';
import { execSync } from 'child_process';
import { existsSync, readFileSync, readdirSync, statSync } from 'fs';
import { join, extname } from 'path';

const PROJECT_ROOT = join(__dirname, '../..');
const DIST_DIR = join(PROJECT_ROOT, 'dist');

/**
 * Recursively get all files in a directory
 */
function getAllFiles(dir: string, files: string[] = []): string[] {
  if (!existsSync(dir)) return files;

  const entries = readdirSync(dir);
  for (const entry of entries) {
    const fullPath = join(dir, entry);
    if (statSync(fullPath).isDirectory()) {
      getAllFiles(fullPath, files);
    } else {
      files.push(fullPath);
    }
  }
  return files;
}

/**
 * Get files by extension from the dist directory
 */
function getFilesByExtension(extension: string): string[] {
  const allFiles = getAllFiles(DIST_DIR);
  return allFiles.filter(file => extname(file).toLowerCase() === extension.toLowerCase());
}

describe('Static Site Generation', () => {
  describe('Test Case 1: Build Process', () => {
    it('should complete build successfully without errors', () => {
      // The build should complete without throwing an error
      // We run the build with error checking
      try {
        execSync('npm run build', {
          cwd: PROJECT_ROOT,
          stdio: 'pipe',
          timeout: 120000, // 2 minute timeout
        });
        expect(true).toBe(true); // Build succeeded
      } catch (error: unknown) {
        const buildError = error as { stderr?: Buffer; stdout?: Buffer };
        const stderr = buildError.stderr?.toString() || '';
        const stdout = buildError.stdout?.toString() || '';
        throw new Error(`Build failed:\nstdout: ${stdout}\nstderr: ${stderr}`);
      }
    });
  });

  describe('Test Case 2: Build Output Structure', () => {
    beforeAll(() => {
      // Ensure build exists before running output tests
      if (!existsSync(DIST_DIR)) {
        execSync('npm run build', {
          cwd: PROJECT_ROOT,
          stdio: 'pipe',
          timeout: 120000,
        });
      }
    });

    it('should have a dist directory after build', () => {
      expect(existsSync(DIST_DIR)).toBe(true);
    });

    it('should contain static HTML files', () => {
      const htmlFiles = getFilesByExtension('.html');
      expect(htmlFiles.length).toBeGreaterThan(0);

      // Should have at least the index.html
      const indexHtml = htmlFiles.find(f => f.endsWith('index.html'));
      expect(indexHtml).toBeDefined();
    });

    it('should contain static CSS files', () => {
      const cssFiles = getFilesByExtension('.css');
      expect(cssFiles.length).toBeGreaterThan(0);
    });

    it('should contain static JS files', () => {
      const jsFiles = getFilesByExtension('.js');
      expect(jsFiles.length).toBeGreaterThan(0);
    });

    it('should have proper directory structure for static hosting', () => {
      // Check for _astro directory (where Astro puts assets)
      const astroAssetsDir = join(DIST_DIR, '_astro');
      // Assets may be in root or _astro depending on config
      const hasStaticAssets =
        (existsSync(astroAssetsDir) && readdirSync(astroAssetsDir).length > 0) ||
        getFilesByExtension('.css').length > 0 ||
        getFilesByExtension('.js').length > 0;

      expect(hasStaticAssets).toBe(true);
    });
  });

  describe('Test Case 3: HTML Output Validation', () => {
    beforeAll(() => {
      if (!existsSync(DIST_DIR)) {
        execSync('npm run build', {
          cwd: PROJECT_ROOT,
          stdio: 'pipe',
          timeout: 120000,
        });
      }
    });

    it('should have valid HTML5 doctype', () => {
      const htmlFiles = getFilesByExtension('.html');
      expect(htmlFiles.length).toBeGreaterThan(0);

      const indexHtml = htmlFiles.find(f => f.endsWith('index.html'));
      expect(indexHtml).toBeDefined();

      const content = readFileSync(indexHtml!, 'utf-8');
      // HTML5 doctype should be present (case-insensitive)
      expect(content.toLowerCase()).toMatch(/<!doctype html>/i);
    });

    it('should have proper html element with lang attribute', () => {
      const htmlFiles = getFilesByExtension('.html');
      const indexHtml = htmlFiles.find(f => f.endsWith('index.html'));
      const content = readFileSync(indexHtml!, 'utf-8');

      // Should have html element with lang attribute
      expect(content).toMatch(/<html[^>]*lang=/i);
    });

    it('should have head element with required meta tags', () => {
      const htmlFiles = getFilesByExtension('.html');
      const indexHtml = htmlFiles.find(f => f.endsWith('index.html'));
      const content = readFileSync(indexHtml!, 'utf-8');

      // Should have head element
      expect(content).toMatch(/<head[^>]*>/i);
      // Should have charset meta
      expect(content).toMatch(/<meta[^>]*charset/i);
      // Should have viewport meta
      expect(content).toMatch(/<meta[^>]*viewport/i);
    });

    it('should have title element', () => {
      const htmlFiles = getFilesByExtension('.html');
      const indexHtml = htmlFiles.find(f => f.endsWith('index.html'));
      const content = readFileSync(indexHtml!, 'utf-8');

      expect(content).toMatch(/<title[^>]*>[^<]+<\/title>/i);
    });

    it('should have body element', () => {
      const htmlFiles = getFilesByExtension('.html');
      const indexHtml = htmlFiles.find(f => f.endsWith('index.html'));
      const content = readFileSync(indexHtml!, 'utf-8');

      expect(content).toMatch(/<body[^>]*>/i);
      expect(content).toMatch(/<\/body>/i);
    });

    it('should have properly closed tags (no critical HTML errors)', () => {
      const htmlFiles = getFilesByExtension('.html');
      const indexHtml = htmlFiles.find(f => f.endsWith('index.html'));
      const content = readFileSync(indexHtml!, 'utf-8');

      // Check for basic tag closure
      expect(content).toMatch(/<\/html>/i);
      expect(content).toMatch(/<\/head>/i);
      expect(content).toMatch(/<\/body>/i);
    });
  });

  describe('Test Case 4: CSS Output Validation', () => {
    beforeAll(() => {
      if (!existsSync(DIST_DIR)) {
        execSync('npm run build', {
          cwd: PROJECT_ROOT,
          stdio: 'pipe',
          timeout: 120000,
        });
      }
    });

    it('should have CSS files with valid content', () => {
      const cssFiles = getFilesByExtension('.css');
      expect(cssFiles.length).toBeGreaterThan(0);

      // Each CSS file should have content
      for (const cssFile of cssFiles) {
        const content = readFileSync(cssFile, 'utf-8');
        expect(content.length).toBeGreaterThan(0);
      }
    });

    it('should not have CSS syntax errors (basic validation)', () => {
      const cssFiles = getFilesByExtension('.css');

      for (const cssFile of cssFiles) {
        const content = readFileSync(cssFile, 'utf-8');

        // Check for balanced braces (simple CSS validation)
        const openBraces = (content.match(/{/g) || []).length;
        const closeBraces = (content.match(/}/g) || []).length;
        expect(openBraces).toBe(closeBraces);
      }
    });

    it('should reference CSS from HTML', () => {
      const htmlFiles = getFilesByExtension('.html');
      const indexHtml = htmlFiles.find(f => f.endsWith('index.html'));
      const htmlContent = readFileSync(indexHtml!, 'utf-8');

      // Should have link to stylesheet or inline styles
      const hasStylesheet = htmlContent.match(/<link[^>]*stylesheet/i) !== null;
      const hasInlineStyle = htmlContent.match(/<style[^>]*>/i) !== null;

      expect(hasStylesheet || hasInlineStyle).toBe(true);
    });
  });

  describe('Test Case 5: Static Server Compatibility', () => {
    beforeAll(() => {
      if (!existsSync(DIST_DIR)) {
        execSync('npm run build', {
          cwd: PROJECT_ROOT,
          stdio: 'pipe',
          timeout: 120000,
        });
      }
    });

    it('should have all necessary files for static hosting', () => {
      // A static site should have:
      // 1. An index.html at the root
      const indexHtml = join(DIST_DIR, 'index.html');
      expect(existsSync(indexHtml)).toBe(true);
    });

    it('should have relative or properly hashed asset paths', () => {
      const htmlFiles = getFilesByExtension('.html');
      const indexHtml = htmlFiles.find(f => f.endsWith('index.html'));
      const content = readFileSync(indexHtml!, 'utf-8');

      // Asset paths should be relative (starting with ./ or /) or absolute URLs
      // They should NOT contain localhost or 127.0.0.1 (would fail on static hosting)
      expect(content).not.toMatch(/src=["']http:\/\/localhost/i);
      expect(content).not.toMatch(/href=["']http:\/\/localhost/i);
      expect(content).not.toMatch(/src=["']http:\/\/127\.0\.0\.1/i);
      expect(content).not.toMatch(/href=["']http:\/\/127\.0\.0\.1/i);
    });

    it('should have all referenced assets present in dist', () => {
      const htmlFiles = getFilesByExtension('.html');
      const indexHtml = htmlFiles.find(f => f.endsWith('index.html'));
      const content = readFileSync(indexHtml!, 'utf-8');

      // Extract local asset references from link and script tags
      const assetMatches = content.match(/(?:href|src)=["'](\/[^"']+|\.\/[^"']+)["']/g) || [];

      for (const match of assetMatches) {
        // Extract the path from the match
        const pathMatch = match.match(/["']([^"']+)["']/);
        if (pathMatch) {
          let assetPath = pathMatch[1];

          // Skip external URLs and data URIs
          if (assetPath.startsWith('http') || assetPath.startsWith('data:') || assetPath.startsWith('#')) {
            continue;
          }

          // Convert to absolute path in dist
          if (assetPath.startsWith('/')) {
            assetPath = assetPath.substring(1);
          } else if (assetPath.startsWith('./')) {
            assetPath = assetPath.substring(2);
          }

          const fullPath = join(DIST_DIR, assetPath);

          // Check if file exists (optional assets may be inlined)
          // We just verify no broken mandatory references
          if (assetPath.endsWith('.css') || assetPath.endsWith('.js')) {
            expect(existsSync(fullPath)).toBe(true);
          }
        }
      }
    });
  });

  describe('Test Case 6: No Server-Side Dependencies', () => {
    beforeAll(() => {
      if (!existsSync(DIST_DIR)) {
        execSync('npm run build', {
          cwd: PROJECT_ROOT,
          stdio: 'pipe',
          timeout: 120000,
        });
      }
    });

    it('should not contain server-side runtime code indicators', () => {
      const jsFiles = getFilesByExtension('.js');

      for (const jsFile of jsFiles) {
        const content = readFileSync(jsFile, 'utf-8');

        // Check for common server-side patterns that shouldn't be in client bundle
        // These would indicate SSR is required at runtime
        expect(content).not.toMatch(/require\s*\(\s*['"]fs['"]\s*\)/);
        expect(content).not.toMatch(/require\s*\(\s*['"]http['"]\s*\)/);
        expect(content).not.toMatch(/require\s*\(\s*['"]https['"]\s*\)/);
        expect(content).not.toMatch(/require\s*\(\s*['"]path['"]\s*\)/);
        expect(content).not.toMatch(/require\s*\(\s*['"]child_process['"]\s*\)/);
      }
    });

    it('should have output mode set to static in astro config', () => {
      const astroConfig = readFileSync(join(PROJECT_ROOT, 'astro.config.mjs'), 'utf-8');

      // Verify output mode is 'static'
      expect(astroConfig).toMatch(/output:\s*['"]static['"]/);
    });

    it('should not have adapter configuration (no SSR adapter)', () => {
      const astroConfig = readFileSync(join(PROJECT_ROOT, 'astro.config.mjs'), 'utf-8');

      // SSR adapters like @astrojs/node, @astrojs/vercel (SSR mode), etc. indicate server-side requirements
      expect(astroConfig).not.toMatch(/adapter:\s*\w+\(\)/);
    });

    it('should have no .mjs/.cjs server files in dist (only client JS)', () => {
      // Server-side Astro builds create server files
      // A pure static build should not have these
      const serverDir = join(DIST_DIR, 'server');
      const hasServerDir = existsSync(serverDir);

      if (hasServerDir) {
        const serverFiles = readdirSync(serverDir);
        // Server directory should be empty or not exist for static builds
        expect(serverFiles.length).toBe(0);
      } else {
        expect(hasServerDir).toBe(false);
      }
    });

    it('should be deployable to GitHub Pages (static file structure)', () => {
      // GitHub Pages requirements:
      // 1. index.html at root
      expect(existsSync(join(DIST_DIR, 'index.html'))).toBe(true);

      // 2. All assets accessible via relative paths
      const allFiles = getAllFiles(DIST_DIR);
      expect(allFiles.length).toBeGreaterThan(0);

      // 3. No server configuration files
      expect(existsSync(join(DIST_DIR, 'server.js'))).toBe(false);
      expect(existsSync(join(DIST_DIR, 'handler.js'))).toBe(false);
    });
  });
});
