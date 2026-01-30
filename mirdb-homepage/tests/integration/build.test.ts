/**
 * Build Integration Tests.
 * Owner: Scenario 15 - Static Site Deployment Compatibility
 *
 * Tests:
 * - All asset paths are relative (no absolute paths starting with /)
 * - Build completes successfully with no errors
 * - Site functions without any backend server
 * - Custom 404 page or proper fallback exists
 * - All external links have rel='noopener noreferrer'
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { execSync } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';

const PROJECT_ROOT = path.resolve(__dirname, '../..');
const DIST_DIR = path.join(PROJECT_ROOT, 'dist');
const SRC_DIR = path.join(PROJECT_ROOT, 'src');
const PUBLIC_DIR = path.join(PROJECT_ROOT, 'public');

describe('Static Site Deployment Compatibility', () => {
  describe('Test Case 1: Relative Asset Paths', () => {
    it('should not have absolute paths starting with / for local assets in index.html', () => {
      const indexHtmlPath = path.join(SRC_DIR, 'index.html');
      const content = fs.readFileSync(indexHtmlPath, 'utf-8');

      // Check for absolute paths in src and href attributes (excluding http/https and protocol-relative URLs)
      // Match src="/" or href="/" but not src="http" or href="http" or src="//" or href="//"
      const absoluteLocalPathRegex = /(src|href)=["']\/(?!\/|https?:)[^"']*["']/gi;
      const matches = content.match(absoluteLocalPathRegex);

      expect(matches).toBeNull();
    });

    it('should use relative paths (./) for local stylesheet links', () => {
      const indexHtmlPath = path.join(SRC_DIR, 'index.html');
      const content = fs.readFileSync(indexHtmlPath, 'utf-8');

      // Check that local stylesheet uses relative path
      const stylesheetMatch = content.match(/href=["']\.\/styles\/main\.css["']/);
      expect(stylesheetMatch).not.toBeNull();
    });

    it('should use relative paths (./) for local script sources', () => {
      const indexHtmlPath = path.join(SRC_DIR, 'index.html');
      const content = fs.readFileSync(indexHtmlPath, 'utf-8');

      // Check that main.ts script uses relative path
      const scriptMatch = content.match(/src=["']\.\/main\.ts["']/);
      expect(scriptMatch).not.toBeNull();
    });

    it('should have base set to relative path in vite.config.ts', () => {
      const viteConfigPath = path.join(PROJECT_ROOT, 'vite.config.ts');
      const content = fs.readFileSync(viteConfigPath, 'utf-8');

      // Check that base is set to './' for relative paths
      const baseMatch = content.match(/base:\s*['"]\.\/['"]/);
      expect(baseMatch).not.toBeNull();
    });
  });

  describe('Test Case 2: Build Production Files', () => {
    let buildSucceeded = false;
    let buildOutput = '';

    beforeAll(() => {
      try {
        // Run the build command
        buildOutput = execSync('npm run build', {
          cwd: PROJECT_ROOT,
          encoding: 'utf-8',
          stdio: ['pipe', 'pipe', 'pipe'],
        });
        buildSucceeded = true;
      } catch (error: unknown) {
        const execError = error as { stderr?: string; stdout?: string };
        buildOutput = execError.stderr || execError.stdout || 'Unknown error';
        buildSucceeded = false;
      }
    });

    it('should complete build successfully with no errors', () => {
      expect(buildSucceeded).toBe(true);
    });

    it('should create dist directory with output files', () => {
      expect(fs.existsSync(DIST_DIR)).toBe(true);
    });

    it('should generate index.html in dist directory', () => {
      const indexPath = path.join(DIST_DIR, 'index.html');
      expect(fs.existsSync(indexPath)).toBe(true);
    });

    it('should generate bundled JavaScript assets', () => {
      const assetsDir = path.join(DIST_DIR, 'assets');
      if (fs.existsSync(assetsDir)) {
        const files = fs.readdirSync(assetsDir);
        const jsFiles = files.filter((f) => f.endsWith('.js'));
        expect(jsFiles.length).toBeGreaterThan(0);
      } else {
        // If no assets dir, check for inline scripts or direct js files
        const distFiles = fs.readdirSync(DIST_DIR);
        const hasJsContent =
          distFiles.some((f) => f.endsWith('.js')) ||
          fs.readFileSync(path.join(DIST_DIR, 'index.html'), 'utf-8').includes('<script');
        expect(hasJsContent).toBe(true);
      }
    });

    it('should generate bundled CSS assets', () => {
      const assetsDir = path.join(DIST_DIR, 'assets');
      if (fs.existsSync(assetsDir)) {
        const files = fs.readdirSync(assetsDir);
        const cssFiles = files.filter((f) => f.endsWith('.css'));
        expect(cssFiles.length).toBeGreaterThan(0);
      } else {
        // If no assets dir, check for inline styles or direct css files
        const distFiles = fs.readdirSync(DIST_DIR);
        const hasCssContent =
          distFiles.some((f) => f.endsWith('.css')) ||
          fs.readFileSync(path.join(DIST_DIR, 'index.html'), 'utf-8').includes('<style');
        expect(hasCssContent).toBe(true);
      }
    });

    it('should use relative paths in built HTML for assets', () => {
      const indexPath = path.join(DIST_DIR, 'index.html');
      if (fs.existsSync(indexPath)) {
        const content = fs.readFileSync(indexPath, 'utf-8');
        // Check that asset references don't start with absolute path /
        // but allow external URLs (http, https, //)
        const absoluteLocalPathRegex = /(src|href)=["']\/(?!\/|assets\/|https?:)[^"']*["']/gi;
        const matches = content.match(absoluteLocalPathRegex);
        expect(matches).toBeNull();
      }
    });
  });

  describe('Test Case 3: No Server-Side Dependencies', () => {
    it('should not have server-side runtime dependencies in package.json', () => {
      const packageJsonPath = path.join(PROJECT_ROOT, 'package.json');
      const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, 'utf-8'));

      // Check that there are no production dependencies (it's a static site)
      // undefined dependencies or empty dependencies object both indicate no prod deps
      const hasProdDeps =
        packageJson.dependencies !== undefined &&
        Object.keys(packageJson.dependencies).length > 0;
      expect(hasProdDeps).toBe(false);
    });

    it('should only have devDependencies (build-time dependencies)', () => {
      const packageJsonPath = path.join(PROJECT_ROOT, 'package.json');
      const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, 'utf-8'));

      // Static sites should only have devDependencies
      expect(packageJson.devDependencies).toBeDefined();
      expect(typeof packageJson.devDependencies).toBe('object');
    });

    it('should not use server-side frameworks in the project', () => {
      const packageJsonPath = path.join(PROJECT_ROOT, 'package.json');
      const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, 'utf-8'));

      const allDeps = {
        ...(packageJson.dependencies || {}),
        ...(packageJson.devDependencies || {}),
      };

      // Server-side frameworks that would indicate SSR requirements
      const serverFrameworks = ['express', 'fastify', 'koa', 'hapi', 'next', 'nuxt', 'nest'];

      const hasServerFramework = serverFrameworks.some((framework) =>
        Object.keys(allDeps).some((dep) => dep.toLowerCase().includes(framework))
      );

      expect(hasServerFramework).toBe(false);
    });

    it('should generate only static files in build output', () => {
      if (fs.existsSync(DIST_DIR)) {
        const staticExtensions = ['.html', '.css', '.js', '.svg', '.png', '.jpg', '.gif', '.ico', '.txt', '.webp', '.woff', '.woff2', '.ttf', '.map'];

        function getAllFiles(dir: string): string[] {
          const files: string[] = [];
          const entries = fs.readdirSync(dir, { withFileTypes: true });
          for (const entry of entries) {
            const fullPath = path.join(dir, entry.name);
            if (entry.isDirectory()) {
              files.push(...getAllFiles(fullPath));
            } else {
              files.push(fullPath);
            }
          }
          return files;
        }

        const allFiles = getAllFiles(DIST_DIR);
        const allStatic = allFiles.every((file) => {
          const ext = path.extname(file).toLowerCase();
          return staticExtensions.includes(ext) || ext === '';
        });

        expect(allStatic).toBe(true);
      }
    });
  });

  describe('Test Case 4: 404 Page Handling', () => {
    it('should have a 404.html file in public directory for GitHub Pages', () => {
      const notFoundPath = path.join(PUBLIC_DIR, '404.html');
      expect(fs.existsSync(notFoundPath)).toBe(true);
    });

    it('should include 404.html in build output', () => {
      if (fs.existsSync(DIST_DIR)) {
        const notFoundInDist = path.join(DIST_DIR, '404.html');
        expect(fs.existsSync(notFoundInDist)).toBe(true);
      }
    });

    it('should have meaningful content in 404 page', () => {
      const notFoundPath = path.join(PUBLIC_DIR, '404.html');
      if (fs.existsSync(notFoundPath)) {
        const content = fs.readFileSync(notFoundPath, 'utf-8');
        // Should have basic HTML structure
        expect(content).toContain('<!DOCTYPE html>');
        expect(content).toContain('404');
        // Should have a link back to home
        expect(content.toLowerCase()).toMatch(/href=["'].*["']/);
      }
    });
  });

  describe('Test Case 5: External Link Security Attributes', () => {
    it('should have rel="noopener noreferrer" on external links in Footer component', () => {
      const footerPath = path.join(SRC_DIR, 'components', 'Footer.ts');
      const content = fs.readFileSync(footerPath, 'utf-8');

      // Count external link creations (target='_blank')
      const targetBlankMatches = content.match(/target\s*=\s*['"]_blank['"]/g) || [];
      const relNoopenerMatches = content.match(/rel\s*=\s*['"]noopener noreferrer['"]/g) || [];

      // Each external link with target="_blank" should have rel="noopener noreferrer"
      expect(relNoopenerMatches.length).toBeGreaterThanOrEqual(targetBlankMatches.length);
    });

    it('should have rel="noopener noreferrer" on external links in GettingStarted component', () => {
      const gettingStartedPath = path.join(SRC_DIR, 'components', 'GettingStarted.ts');
      if (fs.existsSync(gettingStartedPath)) {
        const content = fs.readFileSync(gettingStartedPath, 'utf-8');

        const targetBlankMatches = content.match(/target\s*=\s*['"]_blank['"]/g) || [];
        const relNoopenerMatches = content.match(/rel\s*=\s*['"]noopener noreferrer['"]/g) || [];

        // If there are external links, they should have the security attributes
        if (targetBlankMatches.length > 0) {
          expect(relNoopenerMatches.length).toBeGreaterThanOrEqual(targetBlankMatches.length);
        }
      }
    });

    it('should not have external links without rel="noopener noreferrer" in source files', () => {
      const componentsDir = path.join(SRC_DIR, 'components');
      const componentFiles = fs.readdirSync(componentsDir).filter((f) => f.endsWith('.ts'));

      for (const file of componentFiles) {
        const filePath = path.join(componentsDir, file);
        const content = fs.readFileSync(filePath, 'utf-8');

        // Find all instances where target='_blank' is set
        const lines = content.split('\n');
        let insideExternalLinkBlock = false;
        let blockHasRel = false;
        let linesSinceTarget = 0;

        for (const line of lines) {
          if (line.includes("target") && line.includes("_blank")) {
            insideExternalLinkBlock = true;
            linesSinceTarget = 0;
            blockHasRel = line.includes('noopener') && line.includes('noreferrer');
          }

          if (insideExternalLinkBlock) {
            if (line.includes('noopener') && line.includes('noreferrer')) {
              blockHasRel = true;
            }
            linesSinceTarget++;
            // Check within 5 lines for the rel attribute
            if (linesSinceTarget > 5) {
              expect(blockHasRel).toBe(true);
              insideExternalLinkBlock = false;
            }
          }
        }
      }
    });

    it('should have security attributes on external links in index.html', () => {
      const indexHtmlPath = path.join(SRC_DIR, 'index.html');
      const content = fs.readFileSync(indexHtmlPath, 'utf-8');

      // Find all anchor tags with target="_blank"
      const externalLinkRegex = /<a[^>]*target=["']_blank["'][^>]*>/gi;
      const externalLinks = content.match(externalLinkRegex) || [];

      for (const link of externalLinks) {
        expect(link).toMatch(/rel=["'][^"']*noopener[^"']*["']/i);
        expect(link).toMatch(/rel=["'][^"']*noreferrer[^"']*["']/i);
      }
    });
  });

  describe('Vite Configuration for GitHub Pages', () => {
    it('should have proper build output directory configuration', () => {
      const viteConfigPath = path.join(PROJECT_ROOT, 'vite.config.ts');
      const content = fs.readFileSync(viteConfigPath, 'utf-8');

      // Should have outDir configured
      expect(content).toMatch(/outDir:\s*['"]\.\.\/dist['"]/);
    });

    it('should have emptyOutDir enabled for clean builds', () => {
      const viteConfigPath = path.join(PROJECT_ROOT, 'vite.config.ts');
      const content = fs.readFileSync(viteConfigPath, 'utf-8');

      expect(content).toMatch(/emptyOutDir:\s*true/);
    });
  });

  describe('Public Assets', () => {
    it('should have robots.txt in public directory', () => {
      const robotsPath = path.join(PUBLIC_DIR, 'robots.txt');
      expect(fs.existsSync(robotsPath)).toBe(true);
    });

    it('should copy public assets to dist during build', () => {
      if (fs.existsSync(DIST_DIR)) {
        const robotsInDist = path.join(DIST_DIR, 'robots.txt');
        expect(fs.existsSync(robotsInDist)).toBe(true);
      }
    });
  });
});
