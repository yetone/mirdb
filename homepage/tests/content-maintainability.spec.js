// @ts-check
const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

const HOMEPAGE_DIR = path.join(__dirname, '..');
const CONTENT_FILE = path.join(HOMEPAGE_DIR, 'content.json');
const BUILD_SCRIPT = path.join(HOMEPAGE_DIR, 'build.js');
const INDEX_FILE = path.join(HOMEPAGE_DIR, 'index.html');

/**
 * Test Suite: Content Maintainability (NFR-4)
 *
 * Verifies that content can be easily maintained without requiring code changes.
 * Tests check for:
 * 1. Existence of content configuration files (Markdown or JSON/YAML)
 * 2. Content updates via text file editing
 * 3. Build process that transforms content files into HTML
 */

test.describe('Content Maintainability - NFR-4', () => {

  test.describe('Test Case 1: Content Management via Configuration Files', () => {

    test('content.json exists and contains valid JSON structure', async () => {
      // Verify content.json exists
      expect(fs.existsSync(CONTENT_FILE)).toBe(true);

      // Verify it's valid JSON
      const rawContent = fs.readFileSync(CONTENT_FILE, 'utf8');
      let content;
      expect(() => {
        content = JSON.parse(rawContent);
      }).not.toThrow();

      // Verify essential sections exist
      expect(content).toHaveProperty('site');
      expect(content).toHaveProperty('hero');
      expect(content).toHaveProperty('features');
      expect(content).toHaveProperty('comparison');
      expect(content).toHaveProperty('quickStart');
      expect(content).toHaveProperty('architecture');
      expect(content).toHaveProperty('configuration');
      expect(content).toHaveProperty('projectStatus');
      expect(content).toHaveProperty('footer');
    });

    test('content.json contains all site metadata', async () => {
      const content = JSON.parse(fs.readFileSync(CONTENT_FILE, 'utf8'));

      expect(content.site).toHaveProperty('title');
      expect(content.site).toHaveProperty('description');
      expect(content.site.title).toContain('MirDB');
    });

    test('content.json contains hero section content', async () => {
      const content = JSON.parse(fs.readFileSync(CONTENT_FILE, 'utf8'));

      expect(content.hero).toHaveProperty('title');
      expect(content.hero).toHaveProperty('tagline');
      expect(content.hero).toHaveProperty('description');
      expect(content.hero).toHaveProperty('primaryCta');
      expect(content.hero).toHaveProperty('secondaryCta');
    });

    test('content.json contains features as an array', async () => {
      const content = JSON.parse(fs.readFileSync(CONTENT_FILE, 'utf8'));

      expect(Array.isArray(content.features)).toBe(true);
      expect(content.features.length).toBeGreaterThan(0);

      // Each feature should have title and description
      content.features.forEach((feature) => {
        expect(feature).toHaveProperty('title');
        expect(feature).toHaveProperty('description');
      });
    });

    test('content.json contains configuration values', async () => {
      const content = JSON.parse(fs.readFileSync(CONTENT_FILE, 'utf8'));

      expect(Array.isArray(content.configuration)).toBe(true);
      expect(content.configuration.length).toBeGreaterThan(0);

      // Each config item should have name and value
      content.configuration.forEach((config) => {
        expect(config).toHaveProperty('name');
        expect(config).toHaveProperty('value');
      });
    });

  });

  test.describe('Test Case 2: Content Updates via Text Files', () => {

    test('build script exists', async () => {
      expect(fs.existsSync(BUILD_SCRIPT)).toBe(true);
    });

    test('build script generates valid HTML from content.json', async () => {
      // Backup existing index.html
      const originalHtml = fs.readFileSync(INDEX_FILE, 'utf8');

      try {
        // Run build script
        execSync('node build.js', { cwd: HOMEPAGE_DIR });

        // Verify index.html was generated
        expect(fs.existsSync(INDEX_FILE)).toBe(true);

        // Verify it's valid HTML
        const generatedHtml = fs.readFileSync(INDEX_FILE, 'utf8');
        expect(generatedHtml).toContain('<!DOCTYPE html>');
        expect(generatedHtml).toContain('<html');
        expect(generatedHtml).toContain('</html>');
      } finally {
        // Restore original index.html
        fs.writeFileSync(INDEX_FILE, originalHtml, 'utf8');
      }
    });

    test('content changes in JSON are reflected in generated HTML', async () => {
      // Read original content and HTML
      const originalContent = JSON.parse(fs.readFileSync(CONTENT_FILE, 'utf8'));
      const originalHtml = fs.readFileSync(INDEX_FILE, 'utf8');

      // Modify content
      const modifiedContent = JSON.parse(JSON.stringify(originalContent));
      const testTitle = 'Test Title ' + Date.now();
      modifiedContent.hero.title = testTitle;

      try {
        // Write modified content
        fs.writeFileSync(CONTENT_FILE, JSON.stringify(modifiedContent, null, 2), 'utf8');

        // Run build
        execSync('node build.js', { cwd: HOMEPAGE_DIR });

        // Verify change is reflected in HTML
        const generatedHtml = fs.readFileSync(INDEX_FILE, 'utf8');
        expect(generatedHtml).toContain(testTitle);
      } finally {
        // Restore original files
        fs.writeFileSync(CONTENT_FILE, JSON.stringify(originalContent, null, 2), 'utf8');
        fs.writeFileSync(INDEX_FILE, originalHtml, 'utf8');
      }
    });

    test('no HTML editing required for text content updates', async () => {
      // Read original content
      const content = JSON.parse(fs.readFileSync(CONTENT_FILE, 'utf8'));

      // Verify all text content is in JSON, not hardcoded in HTML template
      // The build script should only contain structure, not content strings
      const buildScript = fs.readFileSync(BUILD_SCRIPT, 'utf8');

      // Build script should reference content object, not hardcode strings
      expect(buildScript).toContain('content.hero.title');
      expect(buildScript).toContain('content.features');
      expect(buildScript).toContain('content.configuration');

      // Build script should NOT contain hardcoded product content
      expect(buildScript).not.toContain('MirDB is a persistent');
      expect(buildScript).not.toContain('Tokio-based async networking');
    });

  });

  test.describe('Test Case 3: Static Site Generator Integration', () => {

    test('build command is defined in package.json', async () => {
      const packageJson = JSON.parse(fs.readFileSync(path.join(HOMEPAGE_DIR, 'package.json'), 'utf8'));
      expect(packageJson.scripts).toHaveProperty('build');
      expect(packageJson.scripts.build).toContain('build.js');
    });

    test('build process transforms content files into valid HTML', async () => {
      const originalHtml = fs.readFileSync(INDEX_FILE, 'utf8');

      try {
        // Run npm build
        execSync('npm run build', { cwd: HOMEPAGE_DIR });

        // Verify HTML structure
        const generatedHtml = fs.readFileSync(INDEX_FILE, 'utf8');

        // Check for essential HTML sections
        expect(generatedHtml).toContain('class="hero"');
        expect(generatedHtml).toContain('class="features"');
        expect(generatedHtml).toContain('class="comparison"');
        expect(generatedHtml).toContain('class="quick-start"');
        expect(generatedHtml).toContain('class="architecture"');
        expect(generatedHtml).toContain('class="configuration"');
        expect(generatedHtml).toContain('class="project-status"');
        expect(generatedHtml).toContain('class="footer"');
      } finally {
        // Restore original
        fs.writeFileSync(INDEX_FILE, originalHtml, 'utf8');
      }
    });

    test('generated HTML contains all content from JSON', async () => {
      const content = JSON.parse(fs.readFileSync(CONTENT_FILE, 'utf8'));
      const originalHtml = fs.readFileSync(INDEX_FILE, 'utf8');

      try {
        // Run build
        execSync('npm run build', { cwd: HOMEPAGE_DIR });

        // Read generated HTML
        const generatedHtml = fs.readFileSync(INDEX_FILE, 'utf8');

        // Verify hero content
        expect(generatedHtml).toContain(content.hero.title);
        expect(generatedHtml).toContain(content.hero.tagline);

        // Verify features
        for (const feature of content.features) {
          expect(generatedHtml).toContain(feature.title);
          expect(generatedHtml).toContain(feature.description);
        }

        // Verify configuration
        for (const config of content.configuration) {
          expect(generatedHtml).toContain(config.name);
          expect(generatedHtml).toContain(config.value);
        }

        // Verify commands
        for (const command of content.projectStatus.commands) {
          expect(generatedHtml).toContain(command.name);
        }
      } finally {
        // Restore original
        fs.writeFileSync(INDEX_FILE, originalHtml, 'utf8');
      }
    });

    test('build process is idempotent', async () => {
      const originalHtml = fs.readFileSync(INDEX_FILE, 'utf8');

      try {
        // Run build twice
        execSync('npm run build', { cwd: HOMEPAGE_DIR });
        const firstBuild = fs.readFileSync(INDEX_FILE, 'utf8');

        execSync('npm run build', { cwd: HOMEPAGE_DIR });
        const secondBuild = fs.readFileSync(INDEX_FILE, 'utf8');

        // Results should be identical
        expect(firstBuild).toBe(secondBuild);
      } finally {
        // Restore original
        fs.writeFileSync(INDEX_FILE, originalHtml, 'utf8');
      }
    });

  });

});
