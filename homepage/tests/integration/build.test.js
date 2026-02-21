/**
 * Static Site Build Tests
 * Owner: Scenario 19 - Static Site Deployment
 *
 * Tests that verify the mdBook build process completes successfully
 * and produces all required static assets for deployment.
 */

const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');

const HOMEPAGE_DIR = path.join(__dirname, '../..');
const BUILD_DIR = path.join(HOMEPAGE_DIR, 'book');

describe('Static Site Deployment - Build Process', () => {
  // Test Case 1: Build completes without errors
  test('mdBook build completes without errors', () => {
    // Clean build directory first
    if (fs.existsSync(BUILD_DIR)) {
      fs.rmSync(BUILD_DIR, { recursive: true, force: true });
    }

    // Run mdBook build
    const mdBookPath = process.env.MDBOOK_PATH || '/tmp/mdbook';
    const result = execSync(`${mdBookPath} build`, {
      cwd: HOMEPAGE_DIR,
      encoding: 'utf-8',
      stdio: ['pipe', 'pipe', 'pipe']
    });

    // Verify build directory was created
    expect(fs.existsSync(BUILD_DIR)).toBe(true);
  });

  // Test Case 2: Output contains index.html
  test('build output contains index.html', () => {
    const indexPath = path.join(BUILD_DIR, 'index.html');
    expect(fs.existsSync(indexPath)).toBe(true);

    // Verify index.html has content
    const content = fs.readFileSync(indexPath, 'utf-8');
    expect(content.length).toBeGreaterThan(0);
    expect(content).toContain('<!DOCTYPE html>');
    expect(content).toContain('MirDB');
  });

  // Test Case 3: Output contains CSS files
  test('build output contains CSS files', () => {
    // mdBook places base CSS in css/ directory
    const cssDir = path.join(BUILD_DIR, 'css');
    expect(fs.existsSync(cssDir)).toBe(true);

    // Check for mdBook base CSS files
    const baseCss = ['general.css', 'chrome.css', 'variables.css'];
    for (const cssFile of baseCss) {
      const cssPath = path.join(cssDir, cssFile);
      expect(fs.existsSync(cssPath)).toBe(true);
      const content = fs.readFileSync(cssPath, 'utf-8');
      expect(content.length).toBeGreaterThan(0);
    }

    // mdBook places additional theme CSS in theme/css/ directory
    const themeCssDir = path.join(BUILD_DIR, 'theme', 'css');
    expect(fs.existsSync(themeCssDir)).toBe(true);

    // Verify custom CSS files exist in theme directory
    const themeCss = [
      'variables.css',
      'general.css',
      'chrome.css',
      'homepage.css',
      'code.css'
    ];

    for (const cssFile of themeCss) {
      const cssPath = path.join(themeCssDir, cssFile);
      expect(fs.existsSync(cssPath)).toBe(true);
      const content = fs.readFileSync(cssPath, 'utf-8');
      expect(content.length).toBeGreaterThan(0);
    }
  });

  // Test Case 4: Output contains image assets (logo)
  test('build output contains image assets including logo', () => {
    const imagesDir = path.join(BUILD_DIR, 'images');
    expect(fs.existsSync(imagesDir)).toBe(true);

    // Verify logo exists
    const logoPath = path.join(imagesDir, 'logo.gif');
    expect(fs.existsSync(logoPath)).toBe(true);

    // Verify logo has content (is not empty)
    const logoStats = fs.statSync(logoPath);
    expect(logoStats.size).toBeGreaterThan(0);

    // Verify architecture diagram exists
    const archPath = path.join(imagesDir, 'architecture.svg');
    expect(fs.existsSync(archPath)).toBe(true);
  });

  // Test that JavaScript files are included in build
  test('build output contains JavaScript files', () => {
    // mdBook places additional JS files in theme/js/ directory
    const jsDir = path.join(BUILD_DIR, 'theme', 'js');
    expect(fs.existsSync(jsDir)).toBe(true);

    // Verify required JS files exist
    const requiredJs = [
      'theme.js',
      'clipboard.js',
      'navigation.js'
    ];

    for (const jsFile of requiredJs) {
      const jsPath = path.join(jsDir, jsFile);
      expect(fs.existsSync(jsPath)).toBe(true);

      // Verify JS file has content
      const content = fs.readFileSync(jsPath, 'utf-8');
      expect(content.length).toBeGreaterThan(0);
    }
  });

  // Test that book.toml configuration is valid
  test('book.toml has valid configuration', () => {
    const bookTomlPath = path.join(HOMEPAGE_DIR, 'book.toml');
    expect(fs.existsSync(bookTomlPath)).toBe(true);

    const content = fs.readFileSync(bookTomlPath, 'utf-8');

    // Verify required configuration keys
    expect(content).toContain('[book]');
    expect(content).toContain('title = "MirDB"');
    expect(content).toContain('[build]');
    expect(content).toContain('build-dir = "book"');
    expect(content).toContain('[output.html]');
    expect(content).toContain('theme = "theme"');
  });

  // Test that HTML includes CSS references
  test('index.html references CSS files correctly', () => {
    const indexPath = path.join(BUILD_DIR, 'index.html');
    const content = fs.readFileSync(indexPath, 'utf-8');

    // Verify CSS files are linked
    expect(content).toContain('css/');
  });

  // Test that build output is suitable for static deployment
  test('build output has no server-side dependencies', () => {
    const indexPath = path.join(BUILD_DIR, 'index.html');
    const content = fs.readFileSync(indexPath, 'utf-8');

    // Verify no PHP, ASP, or other server-side markers
    expect(content).not.toContain('<?php');
    expect(content).not.toContain('<%');

    // Verify it's valid HTML that can be served statically
    expect(content).toContain('<!DOCTYPE html>');
    expect(content).toContain('</html>');
  });
});
