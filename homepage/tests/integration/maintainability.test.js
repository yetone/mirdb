/**
 * Content Maintainability Integration Tests
 * Owner: Scenario 10 - Content Maintainability
 *
 * Integration tests for validating that the MirDB homepage requires no build step
 * and files work directly without transpilation.
 *
 * Requirements traced:
 * - NFR-4: Homepage shall be easy to maintain
 * - NFR-1: No heavy JavaScript frameworks, static files preferred
 */

const fs = require('fs');
const path = require('path');

describe('No Build Step Required', () => {
  const homepageDir = path.join(__dirname, '../..');

  test('HTML files should work directly without build/transpile step', () => {
    const htmlPath = path.join(homepageDir, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf8');

    // HTML should be standard HTML5 - no JSX or template syntax
    expect(htmlContent).toMatch(/<!DOCTYPE html>/i);
    expect(htmlContent).not.toMatch(/<\w+\s+className=/); // No React JSX
    expect(htmlContent).not.toMatch(/\{\{.*\}\}/); // No template literals like Vue/Angular
    expect(htmlContent).not.toMatch(/<script[^>]*type\s*=\s*["']module["'][^>]*>/i); // No ES6 module imports that require bundling

    // Should have standard script tag, not module type
    expect(htmlContent).toMatch(/<script[^>]*src\s*=\s*["'][^"']*\.js["'][^>]*>/i);
  });

  test('CSS files should work directly without preprocessing', () => {
    const cssPath = path.join(homepageDir, 'css/styles.css');
    const cssContent = fs.readFileSync(cssPath, 'utf8');

    // CSS should be standard CSS - no SCSS/SASS/LESS syntax
    expect(cssContent).not.toMatch(/\$[a-zA-Z]/); // No SASS variables ($var)
    expect(cssContent).not.toMatch(/@import\s+["'][^"']+\.scss["']/i); // No SCSS imports
    expect(cssContent).not.toMatch(/@import\s+["'][^"']+\.less["']/i); // No LESS imports
    expect(cssContent).not.toMatch(/@mixin\s+/); // No SASS mixins
    expect(cssContent).not.toMatch(/@include\s+/); // No SASS includes
    expect(cssContent).not.toMatch(/&\s*\{/); // No SCSS nesting with &

    // Should use standard CSS custom properties instead
    expect(cssContent).toMatch(/--[a-zA-Z-]+\s*:/);
    expect(cssContent).toMatch(/var\(\s*--/);
  });

  test('JavaScript files should work directly without transpilation', () => {
    const jsPath = path.join(homepageDir, 'js/main.js');
    const jsContent = fs.readFileSync(jsPath, 'utf8');

    // JavaScript should be browser-compatible ES5/ES6 without requiring Babel
    expect(jsContent).not.toMatch(/^import\s+.*from\s+['"][^'"]+['"]/m); // No ES6 module imports
    expect(jsContent).not.toMatch(/^export\s+(default\s+)?/m); // No ES6 exports
    expect(jsContent).not.toMatch(/import\s*\(/); // No dynamic imports

    // Should be an IIFE or plain JavaScript
    expect(jsContent).toMatch(/\(function\s*\(\)\s*\{|function\s+\w+\s*\(/);
  });

  test('should not require package.json build scripts for production', () => {
    const packagePath = path.join(homepageDir, 'package.json');
    const packageJson = JSON.parse(fs.readFileSync(packagePath, 'utf8'));

    // Should not have complex build scripts
    const scripts = packageJson.scripts || {};

    // Should not have webpack, rollup, vite, or other bundler commands
    const buildScript = scripts.build || '';
    expect(buildScript).not.toMatch(/webpack|rollup|vite|parcel|esbuild/i);

    // Should not have transpilation commands
    expect(buildScript).not.toMatch(/babel|tsc|typescript/i);
  });

  test('should not have build configuration files that indicate required build step', () => {
    const buildConfigFiles = [
      'webpack.config.js',
      'rollup.config.js',
      'vite.config.js',
      'babel.config.js',
      '.babelrc',
      'tsconfig.json',
      'postcss.config.js',
      'tailwind.config.js'
    ];

    buildConfigFiles.forEach(configFile => {
      const configPath = path.join(homepageDir, configFile);
      const exists = fs.existsSync(configPath);
      expect(exists).toBe(false);
    });
  });

  test('all static files should be directly servable', () => {
    // Check that all referenced files exist and are plain text/source files
    const requiredFiles = [
      'index.html',
      'css/styles.css',
      'css/responsive.css',
      'js/main.js'
    ];

    requiredFiles.forEach(file => {
      const filePath = path.join(homepageDir, file);
      expect(fs.existsSync(filePath)).toBe(true);

      // Files should be readable as text
      const content = fs.readFileSync(filePath, 'utf8');
      expect(typeof content).toBe('string');
      expect(content.length).toBeGreaterThan(0);
    });
  });

  test('HTML should reference CSS and JS with relative paths', () => {
    const htmlPath = path.join(homepageDir, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf8');

    // CSS links should use relative paths
    expect(htmlContent).toMatch(/<link[^>]*href\s*=\s*["']css\/styles\.css["']/i);
    expect(htmlContent).toMatch(/<link[^>]*href\s*=\s*["']css\/responsive\.css["']/i);

    // JS script should use relative path
    expect(htmlContent).toMatch(/<script[^>]*src\s*=\s*["']js\/main\.js["']/i);
  });

  test('no node_modules imports in client-side code', () => {
    const jsPath = path.join(homepageDir, 'js/main.js');
    const jsContent = fs.readFileSync(jsPath, 'utf8');

    // Should not reference node_modules
    expect(jsContent).not.toMatch(/require\s*\(\s*['"][^'"./]/); // No require('package')
    expect(jsContent).not.toMatch(/from\s+['"][^'"./]/); // No import from 'package'
  });
});

describe('File Organization Clarity', () => {
  const homepageDir = path.join(__dirname, '../..');

  test('should have clear file structure with logical organization', () => {
    // Check that directories exist and are organized
    const expectedDirs = ['css', 'js'];

    expectedDirs.forEach(dir => {
      const dirPath = path.join(homepageDir, dir);
      expect(fs.existsSync(dirPath)).toBe(true);
      expect(fs.statSync(dirPath).isDirectory()).toBe(true);
    });
  });

  test('should have index.html at root level', () => {
    const indexPath = path.join(homepageDir, 'index.html');
    expect(fs.existsSync(indexPath)).toBe(true);
  });

  test('CSS files should be in css directory', () => {
    const cssDir = path.join(homepageDir, 'css');
    const files = fs.readdirSync(cssDir);

    // All files in css dir should be .css files
    const cssFiles = files.filter(f => f.endsWith('.css'));
    expect(cssFiles.length).toBeGreaterThan(0);
  });

  test('JavaScript files should be in js directory', () => {
    const jsDir = path.join(homepageDir, 'js');
    const files = fs.readdirSync(jsDir);

    // All files in js dir should be .js files
    const jsFiles = files.filter(f => f.endsWith('.js'));
    expect(jsFiles.length).toBeGreaterThan(0);
  });
});
