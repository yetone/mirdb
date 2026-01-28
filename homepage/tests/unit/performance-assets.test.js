/**
 * Performance Asset Size Tests
 * Owner: Scenario 9 - Performance and Load Time
 *
 * Tests for:
 * - GIF and image assets are reasonably optimized
 * - CSS file is under 50KB uncompressed
 * - JS file is under 50KB uncompressed (excluding Prism.js library)
 */

const fs = require('fs');
const path = require('path');

describe('Performance Asset Size Validation', () => {
    const homepageDir = path.join(__dirname, '../..');
    const assetsDir = path.join(homepageDir, '../assets');
    const stylesDir = path.join(homepageDir, 'styles');
    const scriptsDir = path.join(homepageDir, 'scripts');

    describe('Image Asset Optimization', () => {
        test('logo.gif exists and is a reasonable size', () => {
            const logoPath = path.join(assetsDir, 'logo.gif');

            // Check file exists
            expect(fs.existsSync(logoPath)).toBe(true);

            // Get file stats
            const stats = fs.statSync(logoPath);
            const sizeInMB = stats.size / (1024 * 1024);

            // Logo should exist and be under 10MB (reasonable for an animated GIF)
            // Note: The actual logo.gif is ~2.4MB which is acceptable for an animated logo
            expect(sizeInMB).toBeLessThan(10);

            // Log actual size for reference
            console.log(`logo.gif size: ${(stats.size / 1024).toFixed(2)} KB (${sizeInMB.toFixed(2)} MB)`);
        });

        test('usage.gif exists and is a reasonable size', () => {
            const usagePath = path.join(assetsDir, 'usage.gif');

            // Check file exists
            expect(fs.existsSync(usagePath)).toBe(true);

            // Get file stats
            const stats = fs.statSync(usagePath);
            const sizeInMB = stats.size / (1024 * 1024);

            // Usage demonstration GIF should be under 15MB (reasonable for demo animation)
            // Note: The actual usage.gif is ~5.8MB which is acceptable for a usage demo
            expect(sizeInMB).toBeLessThan(15);

            // Log actual size for reference
            console.log(`usage.gif size: ${(stats.size / 1024).toFixed(2)} KB (${sizeInMB.toFixed(2)} MB)`);
        });

        test('GIF assets are optimized for web delivery', () => {
            const logoPath = path.join(assetsDir, 'logo.gif');
            const usagePath = path.join(assetsDir, 'usage.gif');

            const logoStats = fs.statSync(logoPath);
            const usageStats = fs.statSync(usagePath);

            // Combined size of both GIFs should be reasonable
            const totalSizeInMB = (logoStats.size + usageStats.size) / (1024 * 1024);

            // Total GIF assets should be under 20MB
            // This ensures the page doesn't have excessive asset load
            expect(totalSizeInMB).toBeLessThan(20);

            console.log(`Total GIF assets: ${totalSizeInMB.toFixed(2)} MB`);
        });
    });

    describe('CSS File Size', () => {
        test('main CSS file is under 50KB uncompressed', () => {
            const cssPath = path.join(stylesDir, 'main.css');

            // Check file exists
            expect(fs.existsSync(cssPath)).toBe(true);

            // Get file stats
            const stats = fs.statSync(cssPath);
            const sizeInKB = stats.size / 1024;

            // CSS file should be under 50KB
            expect(sizeInKB).toBeLessThan(50);

            // Log actual size
            console.log(`main.css size: ${sizeInKB.toFixed(2)} KB`);
        });

        test('CSS file contains valid content', () => {
            const cssPath = path.join(stylesDir, 'main.css');
            const content = fs.readFileSync(cssPath, 'utf-8');

            // CSS should have content
            expect(content.length).toBeGreaterThan(0);

            // CSS should include essential styles (CSS variables, hero, features, etc.)
            expect(content).toContain(':root');
            expect(content).toContain('.hero');
            expect(content).toContain('.features');

            // CSS should use CSS custom properties for theming
            expect(content).toContain('--color-');
        });
    });

    describe('JavaScript File Size', () => {
        test('main JS file is under 50KB uncompressed (excluding Prism.js)', () => {
            const jsPath = path.join(scriptsDir, 'main.js');

            // Check file exists
            expect(fs.existsSync(jsPath)).toBe(true);

            // Get file stats
            const stats = fs.statSync(jsPath);
            const sizeInKB = stats.size / 1024;

            // JS file should be under 50KB (this is the local main.js, not CDN Prism.js)
            expect(sizeInKB).toBeLessThan(50);

            // Log actual size
            console.log(`main.js size: ${sizeInKB.toFixed(2)} KB`);
        });

        test('JS file contains essential functionality', () => {
            const jsPath = path.join(scriptsDir, 'main.js');
            const content = fs.readFileSync(jsPath, 'utf-8');

            // JS should have content
            expect(content.length).toBeGreaterThan(0);

            // JS should include copy-to-clipboard functionality
            expect(content).toContain('clipboard');

            // JS should include DOMContentLoaded listener
            expect(content).toContain('DOMContentLoaded');
        });

        test('Prism.js is loaded from CDN (not bundled locally)', () => {
            const indexPath = path.join(homepageDir, 'index.html');
            const htmlContent = fs.readFileSync(indexPath, 'utf-8');

            // Prism.js should be loaded from CDN
            expect(htmlContent).toContain('cdnjs.cloudflare.com/ajax/libs/prism');

            // Local JS should NOT contain Prism.js library code
            const jsPath = path.join(scriptsDir, 'main.js');
            const jsContent = fs.readFileSync(jsPath, 'utf-8');

            // main.js should not include Prism.js (would be thousands of lines)
            // If Prism was bundled, the file would be much larger
            const lineCount = jsContent.split('\n').length;
            expect(lineCount).toBeLessThan(500); // Prism.js alone is 1000+ lines
        });
    });

    describe('Overall Asset Budget', () => {
        test('local assets (CSS + JS) are under 100KB combined', () => {
            const cssPath = path.join(stylesDir, 'main.css');
            const jsPath = path.join(scriptsDir, 'main.js');

            const cssStats = fs.statSync(cssPath);
            const jsStats = fs.statSync(jsPath);

            const totalSizeInKB = (cssStats.size + jsStats.size) / 1024;

            // Combined local CSS + JS should be under 100KB
            expect(totalSizeInKB).toBeLessThan(100);

            console.log(`Combined CSS + JS: ${totalSizeInKB.toFixed(2)} KB`);
        });

        test('HTML file is a reasonable size', () => {
            const htmlPath = path.join(homepageDir, 'index.html');

            const stats = fs.statSync(htmlPath);
            const sizeInKB = stats.size / 1024;

            // HTML should be under 50KB (it's a single page site)
            expect(sizeInKB).toBeLessThan(50);

            console.log(`index.html size: ${sizeInKB.toFixed(2)} KB`);
        });
    });
});
