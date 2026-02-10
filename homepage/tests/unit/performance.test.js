/**
 * Performance Unit Tests
 * Owner: Scenario 7 - Performance and Loading
 *
 * Unit tests for validating homepage performance requirements.
 *
 * Expected test coverage:
 * - JavaScript bundle size under 50KB
 * - CSS size under 50KB
 * - No heavy SPA framework imports
 * - Image file sizes under 500KB total
 *
 * Requirements traced:
 * - NFR-1: Quick load with optimal performance
 * - NFR-1: No heavy JavaScript frameworks
 */

const fs = require('fs');
const path = require('path');

// Helper function to get file size
function getFileSize(filePath) {
    try {
        const stats = fs.statSync(filePath);
        return stats.size;
    } catch (e) {
        return 0;
    }
}

// Helper function to read file content
function readFileContent(filePath) {
    try {
        return fs.readFileSync(filePath, 'utf8');
    } catch (e) {
        return '';
    }
}

// Helper function to get all files with extension in directory
function getFilesWithExtension(dir, extension) {
    const files = [];
    try {
        const items = fs.readdirSync(dir, { withFileTypes: true });
        for (const item of items) {
            const fullPath = path.join(dir, item.name);
            if (item.isDirectory()) {
                files.push(...getFilesWithExtension(fullPath, extension));
            } else if (item.name.endsWith(extension)) {
                files.push(fullPath);
            }
        }
    } catch (e) {
        // Directory doesn't exist
    }
    return files;
}

describe('Performance Unit Tests', () => {
    const homepageDir = path.resolve(__dirname, '../..');
    const cssDir = path.join(homepageDir, 'css');
    const jsDir = path.join(homepageDir, 'js');
    const imagesDir = path.join(homepageDir, 'images');
    const assetsDir = path.resolve(homepageDir, '../assets');

    describe('JavaScript Bundle Size (Test Case 3)', () => {
        test('Total JavaScript is under 50KB (no heavy frameworks)', () => {
            const jsFiles = getFilesWithExtension(jsDir, '.js');
            let totalSize = 0;

            for (const file of jsFiles) {
                const size = getFileSize(file);
                totalSize += size;
            }

            // 50KB = 51200 bytes
            expect(totalSize).toBeLessThan(51200);
        });

        test('Individual JS files are reasonably sized', () => {
            const jsFiles = getFilesWithExtension(jsDir, '.js');

            for (const file of jsFiles) {
                const size = getFileSize(file);
                // Each file should be under 30KB for good performance
                expect(size).toBeLessThan(30720);
            }
        });
    });

    describe('CSS Size (Test Case 4)', () => {
        test('Total CSS is under 50KB', () => {
            const cssFiles = getFilesWithExtension(cssDir, '.css');
            let totalSize = 0;

            for (const file of cssFiles) {
                const size = getFileSize(file);
                totalSize += size;
            }

            // 50KB = 51200 bytes
            expect(totalSize).toBeLessThan(51200);
        });

        test('Individual CSS files are reasonably sized', () => {
            const cssFiles = getFilesWithExtension(cssDir, '.css');

            for (const file of cssFiles) {
                const size = getFileSize(file);
                // Each file should be under 30KB
                expect(size).toBeLessThan(30720);
            }
        });
    });

    describe('No Heavy SPA Framework Imports (Test Case 5)', () => {
        test('No React imports in JavaScript files', () => {
            const jsFiles = getFilesWithExtension(jsDir, '.js');

            for (const file of jsFiles) {
                const content = readFileContent(file);
                // Check for React imports and usage patterns
                expect(content).not.toMatch(/import\s+.*\s+from\s+['"]react['"]/);
                expect(content).not.toMatch(/require\s*\(\s*['"]react['"]\s*\)/);
                expect(content).not.toMatch(/ReactDOM/);
                expect(content).not.toMatch(/React\.createElement/);
            }
        });

        test('No Vue imports in JavaScript files', () => {
            const jsFiles = getFilesWithExtension(jsDir, '.js');

            for (const file of jsFiles) {
                const content = readFileContent(file);
                // Check for Vue imports and usage patterns
                expect(content).not.toMatch(/import\s+.*\s+from\s+['"]vue['"]/);
                expect(content).not.toMatch(/require\s*\(\s*['"]vue['"]\s*\)/);
                expect(content).not.toMatch(/new\s+Vue\s*\(/);
                expect(content).not.toMatch(/createApp\s*\(/);
            }
        });

        test('No Angular imports in JavaScript files', () => {
            const jsFiles = getFilesWithExtension(jsDir, '.js');

            for (const file of jsFiles) {
                const content = readFileContent(file);
                // Check for Angular imports and usage patterns
                expect(content).not.toMatch(/import\s+.*\s+from\s+['"]@angular/);
                expect(content).not.toMatch(/require\s*\(\s*['"]@angular/);
                expect(content).not.toMatch(/angular\.module/);
                expect(content).not.toMatch(/ng-app/);
            }
        });

        test('No jQuery imports in JavaScript files', () => {
            const jsFiles = getFilesWithExtension(jsDir, '.js');

            for (const file of jsFiles) {
                const content = readFileContent(file);
                // Check for jQuery imports
                expect(content).not.toMatch(/import\s+.*\s+from\s+['"]jquery['"]/);
                expect(content).not.toMatch(/require\s*\(\s*['"]jquery['"]\s*\)/);
            }
        });

        test('No framework CDN links in HTML', () => {
            const htmlPath = path.join(homepageDir, 'index.html');
            const content = readFileContent(htmlPath);

            // Check for common framework CDN patterns
            expect(content).not.toMatch(/cdn\.jsdelivr\.net.*react/i);
            expect(content).not.toMatch(/unpkg\.com.*react/i);
            expect(content).not.toMatch(/cdn\.jsdelivr\.net.*vue/i);
            expect(content).not.toMatch(/unpkg\.com.*vue/i);
            expect(content).not.toMatch(/ajax\.googleapis\.com.*angular/i);
            expect(content).not.toMatch(/cdnjs\.cloudflare\.com.*react/i);
            expect(content).not.toMatch(/cdnjs\.cloudflare\.com.*vue/i);
            expect(content).not.toMatch(/cdnjs\.cloudflare\.com.*angular/i);
        });
    });

    describe('Image File Sizes (Test Case 7)', () => {
        test('Logo and images are under 500KB total', () => {
            let totalImageSize = 0;

            // Check images in homepage/images directory
            const homepageImages = getFilesWithExtension(imagesDir, '.gif')
                .concat(getFilesWithExtension(imagesDir, '.png'))
                .concat(getFilesWithExtension(imagesDir, '.jpg'))
                .concat(getFilesWithExtension(imagesDir, '.jpeg'))
                .concat(getFilesWithExtension(imagesDir, '.webp'))
                .concat(getFilesWithExtension(imagesDir, '.svg'));

            for (const file of homepageImages) {
                totalImageSize += getFileSize(file);
            }

            // Check for logo in assets directory only if HTML references it
            const htmlPath = path.join(homepageDir, 'index.html');
            const htmlContent = readFileContent(htmlPath);

            // Only count the assets logo if it's actually referenced in HTML
            // The homepage should use optimized images from homepage/images/
            if (htmlContent.includes('../assets/logo.gif') && !htmlContent.includes('images/logo.png')) {
                const logoPath = path.join(assetsDir, 'logo.gif');
                totalImageSize += getFileSize(logoPath);
            }

            // 500KB = 512000 bytes
            expect(totalImageSize).toBeLessThan(512000);
        });

        test('Individual image files are reasonably sized', () => {
            const extensions = ['.gif', '.png', '.jpg', '.jpeg', '.webp', '.svg'];
            const images = extensions.flatMap(ext => getFilesWithExtension(imagesDir, ext));

            for (const file of images) {
                const size = getFileSize(file);
                // Each image should be under 200KB for good performance
                expect(size).toBeLessThan(204800);
            }
        });
    });

    describe('HTML Structure Performance', () => {
        test('HTML file is not bloated', () => {
            const htmlPath = path.join(homepageDir, 'index.html');
            const size = getFileSize(htmlPath);

            // HTML should be under 50KB
            expect(size).toBeLessThan(51200);
        });

        test('No inline large base64 images', () => {
            const htmlPath = path.join(homepageDir, 'index.html');
            const content = readFileContent(htmlPath);

            // Check for very large base64 data URIs (over 10KB encoded)
            const base64Pattern = /data:image\/[^;]+;base64,[A-Za-z0-9+/=]{10000,}/g;
            const matches = content.match(base64Pattern);

            expect(matches).toBeNull();
        });

        test('CSS is linked externally not inline', () => {
            const htmlPath = path.join(homepageDir, 'index.html');
            const content = readFileContent(htmlPath);

            // Check that CSS is loaded via link tags
            expect(content).toMatch(/<link\s+[^>]*rel=["']stylesheet["'][^>]*>/);

            // Check there's no massive inline style block (over 5KB)
            const styleBlocks = content.match(/<style[^>]*>[\s\S]*?<\/style>/g) || [];
            for (const block of styleBlocks) {
                expect(block.length).toBeLessThan(5120);
            }
        });

        test('JavaScript is loaded at end of body (non-blocking)', () => {
            const htmlPath = path.join(homepageDir, 'index.html');
            const content = readFileContent(htmlPath);

            // Check that script tags are at the end of body or have defer/async
            const scriptMatches = content.match(/<script[^>]*src=[^>]*>/gi) || [];

            scriptMatches.forEach(script => {
                const hasDefer = /defer/i.test(script);
                const hasAsync = /async/i.test(script);

                if (!hasDefer && !hasAsync) {
                    // Script should be near end of body
                    const scriptPos = content.indexOf(script);
                    const bodyEndPos = content.indexOf('</body>');

                    // Script should be within 500 characters of body end
                    expect(bodyEndPos - scriptPos).toBeLessThan(500);
                }
            });
        });
    });
});
