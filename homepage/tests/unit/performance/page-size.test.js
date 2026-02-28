/**
 * Page Size Unit Tests
 * Owner: Scenario 12 - Page Load Performance
 *
 * Tests:
 * - Total uncompressed page size is under 500KB
 * - Individual file sizes are reasonable
 */

import { describe, it, expect, beforeAll } from 'vitest';
import fs from 'fs';
import path from 'path';

describe('Page Size Performance', () => {
  const srcDir = path.resolve(__dirname, '../../../src');
  let totalSize = 0;
  let fileSizes = [];

  beforeAll(() => {
    // Calculate total size of all source files
    const getFileSizes = (dir) => {
      const entries = fs.readdirSync(dir, { withFileTypes: true });

      for (const entry of entries) {
        const fullPath = path.join(dir, entry.name);

        if (entry.isDirectory()) {
          getFileSizes(fullPath);
        } else if (entry.isFile()) {
          const ext = path.extname(entry.name).toLowerCase();
          // Only count web assets (HTML, CSS, JS, SVG, images)
          if (['.html', '.css', '.js', '.svg', '.png', '.jpg', '.jpeg', '.gif', '.webp', '.woff', '.woff2'].includes(ext)) {
            const stats = fs.statSync(fullPath);
            const relativePath = path.relative(srcDir, fullPath);
            fileSizes.push({
              path: relativePath,
              size: stats.size,
            });
            totalSize += stats.size;
          }
        }
      }
    };

    getFileSizes(srcDir);
  });

  it('total page size is under 500KB uncompressed for fast loading', () => {
    console.log('\nFile sizes:');
    fileSizes
      .sort((a, b) => b.size - a.size)
      .forEach(f => {
        console.log(`  ${f.path}: ${(f.size / 1024).toFixed(2)} KB`);
      });
    console.log(`\nTotal: ${(totalSize / 1024).toFixed(2)} KB`);

    // Total page size should be under 500KB (500 * 1024 bytes)
    expect(totalSize).toBeLessThan(500 * 1024);
  });

  it('individual CSS files are reasonably sized', () => {
    const cssFiles = fileSizes.filter(f => f.path.endsWith('.css'));

    for (const file of cssFiles) {
      // Each CSS file should be under 50KB uncompressed
      expect(file.size, `${file.path} is too large`).toBeLessThan(50 * 1024);
    }
  });

  it('individual JavaScript files are reasonably sized', () => {
    const jsFiles = fileSizes.filter(f => f.path.endsWith('.js'));

    for (const file of jsFiles) {
      // Each JS file should be under 50KB uncompressed
      expect(file.size, `${file.path} is too large`).toBeLessThan(50 * 1024);
    }
  });

  it('HTML file is reasonably sized', () => {
    const htmlFiles = fileSizes.filter(f => f.path.endsWith('.html'));

    for (const file of htmlFiles) {
      // Each HTML file should be under 100KB uncompressed
      expect(file.size, `${file.path} is too large`).toBeLessThan(100 * 1024);
    }
  });
});
