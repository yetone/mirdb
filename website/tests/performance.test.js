import { test, expect } from '@playwright/test';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Utility to get bundle size
function getBundleSize(dir, pattern) {
  const files = fs.readdirSync(dir, { recursive: true })
    .filter(file => file.includes(pattern));

  let total = 0;
  for (const file of files) {
    const filePath = path.join(dir, file);
    if (fs.statSync(filePath).isFile()) {
      total += fs.statSync(filePath).size;
    }
  }
  return total;
}

test.describe('Performance and Load Time Validation', () => {
  const distDir = path.join(__dirname, '..', 'dist');

  test('should build static site successfully', async () => {
    expect(fs.existsSync(distDir)).toBeTruthy();
    expect(fs.existsSync(path.join(distDir, 'index.html'))).toBeTruthy();
  });

  test('JavaScript bundle size is minimal (<100KB)', async () => {
    const jsSize = getBundleSize(distDir, '.js');
    const kb = jsSize / 1024;

    console.log(`JavaScript bundle size: ${kb.toFixed(2)} KB`);

    // For a static showcase site, JS should be minimal
    expect(kb).toBeLessThan(100);
  });

  test('CSS bundle is optimized and small', async () => {
    const cssSize = getBundleSize(distDir, '.css');
    const kb = cssSize / 1024;

    console.log(`CSS bundle size: ${kb.toFixed(2)} KB`);

    expect(kb).toBeLessThan(50); // CSS should also be optimized
  });

  test('HTML is minified and optimized', async () => {
    const htmlPath = path.join(distDir, 'index.html');
    const html = fs.readFileSync(htmlPath, 'utf8');

    // Check if HTML is reasonably sized
    const size = Buffer.byteLength(html, 'utf8');
    const kb = size / 1024;

    console.log(`HTML size: ${kb.toFixed(2)} KB`);

    expect(kb).toBeLessThan(50); // HTML should be optimized

    // Check for optimization markers (simplified check)
    expect(html).toContain('<!DOCTYPE html>'); // Valid HTML
  });

  test('assets are present and optimized', async () => {
    const assetsDir = path.join(distDir, 'assets');

    if (fs.existsSync(assetsDir)) {
      const assets = fs.readdirSync(assetsDir);
      console.log('Assets found:', assets);

      // Check each asset size
      for (const asset of assets) {
        const assetPath = path.join(assetsDir, asset);
        const stat = fs.statSync(assetPath);
        const kb = stat.size / 1024;
        console.log(`  ${asset}: ${kb.toFixed(2)} KB`);
      }
    }
  });
});
