import { test, expect } from '@playwright/test';
import { exec } from 'child_process';
import { promisify } from 'util';
import * as fs from 'fs';
import * as path from 'path';

const execAsync = promisify(exec);

/**
 * Image Optimization Tests
 *
 * This test suite verifies that images on the MirDB homepage are optimized
 * for web performance. The tests check:
 * 1. Logo image file size is under 500KB (compressed from 2.5MB)
 * 2. Images use WebP or other optimized formats where supported
 * 3. Below-fold images have lazy loading enabled
 * 4. Total image payload is under 1MB
 */

test.describe('Image Optimization', () => {

  test.describe('Test Case 1: Logo Image File Size', () => {
    test('Logo GIF should be under 500KB', async () => {
      const logoPath = path.join(process.cwd(), 'assets', 'logo.gif');
      const stats = fs.statSync(logoPath);
      const sizeInKB = stats.size / 1024;

      console.log(`Logo GIF size: ${sizeInKB.toFixed(2)} KB`);

      // Logo should be under 500KB (was originally 2.5MB)
      expect(sizeInKB).toBeLessThan(500);
    });

    test('Logo WebP should exist and be smaller than GIF', async () => {
      const logoGifPath = path.join(process.cwd(), 'assets', 'logo.gif');
      const logoWebpPath = path.join(process.cwd(), 'assets', 'logo.webp');

      // WebP file should exist
      expect(fs.existsSync(logoWebpPath)).toBe(true);

      const gifStats = fs.statSync(logoGifPath);
      const webpStats = fs.statSync(logoWebpPath);

      console.log(`Logo GIF size: ${(gifStats.size / 1024).toFixed(2)} KB`);
      console.log(`Logo WebP size: ${(webpStats.size / 1024).toFixed(2)} KB`);

      // WebP should be smaller than GIF
      expect(webpStats.size).toBeLessThan(gifStats.size);
    });
  });

  test.describe('Test Case 2: WebP/Modern Format Support', () => {
    test('Page should have picture elements with WebP sources', async ({ page }) => {
      await page.goto('/');

      // Check for picture elements
      const pictureElements = page.locator('picture');
      const pictureCount = await pictureElements.count();

      console.log(`Found ${pictureCount} picture elements`);
      expect(pictureCount).toBeGreaterThan(0);

      // Check for WebP source elements
      const webpSources = page.locator('picture source[type="image/webp"]');
      const webpCount = await webpSources.count();

      console.log(`Found ${webpCount} WebP source elements`);
      expect(webpCount).toBeGreaterThan(0);
    });

    test('Logo should use picture element with WebP source', async ({ page }) => {
      await page.goto('/');

      // Check that logo has WebP source
      const logoPicture = page.locator('picture:has(img[data-image="logo"])');
      await expect(logoPicture).toBeVisible();

      const logoWebpSource = logoPicture.locator('source[type="image/webp"]');
      await expect(logoWebpSource).toHaveCount(1);

      const srcset = await logoWebpSource.getAttribute('srcset');
      expect(srcset).toContain('.webp');
    });

    test('WebP files should exist in assets directory', async () => {
      const assetsPath = path.join(process.cwd(), 'assets');
      const files = fs.readdirSync(assetsPath);

      const webpFiles = files.filter(f => f.endsWith('.webp'));
      console.log(`Found WebP files: ${webpFiles.join(', ')}`);

      expect(webpFiles.length).toBeGreaterThan(0);
      expect(webpFiles).toContain('logo.webp');
    });
  });

  test.describe('Test Case 3: Lazy Loading on Below-Fold Images', () => {
    test('Below-fold images should have loading="lazy" attribute', async ({ page }) => {
      await page.goto('/');

      // Get all images
      const allImages = page.locator('img');
      const imageCount = await allImages.count();

      console.log(`Total images on page: ${imageCount}`);

      // Check for lazy loading on images that are below the fold
      // The logo is above the fold, so we check other images
      for (let i = 0; i < imageCount; i++) {
        const img = allImages.nth(i);
        const dataImage = await img.getAttribute('data-image');
        const loading = await img.getAttribute('loading');
        const src = await img.getAttribute('src');

        console.log(`Image ${i + 1}: data-image="${dataImage}", loading="${loading}", src="${src}"`);

        // Usage/demo images should have lazy loading (they're below the fold)
        if (dataImage === 'usage') {
          expect(loading).toBe('lazy');
        }

        // External badge images don't need lazy loading check as they're small
        // and served from CDN with proper caching
      }
    });

    test('Non-hero images should implement lazy loading', async ({ page }) => {
      await page.goto('/');

      // Check that images with data-image="usage" have lazy loading
      const usageImages = page.locator('img[data-image="usage"]');
      const usageCount = await usageImages.count();

      for (let i = 0; i < usageCount; i++) {
        const loadingAttr = await usageImages.nth(i).getAttribute('loading');
        expect(loadingAttr).toBe('lazy');
      }
    });
  });

  test.describe('Test Case 4: Total Image Payload Size', () => {
    test('WebP image payload should be under 1MB (modern browsers)', async () => {
      const assetsPath = path.join(process.cwd(), 'assets');
      const files = fs.readdirSync(assetsPath);

      // Calculate total size of WebP images (what modern browsers download)
      let totalSize = 0;
      const imageFiles: { name: string; size: number }[] = [];

      for (const file of files) {
        // Skip original backup files
        if (file.includes('.original')) continue;

        if (file.endsWith('.webp')) {
          const filePath = path.join(assetsPath, file);
          const stats = fs.statSync(filePath);
          totalSize += stats.size;
          imageFiles.push({ name: file, size: stats.size });
        }
      }

      console.log('WebP files (served to modern browsers):');
      imageFiles.forEach(f => {
        console.log(`  ${f.name}: ${(f.size / 1024).toFixed(2)} KB`);
      });

      const totalSizeKB = totalSize / 1024;
      console.log(`Total WebP payload: ${totalSizeKB.toFixed(2)} KB`);

      // WebP payload should be under 1MB (1024 KB)
      expect(totalSizeKB).toBeLessThan(1024);
    });

    test('GIF images alone should be under 1MB total', async () => {
      const assetsPath = path.join(process.cwd(), 'assets');
      const files = fs.readdirSync(assetsPath);

      let totalGifSize = 0;
      const gifFiles: { name: string; size: number }[] = [];

      for (const file of files) {
        // Skip original backup files
        if (file.includes('.original')) continue;

        if (file.endsWith('.gif')) {
          const filePath = path.join(assetsPath, file);
          const stats = fs.statSync(filePath);
          totalGifSize += stats.size;
          gifFiles.push({ name: file, size: stats.size });
        }
      }

      console.log('GIF files:');
      gifFiles.forEach(f => {
        console.log(`  ${f.name}: ${(f.size / 1024).toFixed(2)} KB`);
      });

      const totalSizeKB = totalGifSize / 1024;
      console.log(`Total GIF payload: ${totalSizeKB.toFixed(2)} KB`);

      // GIF total should be under 1MB for fallback support
      expect(totalSizeKB).toBeLessThan(1024);
    });

    test('WebP images should provide better compression than GIFs', async () => {
      const assetsPath = path.join(process.cwd(), 'assets');

      const logoGifPath = path.join(assetsPath, 'logo.gif');
      const logoWebpPath = path.join(assetsPath, 'logo.webp');

      const gifStats = fs.statSync(logoGifPath);
      const webpStats = fs.statSync(logoWebpPath);

      // Calculate compression ratio
      const compressionRatio = (1 - (webpStats.size / gifStats.size)) * 100;
      console.log(`Logo compression ratio (WebP vs GIF): ${compressionRatio.toFixed(1)}% smaller`);

      // WebP should provide at least 20% reduction
      expect(compressionRatio).toBeGreaterThan(20);
    });
  });
});
