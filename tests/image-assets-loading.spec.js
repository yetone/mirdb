// @ts-check
const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

test.describe('Image Assets Loading', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Logo image loads without broken image placeholder', async ({ page }) => {
    // Find the logo image in the hero section
    const logoImg = page.locator('.hero img.logo, img[alt*="logo" i], img[src*="logo"]').first();
    await expect(logoImg).toBeVisible();

    // Verify the image source is set
    const src = await logoImg.getAttribute('src');
    expect(src).toBeTruthy();
    expect(src.toLowerCase()).toContain('logo');

    // Check that the image has loaded successfully (naturalWidth > 0 means image loaded)
    const naturalWidth = await logoImg.evaluate((img) => {
      const imgElement = /** @type {HTMLImageElement} */ (img);
      return imgElement.naturalWidth;
    });
    expect(naturalWidth).toBeGreaterThan(0);

    // Check that the image has natural height > 0 (confirms it's not broken)
    const naturalHeight = await logoImg.evaluate((img) => {
      const imgElement = /** @type {HTMLImageElement} */ (img);
      return imgElement.naturalHeight;
    });
    expect(naturalHeight).toBeGreaterThan(0);

    // Verify the image is not showing a broken image icon (complete property should be true)
    const isComplete = await logoImg.evaluate((img) => {
      const imgElement = /** @type {HTMLImageElement} */ (img);
      return imgElement.complete;
    });
    expect(isComplete).toBe(true);
  });

  test('TC2: Usage demonstration image/GIF renders correctly', async ({ page }) => {
    // Navigate to usage section
    const usageSection = page.locator('#usage, section.usage, [data-testid="usage"]').first();
    await expect(usageSection).toBeVisible();

    // Find the usage demo image
    const usageImg = usageSection.locator('img[src*="usage"], img.terminal-demo, [data-testid="usage-demo"]').first();
    await expect(usageImg).toBeVisible();

    // Verify the image source is set
    const src = await usageImg.getAttribute('src');
    expect(src).toBeTruthy();
    expect(src.toLowerCase()).toContain('usage');

    // Check that the image has loaded successfully
    const naturalWidth = await usageImg.evaluate((img) => {
      const imgElement = /** @type {HTMLImageElement} */ (img);
      return imgElement.naturalWidth;
    });
    expect(naturalWidth).toBeGreaterThan(0);

    // Check that the image has natural height > 0
    const naturalHeight = await usageImg.evaluate((img) => {
      const imgElement = /** @type {HTMLImageElement} */ (img);
      return imgElement.naturalHeight;
    });
    expect(naturalHeight).toBeGreaterThan(0);

    // Verify the image is complete
    const isComplete = await usageImg.evaluate((img) => {
      const imgElement = /** @type {HTMLImageElement} */ (img);
      return imgElement.complete;
    });
    expect(isComplete).toBe(true);
  });

  test('TC3: All feature section icons/illustrations render without errors', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features, section.features, [data-testid="features"]').first();
    await expect(featuresSection).toBeVisible();

    // Find all feature cards
    const featureCards = featuresSection.locator('.feature-card, [data-testid="feature-card"]');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(4);

    // Check each feature card has a visible icon (SVG or img)
    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      const icon = card.locator('svg.icon, img.icon, [data-testid="feature-icon"], .icon').first();
      await expect(icon).toBeVisible();

      // Get the tag name to determine how to verify it
      const tagName = await icon.evaluate((el) => el.tagName.toLowerCase());

      if (tagName === 'img') {
        // For image icons, verify they loaded correctly
        const naturalWidth = await icon.evaluate((img) => {
          const imgElement = /** @type {HTMLImageElement} */ (img);
          return imgElement.naturalWidth;
        });
        expect(naturalWidth).toBeGreaterThan(0);

        const isComplete = await icon.evaluate((img) => {
          const imgElement = /** @type {HTMLImageElement} */ (img);
          return imgElement.complete;
        });
        expect(isComplete).toBe(true);
      } else if (tagName === 'svg') {
        // For SVG icons, verify they have content (children)
        const hasContent = await icon.evaluate((svg) => {
          return svg.childNodes.length > 0;
        });
        expect(hasContent).toBe(true);

        // Verify SVG has a valid bounding box (is rendered)
        const boundingBox = await icon.boundingBox();
        expect(boundingBox).not.toBeNull();
        expect(boundingBox?.width).toBeGreaterThan(0);
        expect(boundingBox?.height).toBeGreaterThan(0);
      }
    }
  });
});

test.describe('Image Optimization', () => {
  test('TC4: Images use modern formats (WebP) or are appropriately compressed', async () => {
    // This is an integration test that checks the actual image files

    const assetsDir = path.join(process.cwd(), 'assets');

    // Check if assets directory exists
    expect(fs.existsSync(assetsDir)).toBe(true);

    // Get all image files in the assets directory
    const files = fs.readdirSync(assetsDir);
    const imageFiles = files.filter(file =>
      /\.(gif|png|jpg|jpeg|webp|svg)$/i.test(file)
    );

    expect(imageFiles.length).toBeGreaterThan(0);

    let hasOptimizedOrAcceptableImages = true;
    const imageAnalysis = [];

    for (const file of imageFiles) {
      const filePath = path.join(assetsDir, file);
      const stats = fs.statSync(filePath);
      const fileSizeKB = stats.size / 1024;
      const extension = path.extname(file).toLowerCase();

      const analysis = {
        file,
        extension,
        sizeKB: Math.round(fileSizeKB * 100) / 100,
        isOptimized: false,
        reason: ''
      };

      // Check for modern formats (WebP) or acceptable compression
      if (extension === '.webp') {
        analysis.isOptimized = true;
        analysis.reason = 'Uses modern WebP format';
      } else if (extension === '.svg') {
        analysis.isOptimized = true;
        analysis.reason = 'SVG is vector-based and scalable';
      } else if (extension === '.gif') {
        // GIFs are acceptable for animations, check if size is reasonable
        // For animated GIFs, up to 2MB is acceptable for usage demos
        if (fileSizeKB <= 2048) {
          analysis.isOptimized = true;
          analysis.reason = 'GIF format acceptable for animations, size is reasonable';
        } else {
          analysis.isOptimized = false;
          analysis.reason = `GIF is ${Math.round(fileSizeKB)}KB - consider converting to video or WebP`;
        }
      } else if (extension === '.png' || extension === '.jpg' || extension === '.jpeg') {
        // For static images, check if they are reasonably compressed
        // Images under 500KB are considered acceptable
        if (fileSizeKB <= 500) {
          analysis.isOptimized = true;
          analysis.reason = 'Image size is appropriately compressed';
        } else {
          analysis.isOptimized = false;
          analysis.reason = `Image is ${Math.round(fileSizeKB)}KB - consider compressing or using WebP`;
        }
      }

      imageAnalysis.push(analysis);
    }

    // Log the analysis for debugging
    console.log('Image Analysis:');
    imageAnalysis.forEach(a => {
      console.log(`  ${a.file}: ${a.sizeKB}KB - ${a.isOptimized ? 'OK' : 'NEEDS OPTIMIZATION'} - ${a.reason}`);
    });

    // Check that all images meet the optimization criteria
    const allOptimized = imageAnalysis.every(a => a.isOptimized);

    // For this test, we consider images "appropriately compressed" if:
    // 1. They use modern formats (WebP, SVG), OR
    // 2. GIFs under 2MB (acceptable for animations), OR
    // 3. Static images under 500KB
    expect(allOptimized).toBe(true);
  });
});
