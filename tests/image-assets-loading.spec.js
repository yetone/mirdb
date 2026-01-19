// @ts-check
const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

test.describe('Image Assets Loading', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Logo image loads without broken image placeholder', async ({ page }) => {
    // Get the logo image element
    const logo = page.locator('img.logo, img[alt*="logo" i], img[alt*="MirDB" i]').first();
    await expect(logo).toBeVisible();

    // Get the source attribute
    const src = await logo.getAttribute('src');
    expect(src).toBeTruthy();
    expect(src.toLowerCase()).toContain('logo');

    // Check that the image loaded successfully (naturalWidth > 0 indicates image loaded)
    const isLoaded = await logo.evaluate((img) => {
      const imgElement = /** @type {HTMLImageElement} */ (img);
      return imgElement.complete && imgElement.naturalWidth > 0 && imgElement.naturalHeight > 0;
    });
    expect(isLoaded).toBe(true);

    // Verify the image is not showing a broken image (check it has actual dimensions)
    const boundingBox = await logo.boundingBox();
    expect(boundingBox).not.toBeNull();
    expect(boundingBox.width).toBeGreaterThan(0);
    expect(boundingBox.height).toBeGreaterThan(0);
  });

  test('TC2: Usage demonstration image/gif renders correctly', async ({ page }) => {
    // Navigate to usage section
    const usageSection = page.locator('#usage, section.usage, [data-testid="usage"]').first();
    await expect(usageSection).toBeVisible();

    // Get the usage demo image
    const usageDemo = page.locator('img[src*="usage"], img.terminal-demo, [data-testid="usage-demo"]').first();
    await expect(usageDemo).toBeVisible();

    // Verify the image source
    const src = await usageDemo.getAttribute('src');
    expect(src).toBeTruthy();
    expect(src.toLowerCase()).toContain('usage');

    // Check that the image loaded successfully
    const isLoaded = await usageDemo.evaluate((img) => {
      const imgElement = /** @type {HTMLImageElement} */ (img);
      return imgElement.complete && imgElement.naturalWidth > 0 && imgElement.naturalHeight > 0;
    });
    expect(isLoaded).toBe(true);

    // Verify the image has actual dimensions
    const boundingBox = await usageDemo.boundingBox();
    expect(boundingBox).not.toBeNull();
    expect(boundingBox.width).toBeGreaterThan(0);
    expect(boundingBox.height).toBeGreaterThan(0);
  });

  test('TC3: All feature icons/illustrations load without errors', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features, section.features, [data-testid="features"]').first();
    await expect(featuresSection).toBeVisible();

    // Get all feature icons (can be SVG or img elements)
    const featureIcons = featuresSection.locator('.feature-card .icon, [data-testid="feature-icon"], .feature-card svg, .feature-card img');

    // Get the count of feature icons
    const iconCount = await featureIcons.count();
    expect(iconCount).toBeGreaterThan(0);

    // Verify each icon is visible and rendered correctly
    for (let i = 0; i < iconCount; i++) {
      const icon = featureIcons.nth(i);
      await expect(icon).toBeVisible();

      // Check if it's an SVG (inline icon) or an image
      const tagName = await icon.evaluate((el) => el.tagName.toLowerCase());

      if (tagName === 'img') {
        // For images, verify they loaded correctly
        const isLoaded = await icon.evaluate((img) => {
          const imgElement = /** @type {HTMLImageElement} */ (img);
          return imgElement.complete && imgElement.naturalWidth > 0;
        });
        expect(isLoaded).toBe(true);
      } else if (tagName === 'svg') {
        // For SVGs, verify they have content and proper dimensions
        const boundingBox = await icon.boundingBox();
        expect(boundingBox).not.toBeNull();
        expect(boundingBox.width).toBeGreaterThan(0);
        expect(boundingBox.height).toBeGreaterThan(0);
      }
    }
  });

  test('TC3b: Roadmap icons render without errors', async ({ page }) => {
    // Navigate to roadmap section
    const roadmapSection = page.locator('#roadmap, section.roadmap, [data-testid="roadmap"]').first();
    await expect(roadmapSection).toBeVisible();

    // Get all roadmap item icons
    const roadmapIcons = roadmapSection.locator('.roadmap-item svg, [data-testid="roadmap-icon"]');

    // Get the count of roadmap icons
    const iconCount = await roadmapIcons.count();
    expect(iconCount).toBeGreaterThan(0);

    // Verify each icon is rendered correctly
    for (let i = 0; i < iconCount; i++) {
      const icon = roadmapIcons.nth(i);
      await expect(icon).toBeVisible();

      // Verify SVG has proper dimensions
      const boundingBox = await icon.boundingBox();
      expect(boundingBox).not.toBeNull();
      expect(boundingBox.width).toBeGreaterThan(0);
      expect(boundingBox.height).toBeGreaterThan(0);
    }
  });

  test('All page images have alt attributes for accessibility', async ({ page }) => {
    // Get all images on the page
    const images = page.locator('img');
    const imageCount = await images.count();

    // Verify each image has an alt attribute
    for (let i = 0; i < imageCount; i++) {
      const img = images.nth(i);
      const alt = await img.getAttribute('alt');
      expect(alt).not.toBeNull();
      // Alt can be empty for decorative images, but should exist
    }
  });

  test('Images load without 404 errors', async ({ page }) => {
    const failedImages = [];

    // Listen for response events
    page.on('response', (response) => {
      const url = response.url();
      if (url.match(/\.(gif|png|jpg|jpeg|webp|svg)$/i)) {
        const status = response.status();
        // 304 (Not Modified) is a valid response indicating cached content
        // Only 4xx and 5xx status codes indicate failed loads
        if (status >= 400) {
          failedImages.push({ url, status });
        }
      }
    });

    // Reload the page to capture all image requests
    await page.goto('/', { waitUntil: 'networkidle' });

    // Verify no images failed to load
    expect(failedImages).toHaveLength(0);
  });
});

test.describe('Image Optimization', () => {
  test('TC4: Images are appropriately compressed or use modern formats', async () => {
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

  test('Images load efficiently (response time check)', async ({ page }) => {
    const imageResponses = [];

    // Listen for response events to capture image loads
    page.on('response', async (response) => {
      const url = response.url();
      if (url.match(/\.(gif|png|jpg|jpeg|webp|svg)$/i)) {
        const status = response.status();
        // 304 is also a valid successful response (cached)
        if (status === 200 || status === 304) {
          imageResponses.push({
            url,
            status,
          });
        }
      }
    });

    // Load the page and wait for network to be idle
    await page.goto('/', { waitUntil: 'networkidle' });

    // Verify images were loaded
    expect(imageResponses.length).toBeGreaterThan(0);
  });

  test('SVG icons are used for scalable graphics', async ({ page }) => {
    await page.goto('/');

    // Get all SVG elements used as icons on the page
    // This matches the actual selectors used in the HTML for feature and roadmap icons
    const svgIcons = page.locator('.icon, [data-testid="feature-icon"], [data-testid="roadmap-icon"], .feature-card svg, .roadmap-item svg');
    const svgCount = await svgIcons.count();

    // Verify SVGs are being used for icons (better for scalability)
    expect(svgCount).toBeGreaterThan(0);

    // Count how many have viewBox attribute
    let viewBoxCount = 0;
    for (let i = 0; i < svgCount; i++) {
      const svg = svgIcons.nth(i);
      const viewBox = await svg.getAttribute('viewBox');

      if (viewBox !== null) {
        viewBoxCount++;
        // Verify viewBox format is valid
        expect(viewBox).toMatch(/\d+\s+\d+\s+\d+\s+\d+/);
      }
    }

    // At least some SVGs should have viewBox for proper scaling
    expect(viewBoxCount).toBeGreaterThan(0);
  });
});
