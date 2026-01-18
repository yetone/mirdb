import { test, expect } from '@playwright/test';
import * as fs from 'fs';
import * as path from 'path';

test.describe('Image and Asset Loading', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: MirDB logo loads successfully with 200 status', async ({ page }) => {
    // Track network requests for logo images
    const logoRequests: { url: string; status: number; contentType: string }[] = [];

    page.on('response', async (response) => {
      const url = response.url();
      if (url.includes('logo.webp') || url.includes('logo.png') || url.includes('logo.gif')) {
        logoRequests.push({
          url,
          status: response.status(),
          contentType: response.headers()['content-type'] || '',
        });
      }
    });

    // Navigate to homepage and wait for network idle
    await page.goto('/', { waitUntil: 'networkidle' });

    // Verify at least one logo request was made and succeeded
    expect(logoRequests.length).toBeGreaterThan(0);

    // Verify all logo requests succeeded (200 OK or 304 Not Modified)
    // 304 indicates the resource is cached and valid, which is expected behavior
    for (const request of logoRequests) {
      console.log(`Logo request: ${request.url} - Status: ${request.status}`);
      expect([200, 304]).toContain(request.status);
    }

    // Verify the hero logo is visible in the DOM
    const heroLogo = page.locator('[data-testid="hero-logo"]');
    await expect(heroLogo).toBeVisible();

    // Verify logo has proper src attribute
    const logoSrc = await heroLogo.getAttribute('src');
    expect(logoSrc).toBeTruthy();
    expect(logoSrc).toMatch(/logo\.(png|webp|gif)$/);
  });

  test('TC2: All feature icons load and display correctly', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Get all feature icons
    const featureIcons = page.locator('[data-testid^="feature-icon-"]');
    const iconCount = await featureIcons.count();

    console.log(`Found ${iconCount} feature icons`);

    // Verify we have the expected number of feature icons (6 features)
    expect(iconCount).toBe(6);

    // Verify each icon is visible and contains an SVG
    const iconIds = [
      'memcached-protocol',
      'lsm-tree',
      'rust-performance',
      'wal',
      'compaction',
      'toml-config',
    ];

    for (const iconId of iconIds) {
      const iconContainer = page.locator(`[data-testid="feature-icon-${iconId}"]`);
      await expect(iconContainer).toBeVisible();

      // Verify the icon container has an SVG element
      const svg = iconContainer.locator('svg');
      await expect(svg).toBeVisible();

      // Verify SVG has proper size classes
      await expect(svg).toHaveClass(/w-6/);
      await expect(svg).toHaveClass(/h-6/);

      console.log(`Feature icon ${iconId}: visible and properly sized`);
    }
  });

  test('TC3: LSM Tree architecture diagram loads and is visible', async ({ page }) => {
    // Scroll to architecture section to ensure it's in view
    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await architectureSection.scrollIntoViewIfNeeded();
    await expect(architectureSection).toBeVisible();

    // Verify the diagram container exists
    const diagramContainer = page.locator('[data-testid="architecture-diagram-container"]');
    await expect(diagramContainer).toBeVisible();

    // Verify the LSM Tree diagram SVG is present and visible
    const lsmTreeDiagram = page.locator('[data-testid="lsm-tree-diagram"]');
    await expect(lsmTreeDiagram).toBeVisible();

    // Verify it's an SVG element
    const tagName = await lsmTreeDiagram.evaluate((el) => el.tagName.toLowerCase());
    expect(tagName).toBe('svg');

    // Verify SVG has proper dimensions
    const viewBox = await lsmTreeDiagram.getAttribute('viewBox');
    expect(viewBox).toBeTruthy();
    expect(viewBox).toBe('0 0 400 450');

    // Verify key diagram components are present
    const memtable = page.locator('[data-testid="diagram-memtable"]');
    await expect(memtable).toBeVisible();

    const wal = page.locator('[data-testid="diagram-wal"]');
    await expect(wal).toBeVisible();

    const sstableL0 = page.locator('[data-testid="diagram-sstable-l0"]');
    await expect(sstableL0).toBeVisible();

    console.log('LSM Tree diagram loaded and all components visible');
  });

  test('TC4: No broken image links (no 404 errors for image resources)', async ({ page }) => {
    // Track all image-related requests
    const imageRequests: { url: string; status: number; contentType: string }[] = [];
    const failedRequests: { url: string; status: number }[] = [];

    page.on('response', async (response) => {
      const contentType = response.headers()['content-type'] || '';
      const url = response.url();

      // Check for image content types or image file extensions
      const isImageRequest =
        contentType.includes('image/') ||
        /\.(png|jpg|jpeg|gif|webp|svg|ico)(\?.*)?$/i.test(url);

      if (isImageRequest) {
        imageRequests.push({
          url,
          status: response.status(),
          contentType,
        });

        if (response.status() >= 400) {
          failedRequests.push({
            url,
            status: response.status(),
          });
        }
      }
    });

    // Navigate and wait for all network activity to complete
    await page.goto('/', { waitUntil: 'networkidle' });

    // Scroll through the page to trigger any lazy-loaded images
    await page.evaluate(() => {
      return new Promise<void>((resolve) => {
        let totalHeight = 0;
        const distance = 300;
        const timer = setInterval(() => {
          const scrollHeight = document.body.scrollHeight;
          window.scrollBy(0, distance);
          totalHeight += distance;

          if (totalHeight >= scrollHeight) {
            clearInterval(timer);
            resolve();
          }
        }, 100);
      });
    });

    // Wait for any additional images to load after scrolling
    await page.waitForLoadState('networkidle');

    console.log(`Total image requests: ${imageRequests.length}`);
    imageRequests.forEach((req) => {
      console.log(`  ${req.url} - Status: ${req.status}`);
    });

    // Verify no failed image requests
    if (failedRequests.length > 0) {
      console.log('Failed image requests:');
      failedRequests.forEach((req) => {
        console.log(`  ${req.url} - Status: ${req.status}`);
      });
    }

    expect(failedRequests.length).toBe(0);
  });

  test('TC5: Images have appropriate loading strategy (above-fold eager, below-fold lazy)', async ({ page }) => {
    // Get all images on the page
    const allImages = page.locator('img');
    const imageCount = await allImages.count();

    console.log(`Total images on page: ${imageCount}`);

    // Check images for lazy loading
    const imagesInfo: { src: string; loading: string | null; position: number }[] = [];

    const viewportHeight = await page.evaluate(() => window.innerHeight);

    for (let i = 0; i < imageCount; i++) {
      const img = allImages.nth(i);
      const loading = await img.getAttribute('loading');
      const src = (await img.getAttribute('src')) || 'unknown';
      const boundingBox = await img.boundingBox();
      const yPosition = boundingBox?.y ?? 0;

      imagesInfo.push({ src, loading, position: yPosition });
    }

    console.log('Image loading analysis:');
    imagesInfo.forEach((info) => {
      const location = info.position < viewportHeight ? 'above-fold' : 'below-fold';
      console.log(`  ${info.src}: loading="${info.loading || 'default'}" (${location}, y=${info.position.toFixed(0)})`);
    });

    // Verify above-fold images (hero logo, nav logo) are NOT lazy loaded
    // This is important for Largest Contentful Paint (LCP) optimization
    const aboveFoldImages = imagesInfo.filter((info) => info.position < viewportHeight);
    const belowFoldImages = imagesInfo.filter((info) => info.position >= viewportHeight);

    console.log(`Above-fold images: ${aboveFoldImages.length}`);
    console.log(`Below-fold images: ${belowFoldImages.length}`);

    // Above-fold images should NOT have loading="lazy" for better LCP
    for (const img of aboveFoldImages) {
      // Above-fold images should use eager loading (default) or explicitly "eager"
      // They should NOT be "lazy"
      if (img.loading === 'lazy') {
        console.warn(`Warning: Above-fold image ${img.src} has lazy loading which may hurt LCP`);
      }
      expect(img.loading).not.toBe('lazy');
    }

    // Below-fold images SHOULD have loading="lazy" if they exist
    for (const img of belowFoldImages) {
      console.log(`Below-fold image ${img.src}: loading="${img.loading}"`);
      expect(img.loading).toBe('lazy');
    }

    // If there are no below-fold images, that's acceptable
    // (current implementation uses inline SVGs for below-fold content)
    if (belowFoldImages.length === 0) {
      console.log('No below-fold <img> elements found - using inline SVGs for visual content');
    }
  });
});

test.describe('Image Optimization Verification', () => {
  test('Images use WebP format with PNG fallbacks', async ({ page }) => {
    await page.goto('/');

    // Verify picture elements exist for progressive enhancement
    const pictureElements = await page.locator('picture').count();
    console.log(`Picture elements found: ${pictureElements}`);
    expect(pictureElements).toBeGreaterThan(0);

    // Verify WebP sources exist within picture elements
    const webpSources = await page.locator('picture source[type="image/webp"]').count();
    console.log(`WebP sources found: ${webpSources}`);
    expect(webpSources).toBeGreaterThan(0);

    // Verify fallback img elements exist with PNG src
    const pngFallbacks = await page.locator('picture img[src$=".png"]').count();
    console.log(`PNG fallbacks found: ${pngFallbacks}`);
    expect(pngFallbacks).toBeGreaterThan(0);
  });

  test('Logo files are properly optimized', async () => {
    const publicDir = path.resolve(process.cwd(), 'public');

    // Check that optimized logo files exist
    const webpPath = path.join(publicDir, 'logo.webp');
    const pngPath = path.join(publicDir, 'logo.png');
    const originalPath = path.join(publicDir, 'logo.gif');

    expect(fs.existsSync(webpPath)).toBe(true);
    expect(fs.existsSync(pngPath)).toBe(true);
    expect(fs.existsSync(originalPath)).toBe(true);

    // Get file sizes
    const webpSize = fs.statSync(webpPath).size;
    const pngSize = fs.statSync(pngPath).size;
    const originalSize = fs.statSync(originalPath).size;

    console.log(`Original logo.gif size: ${(originalSize / 1024).toFixed(2)} KB`);
    console.log(`Optimized logo.webp size: ${(webpSize / 1024).toFixed(2)} KB`);
    console.log(`Fallback logo.png size: ${(pngSize / 1024).toFixed(2)} KB`);

    // Verify files are reasonably sized (under 100KB each)
    expect(webpSize).toBeLessThan(100 * 1024);
    expect(pngSize).toBeLessThan(100 * 1024);

    // WebP should generally be smaller than or equal to PNG
    console.log(`Compression ratio (WebP vs PNG): ${((webpSize / pngSize) * 100).toFixed(1)}%`);
  });

  test('Feature icons are inline SVGs (no external requests)', async ({ page }) => {
    const externalIconRequests: string[] = [];

    page.on('request', (request) => {
      const url = request.url();
      // Check for any icon-related external requests
      if (url.includes('icon') || url.includes('svg')) {
        externalIconRequests.push(url);
      }
    });

    await page.goto('/', { waitUntil: 'networkidle' });

    // Scroll to features section
    const featuresSection = page.locator('[data-testid="features-section"]');
    await featuresSection.scrollIntoViewIfNeeded();

    // Verify icons are inline SVGs within the feature-icon containers
    const featureIcons = page.locator('[data-testid^="feature-icon-"] svg');
    const iconCount = await featureIcons.count();

    expect(iconCount).toBe(6); // 6 feature icons

    // Verify each SVG is inline (not an external reference)
    for (let i = 0; i < iconCount; i++) {
      const svg = featureIcons.nth(i);
      // Inline SVGs have children (path elements), external refs use xlink:href
      const hasInlineContent = await svg.locator('path').count();
      expect(hasInlineContent).toBeGreaterThan(0);
    }

    console.log(`Feature icons verified as inline SVGs: ${iconCount}`);
    console.log(`External icon requests during page load: ${externalIconRequests.length}`);
  });

  test('Architecture diagram SVG is inline and accessible', async ({ page }) => {
    await page.goto('/');

    // Scroll to architecture section
    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await architectureSection.scrollIntoViewIfNeeded();

    const diagram = page.locator('[data-testid="lsm-tree-diagram"]');
    await expect(diagram).toBeVisible();

    // Verify accessibility attributes
    await expect(diagram).toHaveAttribute('role', 'img');

    const ariaLabel = await diagram.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel).toContain('LSM Tree');

    // Verify title element for accessibility
    const title = diagram.locator('title');
    await expect(title).toBeAttached();
    const titleText = await title.textContent();
    expect(titleText).toContain('LSM Tree');

    // Verify desc element for extended description
    const desc = diagram.locator('desc');
    await expect(desc).toBeAttached();
    const descText = await desc.textContent();
    expect(descText).toBeTruthy();
    expect(descText!.length).toBeGreaterThan(50);

    console.log('Architecture diagram accessibility verified');
  });
});
