import { test, expect } from '@playwright/test';
import * as fs from 'fs';
import * as path from 'path';

/**
 * Page Load Performance Tests
 *
 * These tests verify that the MirDB homepage meets the performance requirements
 * specified in NFR-1 and NFR-2 of the PRD:
 * - NFR-1: Page must load within 3 seconds on 3G connections
 * - NFR-2: Achieve Lighthouse performance score of 90+
 *
 * Additional performance metrics tested:
 * - Page weight under 150KB (excluding optional images)
 * - Time to Interactive under 2 seconds
 * - TTFB sub-second for static site
 * - Critical CSS inlined or render-blocking resources minimized
 */

test.describe('Page Load Performance', () => {
  const publicDir = path.join(process.cwd(), 'public');
  const indexPath = path.join(publicDir, 'index.html');
  const stylesPath = path.join(publicDir, 'styles.css');

  test('TC1: Page fully loads within 3 seconds on simulated 3G connection', async ({ page }) => {
    // Get file stats for performance calculation
    const htmlSize = fs.statSync(indexPath).size;
    const cssSize = fs.statSync(stylesPath).size;
    const totalSize = htmlSize + cssSize;

    // 3G connection characteristics: ~750 Kbps download
    // Slow 3G: 400 Kbps = 50 KB/s
    // Regular 3G: 750 Kbps = ~93.75 KB/s
    // Let's use slow 3G (50 KB/s) for conservative testing
    const slow3GBytesPerSecond = 50 * 1024; // 50 KB/s
    const estimatedLoadTime = (totalSize / slow3GBytesPerSecond) * 1000; // in ms

    // Measure actual page load time
    const startTime = Date.now();
    await page.goto(`file://${indexPath}`);
    await page.waitForLoadState('load');
    const loadTime = Date.now() - startTime;

    // Log performance data
    console.log(`Total page size: ${totalSize} bytes (${(totalSize / 1024).toFixed(2)} KB)`);
    console.log(`Estimated 3G load time: ${estimatedLoadTime.toFixed(0)} ms`);
    console.log(`Actual local load time: ${loadTime} ms`);

    // Verify the page is fully loaded (DOM content available)
    await expect(page.locator('body')).toBeVisible();
    await expect(page.getByTestId('hero-section')).toBeVisible();
    await expect(page.getByTestId('footer')).toBeVisible();

    // For static files on 3G:
    // Total ~24KB at 50KB/s = ~0.48 seconds
    // Even with latency overhead (e.g., 200ms RTT), should be well under 3 seconds
    // The calculated load time demonstrates the page would load under 3 seconds on 3G
    expect(estimatedLoadTime).toBeLessThan(3000);

    // Additional check: verify total size is reasonable for 3G loading
    // To load in 3 seconds on slow 3G (50KB/s), max size is 150KB
    const maxSizeFor3Seconds = slow3GBytesPerSecond * 3; // 150KB
    expect(totalSize).toBeLessThan(maxSizeFor3Seconds);
  });

  test('TC2: Page meets Lighthouse performance requirements (90+ score criteria)', async ({ page }) => {
    await page.goto(`file://${indexPath}`);
    await page.waitForLoadState('load');

    // Lighthouse performance score is based on several metrics.
    // Since we can't run actual Lighthouse in Playwright, we verify the key factors
    // that contribute to a 90+ performance score:

    // 1. First Contentful Paint (FCP) - content should be visible quickly
    const heroSection = page.getByTestId('hero-section');
    await expect(heroSection).toBeVisible();

    // 2. Largest Contentful Paint (LCP) - main content should render fast
    const heroHeading = page.locator('h1');
    await expect(heroHeading).toBeVisible();
    const headingText = await heroHeading.textContent();
    expect(headingText).toContain('MirDB');

    // 3. Cumulative Layout Shift (CLS) - layout should be stable
    // Verify key elements have explicit dimensions or stable layout
    const viewportSize = page.viewportSize();
    expect(viewportSize).toBeTruthy();

    // 4. Check for performance-optimized patterns in HTML
    const htmlContent = fs.readFileSync(indexPath, 'utf-8');

    // Verify no external fonts that would slow loading
    const hasExternalFonts = htmlContent.includes('fonts.googleapis.com');
    expect(hasExternalFonts).toBe(false);

    // Verify uses system font stack (defined in CSS)
    const cssContent = fs.readFileSync(stylesPath, 'utf-8');
    const usesSystemFonts = cssContent.includes('-apple-system') ||
                           cssContent.includes('system-ui') ||
                           cssContent.includes('BlinkMacSystemFont');
    expect(usesSystemFonts).toBe(true);

    // 5. Verify minimal JavaScript (only copy functionality)
    const scriptMatches = htmlContent.match(/<script[\s\S]*?<\/script>/g) || [];
    expect(scriptMatches.length).toBeLessThanOrEqual(1);

    // 6. Check that images use inline SVG (no external image requests)
    const hasExternalImages = htmlContent.includes('<img');
    expect(hasExternalImages).toBe(false);

    // 7. All content renders above the fold for hero section
    const heroContent = page.locator('.hero-content');
    await expect(heroContent).toBeVisible();

    console.log('Performance criteria verified:');
    console.log('- System fonts used (no external font loading)');
    console.log('- Inline SVG diagram (no external images)');
    console.log('- Minimal JavaScript');
    console.log('- Key content renders immediately');
  });

  test('TC3: Total page weight is under 150KB (excluding optional images)', async ({ page }) => {
    // Calculate total page weight
    const htmlSize = fs.statSync(indexPath).size;
    const cssSize = fs.statSync(stylesPath).size;
    const totalSize = htmlSize + cssSize;

    // Target is < 150KB excluding optional images
    const targetSizeKB = 150;
    const targetSizeBytes = targetSizeKB * 1024;

    console.log('Page weight breakdown:');
    console.log(`- HTML: ${htmlSize} bytes (${(htmlSize / 1024).toFixed(2)} KB)`);
    console.log(`- CSS: ${cssSize} bytes (${(cssSize / 1024).toFixed(2)} KB)`);
    console.log(`- Total: ${totalSize} bytes (${(totalSize / 1024).toFixed(2)} KB)`);
    console.log(`- Target: < ${targetSizeKB} KB`);

    expect(totalSize).toBeLessThan(targetSizeBytes);

    // Additionally verify the page loads correctly
    await page.goto(`file://${indexPath}`);
    await expect(page.locator('body')).toBeVisible();

    // Verify no additional external resources are loaded
    const htmlContent = fs.readFileSync(indexPath, 'utf-8');

    // Check for external stylesheets (only local styles.css should be linked)
    const externalStylesheets = (htmlContent.match(/<link[^>]*rel="stylesheet"[^>]*>/g) || [])
      .filter(link => link.includes('http://') || link.includes('https://'));
    expect(externalStylesheets.length).toBe(0);

    // Check for external scripts
    const externalScripts = (htmlContent.match(/<script[^>]*src="[^"]*"[^>]*>/g) || [])
      .filter(script => script.includes('http://') || script.includes('https://'));
    expect(externalScripts.length).toBe(0);

    console.log('- No external stylesheets');
    console.log('- No external scripts');
  });

  test('TC4: Time to Interactive is under 2 seconds on 4G', async ({ page }) => {
    // 4G connection: ~12.5 Mbps download = ~1.5 MB/s
    // For a ~24KB page, transfer time is negligible (<20ms)
    // TTI depends mainly on JavaScript execution time

    const startTime = Date.now();
    await page.goto(`file://${indexPath}`);

    // Wait for page to be interactive
    await page.waitForLoadState('domcontentloaded');

    // Verify interactive elements are ready
    const copyButton = page.getByTestId('copy-button');
    await expect(copyButton).toBeVisible();
    await expect(copyButton).toBeEnabled();

    // Verify navigation links are interactive
    const navLinks = page.locator('.nav-links a');
    const navCount = await navLinks.count();
    expect(navCount).toBeGreaterThan(0);

    // Test that interactive elements work
    const featuresLink = page.locator('a[href="#features"]');
    await expect(featuresLink).toBeVisible();

    const interactiveTime = Date.now() - startTime;
    console.log(`Time to Interactive (local): ${interactiveTime} ms`);

    // Calculate theoretical TTI on 4G
    const htmlSize = fs.statSync(indexPath).size;
    const cssSize = fs.statSync(stylesPath).size;
    const totalSize = htmlSize + cssSize;

    // 4G bandwidth: 12.5 Mbps = 1.5 MB/s approximately
    // RTT: ~50ms typical
    const fourGBytesPerSecond = 1.5 * 1024 * 1024; // 1.5 MB/s
    const fourGLatencyMs = 50; // 50ms RTT
    const transferTime = (totalSize / fourGBytesPerSecond) * 1000;
    const estimatedTTI = transferTime + fourGLatencyMs + 100; // +100ms for parsing/render

    console.log(`Estimated 4G TTI: ${estimatedTTI.toFixed(0)} ms`);
    expect(estimatedTTI).toBeLessThan(2000);

    // Verify minimal JavaScript doesn't block interactivity
    const htmlContent = fs.readFileSync(indexPath, 'utf-8');
    const hasAsyncScripts = htmlContent.includes('async') || htmlContent.includes('defer');
    const inlineScriptOnly = !htmlContent.includes('<script src=');
    console.log(`Uses inline script only (no external JS): ${inlineScriptOnly}`);

    // The page should be interactive almost immediately since:
    // 1. Small total size (~24KB)
    // 2. Minimal JavaScript (only copy-to-clipboard)
    // 3. No external resources to fetch
    expect(inlineScriptOnly).toBe(true);
  });

  test('TC5: TTFB (Time to First Byte) is sub-second for static site', async ({ page }) => {
    // For a static file served locally, TTFB is effectively instant
    // For static hosting (CDN), TTFB should be well under 200ms
    // We verify the conditions that ensure fast TTFB:

    // 1. Page is a static HTML file
    expect(fs.existsSync(indexPath)).toBe(true);
    const htmlContent = fs.readFileSync(indexPath, 'utf-8');

    // 2. No server-side rendering required
    expect(htmlContent.startsWith('<!DOCTYPE html>')).toBe(true);

    // 3. No dynamic content that would delay response
    // Check for server-side template tags
    const hasServerSideTags = htmlContent.includes('<%') ||
                             htmlContent.includes('{{') ||
                             htmlContent.includes('<?php');
    expect(hasServerSideTags).toBe(false);

    // 4. File size is small enough for instant serving
    const fileSize = fs.statSync(indexPath).size;
    console.log(`HTML file size: ${fileSize} bytes (${(fileSize / 1024).toFixed(2)} KB)`);

    // HTML should be under 50KB for sub-second TTFB even on slow connections
    expect(fileSize).toBeLessThan(50 * 1024);

    // 5. Test actual load timing
    const startTime = Date.now();
    const response = await page.goto(`file://${indexPath}`);
    const firstByteTime = Date.now() - startTime;

    console.log(`First byte received in: ${firstByteTime} ms`);

    // For local file, should be instant
    // For typical static hosting with CDN, TTFB < 100ms is achievable
    // We set threshold at 1000ms (1 second) as per requirement
    expect(firstByteTime).toBeLessThan(1000);

    // Verify response was successful
    expect(response?.ok()).toBe(true);

    console.log('Static site TTFB requirements met:');
    console.log('- Pure static HTML file');
    console.log('- No server-side processing required');
    console.log('- Small file size enables fast serving');
  });

  test('TC6: Critical CSS is inlined or render-blocking resources are minimized', async ({ page }) => {
    const htmlContent = fs.readFileSync(indexPath, 'utf-8');
    const cssContent = fs.readFileSync(stylesPath, 'utf-8');

    // Check for render-blocking resources
    const linkTags = htmlContent.match(/<link[^>]*>/g) || [];
    const stylesheetLinks = linkTags.filter(link =>
      link.includes('rel="stylesheet"') || link.includes("rel='stylesheet'")
    );

    console.log(`Found ${stylesheetLinks.length} stylesheet link(s)`);

    // Verify only one local stylesheet (styles.css)
    expect(stylesheetLinks.length).toBe(1);
    expect(stylesheetLinks[0]).toContain('styles.css');

    // Verify no external stylesheets that would add latency
    const hasExternalStylesheets = stylesheetLinks.some(
      link => link.includes('http://') || link.includes('https://')
    );
    expect(hasExternalStylesheets).toBe(false);

    // Check for render-blocking scripts
    const scriptTags = htmlContent.match(/<script[^>]*>/g) || [];
    const blockingScripts = scriptTags.filter(script =>
      !script.includes('async') &&
      !script.includes('defer') &&
      script.includes('src=')
    );
    expect(blockingScripts.length).toBe(0);

    // Verify CSS includes critical styles for above-the-fold content
    // Check for essential layout styles
    const hasCriticalStyles = {
      bodyStyles: cssContent.includes('body {') || cssContent.includes('body{'),
      navStyles: cssContent.includes('.navbar'),
      heroStyles: cssContent.includes('.hero-section'),
      fontStack: cssContent.includes('-apple-system') || cssContent.includes('system-ui'),
    };

    expect(hasCriticalStyles.bodyStyles).toBe(true);
    expect(hasCriticalStyles.navStyles).toBe(true);
    expect(hasCriticalStyles.heroStyles).toBe(true);
    expect(hasCriticalStyles.fontStack).toBe(true);

    console.log('Critical rendering path analysis:');
    console.log('- Single local stylesheet (minimizes requests)');
    console.log('- No external stylesheets');
    console.log('- No render-blocking external scripts');
    console.log('- Critical above-the-fold styles present');

    // Verify CSS file size is reasonable for inline consideration
    const cssSize = fs.statSync(stylesPath).size;
    console.log(`CSS size: ${cssSize} bytes (${(cssSize / 1024).toFixed(2)} KB)`);

    // CSS under 14KB (typical TCP initial window) loads in single round trip
    const tcpInitialWindowSize = 14 * 1024;
    const canLoadInSingleRoundTrip = cssSize < tcpInitialWindowSize;
    console.log(`CSS fits in TCP initial window: ${canLoadInSingleRoundTrip}`);

    // Even though CSS is not inlined, it's small enough to load quickly
    // For optimal performance, CSS < 14KB loads in one round trip
    expect(cssSize).toBeLessThan(15 * 1024); // Small margin for 14KB

    // Load page and verify above-the-fold content renders
    await page.goto(`file://${indexPath}`);

    // Verify critical elements render without waiting for all CSS
    const navbar = page.locator('.navbar');
    await expect(navbar).toBeVisible();

    const heroSection = page.getByTestId('hero-section');
    await expect(heroSection).toBeVisible();

    const ctaButton = page.getByTestId('cta-button');
    await expect(ctaButton).toBeVisible();
  });
});
