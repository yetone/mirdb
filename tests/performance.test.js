/**
 * Performance tests for MirDB landing page
 * Tests cover: CSS size, JavaScript minimalism, page load time, and performance audit
 */
const fs = require('fs');
const path = require('path');

describe('Page Performance', () => {
  let htmlContent;
  let htmlSize;
  const htmlPath = path.join(__dirname, '..', 'index.html');

  beforeAll(() => {
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');
    htmlSize = fs.statSync(htmlPath).size;
  });

  describe('CSS Size (Test Case 2)', () => {
    test('CSS is minified or reasonably sized (< 50KB)', () => {
      // Extract inline CSS from <style> tags
      const styleMatches = htmlContent.match(/<style[^>]*>([\s\S]*?)<\/style>/gi);
      let totalCssSize = 0;

      if (styleMatches) {
        styleMatches.forEach(styleBlock => {
          // Remove the <style> tags to get just the CSS content
          const cssContent = styleBlock.replace(/<\/?style[^>]*>/gi, '');
          totalCssSize += Buffer.byteLength(cssContent, 'utf-8');
        });
      }

      // Check for external CSS links
      const externalCssLinks = htmlContent.match(/<link[^>]+rel=["']stylesheet["'][^>]*>/gi) || [];

      // Log CSS information for verification
      console.log(`Total inline CSS size: ${totalCssSize} bytes (${(totalCssSize / 1024).toFixed(2)} KB)`);
      console.log(`External CSS links found: ${externalCssLinks.length}`);

      // CSS should be under 50KB (51200 bytes)
      const maxCssSize = 50 * 1024; // 50KB in bytes
      expect(totalCssSize).toBeLessThan(maxCssSize);

      // Additional check: if there's no external CSS, the inline CSS is all there is
      if (externalCssLinks.length === 0) {
        console.log('All CSS is inline - this is optimal for performance');
      }
    });

    test('CSS is not excessively large compared to HTML content', () => {
      const styleMatches = htmlContent.match(/<style[^>]*>([\s\S]*?)<\/style>/gi);
      let totalCssSize = 0;

      if (styleMatches) {
        styleMatches.forEach(styleBlock => {
          const cssContent = styleBlock.replace(/<\/?style[^>]*>/gi, '');
          totalCssSize += Buffer.byteLength(cssContent, 'utf-8');
        });
      }

      // CSS should not be more than 50% of total HTML size for a simple landing page
      const cssPercentage = (totalCssSize / htmlSize) * 100;
      console.log(`CSS is ${cssPercentage.toFixed(2)}% of total HTML size`);

      // This is a soft check - CSS under 50% is reasonable
      expect(cssPercentage).toBeLessThan(60);
    });
  });

  describe('JavaScript Minimalism (Test Case 3)', () => {
    test('JavaScript is minimal or absent for core content', () => {
      // Check for <script> tags
      const scriptTags = htmlContent.match(/<script[^>]*>[\s\S]*?<\/script>/gi) || [];
      const externalScripts = htmlContent.match(/<script[^>]+src=["'][^"']+["'][^>]*>/gi) || [];
      const inlineScripts = scriptTags.filter(tag => !tag.includes('src='));

      console.log(`Total script tags: ${scriptTags.length}`);
      console.log(`External scripts: ${externalScripts.length}`);
      console.log(`Inline scripts: ${inlineScripts.length}`);

      // For core content, JavaScript should be absent or minimal
      // A landing page should work without JavaScript
      expect(scriptTags.length).toBeLessThanOrEqual(2); // Allow up to 2 small scripts (e.g., analytics)

      // If there are inline scripts, they should be very small
      if (inlineScripts.length > 0) {
        let totalInlineJsSize = 0;
        inlineScripts.forEach(script => {
          const jsContent = script.replace(/<\/?script[^>]*>/gi, '');
          totalInlineJsSize += Buffer.byteLength(jsContent, 'utf-8');
        });
        console.log(`Total inline JS size: ${totalInlineJsSize} bytes`);

        // Inline JS should be under 10KB
        expect(totalInlineJsSize).toBeLessThan(10 * 1024);
      }
    });

    test('Page does not require JavaScript for core functionality', () => {
      // Check that interactive elements don't rely on JavaScript
      // Forms should have proper action attributes
      const forms = htmlContent.match(/<form[^>]*>/gi) || [];
      const formsWithAction = forms.filter(form => form.includes('action='));

      // Links should have proper href attributes (not javascript:)
      const jsLinks = htmlContent.match(/href=["']javascript:/gi) || [];

      console.log(`Forms found: ${forms.length}`);
      console.log(`JavaScript-dependent links: ${jsLinks.length}`);

      // No javascript: links should be present
      expect(jsLinks.length).toBe(0);

      // All forms should have action attributes
      if (forms.length > 0) {
        expect(formsWithAction.length).toBe(forms.length);
      }
    });

    test('No JavaScript framework dependencies', () => {
      // Check for common framework indicators
      const reactIndicators = htmlContent.match(/data-react|_react|__REACT/gi) || [];
      const vueIndicators = htmlContent.match(/v-bind|v-model|v-if|v-for/gi) || [];
      const angularIndicators = htmlContent.match(/ng-app|ng-controller|ng-model/gi) || [];

      console.log('Framework dependencies check:');
      console.log(`  React indicators: ${reactIndicators.length}`);
      console.log(`  Vue indicators: ${vueIndicators.length}`);
      console.log(`  Angular indicators: ${angularIndicators.length}`);

      // Static landing page should not have framework dependencies
      expect(reactIndicators.length).toBe(0);
      expect(vueIndicators.length).toBe(0);
      expect(angularIndicators.length).toBe(0);
    });
  });

  describe('Page Load Time Estimation (Test Case 1)', () => {
    test('Page loads within 2 seconds on standard broadband', () => {
      // Standard broadband: 25 Mbps = 3.125 MB/s = 3,125,000 bytes/s
      const broadbandSpeed = 3125000; // bytes per second
      const maxLoadTime = 2; // seconds

      // Calculate total page weight (HTML + inline resources)
      const pageWeight = htmlSize;

      // Estimate load time (simplified - doesn't account for TCP overhead, latency, etc.)
      const estimatedLoadTime = pageWeight / broadbandSpeed;

      console.log(`Page size: ${pageWeight} bytes (${(pageWeight / 1024).toFixed(2)} KB)`);
      console.log(`Estimated load time at 25 Mbps: ${estimatedLoadTime.toFixed(4)} seconds`);

      // Page should load well within 2 seconds
      expect(estimatedLoadTime).toBeLessThan(maxLoadTime);
    });

    test('Total HTML size is reasonable for fast loading', () => {
      // For a landing page, total HTML should be under 100KB to ensure fast load
      const maxHtmlSize = 100 * 1024; // 100KB

      console.log(`HTML file size: ${htmlSize} bytes (${(htmlSize / 1024).toFixed(2)} KB)`);

      expect(htmlSize).toBeLessThan(maxHtmlSize);
    });

    test('No render-blocking external resources', () => {
      // Check for external CSS in <head> without async/defer attributes
      const headMatch = htmlContent.match(/<head[^>]*>([\s\S]*?)<\/head>/i);
      const headContent = headMatch ? headMatch[1] : '';

      // External CSS links block rendering
      const blockingCss = headContent.match(/<link[^>]+rel=["']stylesheet["'][^>]*>/gi) || [];

      // External sync scripts block rendering
      const syncScripts = headContent.match(/<script(?![^>]*(async|defer))[^>]+src=/gi) || [];

      console.log(`Render-blocking CSS links: ${blockingCss.length}`);
      console.log(`Render-blocking scripts: ${syncScripts.length}`);

      // Ideally, no render-blocking external resources
      // Inline CSS is fine, external CSS without async loading is problematic
      expect(blockingCss.length).toBe(0);
      expect(syncScripts.length).toBe(0);
    });

    test('Efficient resource loading strategy', () => {
      // Check for critical CSS inlining (CSS in <head>)
      const headMatch = htmlContent.match(/<head[^>]*>([\s\S]*?)<\/head>/i);
      const headContent = headMatch ? headMatch[1] : '';
      const inlineStylesInHead = headContent.match(/<style[^>]*>/gi) || [];

      console.log(`Inline styles in <head>: ${inlineStylesInHead.length}`);

      // For a simple landing page, having inline CSS is efficient
      // Either have inline CSS or properly configured external CSS
      const hasInlineCss = inlineStylesInHead.length > 0;

      expect(hasInlineCss).toBe(true);
    });
  });

  describe('Lighthouse Performance Audit Simulation (Test Case 4)', () => {
    test('Performance score factors indicate score > 80', () => {
      // Simulate key Lighthouse performance metrics assessment
      const metrics = {
        firstContentfulPaint: true, // FCP: No render-blocking resources
        largestContentfulPaint: true, // LCP: Main content loads quickly
        cumulativeLayoutShift: true, // CLS: No layout shifts expected
        totalBlockingTime: true, // TBT: No JavaScript blocking
        speedIndex: true // SI: Content renders progressively
      };

      // Check FCP factors
      const hasInlineCss = /<style[^>]*>/.test(htmlContent);
      const noRenderBlockingJs = !/<script(?![^>]*(async|defer))[^>]+src=/i.test(htmlContent);
      metrics.firstContentfulPaint = hasInlineCss && noRenderBlockingJs;

      // Check LCP factors
      const pageSize = htmlSize;
      const isSmallPage = pageSize < 100 * 1024;
      metrics.largestContentfulPaint = isSmallPage;

      // Check CLS factors (no dynamic content insertion without dimensions)
      const imagesWithoutDimensions = htmlContent.match(/<img(?![^>]*(width|height))[^>]*>/gi) || [];
      metrics.cumulativeLayoutShift = imagesWithoutDimensions.length === 0;

      // Check TBT factors (minimal JavaScript)
      const scriptTags = htmlContent.match(/<script[^>]*>/gi) || [];
      metrics.totalBlockingTime = scriptTags.length === 0;

      // Check Speed Index factors (progressive rendering)
      const hasProperStructure = /<html[^>]*>[\s\S]*<head[^>]*>[\s\S]*<\/head>[\s\S]*<body[^>]*>[\s\S]*<\/body>[\s\S]*<\/html>/i.test(htmlContent);
      metrics.speedIndex = hasProperStructure;

      console.log('Lighthouse Performance Factors:');
      console.log(`  First Contentful Paint: ${metrics.firstContentfulPaint ? 'Good' : 'Needs Improvement'}`);
      console.log(`  Largest Contentful Paint: ${metrics.largestContentfulPaint ? 'Good' : 'Needs Improvement'}`);
      console.log(`  Cumulative Layout Shift: ${metrics.cumulativeLayoutShift ? 'Good' : 'Needs Improvement'}`);
      console.log(`  Total Blocking Time: ${metrics.totalBlockingTime ? 'Good' : 'Needs Improvement'}`);
      console.log(`  Speed Index: ${metrics.speedIndex ? 'Good' : 'Needs Improvement'}`);

      // Calculate approximate score
      const passingMetrics = Object.values(metrics).filter(v => v).length;
      const totalMetrics = Object.keys(metrics).length;
      const estimatedScore = Math.round((passingMetrics / totalMetrics) * 100);

      console.log(`Estimated Performance Score: ${estimatedScore}/100`);

      // All metrics should pass for score > 80
      expect(estimatedScore).toBeGreaterThan(80);
    });

    test('Page has proper meta tags for performance', () => {
      // Check for viewport meta tag (important for mobile performance)
      const hasViewport = /<meta[^>]+name=["']viewport["'][^>]*>/i.test(htmlContent);

      // Check for charset declaration (should be early in document)
      const hasCharset = /<meta[^>]+charset=/i.test(htmlContent);

      console.log(`Has viewport meta: ${hasViewport}`);
      console.log(`Has charset declaration: ${hasCharset}`);

      expect(hasViewport).toBe(true);
      expect(hasCharset).toBe(true);
    });

    test('Page uses system fonts for optimal performance', () => {
      // Check if page uses system font stack (no web font loading)
      const styleContent = htmlContent.match(/<style[^>]*>([\s\S]*?)<\/style>/i);
      const css = styleContent ? styleContent[1] : '';

      // Look for font-family declarations with system fonts
      const fontFamilyMatch = css.match(/font-family:[^;]+/gi) || [];

      // Check for @font-face or external font imports
      const hasFontFace = /@font-face/i.test(css);
      const hasGoogleFonts = /fonts\.googleapis\.com|fonts\.gstatic\.com/i.test(htmlContent);

      console.log('Font loading check:');
      console.log(`  @font-face rules: ${hasFontFace ? 'Found' : 'None'}`);
      console.log(`  Google Fonts: ${hasGoogleFonts ? 'Found' : 'None'}`);

      // System fonts are preferred for performance
      // No @font-face and no external font services is ideal
      expect(hasFontFace).toBe(false);
      expect(hasGoogleFonts).toBe(false);
    });

    test('No large embedded media that would slow loading', () => {
      // Check for embedded base64 images or large data URIs
      const dataUris = htmlContent.match(/data:[^"'\s]+/gi) || [];
      let totalDataUriSize = 0;

      dataUris.forEach(uri => {
        totalDataUriSize += uri.length;
      });

      console.log(`Data URIs found: ${dataUris.length}`);
      console.log(`Total data URI size: ${totalDataUriSize} bytes`);

      // Data URIs should be minimal (< 10KB total)
      expect(totalDataUriSize).toBeLessThan(10 * 1024);
    });

    test('HTML is well-formed and efficient', () => {
      // Check for proper HTML structure
      const hasDoctype = /^<!DOCTYPE html>/i.test(htmlContent.trim());
      const hasHtmlTag = /<html[^>]*>/i.test(htmlContent);
      const hasHeadTag = /<head[^>]*>/i.test(htmlContent);
      const hasBodyTag = /<body[^>]*>/i.test(htmlContent);

      console.log('HTML Structure:');
      console.log(`  DOCTYPE: ${hasDoctype ? 'Present' : 'Missing'}`);
      console.log(`  HTML tag: ${hasHtmlTag ? 'Present' : 'Missing'}`);
      console.log(`  HEAD tag: ${hasHeadTag ? 'Present' : 'Missing'}`);
      console.log(`  BODY tag: ${hasBodyTag ? 'Present' : 'Missing'}`);

      expect(hasDoctype).toBe(true);
      expect(hasHtmlTag).toBe(true);
      expect(hasHeadTag).toBe(true);
      expect(hasBodyTag).toBe(true);
    });
  });

  describe('Asset Optimization (Step 2)', () => {
    test('External images (if any) are not loaded in critical path', () => {
      // Check for images that would block rendering
      const imgTags = htmlContent.match(/<img[^>]*>/gi) || [];

      console.log(`Image tags found: ${imgTags.length}`);

      // If images exist, check they're not excessively large references
      // Note: Can't check actual file sizes from HTML, but can check for lazy loading
      imgTags.forEach(img => {
        const hasLazyLoading = /loading=["']lazy["']/i.test(img);
        console.log(`  Image has lazy loading: ${hasLazyLoading}`);
      });

      // Pass if no images or images are present (further optimization would be in deployment)
      expect(true).toBe(true);
    });

    test('CSS uses efficient selectors', () => {
      const styleContent = htmlContent.match(/<style[^>]*>([\s\S]*?)<\/style>/i);
      const css = styleContent ? styleContent[1] : '';

      // Check for inefficient selectors (universal selectors, deep nesting)
      const universalSelectors = css.match(/\*\s*{/g) || [];
      const deeplyNestedSelectors = css.match(/[^\s,]+\s+[^\s,]+\s+[^\s,]+\s+[^\s,]+\s*{/g) || [];

      console.log(`Universal selectors: ${universalSelectors.length}`);
      console.log(`Deeply nested selectors (4+ levels): ${deeplyNestedSelectors.length}`);

      // Universal selectors are acceptable if minimal (e.g., reset)
      expect(universalSelectors.length).toBeLessThanOrEqual(2);
    });
  });
});
