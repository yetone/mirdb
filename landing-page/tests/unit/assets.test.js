/**
 * Asset Loading Unit Tests
 * Owner: Scenario 17 - Asset Loading - Logo and GIF
 *
 * Tests for validating that logo and usage GIF assets have proper attributes
 * including meaningful alt text for accessibility.
 */
const fs = require('fs');
const path = require('path');

describe('Asset Loading Tests', () => {
  let htmlContent;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');
  });

  describe('Logo Asset Tests', () => {
    test('Logo image exists in hero section with correct src', () => {
      // Hero logo should have class hero-logo and correct src
      expect(htmlContent).toMatch(/<img[^>]*src="assets\/images\/logo\.gif"[^>]*class="hero-logo"/);
    });

    test('Logo image exists in navigation with correct src', () => {
      // Navigation should contain a logo image with the correct src
      expect(htmlContent).toMatch(/<a[^>]*class="nav-logo"[^>]*>[\s\S]*?<img[^>]*src="assets\/images\/logo\.gif"[^>]*>/);
    });

    test('Logo image exists in footer with correct src', () => {
      // Footer should contain a logo image with the correct src
      expect(htmlContent).toMatch(/<a[^>]*class="footer__logo"[^>]*>[\s\S]*?<img[^>]*src="assets\/images\/logo\.gif"[^>]*>/);
    });

    test('All logo images have alt attribute', () => {
      // Find all logo.gif images
      const logoImgTags = htmlContent.match(/<img[^>]*src="[^"]*logo\.gif"[^>]*>/g) || [];
      expect(logoImgTags.length).toBeGreaterThan(0);

      logoImgTags.forEach((imgTag) => {
        expect(imgTag).toMatch(/alt="[^"]+"/);
      });
    });
  });

  describe('Usage GIF Asset Tests', () => {
    test('Usage GIF exists in usage section with correct src', () => {
      // Usage GIF should have class usage__gif and correct src
      expect(htmlContent).toMatch(/<img[^>]*src="assets\/images\/usage\.gif"[^>]*class="usage__gif"/);
    });

    test('Usage GIF has alt attribute', () => {
      const usageImgMatch = htmlContent.match(/<img[^>]*class="usage__gif"[^>]*>/);
      expect(usageImgMatch).not.toBeNull();
      expect(usageImgMatch[0]).toMatch(/alt="[^"]+"/);
    });

    test('Usage GIF has lazy loading attribute', () => {
      const usageImgMatch = htmlContent.match(/<img[^>]*class="usage__gif"[^>]*>/);
      expect(usageImgMatch).not.toBeNull();
      expect(usageImgMatch[0]).toMatch(/loading="lazy"/);
    });
  });

  describe('TC3: Image Alt Text Accessibility Tests', () => {
    test('Logo images have meaningful alt text for accessibility', () => {
      const logoImgTags = htmlContent.match(/<img[^>]*src="[^"]*logo\.gif"[^>]*>/g) || [];
      expect(logoImgTags.length).toBeGreaterThan(0);

      logoImgTags.forEach((imgTag) => {
        const altMatch = imgTag.match(/alt="([^"]*)"/);
        expect(altMatch).not.toBeNull();
        const altText = altMatch[1];
        // Alt text should contain meaningful description
        expect(altText.toLowerCase()).toContain('mirdb');
        expect(altText.toLowerCase()).toContain('logo');
      });
    });

    test('Usage GIF has meaningful alt text for accessibility', () => {
      const usageImgMatch = htmlContent.match(/<img[^>]*class="usage__gif"[^>]*>/);
      expect(usageImgMatch).not.toBeNull();

      const altMatch = usageImgMatch[0].match(/alt="([^"]*)"/);
      expect(altMatch).not.toBeNull();
      const altText = altMatch[1];
      // Alt text should describe what the GIF demonstrates
      expect(altText.length).toBeGreaterThan(10);
      // Should contain keywords describing the content
      expect(altText.toLowerCase()).toMatch(/terminal|demonstration|usage|mirdb|command/);
    });

    test('All images on the page have non-empty alt text', () => {
      const allImgTags = htmlContent.match(/<img[^>]*>/g) || [];
      expect(allImgTags.length).toBeGreaterThan(0);

      allImgTags.forEach((imgTag) => {
        const altMatch = imgTag.match(/alt="([^"]*)"/);
        expect(altMatch).not.toBeNull();
        expect(altMatch[1].trim()).not.toBe('');
      });
    });
  });

  describe('Asset File References', () => {
    test('HTML references logo.gif from correct path', () => {
      expect(htmlContent).toContain('assets/images/logo.gif');
    });

    test('HTML references usage.gif from correct path', () => {
      expect(htmlContent).toContain('assets/images/usage.gif');
    });

    test('Asset paths use relative URLs (no absolute paths)', () => {
      const logoSrcs = htmlContent.match(/src=["'][^"']*logo\.gif["']/g);
      expect(logoSrcs).not.toBeNull();
      logoSrcs.forEach((src) => {
        expect(src).not.toContain('http://');
        expect(src).not.toContain('https://');
      });

      const usageSrcs = htmlContent.match(/src=["'][^"']*usage\.gif["']/g);
      expect(usageSrcs).not.toBeNull();
      usageSrcs.forEach((src) => {
        expect(src).not.toContain('http://');
        expect(src).not.toContain('https://');
      });
    });
  });

  describe('Image Container Layout Tests', () => {
    test('Hero logo is within hero section container', () => {
      // Extract hero section content
      const heroSectionMatch = htmlContent.match(/<!-- SECTION: Hero[^>]*>[\s\S]*?<!-- END SECTION: Hero -->/);
      expect(heroSectionMatch).not.toBeNull();
      const heroSection = heroSectionMatch[0];

      // Verify hero-logo is within hero section
      expect(heroSection).toMatch(/class="hero-logo"/);
    });

    test('Usage GIF is within usage demo container', () => {
      // Extract usage section content
      const usageSectionMatch = htmlContent.match(/<!-- SECTION: Usage[^>]*>[\s\S]*?<!-- END SECTION: Usage -->/);
      expect(usageSectionMatch).not.toBeNull();
      const usageSection = usageSectionMatch[0];

      // Verify usage__gif is within usage section
      expect(usageSection).toMatch(/class="usage__gif"/);
    });

    test('Usage demo container exists for GIF presentation', () => {
      expect(htmlContent).toMatch(/class="usage__demo-container"/);
    });
  });

  describe('Asset Physical File Existence', () => {
    test('Logo GIF file exists on disk', () => {
      const logoPath = path.join(__dirname, '../../assets/images/logo.gif');
      expect(fs.existsSync(logoPath)).toBe(true);
    });

    test('Usage GIF file exists on disk', () => {
      const usagePath = path.join(__dirname, '../../assets/images/usage.gif');
      expect(fs.existsSync(usagePath)).toBe(true);
    });

    test('Logo file is non-empty', () => {
      const logoPath = path.join(__dirname, '../../assets/images/logo.gif');
      const stats = fs.statSync(logoPath);
      expect(stats.size).toBeGreaterThan(0);
    });

    test('Usage GIF file is non-empty', () => {
      const usagePath = path.join(__dirname, '../../assets/images/usage.gif');
      const stats = fs.statSync(usagePath);
      expect(stats.size).toBeGreaterThan(0);
    });
  });

  describe('Multiple Logo Instances', () => {
    test('Logo appears at least 3 times in the page (nav, hero, footer)', () => {
      const logoCount = (htmlContent.match(/src="assets\/images\/logo\.gif"/g) || []).length;
      expect(logoCount).toBeGreaterThanOrEqual(3);
    });

    test('All logo instances use the same source file', () => {
      // All logos should point to the same asset file
      const logoSrcs = htmlContent.match(/src="[^"]*logo[^"]*"/g) || [];
      expect(logoSrcs.length).toBeGreaterThan(0);
      logoSrcs.forEach((src) => {
        expect(src).toContain('assets/images/logo.gif');
      });
    });
  });

  describe('Image Attributes', () => {
    test('Navigation logo has width and height attributes', () => {
      const navLogoMatch = htmlContent.match(/<a[^>]*class="nav-logo"[^>]*>[\s\S]*?<img[^>]*>/);
      expect(navLogoMatch).not.toBeNull();
      const imgTag = navLogoMatch[0].match(/<img[^>]*>/)[0];
      expect(imgTag).toMatch(/width="[^"]+"/);
      expect(imgTag).toMatch(/height="[^"]+"/);
    });

    test('Footer logo has width and height attributes', () => {
      const footerLogoMatch = htmlContent.match(/<a[^>]*class="footer__logo"[^>]*>[\s\S]*?<img[^>]*>/);
      expect(footerLogoMatch).not.toBeNull();
      const imgTag = footerLogoMatch[0].match(/<img[^>]*>/)[0];
      expect(imgTag).toMatch(/width="[^"]+"/);
      expect(imgTag).toMatch(/height="[^"]+"/);
    });
  });
});
