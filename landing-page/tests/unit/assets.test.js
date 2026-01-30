/**
 * Asset Loading Tests
 * Owner: Scenario 17 - Asset Loading - Logo and GIF
 *
 * Tests for verifying logo and usage GIF assets load correctly,
 * have proper alt text for accessibility, and handle failures gracefully.
 */

const fs = require('fs');
const path = require('path');

describe('Asset Loading - Logo and GIF', () => {
  let htmlContent;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');
  });

  describe('Logo Asset', () => {
    test('logo.gif file exists in assets/images directory', () => {
      const logoPath = path.join(__dirname, '../../assets/images/logo.gif');
      expect(fs.existsSync(logoPath)).toBe(true);
    });

    test('logo image is referenced in hero section', () => {
      // Check for logo in hero section
      const heroSectionMatch = htmlContent.match(/<!-- SECTION: Hero[\s\S]*?<!-- END SECTION: Hero -->/);
      expect(heroSectionMatch).not.toBeNull();

      const heroSection = heroSectionMatch[0];
      expect(heroSection).toMatch(/src=["']assets\/images\/logo\.gif["']/);
    });

    test('logo image in hero has meaningful alt text', () => {
      // Find the hero logo img tag and check alt attribute
      const heroLogoMatch = htmlContent.match(/<img[^>]*class=["']hero-logo["'][^>]*>/);
      expect(heroLogoMatch).not.toBeNull();

      const logoTag = heroLogoMatch[0];
      const altMatch = logoTag.match(/alt=["']([^"']+)["']/);
      expect(altMatch).not.toBeNull();
      expect(altMatch[1].length).toBeGreaterThan(0);
      // Alt text should be descriptive (contains "logo" or "MirDB")
      expect(altMatch[1].toLowerCase()).toMatch(/logo|mirdb/i);
    });

    test('logo in navigation has meaningful alt text', () => {
      // Find the nav logo img tag
      const navLogoMatch = htmlContent.match(/<nav[\s\S]*?<img[^>]*alt=["']([^"']+)["'][^>]*>/);
      expect(navLogoMatch).not.toBeNull();
      expect(navLogoMatch[1].length).toBeGreaterThan(0);
    });

    test('logo in footer has meaningful alt text', () => {
      // Find the footer section and check for logo alt text
      const footerSectionMatch = htmlContent.match(/<!-- SECTION: Footer[\s\S]*?<!-- END SECTION: Footer -->/);
      expect(footerSectionMatch).not.toBeNull();

      const footerSection = footerSectionMatch[0];
      const footerLogoMatch = footerSection.match(/<img[^>]*alt=["']([^"']+)["'][^>]*>/);
      expect(footerLogoMatch).not.toBeNull();
      expect(footerLogoMatch[1].length).toBeGreaterThan(0);
    });
  });

  describe('Usage GIF Asset', () => {
    test('usage.gif file exists in assets/images directory', () => {
      const usagePath = path.join(__dirname, '../../assets/images/usage.gif');
      expect(fs.existsSync(usagePath)).toBe(true);
    });

    test('usage GIF is referenced in usage section', () => {
      // Check for usage GIF in usage section
      const usageSectionMatch = htmlContent.match(/<!-- SECTION: Usage[\s\S]*?<!-- END SECTION: Usage -->/);
      expect(usageSectionMatch).not.toBeNull();

      const usageSection = usageSectionMatch[0];
      expect(usageSection).toMatch(/src=["']assets\/images\/usage\.gif["']/);
    });

    test('usage GIF has meaningful alt text for accessibility', () => {
      // Find the usage GIF img tag and check alt attribute
      const usageGifMatch = htmlContent.match(/<img[^>]*class=["']usage__gif["'][^>]*>/);
      expect(usageGifMatch).not.toBeNull();

      const gifTag = usageGifMatch[0];
      const altMatch = gifTag.match(/alt=["']([^"']+)["']/);
      expect(altMatch).not.toBeNull();
      expect(altMatch[1].length).toBeGreaterThan(10); // Should be descriptive
      // Alt text should describe the demonstration
      expect(altMatch[1].toLowerCase()).toMatch(/terminal|demo|usage|command|mirdb/i);
    });

    test('usage GIF has loading="lazy" attribute for performance', () => {
      const usageGifMatch = htmlContent.match(/<img[^>]*class=["']usage__gif["'][^>]*>/);
      expect(usageGifMatch).not.toBeNull();
      expect(usageGifMatch[0]).toMatch(/loading=["']lazy["']/);
    });
  });

  describe('Alt Text Accessibility', () => {
    test('all images have non-empty alt attributes', () => {
      // Find all img tags
      const imgTags = htmlContent.match(/<img[^>]*>/g) || [];
      expect(imgTags.length).toBeGreaterThan(0);

      imgTags.forEach((imgTag, index) => {
        const altMatch = imgTag.match(/alt=["']([^"']*)["']/);
        expect(altMatch).not.toBeNull();
        // Alt text should not be empty (except for decorative images which should have alt="")
        // For our scenario, all images are meaningful and should have descriptive alt text
        if (!imgTag.includes('aria-hidden="true"')) {
          expect(altMatch[1].length).toBeGreaterThan(0);
        }
      });
    });

    test('logo alt text is descriptive and meaningful', () => {
      // Check hero logo specifically - use a more flexible pattern
      const heroSectionMatch = htmlContent.match(/<!-- SECTION: Hero[\s\S]*?<!-- END SECTION: Hero -->/);
      expect(heroSectionMatch).not.toBeNull();

      const heroSection = heroSectionMatch[0];
      const heroLogoMatch = heroSection.match(/<img[^>]*class=["']hero-logo["'][^>]*>/);
      expect(heroLogoMatch).not.toBeNull();

      const logoTag = heroLogoMatch[0];
      const altMatch = logoTag.match(/alt=["']([^"']+)["']/);
      expect(altMatch).not.toBeNull();

      const altText = altMatch[1];
      // Alt text should contain meaningful description
      expect(altText).toMatch(/MirDB|Logo/i);
    });

    test('usage GIF alt text describes the content', () => {
      // Use a more flexible pattern that doesn't depend on attribute order
      const usageSectionMatch = htmlContent.match(/<!-- SECTION: Usage[\s\S]*?<!-- END SECTION: Usage -->/);
      expect(usageSectionMatch).not.toBeNull();

      const usageSection = usageSectionMatch[0];
      const usageGifMatch = usageSection.match(/<img[^>]*class=["']usage__gif["'][^>]*>/);
      expect(usageGifMatch).not.toBeNull();

      const gifTag = usageGifMatch[0];
      const altMatch = gifTag.match(/alt=["']([^"']+)["']/);
      expect(altMatch).not.toBeNull();

      const altText = altMatch[1];
      // Alt text should describe what the GIF demonstrates
      expect(altText.split(' ').length).toBeGreaterThan(3); // Should be a phrase, not just "gif"
    });
  });

  describe('Image File Integrity', () => {
    test('logo.gif has non-zero file size', () => {
      const logoPath = path.join(__dirname, '../../assets/images/logo.gif');
      const stats = fs.statSync(logoPath);
      expect(stats.size).toBeGreaterThan(0);
    });

    test('usage.gif has non-zero file size', () => {
      const usagePath = path.join(__dirname, '../../assets/images/usage.gif');
      const stats = fs.statSync(usagePath);
      expect(stats.size).toBeGreaterThan(0);
    });

    test('logo.gif is a valid GIF file (starts with GIF header)', () => {
      const logoPath = path.join(__dirname, '../../assets/images/logo.gif');
      const buffer = fs.readFileSync(logoPath);
      // GIF files start with "GIF87a" or "GIF89a"
      const header = buffer.slice(0, 6).toString('ascii');
      expect(header).toMatch(/^GIF8[79]a$/);
    });

    test('usage.gif is a valid GIF file (starts with GIF header)', () => {
      const usagePath = path.join(__dirname, '../../assets/images/usage.gif');
      const buffer = fs.readFileSync(usagePath);
      // GIF files start with "GIF87a" or "GIF89a"
      const header = buffer.slice(0, 6).toString('ascii');
      expect(header).toMatch(/^GIF8[79]a$/);
    });
  });

  describe('Asset Path Consistency', () => {
    test('all logo references use consistent path', () => {
      const logoReferences = htmlContent.match(/src=["'][^"']*logo\.gif["']/g) || [];
      expect(logoReferences.length).toBeGreaterThanOrEqual(3); // nav, hero, footer

      logoReferences.forEach(ref => {
        expect(ref).toMatch(/assets\/images\/logo\.gif/);
      });
    });

    test('usage GIF reference uses correct path', () => {
      const usageReferences = htmlContent.match(/src=["'][^"']*usage\.gif["']/g) || [];
      expect(usageReferences.length).toBeGreaterThanOrEqual(1);

      usageReferences.forEach(ref => {
        expect(ref).toMatch(/assets\/images\/usage\.gif/);
      });
    });
  });
});
