/**
 * Product Branding and Visual Identity Tests
 *
 * Tests for verifying MirDB homepage branding including:
 * - Logo presence and correct path
 * - Image alt attributes for accessibility
 * - All images loading correctly
 * - Usage demonstration GIF presence
 */

const fs = require('fs');
const path = require('path');

// Simple HTML parser using regex for basic DOM-like operations
class SimpleHTMLParser {
  constructor(html) {
    this.html = html;
  }

  querySelectorAll(selector) {
    if (selector === 'img') {
      return this.getImages();
    }
    if (selector === 'a') {
      return this.getLinks();
    }
    if (selector === 'title') {
      return this.getTitles();
    }
    if (selector === 'header') {
      return this.getHeaders();
    }
    if (selector.startsWith('meta[name="')) {
      const name = selector.match(/meta\[name="([^"]+)"\]/)?.[1];
      return this.getMeta(name);
    }
    return [];
  }

  querySelector(selector) {
    const results = this.querySelectorAll(selector);
    return results.length > 0 ? results[0] : null;
  }

  getImages() {
    const imgRegex = /<img\s+([^>]*)>/gi;
    const images = [];
    let match;

    while ((match = imgRegex.exec(this.html)) !== null) {
      const attributes = match[1];
      images.push({
        getAttribute: (attr) => {
          const attrRegex = new RegExp(`${attr}=["']([^"']*)["']`, 'i');
          const attrMatch = attributes.match(attrRegex);
          return attrMatch ? attrMatch[1] : null;
        },
        _raw: match[0]
      });
    }
    return images;
  }

  getLinks() {
    const linkRegex = /<a\s+([^>]*)>/gi;
    const links = [];
    let match;

    while ((match = linkRegex.exec(this.html)) !== null) {
      const attributes = match[1];
      links.push({
        getAttribute: (attr) => {
          const attrRegex = new RegExp(`${attr}=["']([^"']*)["']`, 'i');
          const attrMatch = attributes.match(attrRegex);
          return attrMatch ? attrMatch[1] : null;
        }
      });
    }
    return links;
  }

  getTitles() {
    const titleRegex = /<title[^>]*>([^<]*)<\/title>/i;
    const match = this.html.match(titleRegex);
    if (match) {
      return [{
        textContent: match[1],
        getAttribute: () => null
      }];
    }
    return [];
  }

  getMeta(name) {
    const metaRegex = new RegExp(`<meta\\s+[^>]*name=["']${name}["'][^>]*>`, 'gi');
    const metas = [];
    let match;

    while ((match = metaRegex.exec(this.html)) !== null) {
      const tag = match[0];
      metas.push({
        getAttribute: (attr) => {
          const attrRegex = new RegExp(`${attr}=["']([^"']*)["']`, 'i');
          const attrMatch = tag.match(attrRegex);
          return attrMatch ? attrMatch[1] : null;
        }
      });
    }
    return metas;
  }

  getHeaders() {
    const headerRegex = /<header[^>]*>([\s\S]*?)<\/header>/i;
    const match = this.html.match(headerRegex);
    if (match) {
      const headerContent = match[0];
      const headerParser = new SimpleHTMLParser(headerContent);
      return [{
        querySelector: (sel) => headerParser.querySelector(sel),
        querySelectorAll: (sel) => headerParser.querySelectorAll(sel)
      }];
    }
    return [];
  }
}

describe('Product Branding and Visual Identity', () => {
  let htmlContent;
  let document;

  beforeAll(() => {
    // Read the index.html file
    const htmlPath = path.join(__dirname, '..', 'index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');
    document = new SimpleHTMLParser(htmlContent);
  });

  describe('Test Case 1: Logo Image Presence', () => {
    test('Logo image is present and references assets/logo.gif', () => {
      // Find all img elements with logo in src or class
      const images = document.querySelectorAll('img');
      const logoImages = images.filter(img => {
        const src = img.getAttribute('src') || '';
        const className = img.getAttribute('class') || '';
        return src.includes('logo') || className.includes('logo');
      });

      // Verify at least one logo image exists
      expect(logoImages.length).toBeGreaterThan(0);

      // Verify logo references assets/logo.gif
      const hasCorrectLogoPath = logoImages.some(img => {
        const src = img.getAttribute('src');
        return src === 'assets/logo.gif' || src.includes('assets/logo.gif');
      });
      expect(hasCorrectLogoPath).toBe(true);

      // Verify the logo file actually exists
      const logoPath = path.join(__dirname, '..', 'assets', 'logo.gif');
      expect(fs.existsSync(logoPath)).toBe(true);
    });
  });

  describe('Test Case 2: Image Alt Attributes', () => {
    test('All <img> elements have non-empty alt attributes for accessibility', () => {
      const images = document.querySelectorAll('img');

      // Ensure there are images on the page
      expect(images.length).toBeGreaterThan(0);

      // Check each image has a non-empty alt attribute
      const imagesWithoutAlt = [];
      images.forEach((img, index) => {
        const alt = img.getAttribute('alt');
        if (!alt || alt.trim() === '') {
          imagesWithoutAlt.push({
            index,
            src: img.getAttribute('src'),
            class: img.getAttribute('class')
          });
        }
      });

      // Report which images are missing alt text
      if (imagesWithoutAlt.length > 0) {
        console.log('Images missing alt text:', imagesWithoutAlt);
      }

      expect(imagesWithoutAlt.length).toBe(0);
    });

    test('Alt attributes are descriptive (not just placeholder text)', () => {
      const images = document.querySelectorAll('img');

      images.forEach(img => {
        const alt = img.getAttribute('alt');
        // Alt text should be at least 5 characters to be descriptive
        expect(alt.length).toBeGreaterThanOrEqual(5);
        // Alt text should not be generic placeholders
        expect(alt.toLowerCase()).not.toBe('image');
        expect(alt.toLowerCase()).not.toBe('logo');
        expect(alt.toLowerCase()).not.toBe('img');
      });
    });
  });

  describe('Test Case 3: Image Assets Exist', () => {
    test('All image files referenced in HTML exist in the filesystem', () => {
      const images = document.querySelectorAll('img');
      const missingImages = [];

      images.forEach(img => {
        const src = img.getAttribute('src');
        if (src && !src.startsWith('http://') && !src.startsWith('https://') && !src.startsWith('data:')) {
          const imagePath = path.join(__dirname, '..', src);
          if (!fs.existsSync(imagePath)) {
            missingImages.push(src);
          }
        }
      });

      if (missingImages.length > 0) {
        console.log('Missing image files:', missingImages);
      }

      expect(missingImages.length).toBe(0);
    });

    test('Logo file has valid content (not empty)', () => {
      const logoPath = path.join(__dirname, '..', 'assets', 'logo.gif');
      const stats = fs.statSync(logoPath);
      expect(stats.size).toBeGreaterThan(0);
    });
  });

  describe('Test Case 4: Usage Demonstration GIF', () => {
    test('Usage demonstration GIF is displayed or linked on the page', () => {
      // Check for usage.gif in image sources
      const images = document.querySelectorAll('img');
      const usageImages = images.filter(img => {
        const src = img.getAttribute('src') || '';
        return src.includes('usage.gif') || src.includes('usage');
      });

      // Check for usage.gif in anchor links
      const links = document.querySelectorAll('a');
      const usageLinks = links.filter(link => {
        const href = link.getAttribute('href') || '';
        return href.includes('usage.gif') || href.includes('usage');
      });

      // Either an image or link should reference usage.gif
      const hasUsageReference = usageImages.length > 0 || usageLinks.length > 0;
      expect(hasUsageReference).toBe(true);

      // If there's an image, verify the file exists
      if (usageImages.length > 0) {
        const usagePath = path.join(__dirname, '..', 'assets', 'usage.gif');
        expect(fs.existsSync(usagePath)).toBe(true);
      }
    });

    test('Usage GIF file has valid content (not empty)', () => {
      const usagePath = path.join(__dirname, '..', 'assets', 'usage.gif');
      const stats = fs.statSync(usagePath);
      expect(stats.size).toBeGreaterThan(0);
    });

    test('Usage demonstration image has descriptive alt text', () => {
      const images = document.querySelectorAll('img');
      const usageImage = images.find(img => {
        const src = img.getAttribute('src') || '';
        return src.includes('usage');
      });

      if (usageImage) {
        const alt = usageImage.getAttribute('alt');
        expect(alt).toBeTruthy();
        expect(alt.length).toBeGreaterThan(10); // Should be descriptive
      }
    });
  });

  describe('Branding Consistency', () => {
    test('MirDB name appears in the page title', () => {
      const title = document.querySelector('title');
      expect(title).toBeTruthy();
      expect(title.textContent.toLowerCase()).toContain('mirdb');
    });

    test('Page has proper meta description with MirDB branding', () => {
      const metaDesc = document.querySelector('meta[name="description"]');
      expect(metaDesc).toBeTruthy();
      const content = metaDesc.getAttribute('content');
      expect(content.toLowerCase()).toContain('mirdb');
    });

    test('Header contains logo image', () => {
      const header = document.querySelector('header');
      expect(header).toBeTruthy();
      const headerLogo = header.querySelector('img');
      expect(headerLogo).toBeTruthy();
      expect(headerLogo.getAttribute('src')).toContain('logo');
    });
  });
});
