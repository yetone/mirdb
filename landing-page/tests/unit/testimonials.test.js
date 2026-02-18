/**
 * Testimonials/Social Proof Section Unit Tests
 * Owner: Scenario 5 - Social Proof & Testimonials
 *
 * Test cases:
 * - Social proof section element exists
 * - At least one testimonial quote with text content
 * - Testimonials have attribution elements with author info
 * - Customer logos or trust badges present
 * - Avatar/photo images have meaningful alt attributes
 * - Testimonials use blockquote or q elements with cite
 */

const fs = require('fs');
const path = require('path');

describe('Social Proof & Testimonials Section', () => {
  let document;
  let testimonialsSection;

  beforeAll(() => {
    // Read the HTML file
    const htmlPath = path.join(__dirname, '../../index.html');
    const html = fs.readFileSync(htmlPath, 'utf8');

    // Create a DOM from the HTML
    document = new DOMParser().parseFromString(html, 'text/html');
    testimonialsSection = document.getElementById('testimonials');
  });

  describe('Test Case 1: Social proof section element exists', () => {
    test('testimonials section exists', () => {
      expect(testimonialsSection).not.toBeNull();
    });

    test('testimonials section has correct tag name', () => {
      expect(testimonialsSection.tagName.toLowerCase()).toBe('section');
    });

    test('testimonials section has aria-labelledby attribute', () => {
      expect(testimonialsSection.getAttribute('aria-labelledby')).toBe('testimonials-title');
    });

    test('testimonials section has testimonials class', () => {
      expect(testimonialsSection.classList.contains('testimonials')).toBe(true);
    });
  });

  describe('Test Case 2: At least one testimonial quote with text content', () => {
    test('testimonial quotes exist', () => {
      const testimonials = testimonialsSection.querySelectorAll('.testimonial-card, .testimonial, [class*="testimonial"]');
      expect(testimonials.length).toBeGreaterThan(0);
    });

    test('at least one testimonial has quote text', () => {
      const quotes = testimonialsSection.querySelectorAll('blockquote, q, .testimonial-quote, [class*="quote"]');
      expect(quotes.length).toBeGreaterThan(0);

      // Check at least one has content
      const hasContent = Array.from(quotes).some(quote => quote.textContent.trim().length > 0);
      expect(hasContent).toBe(true);
    });

    test('testimonial quotes have meaningful text content', () => {
      const quotes = testimonialsSection.querySelectorAll('blockquote, q, .testimonial-quote');
      quotes.forEach((quote) => {
        const textContent = quote.textContent.trim();
        expect(textContent.length).toBeGreaterThan(10); // Meaningful quote should have substantial text
      });
    });
  });

  describe('Test Case 3: Testimonials have attribution elements with author info', () => {
    test('testimonials have author attribution', () => {
      const testimonials = testimonialsSection.querySelectorAll('.testimonial-card, .testimonial');
      expect(testimonials.length).toBeGreaterThan(0);

      testimonials.forEach((testimonial) => {
        const attribution = testimonial.querySelector('cite, .testimonial-author, .testimonial-attribution, [class*="author"]');
        expect(attribution).not.toBeNull();
      });
    });

    test('testimonial attributions have author name', () => {
      const testimonials = testimonialsSection.querySelectorAll('.testimonial-card, .testimonial');

      testimonials.forEach((testimonial) => {
        const authorName = testimonial.querySelector('.testimonial-author-name, [class*="author-name"], cite strong, cite .name');
        expect(authorName).not.toBeNull();
        expect(authorName.textContent.trim().length).toBeGreaterThan(0);
      });
    });

    test('testimonial attributions have role or company info', () => {
      const testimonials = testimonialsSection.querySelectorAll('.testimonial-card, .testimonial');

      testimonials.forEach((testimonial) => {
        const roleOrCompany = testimonial.querySelector('.testimonial-author-role, .testimonial-author-company, [class*="role"], [class*="company"]');
        expect(roleOrCompany).not.toBeNull();
        expect(roleOrCompany.textContent.trim().length).toBeGreaterThan(0);
      });
    });
  });

  describe('Test Case 4: Customer logos or trust badges present', () => {
    test('customer logos or trust indicators section exists', () => {
      const logosSection = testimonialsSection.querySelector('.customer-logos, .trust-indicators, .logo-grid, [class*="logos"], [class*="trust"]');
      expect(logosSection).not.toBeNull();
    });

    test('customer logos are present as images or SVGs', () => {
      const logosSection = testimonialsSection.querySelector('.customer-logos, .trust-indicators, .logo-grid, [class*="logos"], [class*="trust"]');
      const logos = logosSection.querySelectorAll('img, svg, .logo');
      expect(logos.length).toBeGreaterThan(0);
    });

    test('at least 3 customer logos are present', () => {
      const logosSection = testimonialsSection.querySelector('.customer-logos, .trust-indicators, .logo-grid, [class*="logos"], [class*="trust"]');
      const logos = logosSection.querySelectorAll('img, svg, .logo, .customer-logo');
      expect(logos.length).toBeGreaterThanOrEqual(3);
    });
  });

  describe('Test Case 5: Avatar/photo images have meaningful alt attributes', () => {
    test('testimonial avatars/photos exist', () => {
      const avatars = testimonialsSection.querySelectorAll('.testimonial-avatar img, .testimonial-photo img, .testimonial-card img, [class*="avatar"] img');
      expect(avatars.length).toBeGreaterThan(0);
    });

    test('all testimonial avatar images have alt attribute', () => {
      const avatars = testimonialsSection.querySelectorAll('.testimonial-avatar img, .testimonial-photo img, .testimonial-card img');
      avatars.forEach((avatar) => {
        expect(avatar.hasAttribute('alt')).toBe(true);
      });
    });

    test('testimonial avatar images have meaningful alt text (not empty)', () => {
      const avatars = testimonialsSection.querySelectorAll('.testimonial-avatar img, .testimonial-photo img, .testimonial-card img');
      avatars.forEach((avatar) => {
        const altText = avatar.getAttribute('alt');
        expect(altText).not.toBe('');
        expect(altText.length).toBeGreaterThan(0);
      });
    });

    test('customer logo images have alt text for accessibility', () => {
      const logosSection = testimonialsSection.querySelector('.customer-logos, .trust-indicators, .logo-grid, [class*="logos"]');
      if (logosSection) {
        const logos = logosSection.querySelectorAll('img');
        logos.forEach((logo) => {
          expect(logo.hasAttribute('alt')).toBe(true);
          const altText = logo.getAttribute('alt');
          expect(altText.length).toBeGreaterThan(0);
        });
      }
    });
  });

  describe('Test Case 6: Testimonials use blockquote or q elements with cite', () => {
    test('testimonials use semantic blockquote elements', () => {
      const blockquotes = testimonialsSection.querySelectorAll('blockquote');
      expect(blockquotes.length).toBeGreaterThan(0);
    });

    test('blockquotes have associated cite elements for attribution', () => {
      const testimonials = testimonialsSection.querySelectorAll('.testimonial-card, .testimonial');

      testimonials.forEach((testimonial) => {
        const blockquote = testimonial.querySelector('blockquote');
        const cite = testimonial.querySelector('cite, footer cite');

        // Either blockquote has cite attribute or there's a cite element in the testimonial
        const hasCiteAttribute = blockquote && blockquote.hasAttribute('cite');
        const hasCiteElement = cite !== null;

        expect(hasCiteAttribute || hasCiteElement).toBe(true);
      });
    });

    test('section has proper heading hierarchy', () => {
      const h2 = testimonialsSection.querySelector('h2');
      expect(h2).not.toBeNull();
      expect(h2.id).toBe('testimonials-title');
      expect(h2.textContent.trim().length).toBeGreaterThan(0);
    });
  });
});
