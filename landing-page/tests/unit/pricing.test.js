/**
 * Pricing Section Unit Tests
 * Owner: Scenario 6 - Pricing Section
 *
 * Test cases:
 * 1. Pricing section element exists
 * 2. Pricing tiers or pricing information present
 * 3. CTA button exists in pricing section
 * 4. Section has h2 heading with pricing-related text
 */

const fs = require('fs');
const path = require('path');

describe('Pricing Section', () => {
  let document;
  let pricingSection;

  beforeAll(() => {
    // Read the HTML file
    const htmlPath = path.join(__dirname, '../../index.html');
    const html = fs.readFileSync(htmlPath, 'utf8');

    // Create a DOM from the HTML
    document = new DOMParser().parseFromString(html, 'text/html');
    pricingSection = document.getElementById('pricing');
  });

  describe('Test Case 1: Pricing section element exists', () => {
    test('pricing section exists by id', () => {
      expect(pricingSection).not.toBeNull();
    });

    test('pricing section is a section element', () => {
      expect(pricingSection.tagName.toLowerCase()).toBe('section');
    });

    test('pricing section has pricing class', () => {
      expect(pricingSection.classList.contains('pricing')).toBe(true);
    });

    test('pricing section has aria-labelledby attribute', () => {
      expect(pricingSection.getAttribute('aria-labelledby')).toBe('pricing-title');
    });
  });

  describe('Test Case 2: Pricing tiers or pricing information present', () => {
    test('pricing section has pricing cards OR contact for pricing message', () => {
      const pricingCards = pricingSection.querySelectorAll('.pricing-card, .pricing-tier');
      const contactMessage = pricingSection.querySelector('.pricing-contact, .pricing-cta-section');

      // Either pricing cards exist OR a contact/pricing message exists
      const hasPricingContent = pricingCards.length > 0 || contactMessage !== null;
      expect(hasPricingContent).toBe(true);
    });

    test('pricing information is visible (not hidden)', () => {
      const pricingCards = pricingSection.querySelectorAll('.pricing-card, .pricing-tier');
      const contactMessage = pricingSection.querySelector('.pricing-contact, .pricing-cta-section');

      if (pricingCards.length > 0) {
        pricingCards.forEach(card => {
          expect(card.getAttribute('aria-hidden')).not.toBe('true');
        });
      } else if (contactMessage) {
        expect(contactMessage.getAttribute('aria-hidden')).not.toBe('true');
      }
    });

    test('pricing content has descriptive text', () => {
      const content = pricingSection.querySelector('.pricing-card, .pricing-tier, .pricing-contact, .pricing-cta-section, .pricing-description');
      expect(content).not.toBeNull();
      expect(content.textContent.trim().length).toBeGreaterThan(0);
    });
  });

  describe('Test Case 3: CTA button exists in pricing section', () => {
    test('pricing section has at least one CTA button', () => {
      const ctaButtons = pricingSection.querySelectorAll('a.btn, button.btn, .pricing-cta, [class*="cta"]');
      expect(ctaButtons.length).toBeGreaterThanOrEqual(1);
    });

    test('pricing CTA button has href or onclick', () => {
      const ctaButton = pricingSection.querySelector('a.btn, button.btn, .pricing-cta, [class*="cta"]');
      expect(ctaButton).not.toBeNull();

      const hasHref = ctaButton.hasAttribute('href');
      const hasOnclick = ctaButton.hasAttribute('onclick');
      const isButton = ctaButton.tagName.toLowerCase() === 'button';

      expect(hasHref || hasOnclick || isButton).toBe(true);
    });

    test('pricing CTA button has visible text', () => {
      const ctaButton = pricingSection.querySelector('a.btn, button.btn, .pricing-cta, [class*="cta"]');
      expect(ctaButton).not.toBeNull();
      expect(ctaButton.textContent.trim().length).toBeGreaterThan(0);
    });

    test('pricing CTA button is not disabled', () => {
      const ctaButton = pricingSection.querySelector('a.btn, button.btn, .pricing-cta, [class*="cta"]');
      expect(ctaButton).not.toBeNull();
      expect(ctaButton.hasAttribute('disabled')).toBe(false);
    });
  });

  describe('Test Case 4: Section has h2 heading with pricing-related text', () => {
    test('pricing section has an h2 heading', () => {
      const h2 = pricingSection.querySelector('h2');
      expect(h2).not.toBeNull();
    });

    test('pricing section h2 has id "pricing-title"', () => {
      const h2 = pricingSection.querySelector('h2');
      expect(h2.id).toBe('pricing-title');
    });

    test('pricing heading contains pricing-related text', () => {
      const h2 = pricingSection.querySelector('h2');
      const headingText = h2.textContent.toLowerCase();

      // Should contain "pricing", "price", "plans", "cost", or similar
      const pricingRelatedTerms = ['pricing', 'price', 'plan', 'cost', 'subscription', 'tier'];
      const hasPricingTerm = pricingRelatedTerms.some(term => headingText.includes(term));

      expect(hasPricingTerm).toBe(true);
    });

    test('pricing heading has non-empty text', () => {
      const h2 = pricingSection.querySelector('h2');
      expect(h2.textContent.trim().length).toBeGreaterThan(0);
    });
  });
});
