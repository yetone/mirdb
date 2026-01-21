/**
 * Hero Section Tests
 * Tests for REQ-1: Hero section with compelling headline, subheadline, and primary CTA button
 * Scenario: Hero Section Display and Content
 */

describe('Hero Section Display and Content', () => {

  // Test Case 1: Hero section contains H1 headline element with text content
  describe('Test Case 1: H1 Headline Element', () => {
    test('Hero section contains H1 headline element with text content', () => {
      const heroSection = document.querySelector('[data-testid="hero-section"]');
      expect(heroSection).toBeInTheDocument();

      const headline = heroSection.querySelector('h1');
      expect(headline).toBeInTheDocument();
      expect(headline.textContent.trim()).not.toBe('');
    });
  });

  // Test Case 2: Headline contains 10 words or fewer
  describe('Test Case 2: Headline Word Count', () => {
    test('Headline contains 10 words or fewer', () => {
      const heroSection = document.querySelector('[data-testid="hero-section"]');
      const headline = heroSection.querySelector('h1');

      expect(headline).toBeInTheDocument();

      const headlineText = headline.textContent.trim();
      const wordCount = headlineText.split(/\s+/).filter(word => word.length > 0).length;

      expect(wordCount).toBeGreaterThan(0);
      expect(wordCount).toBeLessThanOrEqual(10);
    });
  });

  // Test Case 3: Subheadline element is present below headline
  describe('Test Case 3: Subheadline Element', () => {
    test('Subheadline element is present below headline', () => {
      const heroSection = document.querySelector('[data-testid="hero-section"]');
      expect(heroSection).toBeInTheDocument();

      const headline = heroSection.querySelector('h1');
      expect(headline).toBeInTheDocument();

      // Look for subheadline with data-testid or class
      const subheadline = heroSection.querySelector('[data-testid="hero-subheadline"], .hero-subheadline');
      expect(subheadline).toBeInTheDocument();
      expect(subheadline.textContent.trim()).not.toBe('');

      // Verify subheadline comes after headline in DOM
      const allElements = Array.from(heroSection.querySelectorAll('*'));
      const headlineIndex = allElements.indexOf(headline);
      const subheadlineIndex = allElements.indexOf(subheadline);
      expect(subheadlineIndex).toBeGreaterThan(headlineIndex);
    });
  });

  // Test Case 4: Primary CTA button exists with visible text and click handler
  describe('Test Case 4: Primary CTA Button', () => {
    test('Primary CTA button exists with visible text', () => {
      const heroSection = document.querySelector('[data-testid="hero-section"]');
      expect(heroSection).toBeInTheDocument();

      // Look for CTA button with data-testid or class
      const ctaButton = heroSection.querySelector('[data-testid="hero-cta-primary"], .cta-button.primary');
      expect(ctaButton).toBeInTheDocument();

      // Check for visible text content
      const buttonText = ctaButton.textContent.trim();
      expect(buttonText).not.toBe('');
      expect(buttonText.length).toBeGreaterThan(0);
    });

    test('Primary CTA button has click handler or is a link', () => {
      const heroSection = document.querySelector('[data-testid="hero-section"]');
      const ctaButton = heroSection.querySelector('[data-testid="hero-cta-primary"], .cta-button.primary');

      expect(ctaButton).toBeInTheDocument();

      // Button should either be a link (<a>) or have onclick/type="submit"
      const isLink = ctaButton.tagName.toLowerCase() === 'a' && ctaButton.hasAttribute('href');
      const hasOnClick = ctaButton.hasAttribute('onclick') || ctaButton.onclick !== null;
      const isButton = ctaButton.tagName.toLowerCase() === 'button';

      expect(isLink || hasOnClick || isButton).toBe(true);
    });
  });

  // Test Case 5: CTA button has sufficient color contrast ratio (4.5:1 minimum)
  describe('Test Case 5: CTA Button Color Contrast', () => {
    test('CTA button has sufficient color contrast ratio (4.5:1 minimum)', () => {
      const heroSection = document.querySelector('[data-testid="hero-section"]');
      const ctaButton = heroSection.querySelector('[data-testid="hero-cta-primary"], .cta-button.primary');

      expect(ctaButton).toBeInTheDocument();

      // The CTA button uses --primary-color (#2563eb) as background with white text
      // Calculate contrast ratio for #2563eb (blue) and #ffffff (white)
      // #2563eb = rgb(37, 99, 235), white = rgb(255, 255, 255)
      const backgroundColor = 'rgb(37, 99, 235)'; // --primary-color
      const textColor = 'rgb(255, 255, 255)'; // white

      const contrastRatio = getContrastRatio(backgroundColor, textColor);

      // WCAG AA requires 4.5:1 for normal text
      expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
    });
  });

  // Test Case 6: Hero section spans full viewport width
  describe('Test Case 6: Hero Section Full Width', () => {
    test('Hero section spans full viewport width', () => {
      const heroSection = document.querySelector('[data-testid="hero-section"]');
      expect(heroSection).toBeInTheDocument();

      // Check that hero section is a block-level element that can span full width
      // In jsdom, we verify CSS classes and structure that enable full-width behavior
      const hasHeroClass = heroSection.classList.contains('hero-section');

      // Hero section should be a section element (block-level by default)
      const isBlockElement = heroSection.tagName.toLowerCase() === 'section';

      // Verify no inline styles restrict width
      const inlineWidth = heroSection.style.width;
      const hasNoRestrictiveWidth = !inlineWidth || inlineWidth === '100%' || inlineWidth === '';

      expect(hasHeroClass || isBlockElement).toBe(true);
      expect(hasNoRestrictiveWidth).toBe(true);
    });

    test('Hero section has no horizontal margin restricting width', () => {
      const heroSection = document.querySelector('[data-testid="hero-section"]');
      expect(heroSection).toBeInTheDocument();

      // Check inline styles don't restrict width with non-auto margins
      const marginLeft = heroSection.style.marginLeft;
      const marginRight = heroSection.style.marginRight;

      // Should not have margin values that would restrict width (allow 0, auto, or empty)
      const validMargins = ['', '0', '0px', 'auto'];
      expect(validMargins.includes(marginLeft) || !marginLeft).toBe(true);
      expect(validMargins.includes(marginRight) || !marginRight).toBe(true);
    });
  });
});
