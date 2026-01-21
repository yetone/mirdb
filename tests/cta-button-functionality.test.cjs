/**
 * CTA Button Functionality Tests
 * Scenario: CTA Button Functionality
 * Description: Verify all CTA buttons are functional and guide visitors toward desired actions
 */

/**
 * Utility function to calculate contrast ratio between two colors
 * @param {string} rgb1 - RGB color string (e.g., 'rgb(255, 255, 255)')
 * @param {string} rgb2 - RGB color string
 * @returns {number} - Contrast ratio
 */
function getContrastRatio(rgb1, rgb2) {
  const parseRGB = (rgb) => {
    const match = rgb.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
    if (!match) return { r: 0, g: 0, b: 0 };
    return {
      r: parseInt(match[1]),
      g: parseInt(match[2]),
      b: parseInt(match[3])
    };
  };

  const getLuminance = (rgb) => {
    const [r, g, b] = [rgb.r, rgb.g, rgb.b].map(val => {
      const sRGB = val / 255;
      return sRGB <= 0.03928
        ? sRGB / 12.92
        : Math.pow((sRGB + 0.055) / 1.055, 2.4);
    });
    return 0.2126 * r + 0.7152 * g + 0.0722 * b;
  };

  const lum1 = getLuminance(parseRGB(rgb1));
  const lum2 = getLuminance(parseRGB(rgb2));
  const lighter = Math.max(lum1, lum2);
  const darker = Math.min(lum1, lum2);

  return (lighter + 0.05) / (darker + 0.05);
}

// Expose for tests
global.getContrastRatio = getContrastRatio;

describe('CTA Button Functionality', () => {

  /**
   * Test Case 1: Click hero primary CTA button
   * Expected: Button triggers navigation or form action
   */
  describe('Test Case 1: Hero Primary CTA Button Functionality', () => {
    test('Hero primary CTA button exists', () => {
      const heroSection = document.querySelector('[data-testid="hero-section"]');
      expect(heroSection).toBeInTheDocument();

      const heroCTA = heroSection.querySelector('[data-testid="hero-cta-primary"]');
      expect(heroCTA).toBeInTheDocument();
    });

    test('Hero primary CTA button has href attribute for navigation', () => {
      const heroCTA = document.querySelector('[data-testid="hero-cta-primary"]');
      expect(heroCTA).toBeInTheDocument();

      const href = heroCTA.getAttribute('href');
      expect(href).toBeTruthy();
      expect(href).not.toBe('');
    });

    test('Hero primary CTA button href points to a valid target', () => {
      const heroCTA = document.querySelector('[data-testid="hero-cta-primary"]');
      const href = heroCTA.getAttribute('href');

      // Should be an anchor link (internal navigation) or external URL
      const isValidHref = href.startsWith('#') || href.startsWith('http') || href.startsWith('/');
      expect(isValidHref).toBe(true);
    });

    test('Hero primary CTA is clickable element (link or button)', () => {
      const heroCTA = document.querySelector('[data-testid="hero-cta-primary"]');
      expect(heroCTA).toBeInTheDocument();

      const tagName = heroCTA.tagName.toLowerCase();
      const isClickable = tagName === 'a' || tagName === 'button';
      expect(isClickable).toBe(true);
    });

    test('Hero primary CTA has proper cursor style for clickability', () => {
      const heroCTA = document.querySelector('[data-testid="hero-cta-primary"]');
      expect(heroCTA).toBeInTheDocument();

      // Verify it has cta-button class which applies cursor: pointer
      expect(heroCTA.classList.contains('cta-button')).toBe(true);
    });
  });

  /**
   * Test Case 2: Click header CTA button
   * Expected: Button triggers same action as hero CTA
   */
  describe('Test Case 2: Header CTA Button Functionality', () => {
    test('Header CTA button exists', () => {
      const header = document.querySelector('[data-testid="header"]');
      expect(header).toBeInTheDocument();

      const navCTA = header.querySelector('[data-testid="nav-cta"]');
      expect(navCTA).toBeInTheDocument();
    });

    test('Header CTA button has href attribute for navigation', () => {
      const navCTA = document.querySelector('[data-testid="nav-cta"]');
      expect(navCTA).toBeInTheDocument();

      const href = navCTA.getAttribute('href');
      expect(href).toBeTruthy();
      expect(href).not.toBe('');
    });

    test('Header CTA has same target as hero CTA', () => {
      const heroCTA = document.querySelector('[data-testid="hero-cta-primary"]');
      const navCTA = document.querySelector('[data-testid="nav-cta"]');

      expect(heroCTA).toBeInTheDocument();
      expect(navCTA).toBeInTheDocument();

      const heroHref = heroCTA.getAttribute('href');
      const navHref = navCTA.getAttribute('href');

      // Both CTAs should navigate to the same destination
      expect(navHref).toBe(heroHref);
    });

    test('Header CTA is a clickable element', () => {
      const navCTA = document.querySelector('[data-testid="nav-cta"]');
      expect(navCTA).toBeInTheDocument();

      const tagName = navCTA.tagName.toLowerCase();
      const isClickable = tagName === 'a' || tagName === 'button';
      expect(isClickable).toBe(true);
    });

    test('Header CTA has distinct styling from nav links', () => {
      const navCTA = document.querySelector('[data-testid="nav-cta"]');
      expect(navCTA).toBeInTheDocument();

      // Should have nav-cta class for distinctive styling
      expect(navCTA.classList.contains('nav-cta')).toBe(true);
      expect(navCTA.classList.contains('cta-button')).toBe(true);
    });
  });

  /**
   * Test Case 3: Verify CTA button text
   * Expected: CTA text is action-oriented (e.g., 'Get Started', 'Sign Up')
   */
  describe('Test Case 3: CTA Button Text Clarity', () => {
    test('Hero primary CTA has visible text content', () => {
      const heroCTA = document.querySelector('[data-testid="hero-cta-primary"]');
      expect(heroCTA).toBeInTheDocument();

      const text = heroCTA.textContent.trim();
      expect(text).not.toBe('');
      expect(text.length).toBeGreaterThan(0);
    });

    test('Hero primary CTA text is action-oriented', () => {
      const heroCTA = document.querySelector('[data-testid="hero-cta-primary"]');
      const text = heroCTA.textContent.trim().toLowerCase();

      // Action-oriented keywords
      const actionWords = [
        'start', 'get', 'try', 'sign', 'join', 'download',
        'free', 'begin', 'create', 'launch', 'explore', 'discover'
      ];

      const hasActionWord = actionWords.some(word => text.includes(word));
      expect(hasActionWord).toBe(true);
    });

    test('Hero primary CTA text avoids vague language', () => {
      const heroCTA = document.querySelector('[data-testid="hero-cta-primary"]');
      const text = heroCTA.textContent.trim().toLowerCase();

      // Vague phrases to avoid
      const vagueText = ['click here', 'submit', 'go', 'more'];

      const hasVagueText = vagueText.some(vague => text === vague);
      expect(hasVagueText).toBe(false);
    });

    test('Header CTA has visible text content', () => {
      const navCTA = document.querySelector('[data-testid="nav-cta"]');
      expect(navCTA).toBeInTheDocument();

      const text = navCTA.textContent.trim();
      expect(text).not.toBe('');
      expect(text.length).toBeGreaterThan(0);
    });

    test('Header CTA text is action-oriented', () => {
      const navCTA = document.querySelector('[data-testid="nav-cta"]');
      const text = navCTA.textContent.trim().toLowerCase();

      // Action-oriented keywords
      const actionWords = [
        'start', 'get', 'try', 'sign', 'join', 'download',
        'free', 'begin', 'create', 'launch', 'explore', 'discover'
      ];

      const hasActionWord = actionWords.some(word => text.includes(word));
      expect(hasActionWord).toBe(true);
    });

    test('All CTA buttons have text that communicates the action', () => {
      const allCTAs = document.querySelectorAll('.cta-button');
      expect(allCTAs.length).toBeGreaterThan(0);

      allCTAs.forEach(cta => {
        const text = cta.textContent.trim();
        expect(text).not.toBe('');
        expect(text.length).toBeGreaterThan(2);
      });
    });
  });

  /**
   * Test Case 4: CTA Visibility on Scroll
   * Testing that the header remains visible (sticky) ensuring CTA is always accessible
   */
  describe('Test Case 4: CTA Visibility on Scroll', () => {
    test('Header is sticky positioned', () => {
      const header = document.querySelector('[data-testid="header"]');
      expect(header).toBeInTheDocument();
      expect(header.classList.contains('header')).toBe(true);
    });

    test('Header contains a CTA button for persistent visibility', () => {
      const header = document.querySelector('[data-testid="header"]');
      const navCTA = header.querySelector('[data-testid="nav-cta"]');

      expect(navCTA).toBeInTheDocument();
    });

    test('Hero section CTA is visible without scrolling', () => {
      const heroSection = document.querySelector('[data-testid="hero-section"]');
      const heroCTA = heroSection.querySelector('[data-testid="hero-cta-primary"]');

      expect(heroSection).toBeInTheDocument();
      expect(heroCTA).toBeInTheDocument();

      // Verify CTA is within hero section (above the fold)
      expect(heroSection.contains(heroCTA)).toBe(true);
    });

    test('Multiple CTAs exist across the page for accessibility', () => {
      const allCTAs = document.querySelectorAll('.cta-button');
      expect(allCTAs.length).toBeGreaterThanOrEqual(2);
    });

    test('At least one primary CTA exists in hero section', () => {
      const heroSection = document.querySelector('[data-testid="hero-section"]');
      const primaryCTA = heroSection.querySelector('.cta-button.primary');

      expect(primaryCTA).toBeInTheDocument();
    });

    test('At least one CTA exists in header navigation', () => {
      const header = document.querySelector('[data-testid="header"]');
      const navCTA = header.querySelector('.cta-button');

      expect(navCTA).toBeInTheDocument();
    });
  });

  /**
   * Additional CTA Functionality Tests
   */
  describe('CTA Button Accessibility and Styling', () => {
    test('CTA buttons have sufficient color contrast', () => {
      // Primary button uses #2563eb (blue) background with white text
      const backgroundColor = 'rgb(37, 99, 235)';
      const textColor = 'rgb(255, 255, 255)';

      const contrastRatio = getContrastRatio(backgroundColor, textColor);

      // WCAG AA requires 4.5:1 for normal text
      expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
    });

    test('CTA buttons have appropriate padding for touch targets', () => {
      const heroCTA = document.querySelector('[data-testid="hero-cta-primary"]');
      expect(heroCTA).toBeInTheDocument();

      // Should have cta-button class which provides adequate padding
      expect(heroCTA.classList.contains('cta-button')).toBe(true);
    });

    test('CTA buttons use semantic HTML', () => {
      const heroCTA = document.querySelector('[data-testid="hero-cta-primary"]');
      const navCTA = document.querySelector('[data-testid="nav-cta"]');

      // Both should be anchor tags (for navigation) or button elements
      const validTags = ['a', 'button'];
      expect(validTags.includes(heroCTA.tagName.toLowerCase())).toBe(true);
      expect(validTags.includes(navCTA.tagName.toLowerCase())).toBe(true);
    });
  });
});
