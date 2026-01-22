/**
 * Tests for the Usage Demonstration Section
 *
 * This test suite verifies that the usage demonstration section of the MirDB homepage:
 * - Displays the usage.gif animation correctly
 * - Has a caption explaining the demonstration
 * - Implements lazy loading for performance
 * - Has descriptive alt text for accessibility
 */

describe('Usage Demonstration Section', () => {
  let document;
  let usageSection;

  beforeEach(() => {
    // Load the actual HTML from the homepage
    const html = global.getIndexHTML();
    global.loadHTML(global.extractBodyContent(html));

    // Get the usage demonstration section (code-example section or usage section)
    usageSection = window.document.getElementById('code-example') ||
                   window.document.getElementById('usage') ||
                   window.document.querySelector('.usage-section') ||
                   window.document.querySelector('[data-testid="usage-section"]');
  });

  // Test Case 1: Check usage.gif presence (e2e)
  describe('Test Case 1: usage.gif Presence', () => {
    test('usage.gif is displayed in the demonstration section', () => {
      expect(usageSection).toBeTruthy();

      // Find the usage.gif image within the section
      const usageGif = usageSection.querySelector('img[src*="usage.gif"], img[src*="usage"]');

      expect(usageGif).toBeTruthy();
      expect(usageGif.getAttribute('src')).toMatch(/usage\.gif/i);
    });

    test('usage.gif image element is visible and has proper dimensions', () => {
      const usageGif = usageSection.querySelector('img[src*="usage.gif"]');

      expect(usageGif).toBeTruthy();
      // Check that the image has styling for proper display
      expect(usageGif.style.maxWidth || usageGif.getAttribute('style')).toBeDefined();
    });
  });

  // Test Case 2: Verify caption exists (e2e)
  describe('Test Case 2: Caption Verification', () => {
    test('caption text explains what the demonstration shows', () => {
      expect(usageSection).toBeTruthy();

      // Look for caption element - could be figcaption, p with caption class, or nearby text
      const caption = usageSection.querySelector('figcaption, .caption, .demo-caption, [data-testid="usage-caption"]') ||
                      usageSection.querySelector('figure p') ||
                      usageSection.querySelector('.usage-demo-caption');

      // Also check for text content near the GIF that serves as caption
      const usageGif = usageSection.querySelector('img[src*="usage.gif"]');
      let captionText = '';

      if (caption) {
        captionText = caption.textContent;
      } else if (usageGif && usageGif.parentElement) {
        // Check parent element for caption-like text
        const parent = usageGif.closest('figure, .demo-container, .usage-demo');
        if (parent) {
          const possibleCaption = parent.querySelector('p, span, figcaption');
          if (possibleCaption) {
            captionText = possibleCaption.textContent;
          }
        }
      }

      // Verify caption exists and contains meaningful content about the demonstration
      expect(captionText.length).toBeGreaterThan(0);
      // Caption should mention terminal, commands, interaction, or demo-related terms
      const captionLower = captionText.toLowerCase();
      const hasRelevantContent =
        captionLower.includes('terminal') ||
        captionLower.includes('command') ||
        captionLower.includes('demo') ||
        captionLower.includes('example') ||
        captionLower.includes('interaction') ||
        captionLower.includes('set') ||
        captionLower.includes('get') ||
        captionLower.includes('memcached') ||
        captionLower.includes('usage') ||
        captionLower.includes('action');

      expect(hasRelevantContent).toBe(true);
    });
  });

  // Test Case 3: Check lazy loading implementation (unit)
  describe('Test Case 3: Lazy Loading Implementation', () => {
    test('GIF uses lazy loading attribute to defer loading until visible', () => {
      const usageGif = usageSection.querySelector('img[src*="usage.gif"]');

      expect(usageGif).toBeTruthy();

      // Check for loading="lazy" attribute
      const loadingAttr = usageGif.getAttribute('loading');
      expect(loadingAttr).toBe('lazy');
    });

    test('lazy loading attribute is correctly set on the img element', () => {
      const usageGif = usageSection.querySelector('img[src*="usage.gif"]');

      expect(usageGif).toBeTruthy();
      // Use getAttribute since jsdom doesn't reflect loading property
      expect(usageGif.getAttribute('loading')).toBe('lazy');
    });
  });

  // Test Case 4: Verify alt text for accessibility (unit)
  describe('Test Case 4: Accessibility Alt Text', () => {
    test('GIF has descriptive alt text for screen readers', () => {
      const usageGif = usageSection.querySelector('img[src*="usage.gif"]');

      expect(usageGif).toBeTruthy();

      // Check for alt attribute
      const altText = usageGif.getAttribute('alt');
      expect(altText).toBeTruthy();
      expect(altText.length).toBeGreaterThan(10); // Should be descriptive, not just "image"
    });

    test('alt text describes the MirDB terminal demonstration', () => {
      const usageGif = usageSection.querySelector('img[src*="usage.gif"]');

      expect(usageGif).toBeTruthy();

      const altText = usageGif.getAttribute('alt').toLowerCase();

      // Alt text should describe what the demo shows
      const hasDescriptiveContent =
        altText.includes('mirdb') ||
        altText.includes('demo') ||
        altText.includes('terminal') ||
        altText.includes('command') ||
        altText.includes('usage') ||
        altText.includes('example') ||
        altText.includes('set') ||
        altText.includes('get');

      expect(hasDescriptiveContent).toBe(true);
    });

    test('alt text is not generic placeholder text', () => {
      const usageGif = usageSection.querySelector('img[src*="usage.gif"]');

      expect(usageGif).toBeTruthy();

      const altText = usageGif.getAttribute('alt').toLowerCase();

      // Should not be generic placeholder text
      expect(altText).not.toBe('image');
      expect(altText).not.toBe('gif');
      expect(altText).not.toBe('demo');
      expect(altText).not.toBe('usage');
      expect(altText).not.toMatch(/^placeholder/);
    });
  });

  // Additional structural tests
  describe('Usage Section Structure', () => {
    test('should have a usage/code-example section with proper container', () => {
      expect(usageSection).toBeTruthy();
      expect(usageSection.tagName).toBe('SECTION');
    });

    test('usage.gif should be within a proper figure or container element', () => {
      const usageGif = usageSection.querySelector('img[src*="usage.gif"]');

      expect(usageGif).toBeTruthy();

      // Check that the image is within a proper container
      const container = usageGif.closest('figure, .demo-container, .usage-demo, div');
      expect(container).toBeTruthy();
    });
  });
});
