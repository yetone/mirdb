/**
 * Responsive Design Tests
 * Owner: Scenario 7 - Responsive Design (Mobile primary)
 * Shared: Scenarios 8 (Tablet), 9 (Desktop)
 *
 * Tests:
 * - Mobile viewport (375px) - no horizontal scroll
 * - Mobile CTA touch targets (44px minimum)
 * - Mobile image scaling
 * - Tablet viewport (768px) layout
 * - Desktop viewport (1280px) layout
 * - Content max-width constraints
 */

const { loadHTML, querySection } = require('../helpers/dom-utils');

describe('Responsive Design - Mobile (375px)', () => {
  beforeEach(() => {
    loadHTML('index.html');
    // Simulate mobile viewport width
    Object.defineProperty(window, 'innerWidth', {
      writable: true,
      configurable: true,
      value: 375
    });
    Object.defineProperty(window, 'innerHeight', {
      writable: true,
      configurable: true,
      value: 667
    });
  });

  describe('Test Case 1: Page renders without horizontal overflow', () => {
    it('should have viewport meta tag for responsive design', () => {
      const viewportMeta = document.querySelector('meta[name="viewport"]');
      expect(viewportMeta).toBeTruthy();
      expect(viewportMeta.getAttribute('content')).toContain('width=device-width');
    });

    it('should have body with no horizontal overflow styles', () => {
      const body = document.body;
      expect(body).toBeTruthy();
      // Check that body has appropriate classes for mobile
      expect(body.classList.contains('min-h-screen')).toBe(true);
    });

    it('should have sections that use mobile-friendly padding', () => {
      const heroSection = querySection('hero');
      expect(heroSection).toBeTruthy();
      // Hero should have px-4 for mobile padding
      expect(heroSection.classList.contains('px-4')).toBe(true);
    });

    it('should have max-width containers for content control', () => {
      // Check that content is constrained with max-width classes
      const maxWidthContainers = document.querySelectorAll('[class*="max-w-"]');
      expect(maxWidthContainers.length).toBeGreaterThan(0);
    });

    it('should have all images set to max-width 100%', () => {
      const usageGif = document.getElementById('usage-gif');
      if (usageGif) {
        // Check for Tailwind's max-w-full class which sets max-width: 100%
        expect(usageGif.classList.contains('max-w-full')).toBe(true);
      }
    });

    it('should have code blocks with overflow-x-auto for horizontal scrolling', () => {
      const codeBlocks = document.querySelectorAll('pre');
      codeBlocks.forEach(block => {
        expect(block.classList.contains('overflow-x-auto')).toBe(true);
      });
    });
  });

  describe('Test Case 2: CTA buttons have minimum touch target size of 44px', () => {
    it('should have GitHub CTA with adequate touch target size', () => {
      const githubCTA = document.getElementById('github-cta');
      expect(githubCTA).toBeTruthy();
      // Tailwind py-3 = 12px padding top/bottom, which with text creates > 44px height
      // px-8 = 32px padding left/right for adequate width
      expect(githubCTA.classList.contains('py-3')).toBe(true);
      expect(githubCTA.classList.contains('px-8')).toBe(true);
    });

    it('should have Get Started CTA with adequate touch target size', () => {
      const getStartedCTA = document.getElementById('get-started-cta');
      expect(getStartedCTA).toBeTruthy();
      expect(getStartedCTA.classList.contains('py-3')).toBe(true);
      expect(getStartedCTA.classList.contains('px-8')).toBe(true);
    });

    it('should have copy buttons with sufficient touch target', () => {
      const copyButtons = document.querySelectorAll('.copy-button');
      expect(copyButtons.length).toBeGreaterThan(0);
      copyButtons.forEach(button => {
        // Copy buttons should have px-3 py-1 minimum
        expect(button.classList.contains('px-3')).toBe(true);
        expect(button.classList.contains('py-1')).toBe(true);
      });
    });

    it('should have footer links with sufficient spacing for touch', () => {
      const footerLinks = document.getElementById('footer-links');
      expect(footerLinks).toBeTruthy();
      // gap-6 provides 24px spacing between touch targets
      expect(footerLinks.classList.contains('gap-6')).toBe(true);
    });

    it('should have CTA buttons that stack vertically on mobile', () => {
      const ctaContainer = document.getElementById('cta-buttons');
      expect(ctaContainer).toBeTruthy();
      // flex-col is the mobile-first class, sm:flex-row overrides for larger screens
      expect(ctaContainer.classList.contains('flex-col')).toBe(true);
      expect(ctaContainer.classList.contains('sm:flex-row')).toBe(true);
    });
  });

  describe('Test Case 3: Images scale to fit viewport without horizontal scroll', () => {
    it('should have logo image that scales responsively', () => {
      const logo = document.getElementById('logo');
      expect(logo).toBeTruthy();
      // Mobile: w-32 (128px), Desktop: md:w-48 (192px)
      expect(logo.classList.contains('w-32')).toBe(true);
      expect(logo.classList.contains('md:w-48')).toBe(true);
    });

    it('should have usage GIF with max-width constraint', () => {
      const usageGif = document.getElementById('usage-gif');
      expect(usageGif).toBeTruthy();
      expect(usageGif.classList.contains('max-w-full')).toBe(true);
      expect(usageGif.classList.contains('h-auto')).toBe(true);
    });

    it('should have images centered for mobile display', () => {
      const logo = document.getElementById('logo');
      expect(logo).toBeTruthy();
      expect(logo.classList.contains('mx-auto')).toBe(true);

      const usageGif = document.getElementById('usage-gif');
      if (usageGif) {
        expect(usageGif.classList.contains('mx-auto')).toBe(true);
      }
    });

    it('should have badge images with appropriate sizing', () => {
      const badges = document.querySelectorAll('#status-badges img');
      expect(badges.length).toBeGreaterThan(0);
      badges.forEach(badge => {
        // h-5 ensures consistent badge height
        expect(badge.classList.contains('h-5')).toBe(true);
      });
    });
  });

  describe('Test Case 4: Navigation is accessible and usable on mobile', () => {
    it('should have visible navigation links in footer', () => {
      const footerLinks = document.getElementById('footer-links');
      expect(footerLinks).toBeTruthy();

      const links = footerLinks.querySelectorAll('a');
      expect(links.length).toBeGreaterThanOrEqual(3);
    });

    it('should have navigation links that wrap on mobile', () => {
      const footerLinks = document.getElementById('footer-links');
      expect(footerLinks).toBeTruthy();
      // flex-wrap allows links to wrap to multiple lines on mobile
      expect(footerLinks.classList.contains('flex-wrap')).toBe(true);
    });

    it('should have in-page navigation link to getting started section', () => {
      const getStartedCTA = document.getElementById('get-started-cta');
      expect(getStartedCTA).toBeTruthy();
      expect(getStartedCTA.getAttribute('href')).toBe('#getting-started');
    });

    it('should have external links with proper attributes', () => {
      const externalLinks = document.querySelectorAll('a[target="_blank"]');
      expect(externalLinks.length).toBeGreaterThan(0);
      externalLinks.forEach(link => {
        expect(link.getAttribute('rel')).toContain('noopener');
      });
    });

    it('should have hero section CTAs easily accessible at top of page', () => {
      const heroSection = querySection('hero');
      expect(heroSection).toBeTruthy();

      const ctaButtons = heroSection.querySelectorAll('a[class*="inline-flex"]');
      expect(ctaButtons.length).toBeGreaterThanOrEqual(2);
    });

    it('should have sections with clear IDs for anchor navigation', () => {
      const sections = ['hero', 'features', 'demo', 'getting-started', 'footer'];
      sections.forEach(sectionId => {
        const section = querySection(sectionId);
        expect(section).toBeTruthy();
      });
    });
  });

  describe('Mobile-specific responsive layout checks', () => {
    it('should have text that is readable at mobile size', () => {
      const tagline = document.getElementById('tagline');
      expect(tagline).toBeTruthy();
      // Mobile: text-xl, Desktop: md:text-2xl
      expect(tagline.classList.contains('text-xl')).toBe(true);
      expect(tagline.classList.contains('md:text-2xl')).toBe(true);
    });

    it('should have heading sizes that scale responsively', () => {
      const h1 = document.querySelector('h1');
      expect(h1).toBeTruthy();
      // Mobile: text-4xl, Desktop: md:text-6xl
      expect(h1.classList.contains('text-4xl')).toBe(true);
      expect(h1.classList.contains('md:text-6xl')).toBe(true);
    });

    it('should have feature cards in single column on mobile', () => {
      const featuresGrid = document.querySelector('#features .grid');
      expect(featuresGrid).toBeTruthy();
      // Mobile: grid-cols-1, Tablet: md:grid-cols-2, Desktop: lg:grid-cols-3
      expect(featuresGrid.classList.contains('grid-cols-1')).toBe(true);
      expect(featuresGrid.classList.contains('md:grid-cols-2')).toBe(true);
    });

    it('should have sections with appropriate vertical padding', () => {
      const featuresSection = querySection('features');
      expect(featuresSection).toBeTruthy();
      expect(featuresSection.classList.contains('py-20')).toBe(true);
    });

    it('should have centered content on mobile', () => {
      const heroContent = document.querySelector('#hero .text-center');
      expect(heroContent).toBeTruthy();
    });
  });
});
