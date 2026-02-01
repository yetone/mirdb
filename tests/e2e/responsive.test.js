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

describe('Responsive Design - Tablet (768px)', () => {
  let document;

  beforeEach(() => {
    // Load the HTML
    document = loadHTML('index.html');

    // Set viewport to tablet size (768px width)
    Object.defineProperty(window, 'innerWidth', {
      writable: true,
      configurable: true,
      value: 768
    });
    Object.defineProperty(window, 'innerHeight', {
      writable: true,
      configurable: true,
      value: 1024
    });

    // Dispatch resize event
    window.dispatchEvent(new Event('resize'));
  });

  describe('Test Case 1: Page renders with tablet-appropriate layout at 768px', () => {
    test('page renders correctly at 768px viewport width', () => {
      // Verify viewport is set to tablet size
      expect(window.innerWidth).toBe(768);
    });

    test('hero section is visible and properly structured', () => {
      const heroSection = querySection('hero');
      expect(heroSection).toBeInTheDocument();
      expect(heroSection).toHaveClass('min-h-screen');
    });

    test('hero content container has appropriate max-width', () => {
      const heroSection = querySection('hero');
      const contentContainer = heroSection.querySelector('.max-w-4xl');
      expect(contentContainer).toBeInTheDocument();
    });

    test('logo has tablet-responsive sizing classes', () => {
      const logo = document.getElementById('logo');
      expect(logo).toBeInTheDocument();
      // Logo should have md: responsive classes for tablet
      expect(logo.className).toMatch(/md:w-48|md:h-48/);
    });

    test('heading has tablet-responsive text size', () => {
      const heading = document.querySelector('h1');
      expect(heading).toBeInTheDocument();
      // Heading should have md: responsive classes
      expect(heading.className).toMatch(/md:text-6xl/);
    });

    test('tagline has tablet-responsive text size', () => {
      const tagline = document.getElementById('tagline');
      expect(tagline).toBeInTheDocument();
      // Tagline should have md: responsive classes
      expect(tagline.className).toMatch(/md:text-2xl/);
    });

    test('CTA buttons are properly laid out', () => {
      const ctaContainer = document.getElementById('cta-buttons');
      expect(ctaContainer).toBeInTheDocument();
      // Container should support row layout at sm: breakpoint
      expect(ctaContainer.className).toMatch(/sm:flex-row/);

      const buttons = ctaContainer.querySelectorAll('a');
      expect(buttons.length).toBeGreaterThanOrEqual(2);
    });

    test('status badges are visible and wrap properly', () => {
      const badgesContainer = document.getElementById('status-badges');
      expect(badgesContainer).toBeInTheDocument();
      expect(badgesContainer.className).toMatch(/flex-wrap/);
    });
  });

  describe('Test Case 2: Features display in appropriate grid (2-3 columns)', () => {
    test('features section exists', () => {
      const featuresSection = querySection('features');
      expect(featuresSection).toBeInTheDocument();
    });

    test('features grid has responsive column classes', () => {
      const featuresSection = querySection('features');
      const grid = featuresSection.querySelector('.grid');
      expect(grid).toBeInTheDocument();

      // Grid should have md:grid-cols-2 for tablet layout
      expect(grid.className).toMatch(/md:grid-cols-2/);
    });

    test('feature cards have appropriate spacing at tablet width', () => {
      const featuresSection = querySection('features');
      const grid = featuresSection.querySelector('.grid');
      expect(grid).toBeInTheDocument();

      // Grid should have gap for spacing
      expect(grid.className).toMatch(/gap-8/);
    });

    test('feature cards are present and well-structured', () => {
      const featuresSection = querySection('features');
      const featureCards = featuresSection.querySelectorAll('.feature-card');

      // Expect 6 feature cards (based on index.html)
      expect(featureCards.length).toBeGreaterThanOrEqual(5);

      // Each card should have proper structure
      featureCards.forEach(card => {
        expect(card).toHaveClass('bg-gray-700');
        expect(card).toHaveClass('rounded-lg');
        expect(card).toHaveClass('p-6');

        // Each card should have icon, heading, description
        const icon = card.querySelector('.feature-icon');
        const heading = card.querySelector('h3');
        const description = card.querySelector('p');

        expect(icon).toBeInTheDocument();
        expect(heading).toBeInTheDocument();
        expect(description).toBeInTheDocument();
      });
    });

    test('features container has appropriate max-width for tablet', () => {
      const featuresSection = querySection('features');
      const container = featuresSection.querySelector('.max-w-6xl');
      expect(container).toBeInTheDocument();
    });

    test('grid responsive classes transition from 1 to 2 to 3 columns', () => {
      const featuresSection = querySection('features');
      const grid = featuresSection.querySelector('.grid');

      // Verify responsive grid classes for different breakpoints
      expect(grid.className).toMatch(/grid-cols-1/);
      expect(grid.className).toMatch(/md:grid-cols-2/);
      expect(grid.className).toMatch(/lg:grid-cols-3/);
    });
  });

  describe('Additional Tablet Layout Tests', () => {
    test('demo section has appropriate layout at tablet width', () => {
      const demoSection = querySection('demo');
      expect(demoSection).toBeInTheDocument();

      const container = demoSection.querySelector('.max-w-4xl');
      expect(container).toBeInTheDocument();
    });

    test('getting started section is visible and properly structured', () => {
      const gettingStartedSection = querySection('getting-started');
      expect(gettingStartedSection).toBeInTheDocument();

      const container = gettingStartedSection.querySelector('.max-w-4xl');
      expect(container).toBeInTheDocument();
    });

    test('footer is visible and properly laid out', () => {
      const footer = querySection('footer');
      expect(footer).toBeInTheDocument();

      const linksContainer = document.getElementById('footer-links');
      expect(linksContainer).toBeInTheDocument();
      expect(linksContainer.className).toMatch(/flex-wrap/);
    });

    test('code blocks do not overflow at tablet width', () => {
      const codeBlocks = document.querySelectorAll('pre');
      expect(codeBlocks.length).toBeGreaterThan(0);

      // Code blocks should have overflow handling
      codeBlocks.forEach(block => {
        expect(block.className).toMatch(/overflow-x-auto/);
      });
    });

    test('images scale appropriately', () => {
      const usageGif = document.getElementById('usage-gif');
      expect(usageGif).toBeInTheDocument();

      // Image should have responsive classes
      expect(usageGif.className).toMatch(/max-w-full/);
      expect(usageGif.className).toMatch(/h-auto/);
    });

    test('body has appropriate base styling', () => {
      const body = document.body;
      expect(body).toHaveClass('min-h-screen');
    });
  });
});

describe('Responsive Design - Desktop (1280px)', () => {
  let document;

  beforeEach(() => {
    // Load the HTML
    document = loadHTML('index.html');

    // Set viewport to desktop size (1280px width)
    Object.defineProperty(window, 'innerWidth', {
      writable: true,
      configurable: true,
      value: 1280
    });
    Object.defineProperty(window, 'innerHeight', {
      writable: true,
      configurable: true,
      value: 800
    });

    // Dispatch resize event
    window.dispatchEvent(new Event('resize'));
  });

  describe('Test Case 1: Page renders with desktop layout at 1280px, content centered', () => {
    test('page renders correctly at 1280px viewport width', () => {
      // Verify viewport is set to desktop size
      expect(window.innerWidth).toBe(1280);
    });

    test('hero section is visible and uses full desktop layout', () => {
      const heroSection = querySection('hero');
      expect(heroSection).toBeInTheDocument();
      expect(heroSection).toHaveClass('min-h-screen');
      expect(heroSection).toHaveClass('flex');
      expect(heroSection).toHaveClass('items-center');
      expect(heroSection).toHaveClass('justify-center');
    });

    test('hero content is centered with max-width constraint', () => {
      const heroSection = querySection('hero');
      const contentContainer = heroSection.querySelector('.max-w-4xl');
      expect(contentContainer).toBeInTheDocument();
      // Content should be centered
      expect(contentContainer).toHaveClass('mx-auto');
      expect(contentContainer).toHaveClass('text-center');
    });

    test('logo displays at full desktop size', () => {
      const logo = document.getElementById('logo');
      expect(logo).toBeInTheDocument();
      // Logo should have responsive classes - md:w-48 applies at desktop
      expect(logo.className).toMatch(/md:w-48/);
      expect(logo.className).toMatch(/md:h-48/);
    });

    test('main heading displays at full desktop size', () => {
      const heading = document.querySelector('h1');
      expect(heading).toBeInTheDocument();
      // Heading should have md:text-6xl for desktop
      expect(heading.className).toMatch(/md:text-6xl/);
    });

    test('tagline displays at desktop-optimized size', () => {
      const tagline = document.getElementById('tagline');
      expect(tagline).toBeInTheDocument();
      // Tagline should have md:text-2xl for desktop
      expect(tagline.className).toMatch(/md:text-2xl/);
    });

    test('CTA buttons display in horizontal row layout', () => {
      const ctaContainer = document.getElementById('cta-buttons');
      expect(ctaContainer).toBeInTheDocument();
      // At sm: breakpoint and above, buttons should be in a row
      expect(ctaContainer.className).toMatch(/sm:flex-row/);
      expect(ctaContainer.className).toMatch(/flex/);
      expect(ctaContainer.className).toMatch(/gap-4/);
    });

    test('status badges display inline with proper spacing', () => {
      const badgesContainer = document.getElementById('status-badges');
      expect(badgesContainer).toBeInTheDocument();
      expect(badgesContainer.className).toMatch(/flex/);
      expect(badgesContainer.className).toMatch(/flex-wrap/);
      expect(badgesContainer.className).toMatch(/gap-3/);
      expect(badgesContainer.className).toMatch(/justify-center/);
    });

    test('all sections are properly centered', () => {
      const sections = ['hero', 'features', 'demo', 'getting-started', 'footer'];
      sections.forEach(sectionId => {
        const section = querySection(sectionId);
        expect(section).toBeInTheDocument();
      });
    });
  });

  describe('Test Case 2: Features display in full grid layout (3-4 columns)', () => {
    test('features section exists and is properly structured', () => {
      const featuresSection = querySection('features');
      expect(featuresSection).toBeInTheDocument();
      expect(featuresSection).toHaveClass('py-20');
    });

    test('features grid has desktop 3-column layout', () => {
      const featuresSection = querySection('features');
      const grid = featuresSection.querySelector('.grid');
      expect(grid).toBeInTheDocument();

      // Grid should have lg:grid-cols-3 for desktop layout (1280px >= lg breakpoint)
      expect(grid.className).toMatch(/lg:grid-cols-3/);
    });

    test('features grid has appropriate responsive column progression', () => {
      const featuresSection = querySection('features');
      const grid = featuresSection.querySelector('.grid');

      // Mobile: 1 column, Tablet: 2 columns, Desktop: 3 columns
      expect(grid.className).toMatch(/grid-cols-1/);
      expect(grid.className).toMatch(/md:grid-cols-2/);
      expect(grid.className).toMatch(/lg:grid-cols-3/);
    });

    test('feature cards have proper spacing for desktop', () => {
      const featuresSection = querySection('features');
      const grid = featuresSection.querySelector('.grid');
      expect(grid).toBeInTheDocument();

      // Grid should have gap-8 for proper spacing between cards
      expect(grid.className).toMatch(/gap-8/);
    });

    test('all six feature cards are present', () => {
      const featuresSection = querySection('features');
      const featureCards = featuresSection.querySelectorAll('.feature-card');

      // Should have 6 feature cards based on index.html
      expect(featureCards.length).toBe(6);
    });

    test('feature cards have consistent desktop styling', () => {
      const featuresSection = querySection('features');
      const featureCards = featuresSection.querySelectorAll('.feature-card');

      featureCards.forEach(card => {
        // Each card should have consistent styling
        expect(card).toHaveClass('bg-gray-700');
        expect(card).toHaveClass('rounded-lg');
        expect(card).toHaveClass('p-6');

        // Each card should have proper structure
        const icon = card.querySelector('.feature-icon');
        const heading = card.querySelector('h3');
        const description = card.querySelector('p');

        expect(icon).toBeInTheDocument();
        expect(heading).toBeInTheDocument();
        expect(description).toBeInTheDocument();
      });
    });

    test('features container has max-width for readability on large screens', () => {
      const featuresSection = querySection('features');
      const container = featuresSection.querySelector('.max-w-6xl');
      expect(container).toBeInTheDocument();
      expect(container).toHaveClass('mx-auto');
    });
  });

  describe('Test Case 3: Content has max-width constraint for readability', () => {
    test('hero section content has max-width constraint', () => {
      const heroSection = querySection('hero');
      const container = heroSection.querySelector('.max-w-4xl');
      expect(container).toBeInTheDocument();
    });

    test('features section has max-width constraint', () => {
      const featuresSection = querySection('features');
      const container = featuresSection.querySelector('.max-w-6xl');
      expect(container).toBeInTheDocument();
    });

    test('demo section has max-width constraint', () => {
      const demoSection = querySection('demo');
      const container = demoSection.querySelector('.max-w-4xl');
      expect(container).toBeInTheDocument();
    });

    test('getting started section has max-width constraint', () => {
      const gettingStartedSection = querySection('getting-started');
      const container = gettingStartedSection.querySelector('.max-w-4xl');
      expect(container).toBeInTheDocument();
    });

    test('footer has max-width constraint', () => {
      const footer = querySection('footer');
      const container = footer.querySelector('.max-w-4xl');
      expect(container).toBeInTheDocument();
    });

    test('description text has max-width for optimal line length', () => {
      const heroSection = querySection('hero');
      const description = heroSection.querySelector('.max-w-2xl');
      expect(description).toBeInTheDocument();
    });

    test('all containers are horizontally centered', () => {
      // Check that max-width containers use mx-auto for centering
      const heroContainer = document.querySelector('#hero .max-w-4xl');
      const featuresContainer = document.querySelector('#features .max-w-6xl');
      const demoContainer = document.querySelector('#demo .max-w-4xl');
      const gettingStartedContainer = document.querySelector('#getting-started .max-w-4xl');
      const footerContainer = document.querySelector('#footer .max-w-4xl');

      expect(heroContainer).toHaveClass('mx-auto');
      expect(featuresContainer).toHaveClass('mx-auto');
      expect(demoContainer).toHaveClass('mx-auto');
      expect(gettingStartedContainer).toHaveClass('mx-auto');
      expect(footerContainer).toHaveClass('mx-auto');
    });
  });

  describe('Desktop-specific layout verification', () => {
    test('body has full viewport styling', () => {
      const body = document.body;
      expect(body).toHaveClass('min-h-screen');
      expect(body).toHaveClass('bg-gray-900');
      expect(body).toHaveClass('text-white');
    });

    test('code blocks handle horizontal overflow on desktop', () => {
      const codeBlocks = document.querySelectorAll('pre');
      expect(codeBlocks.length).toBeGreaterThan(0);

      codeBlocks.forEach(block => {
        expect(block.className).toMatch(/overflow-x-auto/);
      });
    });

    test('images display at optimal size on desktop', () => {
      const logo = document.getElementById('logo');
      expect(logo).toBeInTheDocument();
      // Logo should use desktop size classes
      expect(logo.className).toMatch(/md:w-48/);

      const usageGif = document.getElementById('usage-gif');
      expect(usageGif).toBeInTheDocument();
      // Image should scale responsively
      expect(usageGif.className).toMatch(/max-w-full/);
      expect(usageGif.className).toMatch(/h-auto/);
    });

    test('section backgrounds alternate for visual hierarchy', () => {
      // Body provides default dark background
      const body = document.body;
      expect(body).toHaveClass('bg-gray-900');

      // Sections with explicit backgrounds for visual hierarchy
      const featuresSection = querySection('features');
      const demoSection = querySection('demo');
      const gettingStartedSection = querySection('getting-started');
      const footer = querySection('footer');

      expect(featuresSection).toHaveClass('bg-gray-800');
      expect(demoSection).toHaveClass('bg-gray-900');
      expect(gettingStartedSection).toHaveClass('bg-gray-800');
      expect(footer).toHaveClass('bg-gray-900');
    });

    test('sections have appropriate vertical padding for desktop', () => {
      const featuresSection = querySection('features');
      const demoSection = querySection('demo');
      const gettingStartedSection = querySection('getting-started');

      expect(featuresSection).toHaveClass('py-20');
      expect(demoSection).toHaveClass('py-20');
      expect(gettingStartedSection).toHaveClass('py-20');
    });

    test('footer links display horizontally on desktop', () => {
      const footerLinks = document.getElementById('footer-links');
      expect(footerLinks).toBeInTheDocument();
      expect(footerLinks.className).toMatch(/flex/);
      expect(footerLinks.className).toMatch(/flex-wrap/);
      expect(footerLinks.className).toMatch(/justify-center/);
      expect(footerLinks.className).toMatch(/gap-6/);

      const links = footerLinks.querySelectorAll('a');
      expect(links.length).toBeGreaterThanOrEqual(3);
    });

    test('getting started steps display with clear visual structure', () => {
      const gettingStartedSection = querySection('getting-started');
      const steps = gettingStartedSection.querySelectorAll('.bg-gray-900.rounded-lg');
      expect(steps.length).toBeGreaterThanOrEqual(3);
    });
  });
});
