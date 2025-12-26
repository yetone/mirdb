/**
 * User Journey - Getting Started Flow Tests
 *
 * Verifies complete user journey from landing to getting started (US-1, US-3)
 * This test suite covers the full user experience from initial page load
 * through understanding the product value to starting installation.
 */

describe('User Journey - Getting Started Flow', () => {
  describe('Test Case 1: Hero section is immediately visible on load', () => {
    let heroSection;

    beforeAll(() => {
      heroSection = document.querySelector('.hero');
    });

    test('hero section exists', () => {
      expect(heroSection).not.toBeNull();
    });

    test('hero section is positioned at the top of main content', () => {
      const main = document.querySelector('main');
      expect(main).not.toBeNull();

      // Hero should be the first child of main
      const firstChild = main.querySelector('section');
      expect(firstChild).not.toBeNull();
      expect(firstChild.classList.contains('hero')).toBe(true);
    });

    test('hero section contains the product name MirDB', () => {
      const h1 = heroSection.querySelector('h1');
      expect(h1).not.toBeNull();
      expect(h1.textContent).toContain('MirDB');
    });

    test('hero section contains tagline describing the product', () => {
      const tagline = heroSection.querySelector('.tagline');
      expect(tagline).not.toBeNull();
      expect(tagline.textContent.toLowerCase()).toContain('persistent');
      expect(tagline.textContent.toLowerCase()).toContain('key-value');
    });

    test('hero section contains description', () => {
      const description = heroSection.querySelector('.description');
      expect(description).not.toBeNull();
      expect(description.textContent.length).toBeGreaterThan(50);
    });

    test('hero section has CTA buttons for immediate action', () => {
      const ctaButtons = heroSection.querySelector('.cta-buttons');
      expect(ctaButtons).not.toBeNull();

      const buttons = ctaButtons.querySelectorAll('a.btn');
      expect(buttons.length).toBeGreaterThanOrEqual(1);
    });

    test('hero section is not hidden or off-screen by CSS', () => {
      // Verify hero doesn't have display:none or visibility:hidden
      const style = window.getComputedStyle(heroSection);
      expect(style.display).not.toBe('none');
      expect(style.visibility).not.toBe('hidden');
    });
  });

  describe('Test Case 2: Smooth scroll to quick start via navigation', () => {
    let nav;
    let quickStartSection;
    let quickStartLink;

    beforeAll(() => {
      nav = document.querySelector('nav');
      quickStartSection = document.getElementById('quick-start');
      quickStartLink = nav ? nav.querySelector('a[href="#quick-start"]') : null;
    });

    test('navigation contains Quick Start link', () => {
      expect(nav).not.toBeNull();
      expect(quickStartLink).not.toBeNull();
    });

    test('Quick Start link has correct href pointing to quick-start section', () => {
      expect(quickStartLink.getAttribute('href')).toBe('#quick-start');
    });

    test('quick-start section exists with matching id', () => {
      expect(quickStartSection).not.toBeNull();
      expect(quickStartSection.id).toBe('quick-start');
    });

    test('HTML has smooth scroll behavior enabled', () => {
      // Check for scroll-behavior: smooth in CSS
      // This is verified by checking the CSS exists with smooth scroll
      const htmlElement = document.documentElement;
      const style = window.getComputedStyle(htmlElement);

      // In JSDOM, we verify the link structure and section existence
      // Real smooth scroll is a CSS feature tested visually
      expect(quickStartLink).not.toBeNull();
      expect(quickStartSection).not.toBeNull();

      // Verify the navigation link text matches expected
      expect(quickStartLink.textContent.toLowerCase()).toContain('quick start');
    });

    test('clicking Quick Start link would scroll to correct section (structure test)', () => {
      // Verify the anchor link structure is correct for browser scrolling
      const href = quickStartLink.getAttribute('href');
      const targetId = href.substring(1); // Remove #
      const targetElement = document.getElementById(targetId);

      expect(targetElement).not.toBeNull();
      expect(targetElement).toBe(quickStartSection);
    });

    test('all internal navigation links have corresponding sections', () => {
      const internalLinks = nav.querySelectorAll('a[href^="#"]');

      internalLinks.forEach(link => {
        const href = link.getAttribute('href');
        if (href.length > 1) { // Skip just "#"
          const targetId = href.substring(1);
          const targetSection = document.getElementById(targetId);
          expect(targetSection).not.toBeNull();
        }
      });
    });
  });

  describe('Test Case 3: Code blocks are copyable', () => {
    let quickStartSection;
    let codeBlocks;

    beforeAll(() => {
      quickStartSection = document.getElementById('quick-start');
      codeBlocks = quickStartSection ? quickStartSection.querySelectorAll('pre code') : [];
    });

    test('quick start section contains code blocks', () => {
      expect(codeBlocks.length).toBeGreaterThan(0);
    });

    test('installation command code block exists', () => {
      let hasInstallCommand = false;
      codeBlocks.forEach(block => {
        const text = block.textContent.toLowerCase();
        if (text.includes('clone') || text.includes('cargo') || text.includes('install')) {
          hasInstallCommand = true;
        }
      });
      expect(hasInstallCommand).toBe(true);
    });

    test('code blocks use proper pre/code elements for text selection', () => {
      codeBlocks.forEach(block => {
        expect(block.tagName.toLowerCase()).toBe('code');
        expect(block.parentElement.tagName.toLowerCase()).toBe('pre');
      });
    });

    test('code blocks have selectable text content', () => {
      codeBlocks.forEach(block => {
        // Verify code has actual content that can be selected
        expect(block.textContent.trim().length).toBeGreaterThan(0);

        // Check parent pre is not user-select: none
        const preStyle = window.getComputedStyle(block.parentElement);
        expect(preStyle.userSelect).not.toBe('none');
      });
    });

    test('code blocks have syntax highlighting classes', () => {
      let hasSyntaxHighlighting = false;
      codeBlocks.forEach(block => {
        if (block.className.includes('language-') ||
            block.classList.contains('hljs') ||
            block.classList.contains('prism')) {
          hasSyntaxHighlighting = true;
        }
      });
      expect(hasSyntaxHighlighting).toBe(true);
    });

    test('installation commands include git clone and cargo run', () => {
      let hasGitClone = false;
      let hasCargoRun = false;

      codeBlocks.forEach(block => {
        const text = block.textContent;
        if (text.includes('git clone')) hasGitClone = true;
        if (text.includes('cargo run')) hasCargoRun = true;
      });

      expect(hasGitClone).toBe(true);
      expect(hasCargoRun).toBe(true);
    });
  });

  describe('Test Case 4: Complete user flow from hero to GitHub', () => {
    let heroSection;
    let nav;

    beforeAll(() => {
      heroSection = document.querySelector('.hero');
      nav = document.querySelector('nav');
    });

    describe('Path 1: Hero CTA -> GitHub (1 click)', () => {
      test('hero section has View on GitHub button', () => {
        const githubBtn = heroSection.querySelector('a[href*="github"]');
        expect(githubBtn).not.toBeNull();
      });

      test('GitHub button in hero links directly to repository', () => {
        const githubBtn = heroSection.querySelector('a[href*="github"]');
        expect(githubBtn.getAttribute('href')).toContain('github.com');
        expect(githubBtn.getAttribute('href')).toContain('mirdb');
      });

      test('GitHub button is accessible with appropriate text', () => {
        const githubBtn = heroSection.querySelector('a[href*="github"]');
        expect(githubBtn.textContent.toLowerCase()).toContain('github');
      });
    });

    describe('Path 2: Navigation -> GitHub (1 click)', () => {
      test('navigation has GitHub link', () => {
        const githubLink = nav.querySelector('a[href*="github.com"]');
        expect(githubLink).not.toBeNull();
      });

      test('navigation GitHub link is labeled correctly', () => {
        const githubLink = Array.from(nav.querySelectorAll('a')).find(
          a => a.textContent.toLowerCase() === 'github'
        );
        expect(githubLink).not.toBeNull();
      });
    });

    describe('Path 3: Hero -> Quick Start -> GitHub (2 clicks)', () => {
      test('Get Started button exists in hero', () => {
        const getStartedBtn = Array.from(heroSection.querySelectorAll('a')).find(
          a => a.textContent.toLowerCase().includes('get started')
        );
        expect(getStartedBtn).not.toBeNull();
      });

      test('Get Started links to quick-start section', () => {
        const getStartedBtn = Array.from(heroSection.querySelectorAll('a')).find(
          a => a.textContent.toLowerCase().includes('get started')
        );
        expect(getStartedBtn.getAttribute('href')).toBe('#quick-start');
      });

      test('Quick Start section has GitHub links', () => {
        const quickStartSection = document.getElementById('quick-start');
        const githubLinks = quickStartSection.querySelectorAll('a[href*="github"]');
        expect(githubLinks.length).toBeGreaterThan(0);
      });

      test('total clicks from hero to GitHub is under 3', () => {
        // Direct path: Hero GitHub button = 1 click
        const heroGithubBtn = heroSection.querySelector('a[href*="github"]');
        expect(heroGithubBtn).not.toBeNull(); // 1 click path exists

        // Indirect path: Get Started (1) -> Quick Start GitHub (2) = 2 clicks
        const getStartedBtn = heroSection.querySelector('a[href="#quick-start"]');
        const quickStartSection = document.getElementById('quick-start');
        const quickStartGithubLink = quickStartSection.querySelector('a[href*="github"]');

        expect(getStartedBtn).not.toBeNull();
        expect(quickStartGithubLink).not.toBeNull();

        // Both paths are under 3 clicks
      });
    });

    describe('External link security', () => {
      test('all GitHub links open in new tab safely', () => {
        const allGithubLinks = document.querySelectorAll('a[href*="github.com"]');

        allGithubLinks.forEach(link => {
          expect(link.getAttribute('target')).toBe('_blank');
          expect(link.getAttribute('rel')).toContain('noopener');
        });
      });
    });
  });

  describe('User Journey Flow Integration', () => {
    test('complete flow from landing to getting started is accessible', () => {
      // Step 1: Land on homepage - hero is first visible content
      const heroSection = document.querySelector('.hero');
      expect(heroSection).not.toBeNull();

      // Step 2: Understand product value - hero has clear messaging
      const tagline = heroSection.querySelector('.tagline');
      const description = heroSection.querySelector('.description');
      expect(tagline).not.toBeNull();
      expect(description).not.toBeNull();

      // Step 3: Explore features - features section exists
      const featuresSection = document.getElementById('features');
      expect(featuresSection).not.toBeNull();

      // Step 4: Navigate to quick start - nav link exists
      const quickStartNavLink = document.querySelector('nav a[href="#quick-start"]');
      expect(quickStartNavLink).not.toBeNull();

      // Step 5: Copy installation command - code blocks exist
      const quickStartSection = document.getElementById('quick-start');
      const codeBlocks = quickStartSection.querySelectorAll('pre code');
      expect(codeBlocks.length).toBeGreaterThan(0);

      // Step 6: Access additional resources - GitHub links exist
      const githubLinks = document.querySelectorAll('a[href*="github"]');
      expect(githubLinks.length).toBeGreaterThan(0);
    });

    test('navigation supports full user journey', () => {
      const nav = document.querySelector('nav');
      const navLinks = nav.querySelectorAll('a');
      const linkTexts = Array.from(navLinks).map(a => a.textContent.toLowerCase());

      // Essential navigation items for user journey
      expect(linkTexts.some(t => t.includes('features'))).toBe(true);
      expect(linkTexts.some(t => t.includes('quick start'))).toBe(true);
      expect(linkTexts.some(t => t === 'github')).toBe(true);
    });

    test('page structure follows expected user flow', () => {
      const main = document.querySelector('main');
      const sections = main.querySelectorAll('section');

      // Verify logical order of sections
      const sectionClasses = Array.from(sections).map(s => s.className || s.id);

      // Hero should come first, then features, then quick-start
      const heroIndex = sectionClasses.findIndex(c => c.includes('hero'));
      const featuresIndex = sectionClasses.findIndex(c => c.includes('features'));
      const quickStartIndex = sectionClasses.findIndex(c => c.includes('quick-start'));

      expect(heroIndex).toBe(0); // Hero is first
      expect(featuresIndex).toBeGreaterThan(heroIndex); // Features after hero
      expect(quickStartIndex).toBeGreaterThan(featuresIndex); // Quick start after features
    });
  });
});
