/**
 * DOM Structure Validation Tests
 *
 * Validates HTML structure for:
 * - Hero section (Scenario 1)
 * - Features section (Scenario 2)
 * - Configuration section (Scenario 5)
 * - Footer section (Scenario 7)
 * - SEO meta tags (Scenario 13)
 *
 * Each scenario adds tests for their assigned section.
 */

import { describe, it, expect, beforeAll } from 'vitest';
import { loadHTML } from '../setup.js';

let document;

beforeAll(() => {
  const dom = loadHTML();
  document = dom.window.document;
});

// ===== Scenario 1: Hero Section Tests =====

describe('Hero Section - Value Proposition Display', () => {
  describe('Test Case 1: Hero section displays with MirDB branding and tagline', () => {
    it('should have a hero section with id="hero"', () => {
      const heroSection = document.getElementById('hero');
      expect(heroSection).not.toBeNull();
      expect(heroSection.tagName.toLowerCase()).toBe('section');
    });

    it('should have a hero section with class="hero"', () => {
      const heroSection = document.querySelector('.hero');
      expect(heroSection).not.toBeNull();
      expect(heroSection.classList.contains('hero')).toBe(true);
    });

    it('should display MirDB name/branding in the hero title', () => {
      const heroTitle = document.querySelector('.hero-title');
      expect(heroTitle).not.toBeNull();
      expect(heroTitle.textContent).toContain('MirDB');
    });

    it('should have a tagline explaining persistent key-value store', () => {
      const heroTagline = document.querySelector('.hero-tagline');
      expect(heroTagline).not.toBeNull();
      const taglineText = heroTagline.textContent.toLowerCase();
      expect(taglineText).toContain('key-value');
      expect(taglineText).toContain('persistent');
    });

    it('should mention Memcached compatibility in tagline', () => {
      const heroTagline = document.querySelector('.hero-tagline');
      const taglineText = heroTagline.textContent.toLowerCase();
      expect(taglineText).toContain('memcached');
    });
  });

  describe('Test Case 2: Hero section HTML structure validation', () => {
    it('should contain semantic heading (h1) with project name', () => {
      const heroSection = document.getElementById('hero');
      const h1 = heroSection.querySelector('h1');
      expect(h1).not.toBeNull();
      expect(h1.classList.contains('hero-title')).toBe(true);
      expect(h1.textContent).toContain('MirDB');
    });

    it('should have a descriptive paragraph (hero-tagline)', () => {
      const heroSection = document.getElementById('hero');
      const tagline = heroSection.querySelector('.hero-tagline');
      expect(tagline).not.toBeNull();
      expect(tagline.tagName.toLowerCase()).toBe('p');
      expect(tagline.textContent.trim().length).toBeGreaterThan(10);
    });

    it('should have 3-5 key feature highlights in hero-features list', () => {
      const heroSection = document.getElementById('hero');
      const featuresList = heroSection.querySelector('.hero-features');
      expect(featuresList).not.toBeNull();

      const features = featuresList.querySelectorAll('li');
      expect(features.length).toBeGreaterThanOrEqual(3);
      expect(features.length).toBeLessThanOrEqual(5);
    });

    it('should have hero-features as an unordered list (ul)', () => {
      const heroSection = document.getElementById('hero');
      const featuresList = heroSection.querySelector('.hero-features');
      expect(featuresList.tagName.toLowerCase()).toBe('ul');
    });

    it('each hero feature should have meaningful content', () => {
      const heroSection = document.getElementById('hero');
      const features = heroSection.querySelectorAll('.hero-features li');

      features.forEach((feature, index) => {
        expect(feature.textContent.trim().length, `Feature ${index + 1} should have content`).toBeGreaterThan(10);
      });
    });

    it('should have a hero-content container for layout', () => {
      const heroSection = document.getElementById('hero');
      const heroContent = heroSection.querySelector('.hero-content');
      expect(heroContent).not.toBeNull();
    });
  });

  describe('Hero Section CTA Buttons Structure', () => {
    it('should have a hero-cta container for buttons', () => {
      const heroSection = document.getElementById('hero');
      const ctaContainer = heroSection.querySelector('.hero-cta');
      expect(ctaContainer).not.toBeNull();
    });

    it('should have a Get Started CTA button', () => {
      const getStartedBtn = document.querySelector('.hero-cta .btn-primary');
      expect(getStartedBtn).not.toBeNull();
      expect(getStartedBtn.textContent.trim()).toContain('Get Started');
    });

    it('should have Get Started button linking to #getting-started section', () => {
      const getStartedBtn = document.querySelector('.hero-cta .btn-primary');
      expect(getStartedBtn.getAttribute('href')).toBe('#getting-started');
    });

    it('should have a GitHub CTA button', () => {
      const githubBtn = document.querySelector('.hero-cta .btn-secondary');
      expect(githubBtn).not.toBeNull();
      expect(githubBtn.textContent.trim()).toContain('GitHub');
    });

    it('should have GitHub button linking to external repository', () => {
      const githubBtn = document.querySelector('.hero-cta .btn-secondary');
      const href = githubBtn.getAttribute('href');
      expect(href).toContain('github.com');
      expect(href.toLowerCase()).toContain('mirdb');
    });

    it('should open GitHub link in new tab with security attributes', () => {
      const githubBtn = document.querySelector('.hero-cta .btn-secondary');
      expect(githubBtn.getAttribute('target')).toBe('_blank');
      expect(githubBtn.getAttribute('rel')).toContain('noopener');
    });

    it('should have CTA buttons with proper styling classes', () => {
      const heroSection = document.getElementById('hero');
      const getStartedBtn = heroSection.querySelector('.btn-primary');
      const githubBtn = heroSection.querySelector('.btn-secondary');

      expect(getStartedBtn).not.toBeNull();
      expect(githubBtn).not.toBeNull();
    });
  });
});

// ===== Scenario 2: Features Section Tests =====

describe('Features Section - Key Capabilities', () => {
  describe('Test Case 1: Features section contains all required feature cards', () => {
    it('should have a features section with id="features"', () => {
      const featuresSection = document.getElementById('features');
      expect(featuresSection).not.toBeNull();
      expect(featuresSection.tagName.toLowerCase()).toBe('section');
    });

    it('should contain 6 feature cards', () => {
      const featuresSection = document.getElementById('features');
      const featureCards = featuresSection.querySelectorAll('.feature-card');
      expect(featureCards.length).toBe(6);
    });

    it('should have feature card for Memcached protocol compatibility', () => {
      const featuresSection = document.getElementById('features');
      const titles = Array.from(featuresSection.querySelectorAll('.feature-title'));
      const memcachedFeature = titles.find(t =>
        t.textContent.toLowerCase().includes('memcached')
      );
      expect(memcachedFeature).not.toBeUndefined();
    });

    it('should have feature card for Persistent storage', () => {
      const featuresSection = document.getElementById('features');
      const titles = Array.from(featuresSection.querySelectorAll('.feature-title'));
      const persistentFeature = titles.find(t =>
        t.textContent.toLowerCase().includes('persistent')
      );
      expect(persistentFeature).not.toBeUndefined();
    });

    it('should have feature card for LSM tree architecture', () => {
      const featuresSection = document.getElementById('features');
      const titles = Array.from(featuresSection.querySelectorAll('.feature-title'));
      const lsmFeature = titles.find(t =>
        t.textContent.toLowerCase().includes('lsm')
      );
      expect(lsmFeature).not.toBeUndefined();
    });

    it('should have feature card for Rust performance/safety', () => {
      const featuresSection = document.getElementById('features');
      const titles = Array.from(featuresSection.querySelectorAll('.feature-title'));
      const rustFeature = titles.find(t =>
        t.textContent.toLowerCase().includes('rust')
      );
      expect(rustFeature).not.toBeUndefined();
    });

    it('should have feature card for Compaction support', () => {
      const featuresSection = document.getElementById('features');
      const titles = Array.from(featuresSection.querySelectorAll('.feature-title'));
      const compactionFeature = titles.find(t =>
        t.textContent.toLowerCase().includes('compaction')
      );
      expect(compactionFeature).not.toBeUndefined();
    });

    it('should have feature card for WAL durability', () => {
      const featuresSection = document.getElementById('features');
      const titles = Array.from(featuresSection.querySelectorAll('.feature-title'));
      const walFeature = titles.find(t =>
        t.textContent.toLowerCase().includes('wal')
      );
      expect(walFeature).not.toBeUndefined();
    });
  });

  describe('Test Case 2: Feature card structure validation', () => {
    it('each feature card should have an icon/visual element', () => {
      const featuresSection = document.getElementById('features');
      const featureCards = featuresSection.querySelectorAll('.feature-card');

      featureCards.forEach((card, index) => {
        const icon = card.querySelector('.feature-icon');
        expect(icon, `Feature card ${index + 1} should have an icon`).not.toBeNull();
        // Verify icon has visual content (SVG)
        const svg = icon.querySelector('svg');
        expect(svg, `Feature card ${index + 1} icon should contain SVG`).not.toBeNull();
      });
    });

    it('each feature card should have a title', () => {
      const featuresSection = document.getElementById('features');
      const featureCards = featuresSection.querySelectorAll('.feature-card');

      featureCards.forEach((card, index) => {
        const title = card.querySelector('.feature-title');
        expect(title, `Feature card ${index + 1} should have a title`).not.toBeNull();
        expect(title.textContent.trim().length, `Feature card ${index + 1} title should not be empty`).toBeGreaterThan(0);
      });
    });

    it('each feature card should have a description', () => {
      const featuresSection = document.getElementById('features');
      const featureCards = featuresSection.querySelectorAll('.feature-card');

      featureCards.forEach((card, index) => {
        const description = card.querySelector('.feature-description');
        expect(description, `Feature card ${index + 1} should have a description`).not.toBeNull();
        expect(description.textContent.trim().length, `Feature card ${index + 1} description should not be empty`).toBeGreaterThan(0);
      });
    });

    it('feature cards should use semantic article elements', () => {
      const featuresSection = document.getElementById('features');
      const featureCards = featuresSection.querySelectorAll('.feature-card');

      featureCards.forEach((card, index) => {
        expect(card.tagName.toLowerCase(), `Feature card ${index + 1} should be an article element`).toBe('article');
      });
    });

    it('feature titles should use h3 elements for proper heading hierarchy', () => {
      const featuresSection = document.getElementById('features');
      const titles = featuresSection.querySelectorAll('.feature-title');

      titles.forEach((title, index) => {
        expect(title.tagName.toLowerCase(), `Feature title ${index + 1} should be an h3 element`).toBe('h3');
      });
    });
  });
});

describe('Features Section - Grid Layout Structure', () => {
  it('should have a features-grid container', () => {
    const featuresSection = document.getElementById('features');
    const grid = featuresSection.querySelector('.features-grid');
    expect(grid).not.toBeNull();
  });

  it('all feature cards should be inside the grid container', () => {
    const featuresSection = document.getElementById('features');
    const grid = featuresSection.querySelector('.features-grid');
    const featureCards = grid.querySelectorAll('.feature-card');
    expect(featureCards.length).toBe(6);
  });
});

// ===== Scenario 3: Getting Started Section Tests =====

describe('Getting Started Section - Installation & Usage', () => {
  describe('Test Case 1: Installation section content', () => {
    it('should have a getting-started section with id="getting-started"', () => {
      const section = document.getElementById('getting-started');
      expect(section).not.toBeNull();
      expect(section.tagName.toLowerCase()).toBe('section');
    });

    it('should contain cargo build command or installation instructions', () => {
      const section = document.getElementById('getting-started');
      const installBlock = section.querySelector('#install-block');
      expect(installBlock).not.toBeNull();

      const codeContent = installBlock.querySelector('.code-content');
      expect(codeContent).not.toBeNull();
      expect(codeContent.textContent.toLowerCase()).toMatch(/cargo build|cargo install/);
    });
  });

  describe('Test Case 2: SET command example', () => {
    it('should show SET command syntax with example', () => {
      const section = document.getElementById('getting-started');
      const setBlock = section.querySelector('#set-command-block');
      expect(setBlock).not.toBeNull();

      const codeContent = setBlock.querySelector('.code-content');
      expect(codeContent.textContent.toLowerCase()).toContain('set key');

      // Check copy button data
      const copyBtn = setBlock.querySelector('.copy-btn');
      const copyData = copyBtn.getAttribute('data-copy');
      expect(copyData).toContain('set key 0 0 5');
      expect(copyData).toContain('value');
    });
  });

  describe('Test Case 3: GET command example', () => {
    it('should show GET command syntax with example', () => {
      const section = document.getElementById('getting-started');
      const getBlock = section.querySelector('#get-command-block');
      expect(getBlock).not.toBeNull();

      const codeContent = getBlock.querySelector('.code-content');
      expect(codeContent.textContent.toLowerCase()).toContain('get key');

      // Check copy button data
      const copyBtn = getBlock.querySelector('.copy-btn');
      const copyData = copyBtn.getAttribute('data-copy');
      expect(copyData).toContain('get key');
    });
  });

  describe('Test Case 4: INFO command example', () => {
    it('should show INFO command for server statistics', () => {
      const section = document.getElementById('getting-started');
      const infoBlock = section.querySelector('#info-command-block');
      expect(infoBlock).not.toBeNull();

      const codeContent = infoBlock.querySelector('.code-content');
      expect(codeContent.textContent.toLowerCase()).toContain('info');
      // Should mention statistics/server info
      expect(codeContent.textContent.toLowerCase()).toMatch(/statistics|status|info/);

      // Check copy button data
      const copyBtn = infoBlock.querySelector('.copy-btn');
      const copyData = copyBtn.getAttribute('data-copy');
      expect(copyData).toContain('info');
    });
  });

  describe('Code blocks structure', () => {
    it('each code block should have a copy button with data-copy attribute', () => {
      const section = document.getElementById('getting-started');
      const codeBlocks = section.querySelectorAll('.code-block');
      expect(codeBlocks.length).toBeGreaterThan(0);

      codeBlocks.forEach((block, index) => {
        const copyBtn = block.querySelector('.copy-btn');
        expect(copyBtn, `Code block ${index + 1} should have a copy button`).not.toBeNull();
        expect(copyBtn.getAttribute('data-copy'), `Code block ${index + 1} copy button should have data-copy`).not.toBeNull();
      });
    });

    it('copy buttons should have aria-label for accessibility', () => {
      const section = document.getElementById('getting-started');
      const copyBtns = section.querySelectorAll('.copy-btn');

      copyBtns.forEach((btn, index) => {
        const ariaLabel = btn.getAttribute('aria-label');
        expect(ariaLabel, `Copy button ${index + 1} should have aria-label`).not.toBeNull();
        expect(ariaLabel.toLowerCase()).toContain('copy');
      });
    });

    it('code blocks should have code-content with pre and code elements', () => {
      const section = document.getElementById('getting-started');
      const codeBlocks = section.querySelectorAll('.code-block');

      codeBlocks.forEach((block, index) => {
        const codeContent = block.querySelector('.code-content');
        expect(codeContent, `Code block ${index + 1} should have .code-content`).not.toBeNull();
        expect(codeContent.tagName.toLowerCase()).toBe('pre');

        const codeEl = codeContent.querySelector('code');
        expect(codeEl, `Code block ${index + 1} should have code element`).not.toBeNull();
      });
    });
  });

  describe('Test Case 6: Link to full documentation', () => {
    it('should have a link to full documentation/README', () => {
      const section = document.getElementById('getting-started');
      const docsLink = section.querySelector('#full-docs-link');
      expect(docsLink).not.toBeNull();

      const href = docsLink.getAttribute('href');
      expect(href).toContain('github.com/nicksherron/mirdb');
      expect(href.toLowerCase()).toContain('readme');
    });

    it('documentation link should open in new tab with proper security attributes', () => {
      const section = document.getElementById('getting-started');
      const docsLink = section.querySelector('#full-docs-link');

      expect(docsLink.getAttribute('target')).toBe('_blank');
      expect(docsLink.getAttribute('rel')).toContain('noopener');
    });
  });
});
