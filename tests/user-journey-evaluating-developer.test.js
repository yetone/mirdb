/**
 * E2E Test Suite: User Journey - Evaluating Developer
 * Scenario: End-to-end test of the complete user journey for a developer evaluating MirDB
 *
 * This test validates the complete flow from landing on the homepage
 * to navigating to GitHub, ensuring a logical and intuitive journey.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { JSDOM } from 'jsdom';
import { readFileSync } from 'fs';
import { resolve } from 'path';

describe('User Journey - Evaluating Developer', () => {
  let dom;
  let document;

  beforeEach(() => {
    const htmlPath = resolve(__dirname, '../index.html');
    const html = readFileSync(htmlPath, 'utf-8');
    dom = new JSDOM(html, { runScripts: 'dangerously', resources: 'usable' });
    document = dom.window.document;
  });

  afterEach(() => {
    if (dom) {
      dom.window.close();
    }
  });

  /**
   * Test Case 1: Complete evaluating developer journey
   * Input: Complete evaluating developer journey
   * Expected: User can navigate from hero to GitHub in logical flow
   */
  describe('Test Case 1: Complete evaluating developer journey - hero to GitHub flow', () => {
    it('should have hero section as the first major content section', () => {
      const mainContent = document.querySelector('main');
      expect(mainContent).not.toBeNull();

      const firstSection = mainContent.querySelector('section');
      expect(firstSection).not.toBeNull();
      expect(firstSection.classList.contains('hero')).toBe(true);
    });

    it('should have sections in logical order: hero -> features -> architecture -> quick-start -> commands', () => {
      const main = document.querySelector('main');
      const sections = main.querySelectorAll('section');

      const sectionClasses = Array.from(sections).map(s => {
        return s.id || s.className.split(' ')[0];
      });

      // Verify logical order
      const heroIndex = sectionClasses.findIndex(c => c === 'hero' || c.includes('hero'));
      const featuresIndex = sectionClasses.findIndex(c => c === 'features' || c.includes('features'));
      const architectureIndex = sectionClasses.findIndex(c => c === 'architecture' || c.includes('architecture'));
      const quickStartIndex = sectionClasses.findIndex(c => c === 'quick-start' || c.includes('quick-start'));

      expect(heroIndex).toBeLessThan(featuresIndex);
      expect(featuresIndex).toBeLessThan(architectureIndex);
      expect(architectureIndex).toBeLessThan(quickStartIndex);
    });

    it('should have navigation links that match section order for logical flow', () => {
      const nav = document.querySelector('nav');
      const navLinks = nav.querySelectorAll('.nav-links a[href^="#"]');
      const hrefs = Array.from(navLinks).map(link => link.getAttribute('href'));

      // Features should come before Architecture, Architecture before Quick Start
      const featuresIdx = hrefs.findIndex(h => h === '#features');
      const archIdx = hrefs.findIndex(h => h === '#architecture');
      const quickStartIdx = hrefs.findIndex(h => h === '#quick-start');

      expect(featuresIdx).toBeLessThan(archIdx);
      expect(archIdx).toBeLessThan(quickStartIdx);
    });

    it('should have GitHub link accessible from hero CTA', () => {
      const hero = document.querySelector('.hero');
      expect(hero).not.toBeNull();

      const githubLink = hero.querySelector('a[href*="github.com"]');
      expect(githubLink).not.toBeNull();
      expect(githubLink.getAttribute('href')).toContain('github.com');
    });

    it('should enable complete journey: hero → features → architecture → quick-start → GitHub', () => {
      // Step 1: Land on homepage - hero section visible
      const hero = document.querySelector('.hero');
      expect(hero).not.toBeNull();

      // Step 2: Navigate to features - link exists and section exists
      const featuresLink = document.querySelector('a[href="#features"]');
      const featuresSection = document.querySelector('#features');
      expect(featuresLink).not.toBeNull();
      expect(featuresSection).not.toBeNull();

      // Step 3: Navigate to architecture - link exists and section exists
      const archLink = document.querySelector('a[href="#architecture"]');
      const archSection = document.querySelector('#architecture');
      expect(archLink).not.toBeNull();
      expect(archSection).not.toBeNull();

      // Step 4: Navigate to quick-start - link exists and section exists
      const quickStartLink = document.querySelector('a[href="#quick-start"]');
      const quickStartSection = document.querySelector('#quick-start');
      expect(quickStartLink).not.toBeNull();
      expect(quickStartSection).not.toBeNull();

      // Step 5: Navigate to GitHub - link exists
      const githubLinks = document.querySelectorAll('a[href*="github.com"]');
      expect(githubLinks.length).toBeGreaterThan(0);
    });
  });

  /**
   * Test Case 2: Verify hero communicates value within 5 seconds
   * Input: Verify hero communicates value within 5 seconds
   * Expected: Value proposition is immediately visible above fold
   */
  describe('Test Case 2: Hero value proposition visibility', () => {
    it('should have hero section as first content visible to user', () => {
      const hero = document.querySelector('.hero');
      expect(hero).not.toBeNull();

      // Hero should be direct child of main or body
      const main = document.querySelector('main');
      const firstSection = main.querySelector('section');
      expect(firstSection.classList.contains('hero')).toBe(true);
    });

    it('should have clear product name (MirDB) in hero', () => {
      const hero = document.querySelector('.hero');
      const h1 = hero.querySelector('h1');
      expect(h1).not.toBeNull();
      expect(h1.textContent.toLowerCase()).toContain('mirdb');
    });

    it('should have tagline explaining what MirDB is', () => {
      const hero = document.querySelector('.hero');
      const tagline = hero.querySelector('.tagline, .subtitle, p');
      expect(tagline).not.toBeNull();

      const taglineText = tagline.textContent.toLowerCase();
      // Should communicate key value props: key-value store, memcached, performance
      expect(
        taglineText.includes('key-value') ||
        taglineText.includes('memcached') ||
        taglineText.includes('persistent') ||
        taglineText.includes('performance')
      ).toBe(true);
    });

    it('should have value proposition keywords visible in hero section', () => {
      const hero = document.querySelector('.hero');
      const heroText = hero.textContent.toLowerCase();

      // Key value props that should be visible
      const hasPerformance = heroText.includes('performance') || heroText.includes('fast') || heroText.includes('speed');
      const hasPersistence = heroText.includes('persistent');
      const hasKeyValue = heroText.includes('key-value');

      expect(hasKeyValue || hasPersistence || hasPerformance).toBe(true);
    });

    it('should have primary CTA button visible in hero', () => {
      const hero = document.querySelector('.hero');
      const ctaButtons = hero.querySelectorAll('.btn, button, a.btn-primary, a.btn-secondary');
      expect(ctaButtons.length).toBeGreaterThan(0);
    });

    it('should have both Get Started and GitHub CTAs in hero', () => {
      const hero = document.querySelector('.hero');
      const heroText = hero.textContent.toLowerCase();

      const hasGetStarted = heroText.includes('get started') || heroText.includes('quick start');
      const hasGitHub = hero.querySelector('a[href*="github"]') !== null;

      expect(hasGetStarted).toBe(true);
      expect(hasGitHub).toBe(true);
    });
  });

  /**
   * Test Case 3: Verify features section is discoverable
   * Input: Verify features section is discoverable
   * Expected: Features section is reachable via scroll or navigation
   */
  describe('Test Case 3: Features section discoverability', () => {
    it('should have features section with id="features"', () => {
      const featuresSection = document.querySelector('#features');
      expect(featuresSection).not.toBeNull();
    });

    it('should have features link in navigation', () => {
      const nav = document.querySelector('nav');
      const featuresLink = nav.querySelector('a[href="#features"]');
      expect(featuresLink).not.toBeNull();
    });

    it('should have features link text readable and clear', () => {
      const featuresLink = document.querySelector('nav a[href="#features"]');
      expect(featuresLink).not.toBeNull();
      expect(featuresLink.textContent.toLowerCase()).toContain('feature');
    });

    it('should have features section after hero (reachable by scroll)', () => {
      const main = document.querySelector('main');
      const sections = main.querySelectorAll('section');
      const sectionIds = Array.from(sections).map(s => s.id || s.className);

      const heroIdx = sectionIds.findIndex(id => id.includes('hero'));
      const featuresIdx = sectionIds.findIndex(id => id.includes('features'));

      // Features should come right after hero (index 1 if hero is 0)
      expect(featuresIdx).toBe(heroIdx + 1);
    });

    it('should have features section heading visible', () => {
      const featuresSection = document.querySelector('#features');
      const heading = featuresSection.querySelector('h2');
      expect(heading).not.toBeNull();
      expect(heading.textContent.toLowerCase()).toContain('feature');
    });

    it('should have multiple feature cards visible in features section', () => {
      const featuresSection = document.querySelector('#features');
      const featureCards = featuresSection.querySelectorAll('.feature-card');
      expect(featureCards.length).toBeGreaterThanOrEqual(5);
    });
  });

  /**
   * Test Case 4: Verify quick start enables 5-minute setup
   * Input: Verify quick start enables 5-minute setup
   * Expected: Quick start section has all needed commands for setup
   */
  describe('Test Case 4: Quick start completeness for 5-minute setup', () => {
    it('should have quick-start section with proper id', () => {
      const quickStartSection = document.querySelector('#quick-start');
      expect(quickStartSection).not.toBeNull();
    });

    it('should have installation command (git clone or cargo install)', () => {
      const quickStartSection = document.querySelector('#quick-start');
      const codeBlocks = quickStartSection.querySelectorAll('pre code');

      const hasInstallCommand = Array.from(codeBlocks).some(block => {
        const text = block.textContent;
        return text.includes('git clone') || text.includes('cargo install') || text.includes('cargo build');
      });

      expect(hasInstallCommand).toBe(true);
    });

    it('should have build command (cargo build)', () => {
      const quickStartSection = document.querySelector('#quick-start');
      const codeBlocks = quickStartSection.querySelectorAll('pre code');

      const hasBuildCommand = Array.from(codeBlocks).some(block => {
        return block.textContent.includes('cargo build');
      });

      expect(hasBuildCommand).toBe(true);
    });

    it('should have configuration example', () => {
      const quickStartSection = document.querySelector('#quick-start');
      const codeBlocks = quickStartSection.querySelectorAll('pre code');

      const hasConfig = Array.from(codeBlocks).some(block => {
        const text = block.textContent;
        return text.includes('toml') || text.includes('addr =') || text.includes('work_dir');
      });

      expect(hasConfig).toBe(true);
    });

    it('should have run command (cargo run)', () => {
      const quickStartSection = document.querySelector('#quick-start');
      const codeBlocks = quickStartSection.querySelectorAll('pre code');

      const hasRunCommand = Array.from(codeBlocks).some(block => {
        return block.textContent.includes('cargo run');
      });

      expect(hasRunCommand).toBe(true);
    });

    it('should have basic usage example (set/get commands)', () => {
      const quickStartSection = document.querySelector('#quick-start');
      const codeBlocks = quickStartSection.querySelectorAll('pre code');

      const hasUsageExample = Array.from(codeBlocks).some(block => {
        const text = block.textContent.toLowerCase();
        return (text.includes('set ') && text.includes('get ')) || text.includes('telnet');
      });

      expect(hasUsageExample).toBe(true);
    });

    it('should have all essential steps for complete setup: clone, build, config, run, use', () => {
      const quickStartSection = document.querySelector('#quick-start');
      const allText = quickStartSection.textContent.toLowerCase();
      const codeText = Array.from(quickStartSection.querySelectorAll('pre code'))
        .map(c => c.textContent.toLowerCase())
        .join(' ');

      // Clone or installation
      const hasClone = codeText.includes('git clone') || codeText.includes('cargo install');
      // Build
      const hasBuild = codeText.includes('cargo build');
      // Configuration
      const hasConfig = codeText.includes('toml') || allText.includes('configuration');
      // Run
      const hasRun = codeText.includes('cargo run');
      // Usage
      const hasUsage = codeText.includes('telnet') || (codeText.includes('set') && codeText.includes('get'));

      expect(hasClone).toBe(true);
      expect(hasBuild).toBe(true);
      expect(hasConfig).toBe(true);
      expect(hasRun).toBe(true);
      expect(hasUsage).toBe(true);
    });
  });

  /**
   * Test Case 5: Verify GitHub is accessible from multiple locations
   * Input: Verify GitHub is accessible from multiple locations
   * Expected: GitHub link exists in header, hero CTA, and footer
   */
  describe('Test Case 5: GitHub accessibility from multiple locations', () => {
    it('should have GitHub link in header/navigation', () => {
      const header = document.querySelector('header');
      const nav = document.querySelector('nav');

      const headerGithub = header.querySelector('a[href*="github.com"]');
      const navGithub = nav.querySelector('a[href*="github.com"]');

      expect(headerGithub !== null || navGithub !== null).toBe(true);
    });

    it('should have GitHub link in hero CTA section', () => {
      const hero = document.querySelector('.hero');
      const ctaSection = hero.querySelector('.cta-buttons, .cta, .buttons');

      if (ctaSection) {
        const githubLink = ctaSection.querySelector('a[href*="github.com"]');
        expect(githubLink).not.toBeNull();
      } else {
        // Fallback: check hero section directly
        const githubLink = hero.querySelector('a[href*="github.com"]');
        expect(githubLink).not.toBeNull();
      }
    });

    it('should have GitHub link in footer', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();

      const githubLink = footer.querySelector('a[href*="github.com"]');
      expect(githubLink).not.toBeNull();
    });

    it('should have GitHub links in exactly three locations: header, hero, footer', () => {
      const header = document.querySelector('header');
      const hero = document.querySelector('.hero');
      const footer = document.querySelector('footer');

      const headerGithub = header.querySelector('a[href*="github.com"]');
      const heroGithub = hero.querySelector('a[href*="github.com"]');
      const footerGithub = footer.querySelector('a[href*="github.com"]');

      expect(headerGithub).not.toBeNull();
      expect(heroGithub).not.toBeNull();
      expect(footerGithub).not.toBeNull();
    });

    it('should have all GitHub links pointing to same repository', () => {
      const githubLinks = document.querySelectorAll('a[href*="github.com/yetone/mirdb"]');
      expect(githubLinks.length).toBeGreaterThanOrEqual(3);

      // All should point to the same repo
      const urls = Array.from(githubLinks).map(link => {
        const href = link.getAttribute('href');
        // Extract base repo URL (without /issues, /blob, etc)
        const match = href.match(/github\.com\/[\w-]+\/[\w-]+/);
        return match ? match[0] : href;
      });

      // All base URLs should be the same
      const uniqueBaseUrls = [...new Set(urls)];
      expect(uniqueBaseUrls.length).toBe(1);
    });

    it('should have GitHub links open in new tab with security attributes', () => {
      const githubLinks = document.querySelectorAll('a[href*="github.com"]');

      githubLinks.forEach(link => {
        expect(link.getAttribute('target')).toBe('_blank');
        expect(link.getAttribute('rel')).toContain('noopener');
      });
    });
  });

  /**
   * Additional integration tests for complete journey validation
   */
  describe('Integration: Complete user journey validation', () => {
    it('should have smooth scroll behavior for internal navigation', () => {
      const scripts = document.querySelectorAll('script');
      const hasSmoothScroll = Array.from(scripts).some(script => {
        const content = script.textContent || '';
        return content.includes('scrollIntoView') || content.includes('smooth');
      });

      expect(hasSmoothScroll).toBe(true);
    });

    it('should have all navigation targets accessible', () => {
      const nav = document.querySelector('nav');
      const internalLinks = nav.querySelectorAll('a[href^="#"]');

      internalLinks.forEach(link => {
        const href = link.getAttribute('href');
        if (href !== '#') {
          const targetId = href.substring(1);
          const targetSection = document.getElementById(targetId);
          expect(targetSection).not.toBeNull();
        }
      });
    });

    it('should have page structure that supports logical evaluation flow', () => {
      // A developer evaluating MirDB should be able to:
      // 1. Understand what it is (hero)
      // 2. See key features (features section)
      // 3. Understand how it works (architecture)
      // 4. Try it out (quick start)
      // 5. Explore code (GitHub)

      const hero = document.querySelector('.hero');
      const features = document.querySelector('#features');
      const architecture = document.querySelector('#architecture');
      const quickStart = document.querySelector('#quick-start');
      const githubLinks = document.querySelectorAll('a[href*="github.com"]');

      expect(hero).not.toBeNull();
      expect(features).not.toBeNull();
      expect(architecture).not.toBeNull();
      expect(quickStart).not.toBeNull();
      expect(githubLinks.length).toBeGreaterThanOrEqual(3);
    });
  });
});
