/**
 * User Journey E2E Tests
 * Owner: Scenario 7 - Accessibility and Performance
 *
 * Tests for:
 * - Complete user flow from landing to GitHub link
 * - Navigation between sections
 * - All CTAs working correctly
 */

const fs = require('fs');
const path = require('path');

describe('User Journey E2E Tests', () => {
  let document;
  let htmlContent;

  beforeAll(() => {
    // Read the HTML file
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // Create a mock DOM
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
  });

  describe('Landing Page Experience', () => {
    test('Page loads with visible hero section', () => {
      const hero = document.querySelector('#hero');
      expect(hero).toBeTruthy();
    });

    test('Hero displays project name immediately', () => {
      const heroTitle = document.querySelector('.hero-title');
      expect(heroTitle).toBeTruthy();
      expect(heroTitle.textContent.trim()).toBe('MirDB');
    });

    test('Hero displays tagline', () => {
      const tagline = document.querySelector('.hero-tagline');
      expect(tagline).toBeTruthy();
      expect(tagline.textContent).toContain('Persistent Key-Value Store');
      expect(tagline.textContent).toContain('Memcached');
    });

    test('Hero has visible CTA buttons', () => {
      const ctaButtons = document.querySelectorAll('.hero-cta .btn');
      expect(ctaButtons.length).toBeGreaterThanOrEqual(2);
    });

    test('Primary CTA links to GitHub', () => {
      const primaryCta = document.querySelector('.hero-cta .btn-primary');
      expect(primaryCta).toBeTruthy();
      expect(primaryCta.getAttribute('href')).toContain('github.com');
    });

    test('Secondary CTA links to Getting Started', () => {
      const secondaryCta = document.querySelector('.hero-cta .btn-secondary');
      expect(secondaryCta).toBeTruthy();
      expect(secondaryCta.getAttribute('href')).toBe('#getting-started');
    });
  });

  describe('Navigation Flow', () => {
    test('Navigation header is visible', () => {
      const header = document.querySelector('.header');
      expect(header).toBeTruthy();
    });

    test('Navigation contains all main section links', () => {
      const navLinks = document.querySelectorAll('.nav-links .nav-link');
      const navTexts = Array.from(navLinks).map(link => link.textContent.trim().toLowerCase());

      expect(navTexts).toContain('features');
      expect(navTexts).toContain('usage');
      expect(navTexts.join(',')).toContain('getting started');
    });

    test('Navigation has GitHub link', () => {
      const githubLink = document.querySelector('.nav-link-github');
      expect(githubLink).toBeTruthy();
      expect(githubLink.getAttribute('href')).toContain('github.com');
    });

    test('All navigation links point to valid section IDs', () => {
      const navLinks = document.querySelectorAll('.nav-links a[href^="#"]');

      for (const link of navLinks) {
        const href = link.getAttribute('href');
        if (href && href !== '#') {
          const targetId = href.slice(1);
          const target = document.getElementById(targetId);
          expect(target).toBeTruthy();
        }
      }
    });
  });

  describe('Content Sections Journey', () => {
    test('Features section is reachable', () => {
      const featuresSection = document.querySelector('#features');
      expect(featuresSection).toBeTruthy();
    });

    test('Usage section is reachable', () => {
      const usageSection = document.querySelector('#usage');
      expect(usageSection).toBeTruthy();
    });

    test('Usage section has code examples', () => {
      const codeBlocks = document.querySelectorAll('#usage .code-block');
      expect(codeBlocks.length).toBeGreaterThan(0);
    });

    test('Getting Started section is reachable', () => {
      const gettingStarted = document.querySelector('#getting-started');
      expect(gettingStarted).toBeTruthy();
    });

    test('Getting Started has installation steps', () => {
      const steps = document.querySelectorAll('.getting-started-step');
      expect(steps.length).toBeGreaterThanOrEqual(2);
    });

    test('Documentation links are provided', () => {
      const docLinks = document.querySelectorAll('.getting-started-links a');
      expect(docLinks.length).toBeGreaterThan(0);
    });
  });

  describe('CTA Functionality', () => {
    test('GitHub repository link opens in new tab', () => {
      const githubLinks = document.querySelectorAll('a[href*="github.com/yetone/mirdb"]');

      for (const link of githubLinks) {
        expect(link.getAttribute('target')).toBe('_blank');
      }
    });

    test('External links have security attributes', () => {
      const externalLinks = document.querySelectorAll('a[target="_blank"]');

      for (const link of externalLinks) {
        const rel = link.getAttribute('rel');
        expect(rel).toContain('noopener');
      }
    });

    test('Internal anchor links are smooth-scrollable', () => {
      // Check that CSS file has smooth scroll behavior
      const fs = require('fs');
      const path = require('path');
      const cssPath = path.join(__dirname, '../../css/styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf-8');
      expect(cssContent).toContain('scroll-behavior: smooth');
    });

    test('Get Started button links to correct section', () => {
      const getStartedButton = document.querySelector('a.btn-secondary[href="#getting-started"]');
      expect(getStartedButton).toBeTruthy();

      const targetSection = document.querySelector('#getting-started');
      expect(targetSection).toBeTruthy();
    });
  });

  describe('Footer Journey', () => {
    test('Footer is visible at page bottom', () => {
      const footer = document.querySelector('.footer');
      expect(footer).toBeTruthy();
    });

    test('Footer contains navigation links', () => {
      const footerNavLinks = document.querySelectorAll('.footer-nav-list a');
      expect(footerNavLinks.length).toBeGreaterThan(0);
    });

    test('Footer has social links', () => {
      const socialLinks = document.querySelectorAll('.footer-social a');
      expect(socialLinks.length).toBeGreaterThan(0);
    });

    test('Footer social links work correctly', () => {
      const githubSocial = document.querySelector('.footer-social-link[href*="github.com"]');
      expect(githubSocial).toBeTruthy();
      expect(githubSocial.getAttribute('target')).toBe('_blank');
    });

    test('Footer shows copyright with year placeholder', () => {
      const copyright = document.querySelector('.footer-copyright');
      expect(copyright).toBeTruthy();

      const yearSpan = document.querySelector('#footer-year');
      expect(yearSpan).toBeTruthy();
    });
  });

  describe('Information Hierarchy', () => {
    test('User can understand product purpose in hero', () => {
      const heroContent = document.querySelector('.hero-content');
      expect(heroContent.textContent).toContain('MirDB');
      expect(heroContent.textContent).toContain('Key-Value Store');
    });

    test('User can see build status', () => {
      const buildBadge = document.querySelector('.hero-badge');
      expect(buildBadge).toBeTruthy();
    });

    test('User can navigate to source code within 1 click', () => {
      const githubLinks = document.querySelectorAll('a[href*="github.com/yetone/mirdb"]');
      expect(githubLinks.length).toBeGreaterThan(0);
    });

    test('User can access getting started within 1 click', () => {
      const getStartedLinks = document.querySelectorAll('a[href="#getting-started"]');
      expect(getStartedLinks.length).toBeGreaterThan(0);
    });
  });

  describe('Mobile User Journey', () => {
    test('Mobile menu toggle exists', () => {
      const mobileToggle = document.querySelector('.nav-mobile-toggle');
      expect(mobileToggle).toBeTruthy();
    });

    test('Mobile toggle has correct ARIA attributes', () => {
      const mobileToggle = document.querySelector('.nav-mobile-toggle');
      expect(mobileToggle.getAttribute('aria-label')).toBeTruthy();
      expect(mobileToggle.getAttribute('aria-expanded')).toBe('false');
      expect(mobileToggle.getAttribute('aria-controls')).toBe('nav-links');
    });

    test('Mobile navigation links container exists', () => {
      const navLinks = document.querySelector('#nav-links');
      expect(navLinks).toBeTruthy();
    });
  });

  describe('Page Completeness', () => {
    test('All major sections are present', () => {
      const requiredSections = ['hero', 'features', 'usage', 'getting-started'];

      for (const sectionId of requiredSections) {
        const section = document.getElementById(sectionId);
        expect(section).toBeTruthy();
      }
    });

    test('Page has header and footer', () => {
      const header = document.querySelector('header');
      const footer = document.querySelector('footer');

      expect(header).toBeTruthy();
      expect(footer).toBeTruthy();
    });

    test('Main content area is properly marked', () => {
      const main = document.querySelector('main');
      expect(main).toBeTruthy();
      expect(main.id).toBe('main-content');
    });

    test('Page title is descriptive', () => {
      const title = document.querySelector('title');
      expect(title).toBeTruthy();
      expect(title.textContent).toContain('MirDB');
      expect(title.textContent.length).toBeGreaterThan(20);
    });

    test('Meta description is present and descriptive', () => {
      const metaDesc = document.querySelector('meta[name="description"]');
      expect(metaDesc).toBeTruthy();
      expect(metaDesc.getAttribute('content').length).toBeGreaterThan(50);
    });
  });
});
