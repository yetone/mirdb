/**
 * Integration tests for accessibility compliance.
 * Owner: Scenario 10 - Accessibility Compliance
 *
 * Tests for WCAG 2.1 Level AA requirements:
 * - Axe accessibility audit (via component analysis)
 * - Heading hierarchy validation
 * - Image alt text validation
 * - Color contrast compliance
 * - ARIA labels on code blocks
 */
import { describe, it, expect } from 'vitest';
import { readFileSync } from 'fs';
import { join } from 'path';

// Read all component files for static analysis
const componentsDir = join(__dirname, '../../src/components');
const layoutsDir = join(__dirname, '../../src/layouts');
const pagesDir = join(__dirname, '../../src/pages');

const heroComponent = readFileSync(join(componentsDir, 'Hero.astro'), 'utf-8');
const featuresComponent = readFileSync(join(componentsDir, 'Features.astro'), 'utf-8');
const architectureComponent = readFileSync(join(componentsDir, 'Architecture.astro'), 'utf-8');
const installationComponent = readFileSync(join(componentsDir, 'Installation.astro'), 'utf-8');
const usageExamplesComponent = readFileSync(join(componentsDir, 'UsageExamples.astro'), 'utf-8');
const footerComponent = readFileSync(join(componentsDir, 'Footer.astro'), 'utf-8');
const codeBlockComponent = readFileSync(join(componentsDir, 'CodeBlock.astro'), 'utf-8');
const baseLayout = readFileSync(join(layoutsDir, 'BaseLayout.astro'), 'utf-8');
const indexPage = readFileSync(join(pagesDir, 'index.astro'), 'utf-8');
const globalCss = readFileSync(join(__dirname, '../../src/styles/global.css'), 'utf-8');

// Combine all content for full page analysis
const allComponents = [
  heroComponent,
  featuresComponent,
  architectureComponent,
  installationComponent,
  usageExamplesComponent,
  footerComponent,
  codeBlockComponent,
  baseLayout,
  indexPage,
].join('\n');

describe('Accessibility Compliance', () => {
  describe('Test Case 1: Axe Accessibility Audit Preparedness', () => {
    it('should have proper HTML lang attribute', () => {
      expect(baseLayout).toContain('lang="en"');
    });

    it('should have proper viewport meta tag', () => {
      expect(baseLayout).toContain('name="viewport"');
      expect(baseLayout).toContain('width=device-width');
    });

    it('should have proper document structure with semantic HTML', () => {
      // Check for semantic elements
      expect(allComponents).toContain('<main>');
      expect(allComponents).toContain('</main>');
      expect(allComponents).toContain('<footer');
      expect(allComponents).toContain('<section');
    });

    it('should have skip link or proper navigation structure', () => {
      // Check for section IDs that allow navigation
      expect(heroComponent).toContain('id="hero"');
      expect(featuresComponent).toContain('id="features"');
      expect(architectureComponent).toContain('id="architecture"');
      expect(installationComponent).toContain('id="installation"');
    });

    it('should have focus-visible styles for accessibility', () => {
      expect(globalCss).toContain(':focus-visible');
      expect(globalCss).toContain('outline');
    });

    it('should have reduced motion support', () => {
      expect(globalCss).toContain('prefers-reduced-motion');
      expect(globalCss).toContain('animation-duration: 0.01ms');
      expect(globalCss).toContain('transition-duration: 0.01ms');
    });
  });

  describe('Test Case 3: Heading Hierarchy', () => {
    it('should have a single H1 heading (in Hero component)', () => {
      // Count H1 occurrences across all components
      const h1Pattern = /<h1[\s>]/gi;
      const h1Matches = allComponents.match(h1Pattern) || [];

      // There should be exactly one H1 in the entire page (in Hero)
      expect(h1Matches.length).toBe(1);
      expect(heroComponent).toMatch(/<h1[\s>]/);
    });

    it('should have H2 headings for major sections', () => {
      // Each major section should use H2 for its title
      expect(featuresComponent).toMatch(/<h2[\s>]/);
      expect(architectureComponent).toMatch(/<h2[\s>]/);
      expect(installationComponent).toMatch(/<h2[\s>]/);
      expect(usageExamplesComponent).toMatch(/<h2[\s>]/);
    });

    it('should have H3 headings for subsections under H2', () => {
      // Architecture and UsageExamples have subsections that should use H3
      expect(architectureComponent).toMatch(/<h3[\s>]/);
      expect(usageExamplesComponent).toMatch(/<h3[\s>]/);
      expect(installationComponent).toMatch(/<h3[\s>]/);
    });

    it('should have H4 headings for nested subsections', () => {
      // Architecture has component descriptions that use H4
      expect(architectureComponent).toMatch(/<h4[\s>]/);
    });

    it('should not skip heading levels (no H1 followed by H3 without H2)', () => {
      // Verify heading hierarchy in each component follows proper order

      // Hero has H1, no other headings needed
      const heroH2 = heroComponent.match(/<h2[\s>]/gi) || [];
      expect(heroH2.length).toBe(0); // Hero should not have H2

      // Features has H2, no H1 or skipping to H4
      expect(featuresComponent).not.toMatch(/<h1[\s>]/);
      const featuresHasH2BeforeH3 = featuresComponent.indexOf('<h2') < featuresComponent.indexOf('<h3') ||
        !featuresComponent.includes('<h3');
      expect(featuresHasH2BeforeH3).toBe(true);
    });

    it('should have proper heading content (not empty)', () => {
      // Check that headings contain actual text content
      const h1Content = heroComponent.match(/<h1[^>]*>[\s\S]*?<\/h1>/gi);
      expect(h1Content).not.toBeNull();
      expect(h1Content![0]).toContain('MirDB');

      // H2 headings should have meaningful content
      expect(featuresComponent).toContain('Key Features');
      expect(architectureComponent).toContain('Architecture Overview');
      expect(installationComponent).toContain('Installation');
      expect(usageExamplesComponent).toContain('Usage Examples');
    });
  });

  describe('Test Case 4: Image Alt Text', () => {
    it('should have alt text on the logo image', () => {
      // Check Hero component logo
      const logoImgPattern = /<img[^>]*src="[^"]*logo[^"]*"[^>]*>/gi;
      const logoImg = heroComponent.match(logoImgPattern);
      expect(logoImg).not.toBeNull();
      expect(heroComponent).toContain('alt="MirDB Logo"');
    });

    it('should have descriptive alt text on the architecture diagram', () => {
      const archImgPattern = /<img[^>]*src="[^"]*architecture[^"]*"[^>]*>/gi;
      const archImg = architectureComponent.match(archImgPattern);
      expect(archImg).not.toBeNull();

      // Check for descriptive alt text (not just "architecture diagram")
      expect(architectureComponent).toMatch(/alt="[^"]*data flow[^"]*"/i);
    });

    it('should have alt text on CircleCI badge', () => {
      // Footer has CircleCI badge image
      const badgeImgPattern = /<img[^>]*CircleCI[^>]*>/gi;
      const altMatch = footerComponent.match(/alt="[^"]*Build Status[^"]*"/i);
      expect(altMatch).not.toBeNull();
    });

    it('should have decorative images marked with aria-hidden or empty alt', () => {
      // Terminal dots are decorative and should be marked appropriately
      expect(codeBlockComponent).toContain('aria-hidden="true"');
    });

    it('should not have images with missing alt attribute', () => {
      // Regex to find img tags without alt attribute
      const imgWithoutAlt = /<img(?![^>]*alt=)[^>]*>/gi;

      // Check each component for images without alt
      const heroMissing = heroComponent.match(imgWithoutAlt);
      const archMissing = architectureComponent.match(imgWithoutAlt);
      const footerMissing = footerComponent.match(imgWithoutAlt);

      expect(heroMissing).toBeNull();
      expect(archMissing).toBeNull();
      expect(footerMissing).toBeNull();
    });
  });

  describe('Test Case 5: Color Contrast Compliance', () => {
    it('should define CSS custom properties for colors', () => {
      expect(globalCss).toContain('--color-text');
      expect(globalCss).toContain('--color-bg');
      expect(globalCss).toContain('--color-text-secondary');
    });

    it('should have dark mode color scheme defined', () => {
      expect(globalCss).toContain('.dark');
      expect(globalCss).toMatch(/\.dark\s*\{[^}]*--color-text/);
      expect(globalCss).toMatch(/\.dark\s*\{[^}]*--color-bg/);
    });

    it('should have sufficient contrast for terminal text', () => {
      // Terminal green (#00ff00) on terminal bg (#0d1117) has good contrast
      expect(globalCss).toContain('--color-terminal-green: #00ff00');
      expect(globalCss).toContain('--color-terminal-bg: #0d1117');
    });

    it('should use appropriate text colors for readability', () => {
      // Light mode: dark text on light background
      expect(globalCss).toContain('--color-text: #24292f'); // Dark text
      expect(globalCss).toContain('--color-bg: #ffffff'); // Light background

      // Dark mode: light text on dark background
      expect(globalCss).toMatch(/\.dark\s*\{[^}]*--color-text: #c9d1d9/);
      expect(globalCss).toMatch(/\.dark\s*\{[^}]*--color-bg: #0d1117/);
    });

    it('should have proper focus indicator colors', () => {
      // Focus indicators should use primary color with good visibility
      expect(globalCss).toContain('--color-primary');
      expect(globalCss).toMatch(/:focus-visible[^{]*\{[^}]*var\(--color-primary\)/);
    });

    it('should use high-contrast colors for interactive elements', () => {
      // Links and buttons should use primary color
      expect(heroComponent).toContain('bg-terminal-green');
      expect(heroComponent).toContain('text-terminal-bg'); // Ensures contrast
    });
  });

  describe('Test Case 6: ARIA Labels on Code Blocks', () => {
    it('should have role="region" on code block containers', () => {
      expect(codeBlockComponent).toContain('role="region"');
    });

    it('should have aria-label on code block regions', () => {
      expect(codeBlockComponent).toContain('aria-label=');
      expect(codeBlockComponent).toMatch(/aria-label=\{[^}]*Code example[^}]*\}/);
    });

    it('should have aria-label on pre elements for screen readers', () => {
      expect(codeBlockComponent).toMatch(/<pre[^>]*aria-label/);
    });

    it('should have tabindex on code blocks for keyboard focus', () => {
      expect(codeBlockComponent).toMatch(/<pre[^>]*tabindex="0"/);
    });

    it('should have proper ARIA attributes on tabbed interfaces', () => {
      // Usage Examples tab navigation
      expect(usageExamplesComponent).toContain('role="tablist"');
      expect(usageExamplesComponent).toContain('role="tab"');
      expect(usageExamplesComponent).toContain('role="tabpanel"');
      expect(usageExamplesComponent).toContain('aria-selected');
      expect(usageExamplesComponent).toContain('aria-controls');
      expect(usageExamplesComponent).toContain('aria-labelledby');

      // Installation tabs
      expect(installationComponent).toContain('role="tablist"');
      expect(installationComponent).toContain('role="tab"');
      expect(installationComponent).toContain('role="tabpanel"');
    });

    it('should have aria-label on tablist elements', () => {
      expect(usageExamplesComponent).toMatch(/role="tablist"[^>]*aria-label/);
      expect(installationComponent).toMatch(/role="tablist"[^>]*aria-label/);
    });
  });

  describe('Additional Accessibility Features', () => {
    it('should have external links marked with rel="noopener noreferrer"', () => {
      // Check Footer external links
      expect(footerComponent).toContain('rel="noopener noreferrer"');
      expect(heroComponent).toContain('rel="noopener noreferrer"');
    });

    it('should have focus ring styles on interactive elements', () => {
      // Check for focus:ring classes in components
      expect(heroComponent).toContain('focus:ring');
      expect(heroComponent).toContain('focus:outline-none');
    });

    it('should have keyboard navigation support in tabs', () => {
      // Check for keyboard event handling in tabs
      expect(usageExamplesComponent).toContain('keydown');
      expect(usageExamplesComponent).toContain('ArrowLeft');
      expect(usageExamplesComponent).toContain('ArrowRight');

      expect(installationComponent).toContain('keydown');
      expect(installationComponent).toContain('ArrowLeft');
      expect(installationComponent).toContain('ArrowRight');
    });

    it('should have accessible copy button with proper labeling', () => {
      const copyButtonComponent = readFileSync(join(componentsDir, 'CopyButton.astro'), 'utf-8');
      // Check for accessible button labeling
      expect(copyButtonComponent).toMatch(/<button[^>]*(aria-label|title)/);
    });

    it('should have section landmarks with proper IDs', () => {
      // All major sections should have IDs for navigation
      expect(heroComponent).toContain('id="hero"');
      expect(featuresComponent).toContain('id="features"');
      expect(usageExamplesComponent).toContain('id="examples"');
      expect(architectureComponent).toContain('id="architecture"');
      expect(installationComponent).toContain('id="installation"');
    });

    it('should have descriptive link text (no "click here")', () => {
      // Ensure links have meaningful text
      const clickHerePattern = />click here</i;
      expect(allComponents).not.toMatch(clickHerePattern);
    });
  });
});
