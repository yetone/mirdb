/**
 * Scroll Animations Unit Tests
 * Owner: Scenario 15 - Animations and Transitions
 *
 * Tests for verifying scroll animations using Intersection Observer API.
 * Tests reduced motion preference handling.
 */
const fs = require('fs');
const path = require('path');

describe('Animations and Transitions - Unit Tests', () => {
  let animationsCssContent;
  let scrollAnimationsJsContent;
  let htmlContent;
  let helpersJsContent;

  beforeAll(() => {
    const animationsPath = path.join(__dirname, '../../css/utilities/animations.css');
    animationsCssContent = fs.readFileSync(animationsPath, 'utf-8');

    const scrollAnimationsPath = path.join(__dirname, '../../js/components/scroll-animations.js');
    scrollAnimationsJsContent = fs.readFileSync(scrollAnimationsPath, 'utf-8');

    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    const helpersPath = path.join(__dirname, '../../js/utils/helpers.js');
    helpersJsContent = fs.readFileSync(helpersPath, 'utf-8');
  });

  describe('TC4: Intersection Observer Implementation', () => {
    test('scroll-animations.js uses Intersection Observer API', () => {
      expect(scrollAnimationsJsContent).toContain('IntersectionObserver');
    });

    test('Intersection Observer is created with proper options', () => {
      // Should have root, rootMargin, and threshold options
      expect(scrollAnimationsJsContent).toMatch(/new IntersectionObserver\s*\([^)]+,\s*\{/);
      expect(scrollAnimationsJsContent).toContain('rootMargin');
      expect(scrollAnimationsJsContent).toContain('threshold');
    });

    test('Observer handles intersection entries correctly', () => {
      expect(scrollAnimationsJsContent).toContain('isIntersecting');
      expect(scrollAnimationsJsContent).toContain('entry.target');
    });

    test('Elements are unobserved after animation triggers', () => {
      expect(scrollAnimationsJsContent).toContain('observer.unobserve');
    });

    test('Fallback exists when Intersection Observer is not supported', () => {
      expect(scrollAnimationsJsContent).toMatch(/'IntersectionObserver'\s*in\s*window/);
    });

    test('isIntersectionObserverSupported function is exported', () => {
      expect(scrollAnimationsJsContent).toContain('export function isIntersectionObserverSupported');
    });
  });

  describe('CSS Animation Keyframes', () => {
    test('fadeIn keyframe animation is defined', () => {
      expect(animationsCssContent).toMatch(/@keyframes\s+fadeIn/);
    });

    test('slideUp keyframe animation is defined', () => {
      expect(animationsCssContent).toMatch(/@keyframes\s+slideUp/);
    });

    test('fadeIn animation changes opacity from 0 to 1', () => {
      // Match the full @keyframes fadeIn block using a greedy pattern that captures nested braces
      const fadeInMatch = animationsCssContent.match(/@keyframes\s+fadeIn\s*\{[\s\S]*?to\s*\{[^}]*\}\s*\}/);
      expect(fadeInMatch).not.toBeNull();
      expect(fadeInMatch[0]).toContain('opacity: 0');
      expect(fadeInMatch[0]).toContain('opacity: 1');
    });

    test('slideUp animation includes transform translateY', () => {
      const slideUpMatch = animationsCssContent.match(/@keyframes\s+slideUp\s*\{[\s\S]*?\}/);
      expect(slideUpMatch).not.toBeNull();
      expect(slideUpMatch[0]).toContain('translateY');
    });
  });

  describe('Scroll Reveal CSS Classes', () => {
    test('.scroll-reveal class is defined', () => {
      expect(animationsCssContent).toMatch(/\.scroll-reveal\s*\{/);
    });

    test('.scroll-reveal starts with opacity 0', () => {
      const scrollRevealMatch = animationsCssContent.match(/\.scroll-reveal\s*\{[^}]*\}/);
      expect(scrollRevealMatch).not.toBeNull();
      expect(scrollRevealMatch[0]).toContain('opacity: 0');
    });

    test('.scroll-reveal.visible class is defined', () => {
      expect(animationsCssContent).toMatch(/\.scroll-reveal\.visible\s*\{/);
    });

    test('.scroll-reveal.visible has opacity 1', () => {
      const visibleMatch = animationsCssContent.match(/\.scroll-reveal\.visible\s*\{[^}]*\}/);
      expect(visibleMatch).not.toBeNull();
      expect(visibleMatch[0]).toContain('opacity: 1');
    });

    test('.scroll-reveal has transition for smooth animation', () => {
      expect(animationsCssContent).toMatch(/\.scroll-reveal\s*\{[\s\S]*?transition:/);
    });
  });

  describe('Hover Effects CSS', () => {
    test('.hover-lift class is defined', () => {
      expect(animationsCssContent).toMatch(/\.hover-lift\s*\{/);
    });

    test('.hover-lift:hover applies translateY transform', () => {
      expect(animationsCssContent).toMatch(/\.hover-lift:hover\s*\{[\s\S]*?transform:/);
      expect(animationsCssContent).toMatch(/\.hover-lift:hover[\s\S]*?translateY\s*\(\s*-/);
    });

    test('.hover-lift:hover applies box-shadow', () => {
      expect(animationsCssContent).toMatch(/\.hover-lift:hover\s*\{[\s\S]*?box-shadow:/);
    });

    test('.hover-lift has transition property for smooth effect', () => {
      const hoverLiftMatch = animationsCssContent.match(/\.hover-lift\s*\{[^}]*\}/);
      expect(hoverLiftMatch).not.toBeNull();
      expect(hoverLiftMatch[0]).toContain('transition');
    });
  });

  describe('Reduced Motion Preference', () => {
    test('prefers-reduced-motion media query is defined', () => {
      expect(animationsCssContent).toMatch(/@media\s*\(\s*prefers-reduced-motion:\s*reduce\s*\)/);
    });

    test('Animations are disabled when reduced motion is preferred', () => {
      const reducedMotionMatch = animationsCssContent.match(/@media\s*\(\s*prefers-reduced-motion:\s*reduce\s*\)\s*\{[\s\S]*?\}/);
      expect(reducedMotionMatch).not.toBeNull();
      expect(reducedMotionMatch[0]).toContain('animation: none');
    });

    test('Scroll reveal elements are visible without animation when reduced motion is preferred', () => {
      // Match the entire @media block including all nested rules (until end of file or next media query)
      const reducedMotionMatch = animationsCssContent.match(/@media\s*\(\s*prefers-reduced-motion:\s*reduce\s*\)\s*\{[\s\S]*$/);
      expect(reducedMotionMatch).not.toBeNull();
      expect(reducedMotionMatch[0]).toContain('.scroll-reveal');
      expect(reducedMotionMatch[0]).toContain('opacity: 1');
    });

    test('Hover transforms are disabled when reduced motion is preferred', () => {
      // Match the entire @media block including all nested rules (until end of file or next media query)
      const reducedMotionMatch = animationsCssContent.match(/@media\s*\(\s*prefers-reduced-motion:\s*reduce\s*\)\s*\{[\s\S]*$/);
      expect(reducedMotionMatch).not.toBeNull();
      expect(reducedMotionMatch[0]).toContain('.hover-lift:hover');
      expect(reducedMotionMatch[0]).toContain('transform: none');
    });

    test('JavaScript respects prefers-reduced-motion', () => {
      expect(scrollAnimationsJsContent).toContain('prefersReducedMotion');
    });

    test('prefersReducedMotion helper function is defined', () => {
      expect(helpersJsContent).toContain('prefersReducedMotion');
      expect(helpersJsContent).toContain('prefers-reduced-motion');
    });
  });

  describe('HTML Integration', () => {
    test('scroll-reveal class is applied to feature cards', () => {
      expect(htmlContent).toMatch(/class="[^"]*feature-card[^"]*scroll-reveal[^"]*"/);
    });

    test('scroll-reveal class is applied to section titles', () => {
      // Check that at least one section title has scroll-reveal
      const hasTitleWithScrollReveal =
        htmlContent.includes('features__title scroll-reveal') ||
        htmlContent.includes('usage__title scroll-reveal') ||
        htmlContent.includes('architecture__title scroll-reveal') ||
        htmlContent.includes('getting-started__title scroll-reveal');
      expect(hasTitleWithScrollReveal).toBe(true);
    });

    test('Multiple elements have scroll-reveal class', () => {
      const scrollRevealCount = (htmlContent.match(/scroll-reveal/g) || []).length;
      expect(scrollRevealCount).toBeGreaterThan(3);
    });
  });

  describe('Animation Utility Classes', () => {
    test('.animate-fadeIn class is defined', () => {
      expect(animationsCssContent).toMatch(/\.animate-fadeIn\s*\{/);
    });

    test('.animate-slideUp class is defined', () => {
      expect(animationsCssContent).toMatch(/\.animate-slideUp\s*\{/);
    });

    test('Animation utility classes use CSS variables for duration', () => {
      expect(animationsCssContent).toMatch(/animation:.*var\(--transition-/);
    });
  });

  describe('JavaScript Module Structure', () => {
    test('initScrollAnimations function is exported', () => {
      expect(scrollAnimationsJsContent).toContain('export function initScrollAnimations');
    });

    test('scroll-animations.js imports prefersReducedMotion helper', () => {
      expect(scrollAnimationsJsContent).toMatch(/import\s*\{[^}]*prefersReducedMotion[^}]*\}\s*from/);
    });

    test('Observer observes elements with scroll-reveal class', () => {
      expect(scrollAnimationsJsContent).toContain("'.scroll-reveal'");
      expect(scrollAnimationsJsContent).toContain('observer.observe');
    });

    test('Visible class is added when element enters viewport', () => {
      expect(scrollAnimationsJsContent).toMatch(/classList\.add\s*\(\s*['"]visible['"]\s*\)/);
    });
  });
});
