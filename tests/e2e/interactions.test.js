/**
 * User Interaction and Animation Tests
 * Owner: Scenario 19 - CSS Animation and Visual Effects
 *
 * Tests:
 * - CSS animations present
 * - GPU-accelerated properties used
 * - Reduced motion preference respected
 */
const fs = require('fs');
const path = require('path');
const { loadHTML } = require('../helpers/dom-utils');

describe('CSS Animation and Visual Effects', () => {
  let cssContent;

  beforeAll(() => {
    // Load the HTML file
    loadHTML('index.html');

    // Load the custom CSS file to inspect animation definitions
    const cssPath = path.resolve(__dirname, '../../assets/css/custom.css');
    cssContent = fs.readFileSync(cssPath, 'utf8');
  });

  describe('Test Case 1: CSS animations present', () => {
    test('page uses CSS animations for visual effects', () => {
      // Check for @keyframes animation definitions in CSS
      const hasKeyframes = cssContent.includes('@keyframes');
      expect(hasKeyframes).toBe(true);
    });

    test('fadeInUp animation is defined', () => {
      // Check for the fadeInUp keyframe animation
      const hasFadeInUp = cssContent.includes('@keyframes fadeInUp');
      expect(hasFadeInUp).toBe(true);
    });

    test('animation properties are applied to hero section elements', () => {
      // Check that hero section elements have animation applied
      const heroH1AnimationApplied = cssContent.includes('#hero h1') && cssContent.includes('animation');
      const taglineAnimationApplied = cssContent.includes('#tagline') && cssContent.includes('animation');
      const ctaAnimationApplied = cssContent.includes('#cta-buttons') && cssContent.includes('animation');

      expect(heroH1AnimationApplied).toBe(true);
      expect(taglineAnimationApplied).toBe(true);
      expect(ctaAnimationApplied).toBe(true);
    });

    test('CSS transitions are defined for interactive elements', () => {
      // Check for transition properties on buttons and cards
      const hasTransitions = cssContent.includes('transition');
      expect(hasTransitions).toBe(true);
    });

    test('feature cards have hover transitions', () => {
      // Check that feature cards have transition defined
      const featureCardTransition = cssContent.includes('.feature-card') && cssContent.includes('transition');
      expect(featureCardTransition).toBe(true);
    });
  });

  describe('Test Case 2: Animations use GPU-accelerated properties', () => {
    test('fadeInUp animation uses transform property', () => {
      // The fadeInUp animation should use transform for GPU acceleration
      const usesTransform = cssContent.includes('transform: translateY');
      expect(usesTransform).toBe(true);
    });

    test('fadeInUp animation uses opacity property', () => {
      // The fadeInUp animation should use opacity for GPU acceleration
      const usesOpacity = cssContent.includes('opacity: 0') || cssContent.includes('opacity: 1');
      expect(usesOpacity).toBe(true);
    });

    test('hover effects use transform instead of layout properties', () => {
      // Check that hover effects use translateY instead of margin/padding
      const hoverUsesTransform = cssContent.includes(':hover') && cssContent.includes('transform');
      expect(hoverUsesTransform).toBe(true);
    });

    test('no layout-triggering animations (width, height, margin, top, left)', () => {
      // Extract all @keyframes blocks
      const keyframesRegex = /@keyframes\s+\w+\s*\{[^}]+(?:\{[^}]*\}[^}]*)*\}/g;
      const keyframesBlocks = cssContent.match(keyframesRegex) || [];

      // Check that keyframes don't animate layout properties
      const layoutProperties = ['width:', 'height:', 'margin:', 'padding:', 'top:', 'left:', 'right:', 'bottom:'];

      keyframesBlocks.forEach(block => {
        layoutProperties.forEach(prop => {
          // Ensure layout properties are not animated in keyframes
          expect(block.toLowerCase()).not.toContain(prop);
        });
      });
    });

    test('transition properties focus on transform and opacity', () => {
      // Find transition declarations and verify they use GPU-friendly properties
      const transitionLines = cssContent.split('\n').filter(line =>
        line.includes('transition') && !line.includes('transition-')
      );

      // At least some transitions should use transform, opacity, or all
      const usesGpuFriendly = transitionLines.some(line =>
        line.includes('transform') ||
        line.includes('opacity') ||
        line.includes('all') ||
        line.includes('color') ||
        line.includes('background')
      );
      expect(usesGpuFriendly).toBe(true);
    });
  });

  describe('Test Case 3: Reduced motion preference respected', () => {
    test('prefers-reduced-motion media query is present', () => {
      const hasReducedMotionQuery = cssContent.includes('@media (prefers-reduced-motion');
      expect(hasReducedMotionQuery).toBe(true);
    });

    test('prefers-reduced-motion disables or reduces animations', () => {
      // Check that the reduced motion media query contains animation-duration: 0 or similar
      const reducedMotionRegex = /@media\s*\(prefers-reduced-motion[^)]*\)\s*\{[\s\S]*?\}/;
      const reducedMotionBlock = cssContent.match(reducedMotionRegex);

      expect(reducedMotionBlock).not.toBeNull();

      const blockContent = reducedMotionBlock[0];

      // Should reduce animation duration
      const reducesAnimation =
        blockContent.includes('animation-duration') ||
        blockContent.includes('animation-iteration-count') ||
        blockContent.includes('animation: none');

      expect(reducesAnimation).toBe(true);
    });

    test('prefers-reduced-motion also handles transitions', () => {
      const reducedMotionRegex = /@media\s*\(prefers-reduced-motion[^)]*\)\s*\{[\s\S]*?\}/;
      const reducedMotionBlock = cssContent.match(reducedMotionRegex);

      expect(reducedMotionBlock).not.toBeNull();

      const blockContent = reducedMotionBlock[0];

      // Should also reduce transitions
      const reducesTransition = blockContent.includes('transition-duration');
      expect(reducesTransition).toBe(true);
    });

    test('scroll-behavior is set to auto when reduced motion is preferred', () => {
      // Find the start of the reduced motion media query
      const reducedMotionStart = cssContent.indexOf('@media (prefers-reduced-motion');
      expect(reducedMotionStart).toBeGreaterThan(-1);

      // Extract content from the start to find the complete block
      // by counting opening and closing braces
      const substring = cssContent.substring(reducedMotionStart);
      let depth = 0;
      let endIndex = 0;
      let foundFirstBrace = false;

      for (let i = 0; i < substring.length; i++) {
        if (substring[i] === '{') {
          depth++;
          foundFirstBrace = true;
        }
        if (substring[i] === '}') {
          depth--;
        }
        if (foundFirstBrace && depth === 0) {
          endIndex = i + 1;
          break;
        }
      }

      const blockContent = substring.substring(0, endIndex);

      // Should set scroll-behavior to auto
      const hasScrollBehaviorAuto = blockContent.includes('scroll-behavior: auto');
      expect(hasScrollBehaviorAuto).toBe(true);
    });

    test('reduced motion applies to all elements via universal selector', () => {
      const reducedMotionRegex = /@media\s*\(prefers-reduced-motion[^)]*\)\s*\{[\s\S]*?\}/;
      const reducedMotionBlock = cssContent.match(reducedMotionRegex);

      expect(reducedMotionBlock).not.toBeNull();

      const blockContent = reducedMotionBlock[0];

      // Should apply to all elements using * selector
      const usesUniversalSelector = blockContent.includes('*');
      expect(usesUniversalSelector).toBe(true);
    });
  });

  describe('Animation performance considerations', () => {
    test('animations have reasonable duration (not too long)', () => {
      // Extract animation shorthand declarations and parse the duration part
      // Animation shorthand: animation: name duration timing-function delay ...
      const animationRegex = /animation:\s*([^;]+);/g;
      let match;
      const durations = [];

      while ((match = animationRegex.exec(cssContent)) !== null) {
        const animationValue = match[1];
        // Match duration values like 0.8s, 2s, 200ms, etc.
        const durationMatch = animationValue.match(/(\d+\.?\d*)(s|ms)/);
        if (durationMatch) {
          const value = parseFloat(durationMatch[1]);
          const unit = durationMatch[2];
          const durationMs = unit === 's' ? value * 1000 : value;
          durations.push(durationMs);
        }
      }

      // All animations should be under 2 seconds for good UX
      expect(durations.length).toBeGreaterThan(0);
      durations.forEach(duration => {
        expect(duration).toBeLessThanOrEqual(2000);
      });
    });

    test('transition durations are reasonable', () => {
      // Extract transition durations from CSS
      const transitionRegex = /transition[^:]*:\s*[^;]*?(\d+(?:\.\d+)?)(s|ms)/g;
      let match;
      const durations = [];

      while ((match = transitionRegex.exec(cssContent)) !== null) {
        const value = parseFloat(match[1]);
        const unit = match[2];
        const durationMs = unit === 's' ? value * 1000 : value;
        durations.push(durationMs);
      }

      // All transitions should be under 1 second for snappy interactions
      durations.forEach(duration => {
        expect(duration).toBeLessThanOrEqual(1000);
      });
    });
  });
});
