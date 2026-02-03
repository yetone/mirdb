/**
 * Integration tests for responsive design.
 * Owner: Scenario 2 - Features Section Display (responsive test case)
 */
import { describe, it, expect } from 'vitest';
import { readFileSync } from 'fs';
import { join } from 'path';

const featuresComponent = readFileSync(
  join(__dirname, '../../src/components/Features.astro'),
  'utf-8'
);

const tailwindConfig = readFileSync(
  join(__dirname, '../../tailwind.config.mjs'),
  'utf-8'
);

describe('Responsive Design', () => {
  describe('Features Grid Responsive Behavior', () => {
    it('should have mobile-first single column layout', () => {
      expect(featuresComponent).toContain('grid-cols-1');
    });

    it('should have 2 columns at md breakpoint (768px)', () => {
      expect(featuresComponent).toContain('md:grid-cols-2');
    });

    it('should have 3 columns at lg breakpoint (1024px)', () => {
      expect(featuresComponent).toContain('lg:grid-cols-3');
    });

    it('should use gap for spacing between cards', () => {
      expect(featuresComponent).toContain('gap-6');
    });
  });

  describe('Tailwind Breakpoint Configuration', () => {
    it('should use default Tailwind breakpoints', () => {
      // Default Tailwind breakpoints: sm: 640px, md: 768px, lg: 1024px
      // We use md (768px) for 2 columns which matches the test case
      // This test verifies the grid classes are properly responsive

      // The Features component should adapt at 768px (md breakpoint)
      // From 1 column (mobile) to 2 columns (tablet)
      const hasResponsiveGrid =
        featuresComponent.includes('grid-cols-1') &&
        featuresComponent.includes('md:grid-cols-2');

      expect(hasResponsiveGrid).toBe(true);
    });
  });

  describe('Container Responsiveness', () => {
    it('should have max-width container', () => {
      expect(featuresComponent).toContain('max-w-6xl');
    });

    it('should have horizontal padding for mobile', () => {
      expect(featuresComponent).toContain('px-4');
    });

    it('should center the container', () => {
      expect(featuresComponent).toContain('mx-auto');
    });
  });
});
