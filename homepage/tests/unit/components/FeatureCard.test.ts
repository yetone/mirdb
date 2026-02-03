/**
 * Unit tests for FeatureCard component.
 * Owner: Scenario 2 - Features Section Display
 */
import { describe, it, expect } from 'vitest';
import { readFileSync } from 'fs';
import { join } from 'path';

const featureCardComponent = readFileSync(
  join(__dirname, '../../../src/components/FeatureCard.astro'),
  'utf-8'
);

describe('FeatureCard Component', () => {
  describe('Props Interface', () => {
    it('should define Props interface with required fields', () => {
      expect(featureCardComponent).toContain('export interface Props');
      expect(featureCardComponent).toContain('icon: string');
      expect(featureCardComponent).toContain('title: string');
      expect(featureCardComponent).toContain('description: string');
    });

    it('should have optional link prop', () => {
      expect(featureCardComponent).toContain('link?: string');
    });
  });

  describe('Icon Rendering', () => {
    it('should have SVG icons for each feature type', () => {
      // Check that iconMap contains all expected icons
      expect(featureCardComponent).toContain("protocol:");
      expect(featureCardComponent).toContain("storage:");
      expect(featureCardComponent).toContain("performance:");
      expect(featureCardComponent).toContain("compaction:");
      expect(featureCardComponent).toContain("wal:");
      expect(featureCardComponent).toContain("rust:");
    });

    it('should use set:html directive for icon rendering', () => {
      expect(featureCardComponent).toContain('set:html={iconSvg}');
    });

    it('should have aria-hidden on icon for accessibility', () => {
      expect(featureCardComponent).toContain('aria-hidden="true"');
    });
  });

  describe('Accessibility', () => {
    it('should use article element for semantic structure', () => {
      expect(featureCardComponent).toContain('<article');
    });

    it('should use h3 for proper heading hierarchy', () => {
      expect(featureCardComponent).toContain('<h3');
    });
  });

  describe('Dark Mode Support', () => {
    it('should have dark mode classes for background', () => {
      expect(featureCardComponent).toContain('dark:bg-gray-800');
    });

    it('should have dark mode classes for text colors', () => {
      expect(featureCardComponent).toContain('dark:text-gray-100');
      expect(featureCardComponent).toContain('dark:text-gray-400');
    });

    it('should have dark mode classes for border', () => {
      expect(featureCardComponent).toContain('dark:border-gray-700');
    });
  });

  describe('Hover Effects', () => {
    it('should have hover styles for interactivity', () => {
      expect(featureCardComponent).toContain('hover:shadow-lg');
      expect(featureCardComponent).toContain('hover:border-primary');
    });

    it('should have transition class for smooth effects', () => {
      expect(featureCardComponent).toContain('transition');
    });
  });

  describe('Link Rendering', () => {
    it('should conditionally render link when provided', () => {
      expect(featureCardComponent).toContain('{link &&');
      expect(featureCardComponent).toContain('href={link}');
    });

    it('should have hover effect on link', () => {
      expect(featureCardComponent).toContain('hover:underline');
    });
  });
});
