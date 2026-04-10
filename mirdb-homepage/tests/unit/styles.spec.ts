/**
 * Typography and Design Consistency Tests
 * Owner: Scenario 18 - Typography and Design Consistency
 *
 * Tests the design system tokens defined in variables.css
 * and verifies consistent usage across the application.
 */
import { describe, it, expect, beforeAll } from 'vitest';
import { readFileSync } from 'fs';
import { resolve } from 'path';

// Helper function to read CSS file contents
const readCssFile = (relativePath: string): string => {
  const filePath = resolve(__dirname, '../../src', relativePath);
  return readFileSync(filePath, 'utf-8');
};

// Helper to extract CSS variable value from :root
const getCssVariableValue = (css: string, varName: string): string | null => {
  const regex = new RegExp(`${varName}:\\s*([^;]+);`, 'g');
  const match = regex.exec(css);
  return match ? match[1].trim() : null;
};

// Helper to convert rem to pixels (assuming 16px base)
const remToPixels = (rem: string): number => {
  const value = parseFloat(rem);
  return value * 16;
};

describe('Typography and Design Consistency', () => {
  let variablesCss: string;
  let globalsCss: string;
  let buttonCss: string;
  let heroCss: string;

  beforeAll(() => {
    variablesCss = readCssFile('styles/variables.css');
    globalsCss = readCssFile('styles/globals.css');
    buttonCss = readFileSync(resolve(__dirname, '../../src/components/common/Button.css'), 'utf-8');
    heroCss = readFileSync(resolve(__dirname, '../../src/components/sections/Hero.css'), 'utf-8');
  });

  describe('Test Case 1: H1 Typography', () => {
    it('should define font-size-hero variable at 48px+ on desktop', () => {
      const heroFontSize = getCssVariableValue(variablesCss, '--font-size-hero');
      expect(heroFontSize).toBeTruthy();

      // Convert to pixels and verify it's at least 48px
      const pixelValue = remToPixels(heroFontSize!);
      expect(pixelValue).toBeGreaterThanOrEqual(48);
    });

    it('should have hero title using the font-size-hero variable', () => {
      expect(heroCss).toContain('var(--font-size-hero)');
    });

    it('should have desktop media query with larger font size (4rem+)', () => {
      // Check for desktop breakpoint with larger font
      expect(heroCss).toMatch(/@media\s*\(min-width:\s*768px\)/);
      // At 768px+, font should be at least 4rem (64px)
      expect(heroCss).toContain('4rem');
    });

    it('should define complete typography scale', () => {
      const fontSizes = [
        '--font-size-xs',
        '--font-size-sm',
        '--font-size-base',
        '--font-size-md',
        '--font-size-lg',
        '--font-size-xl',
        '--font-size-2xl',
        '--font-size-3xl',
        '--font-size-4xl',
        '--font-size-5xl',
        '--font-size-hero'
      ];

      fontSizes.forEach(size => {
        const value = getCssVariableValue(variablesCss, size);
        expect(value, `${size} should be defined`).toBeTruthy();
      });
    });
  });

  describe('Test Case 2: Section Padding', () => {
    it('should define consistent spacing variables', () => {
      const spacingTokens = [
        '--spacing-xs',
        '--spacing-sm',
        '--spacing-md',
        '--spacing-lg',
        '--spacing-xl',
        '--spacing-2xl',
        '--spacing-3xl'
      ];

      spacingTokens.forEach(token => {
        const value = getCssVariableValue(variablesCss, token);
        expect(value, `${token} should be defined`).toBeTruthy();
      });
    });

    it('should have spacing-lg as 1.5rem (24px equivalent)', () => {
      const spacingLg = getCssVariableValue(variablesCss, '--spacing-lg');
      expect(spacingLg).toBe('1.5rem');
      expect(remToPixels(spacingLg!)).toBe(24);
    });

    it('should apply consistent padding to sections in globals.css', () => {
      // Sections should use spacing variables for padding
      expect(globalsCss).toContain('section');
      expect(globalsCss).toMatch(/section\s*\{[^}]*padding:\s*var\(--spacing-/);
    });

    it('should use spacing tokens for container padding', () => {
      expect(globalsCss).toMatch(/\.container\s*\{[^}]*padding:\s*0\s+var\(--spacing-/);
    });
  });

  describe('Test Case 3: Font Family Consistency', () => {
    it('should define a primary font-family variable', () => {
      const fontFamily = getCssVariableValue(variablesCss, '--font-family');
      expect(fontFamily).toBeTruthy();
      expect(fontFamily).toContain('sans-serif');
    });

    it('should define a monospace font-family for code', () => {
      const monoFamily = getCssVariableValue(variablesCss, '--font-family-mono');
      expect(monoFamily).toBeTruthy();
      expect(monoFamily).toContain('monospace');
    });

    it('should apply font-family to body in globals.css', () => {
      expect(globalsCss).toMatch(/body\s*\{[^}]*font-family:\s*var\(--font-family\)/);
    });

    it('should apply monospace font to code elements', () => {
      expect(globalsCss).toMatch(/code\s*\{[^}]*font-family:\s*var\(--font-family-mono\)/);
    });

    it('should have consistent font-family-sans as alias', () => {
      const fontFamilySans = getCssVariableValue(variablesCss, '--font-family-sans');
      const fontFamily = getCssVariableValue(variablesCss, '--font-family');
      expect(fontFamilySans).toBe(fontFamily);
    });
  });

  describe('Test Case 4: Color Palette Consistency', () => {
    it('should define core color tokens for light theme', () => {
      const lightColors = [
        '--color-primary',
        '--color-secondary',
        '--color-background',
        '--color-surface',
        '--color-text',
        '--color-text-primary',
        '--color-text-secondary',
        '--color-border'
      ];

      lightColors.forEach(color => {
        const value = getCssVariableValue(variablesCss, color);
        expect(value, `${color} should be defined in :root`).toBeTruthy();
      });
    });

    it('should define dark theme overrides', () => {
      expect(variablesCss).toContain('[data-theme="dark"]');

      // Dark theme should override key colors
      const darkThemeMatch = variablesCss.match(/\[data-theme="dark"\]\s*\{([^}]+)\}/);
      expect(darkThemeMatch).toBeTruthy();

      const darkStyles = darkThemeMatch![1];
      expect(darkStyles).toContain('--color-primary');
      expect(darkStyles).toContain('--color-background');
      expect(darkStyles).toContain('--color-text');
    });

    it('should define focus color for accessibility', () => {
      const focusColor = getCssVariableValue(variablesCss, '--color-focus');
      expect(focusColor).toBeTruthy();
    });

    it('should define hover state colors', () => {
      const primaryHover = getCssVariableValue(variablesCss, '--color-primary-hover');
      const secondaryHover = getCssVariableValue(variablesCss, '--color-secondary-hover');
      expect(primaryHover).toBeTruthy();
      expect(secondaryHover).toBeTruthy();
    });

    it('should use color tokens in globals.css', () => {
      expect(globalsCss).toContain('var(--color-text)');
      expect(globalsCss).toContain('var(--color-background)');
      expect(globalsCss).toContain('var(--color-focus)');
    });
  });

  describe('Test Case 5: Button Styling Consistency', () => {
    it('should use spacing tokens for button padding', () => {
      expect(buttonCss).toContain('var(--spacing-xs)');
      expect(buttonCss).toContain('var(--spacing-sm)');
      expect(buttonCss).toContain('var(--spacing-md)');
      expect(buttonCss).toContain('var(--spacing-lg)');
    });

    it('should use font-size tokens for button sizes', () => {
      expect(buttonCss).toContain('var(--font-size-sm)');
      expect(buttonCss).toContain('var(--font-size-md)');
      expect(buttonCss).toContain('var(--font-size-lg)');
    });

    it('should use color tokens for button variants', () => {
      expect(buttonCss).toContain('var(--color-primary)');
      expect(buttonCss).toContain('var(--color-primary-hover)');
      expect(buttonCss).toContain('var(--color-secondary)');
    });

    it('should use border-radius token', () => {
      expect(buttonCss).toContain('var(--radius-md)');
    });

    it('should use transition token for animations', () => {
      expect(buttonCss).toContain('var(--transition-base)');
    });

    it('should have focus-visible styles for accessibility', () => {
      expect(buttonCss).toContain(':focus-visible');
      expect(buttonCss).toContain('var(--color-focus)');
    });

    it('should define consistent button variants', () => {
      // Primary variant
      expect(buttonCss).toContain('.btn--primary');
      // Secondary variant
      expect(buttonCss).toContain('.btn--secondary');
      // Outline variant
      expect(buttonCss).toContain('.btn--outline');
    });

    it('should define consistent button sizes', () => {
      expect(buttonCss).toContain('.btn--sm');
      expect(buttonCss).toContain('.btn--md');
      expect(buttonCss).toContain('.btn--lg');
    });
  });

  describe('Design System Validation', () => {
    it('should define border radius tokens', () => {
      const radiusTokens = [
        '--radius-sm',
        '--radius-md',
        '--radius-lg',
        '--radius-xl',
        '--radius-full'
      ];

      radiusTokens.forEach(token => {
        const value = getCssVariableValue(variablesCss, token);
        expect(value, `${token} should be defined`).toBeTruthy();
      });
    });

    it('should define shadow tokens', () => {
      const shadowTokens = [
        '--shadow-sm',
        '--shadow-md',
        '--shadow-lg'
      ];

      shadowTokens.forEach(token => {
        const value = getCssVariableValue(variablesCss, token);
        expect(value, `${token} should be defined`).toBeTruthy();
      });
    });

    it('should define transition tokens', () => {
      const transitionTokens = [
        '--transition-fast',
        '--transition-base',
        '--transition-slow'
      ];

      transitionTokens.forEach(token => {
        const value = getCssVariableValue(variablesCss, token);
        expect(value, `${token} should be defined`).toBeTruthy();
      });
    });

    it('should define layout tokens', () => {
      const containerMaxWidth = getCssVariableValue(variablesCss, '--container-max-width');
      const headerHeight = getCssVariableValue(variablesCss, '--header-height');

      expect(containerMaxWidth).toBeTruthy();
      expect(headerHeight).toBeTruthy();
    });
  });
});
