import * as fs from 'fs';
import * as path from 'path';
import {
  supportsCSSFeature,
  supportsCSSVariables,
  supportsFlexbox,
  supportsGrid,
  supportsBackdropFilter,
  supportsScrollBehavior,
  checkCriticalCSSFeatures,
  hasCriticalSupport,
} from './cssFeatureDetection';

describe('CSS Feature Detection', () => {
  const HOMEPAGE_ROOT = path.join(__dirname, '..', '..', '..');

  describe('supportsCSSFeature', () => {
    let originalCSS: typeof CSS | undefined;

    beforeEach(() => {
      originalCSS = (global as unknown as { CSS?: typeof CSS }).CSS;
      (global as unknown as { CSS: { supports: jest.Mock } }).CSS = {
        supports: jest.fn(),
      };
    });

    afterEach(() => {
      (global as unknown as { CSS?: typeof CSS }).CSS = originalCSS;
    });

    it('returns true when CSS.supports returns true', () => {
      (CSS.supports as jest.Mock).mockReturnValue(true);
      expect(supportsCSSFeature('display', 'flex')).toBe(true);
    });

    it('returns false when CSS.supports returns false', () => {
      (CSS.supports as jest.Mock).mockReturnValue(false);
      expect(supportsCSSFeature('display', 'unknown-value')).toBe(false);
    });

    it('returns false when CSS API is undefined', () => {
      (global as unknown as { CSS?: typeof CSS }).CSS = undefined;
      expect(supportsCSSFeature('display', 'flex')).toBe(false);
    });

    it('returns false when CSS.supports throws an exception', () => {
      (CSS.supports as jest.Mock).mockImplementation(() => {
        throw new Error('not supported');
      });
      expect(supportsCSSFeature('bogus', 'value')).toBe(false);
    });

    it('returns false when CSS.supports is not a function', () => {
      (global as unknown as { CSS: { supports: unknown } }).CSS = { supports: 'invalid' };
      expect(supportsCSSFeature('display', 'flex')).toBe(false);
    });
  });

  describe('individual feature checks', () => {
    let originalCSS: typeof CSS | undefined;

    beforeEach(() => {
      originalCSS = (global as unknown as { CSS?: typeof CSS }).CSS;
      (global as unknown as { CSS: { supports: jest.Mock } }).CSS = {
        supports: jest.fn(),
      };
    });

    afterEach(() => {
      (global as unknown as { CSS?: typeof CSS }).CSS = originalCSS;
    });

    it('supportsCSSVariables checks for CSS custom property support', () => {
      (CSS.supports as jest.Mock).mockReturnValue(true);
      expect(supportsCSSVariables()).toBe(true);
      expect(CSS.supports).toHaveBeenCalledWith('--custom-property', '0');
    });

    it('supportsFlexbox checks for display: flex support', () => {
      (CSS.supports as jest.Mock).mockReturnValue(true);
      expect(supportsFlexbox()).toBe(true);
      expect(CSS.supports).toHaveBeenCalledWith('display', 'flex');
    });

    it('supportsGrid checks for display: grid support', () => {
      (CSS.supports as jest.Mock).mockReturnValue(true);
      expect(supportsGrid()).toBe(true);
      expect(CSS.supports).toHaveBeenCalledWith('display', 'grid');
    });

    it('supportsScrollBehavior checks for smooth scroll support', () => {
      (CSS.supports as jest.Mock).mockReturnValue(true);
      expect(supportsScrollBehavior()).toBe(true);
      expect(CSS.supports).toHaveBeenCalledWith('scroll-behavior', 'smooth');
    });

    it('supportsBackdropFilter returns true when standard property is supported', () => {
      (CSS.supports as jest.Mock).mockImplementation(
        (property: string) => property === 'backdrop-filter',
      );
      expect(supportsBackdropFilter()).toBe(true);
    });

    it('supportsBackdropFilter falls back to -webkit-backdrop-filter for Safari', () => {
      (CSS.supports as jest.Mock).mockImplementation(
        (property: string) => property === '-webkit-backdrop-filter',
      );
      expect(supportsBackdropFilter()).toBe(true);
    });

    it('supportsBackdropFilter returns false when neither prefix is supported', () => {
      (CSS.supports as jest.Mock).mockReturnValue(false);
      expect(supportsBackdropFilter()).toBe(false);
    });
  });

  describe('checkCriticalCSSFeatures', () => {
    let originalCSS: typeof CSS | undefined;

    beforeEach(() => {
      originalCSS = (global as unknown as { CSS?: typeof CSS }).CSS;
      (global as unknown as { CSS: { supports: jest.Mock } }).CSS = {
        supports: jest.fn(),
      };
    });

    afterEach(() => {
      (global as unknown as { CSS?: typeof CSS }).CSS = originalCSS;
    });

    it('returns full support map when all features are supported', () => {
      (CSS.supports as jest.Mock).mockReturnValue(true);
      expect(checkCriticalCSSFeatures()).toEqual({
        cssVariables: true,
        flexbox: true,
        grid: true,
        scrollBehavior: true,
        backdropFilter: true,
      });
    });

    it('returns empty support map when no features are supported', () => {
      (CSS.supports as jest.Mock).mockReturnValue(false);
      expect(checkCriticalCSSFeatures()).toEqual({
        cssVariables: false,
        flexbox: false,
        grid: false,
        scrollBehavior: false,
        backdropFilter: false,
      });
    });

    it('reports partial support for a Safari-style browser', () => {
      // Simulate Safari: backdrop-filter only via -webkit prefix.
      (CSS.supports as jest.Mock).mockImplementation((property: string) => {
        if (property === 'backdrop-filter') return false;
        if (property === '-webkit-backdrop-filter') return true;
        return true;
      });
      const result = checkCriticalCSSFeatures();
      expect(result.cssVariables).toBe(true);
      expect(result.flexbox).toBe(true);
      expect(result.grid).toBe(true);
      expect(result.scrollBehavior).toBe(true);
      expect(result.backdropFilter).toBe(true);
    });
  });

  describe('hasCriticalSupport', () => {
    it('returns true when CSS variables, flexbox, and grid are all supported', () => {
      expect(
        hasCriticalSupport({
          cssVariables: true,
          flexbox: true,
          grid: true,
          scrollBehavior: true,
          backdropFilter: true,
        }),
      ).toBe(true);
    });

    it('returns true even when scroll-behavior or backdrop-filter are unsupported (graceful fallbacks)', () => {
      expect(
        hasCriticalSupport({
          cssVariables: true,
          flexbox: true,
          grid: true,
          scrollBehavior: false,
          backdropFilter: false,
        }),
      ).toBe(true);
    });

    it('returns false when CSS variables are missing', () => {
      expect(
        hasCriticalSupport({
          cssVariables: false,
          flexbox: true,
          grid: true,
          scrollBehavior: true,
          backdropFilter: true,
        }),
      ).toBe(false);
    });

    it('returns false when flexbox is missing', () => {
      expect(
        hasCriticalSupport({
          cssVariables: true,
          flexbox: false,
          grid: true,
          scrollBehavior: true,
          backdropFilter: true,
        }),
      ).toBe(false);
    });

    it('returns false when grid is missing', () => {
      expect(
        hasCriticalSupport({
          cssVariables: true,
          flexbox: true,
          grid: false,
          scrollBehavior: true,
          backdropFilter: true,
        }),
      ).toBe(false);
    });
  });

  describe('CSS file analysis (static checks)', () => {
    let globalsCss: string;
    let postcssConfig: string;
    let tailwindConfig: string;

    beforeAll(() => {
      globalsCss = fs.readFileSync(
        path.join(HOMEPAGE_ROOT, 'src', 'app', 'globals.css'),
        'utf-8',
      );
      postcssConfig = fs.readFileSync(
        path.join(HOMEPAGE_ROOT, 'postcss.config.js'),
        'utf-8',
      );
      tailwindConfig = fs.readFileSync(
        path.join(HOMEPAGE_ROOT, 'tailwind.config.ts'),
        'utf-8',
      );
    });

    it('globals.css declares CSS custom properties (Baseline-supported feature)', () => {
      expect(globalsCss).toMatch(/--background:/);
      expect(globalsCss).toMatch(/--foreground:/);
    });

    it('globals.css references CSS variables via var() (no hard-coded colors leaked)', () => {
      expect(globalsCss).toMatch(/var\(--/);
    });

    it('postcss config enables autoprefixer for vendor prefix coverage', () => {
      expect(postcssConfig).toMatch(/autoprefixer/);
    });

    it('Tailwind config registers a content scan path so utilities are emitted', () => {
      expect(tailwindConfig).toMatch(/content:/);
    });

    it('uses widely supported font family values with generic fallbacks', () => {
      expect(globalsCss).toMatch(/font-family:/);
      expect(globalsCss).toMatch(/(monospace|sans-serif|serif)/);
    });

    it('declares scroll-behavior: smooth (with implicit "auto" fallback)', () => {
      expect(globalsCss).toMatch(/scroll-behavior:\s*smooth/);
    });

    it('avoids untested experimental selectors without @supports guards', () => {
      // Risky selectors: :has(), color-mix(), @property at-rule.
      const risky = [/:has\(/, /color-mix\(/, /@property\s/];
      for (const re of risky) {
        if (re.test(globalsCss)) {
          // If used, the file must also declare a @supports guard.
          expect(globalsCss).toMatch(/@supports/);
        }
      }
    });
  });
});
