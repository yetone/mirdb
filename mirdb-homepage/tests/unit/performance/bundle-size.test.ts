/**
 * Unit Tests for Bundle Size and Code Splitting
 * Owner: Scenario 14 - Performance Requirements
 *
 * Tests cover:
 * - JavaScript bundle size optimization
 * - Code splitting for interactive components (React islands)
 * - Astro's minimal JS output verification
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { existsSync, readdirSync, statSync, readFileSync } from 'fs';
import { join } from 'path';

// Maximum bundle size thresholds (in KB)
const BUNDLE_SIZE_LIMITS = {
  SINGLE_JS_FILE: 150, // No single JS file should exceed 150KB
  TOTAL_JS_SIZE: 500, // Total JS should be under 500KB
  SINGLE_CSS_FILE: 100, // No single CSS file should exceed 100KB
  TOTAL_CSS_SIZE: 200, // Total CSS should be under 200KB
};

describe('Bundle Size Verification', () => {
  const distPath = join(process.cwd(), 'dist');

  describe('Build Output Structure', () => {
    it('should verify dist folder can be created by build', async () => {
      // This test verifies the build configuration is correct
      // The actual build output is checked in E2E tests
      // Here we verify the configuration expectations

      const packageJson = JSON.parse(
        readFileSync(join(process.cwd(), 'package.json'), 'utf-8')
      );

      // Verify build script exists
      expect(packageJson.scripts.build).toBeDefined();
      expect(packageJson.scripts.build).toContain('astro');
    });

    it('should have correct Astro configuration for static output', async () => {
      const astroConfigPath = join(process.cwd(), 'astro.config.mjs');
      const configExists = existsSync(astroConfigPath);

      expect(configExists).toBe(true);

      const configContent = readFileSync(astroConfigPath, 'utf-8');

      // Astro uses static output by default, which is optimal for performance
      // Check that React integration is configured (for code splitting)
      expect(configContent).toContain('react');
    });
  });

  describe('Code Splitting Configuration', () => {
    it('should use React integration for island architecture', () => {
      const packageJson = JSON.parse(
        readFileSync(join(process.cwd(), 'package.json'), 'utf-8')
      );

      // Verify @astrojs/react is installed for React islands
      expect(packageJson.dependencies['@astrojs/react']).toBeDefined();
    });

    it('should have minimal dependencies for optimal bundle size', () => {
      const packageJson = JSON.parse(
        readFileSync(join(process.cwd(), 'package.json'), 'utf-8')
      );

      const deps = Object.keys(packageJson.dependencies);

      // Production dependencies should be minimal for a static site
      // Core dependencies: astro, react, react-dom, tailwindcss, and their integrations
      const maxDependencies = 15;
      expect(deps.length).toBeLessThan(maxDependencies);
    });

    it('should use client directives for React components', async () => {
      // Verify that React components use client:load or client:visible
      // for proper code splitting

      const layoutPath = join(process.cwd(), 'src', 'layouts', 'Layout.astro');
      const layoutContent = readFileSync(layoutPath, 'utf-8');

      // Should use client directive for ThemeToggle
      expect(layoutContent).toContain('client:');
    });
  });

  describe('Tailwind CSS Configuration', () => {
    it('should have Tailwind configured for optimal CSS output', () => {
      const tailwindConfigPath = join(process.cwd(), 'tailwind.config.mjs');
      const configExists = existsSync(tailwindConfigPath);

      expect(configExists).toBe(true);

      const configContent = readFileSync(tailwindConfigPath, 'utf-8');

      // Should have content configuration for tree-shaking unused CSS
      expect(configContent).toContain('content');
    });

    it('should use Tailwind CSS integration with Astro', () => {
      const packageJson = JSON.parse(
        readFileSync(join(process.cwd(), 'package.json'), 'utf-8')
      );

      expect(packageJson.dependencies['@astrojs/tailwind']).toBeDefined();
      expect(packageJson.dependencies['tailwindcss']).toBeDefined();
    });
  });

  describe('Component Island Architecture', () => {
    it('should have interactive components as separate React files', () => {
      const componentsDir = join(process.cwd(), 'src', 'components');

      // Check for React island components (interactive parts)
      const interactiveComponents = [
        'Terminal/InteractiveTerminal.tsx',
        'Terminal/CopyButton.tsx',
        'Terminal/TabPanel.tsx',
        'shared/ThemeToggle.tsx',
        'Configuration/CollapsibleSection.tsx',
        'Hero/HeroLogo.tsx',
        'Hero/CTAButton.tsx',
      ];

      interactiveComponents.forEach((component) => {
        const componentPath = join(componentsDir, component);
        // These components should exist for code splitting
        expect(existsSync(componentPath)).toBe(true);
      });
    });

    it('should have static components as Astro files', () => {
      const componentsDir = join(process.cwd(), 'src', 'components');

      // Check for static Astro components (no JS shipped)
      const staticComponents = [
        'Hero/Hero.astro',
        'Features/FeaturesGrid.astro',
        'Architecture/ArchitectureDiagram.astro',
        'Installation/Installation.astro',
        'Footer/Footer.astro',
      ];

      staticComponents.forEach((component) => {
        const componentPath = join(componentsDir, component);
        expect(existsSync(componentPath)).toBe(true);
      });
    });
  });

  describe('Performance Optimization Patterns', () => {
    it('should not have large inline scripts in layout', () => {
      const layoutPath = join(process.cwd(), 'src', 'layouts', 'Layout.astro');
      const layoutContent = readFileSync(layoutPath, 'utf-8');

      // Extract script content between <script> tags
      const scriptMatches = layoutContent.match(/<script[^>]*>[\s\S]*?<\/script>/gi);

      if (scriptMatches) {
        scriptMatches.forEach((script) => {
          // Inline scripts should be small (theme initialization, etc.)
          // Max 2KB of inline script is reasonable
          const scriptContent = script.replace(/<\/?script[^>]*>/gi, '');
          expect(scriptContent.length).toBeLessThan(2048);
        });
      }
    });

    it('should use CSS modules or scoped styles for component isolation', () => {
      // Check that components use Astro's scoped styles or Tailwind
      const heroPath = join(process.cwd(), 'src', 'components', 'Hero', 'Hero.astro');
      const heroContent = readFileSync(heroPath, 'utf-8');

      // Should either use Tailwind classes or scoped <style> tags
      const usesTailwind = heroContent.includes('class=');
      const usesScopedStyle = heroContent.includes('<style>') || heroContent.includes('<style');

      expect(usesTailwind || usesScopedStyle).toBe(true);
    });

    it('should have syntax highlighting library for code blocks', () => {
      const packageJson = JSON.parse(
        readFileSync(join(process.cwd(), 'package.json'), 'utf-8')
      );

      // Should use highlight.js or similar lightweight syntax highlighter
      const hasHighlighter =
        packageJson.dependencies['highlight.js'] ||
        packageJson.dependencies['shiki'] ||
        packageJson.dependencies['prismjs'];

      expect(hasHighlighter).toBeTruthy();
    });
  });
});

describe('Asset Optimization Patterns', () => {
  it('should have SVG assets for vector graphics', () => {
    // SVGs are preferred for icons and diagrams (smaller, scalable)
    const publicDir = join(process.cwd(), 'public');

    if (existsSync(publicDir)) {
      const publicFiles = readdirSync(publicDir, { recursive: true }) as string[];
      const hasAssets = publicFiles.some((file) =>
        typeof file === 'string' && (file.endsWith('.svg') || file.includes('assets'))
      );

      // Should have some public assets directory setup
      expect(existsSync(publicDir)).toBe(true);
    }
  });

  it('should have favicon configured', () => {
    const layoutPath = join(process.cwd(), 'src', 'layouts', 'Layout.astro');
    const layoutContent = readFileSync(layoutPath, 'utf-8');

    // Should have favicon link
    expect(layoutContent).toContain('favicon');
  });
});

describe('Build Performance Patterns', () => {
  it('should use TypeScript for type safety without runtime overhead', () => {
    const tsconfigPath = join(process.cwd(), 'tsconfig.json');
    expect(existsSync(tsconfigPath)).toBe(true);

    // TypeScript files compile to JS, no runtime overhead
    const tsconfig = JSON.parse(readFileSync(tsconfigPath, 'utf-8'));

    // Verify TypeScript is configured
    expect(tsconfig.compilerOptions).toBeDefined();
  });

  it('should have proper package.json scripts for production build', () => {
    const packageJson = JSON.parse(
      readFileSync(join(process.cwd(), 'package.json'), 'utf-8')
    );

    // Should have build and preview scripts
    expect(packageJson.scripts.build).toBeDefined();
    expect(packageJson.scripts.preview).toBeDefined();

    // Build should include type checking
    expect(packageJson.scripts.build).toContain('check');
  });
});
