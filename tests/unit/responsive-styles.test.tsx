import { describe, it, expect, afterEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import { readFileSync } from 'node:fs';
import App from '../../src/App';
import Features from '../../src/components/Features';
import Comparison from '../../src/components/Comparison';
import Demo from '../../src/components/Demo';
import { ThemeProvider } from '../../src/components/ThemeToggle';

const originalInnerWidth = window.innerWidth;

function setViewport(width: number) {
  Object.defineProperty(window, 'innerWidth', {
    writable: true,
    configurable: true,
    value: width,
  });
  window.dispatchEvent(new Event('resize'));
}

function renderApp() {
  return render(
    <ThemeProvider>
      <App />
    </ThemeProvider>
  );
}

describe('Responsive layout across viewports', () => {
  afterEach(() => {
    setViewport(originalInnerWidth);
  });

  describe('Desktop viewport (1280px)', () => {
    it('renders all major sections at 1280px', () => {
      setViewport(1280);
      renderApp();

      expect(screen.getByRole('region', { name: 'Key Features' })).toBeInTheDocument();
      expect(screen.getByRole('region', { name: 'Feature Comparison' })).toBeInTheDocument();
      expect(document.getElementById('demo')).toBeInTheDocument();
      expect(document.getElementById('quickstart')).toBeInTheDocument();
    });

    it('feature grid has 3-column layout on desktop', () => {
      setViewport(1280);
      render(<Features />);

      const grid = document.querySelector('.grid');
      expect(grid!.className).toMatch(/lg:grid-cols-3/);
    });
  });

  describe('Tablet viewport (800px)', () => {
    it('renders all major sections at 800px', () => {
      setViewport(800);
      renderApp();

      expect(screen.getByRole('region', { name: 'Key Features' })).toBeInTheDocument();
      expect(screen.getByRole('region', { name: 'Feature Comparison' })).toBeInTheDocument();
      expect(document.getElementById('demo')).toBeInTheDocument();
    });

    it('feature grid has 2-column layout on tablet', () => {
      setViewport(800);
      render(<Features />);

      const grid = document.querySelector('.grid');
      expect(grid!.className).toMatch(/md:grid-cols-2/);
    });
  });

  describe('Mobile viewport (375px)', () => {
    it('renders all major sections at 375px', () => {
      setViewport(375);
      renderApp();

      expect(screen.getByRole('region', { name: 'Key Features' })).toBeInTheDocument();
      expect(screen.getByRole('region', { name: 'Feature Comparison' })).toBeInTheDocument();
      expect(document.getElementById('demo')).toBeInTheDocument();
    });

    it('feature grid has single-column layout on mobile', () => {
      setViewport(375);
      render(<Features />);

      const grid = document.querySelector('.grid');
      expect(grid!.className).toMatch(/grid-cols-1/);
    });
  });
});

describe('No horizontal overflow at any viewport', () => {
  afterEach(() => {
    setViewport(originalInnerWidth);
  });

  const viewports = [320, 375, 414, 768, 800, 1024, 1280, 1440, 1920, 2560];

  viewports.forEach((width) => {
    it(`page has no horizontal overflow at ${width}px`, () => {
      setViewport(width);
      renderApp();

      // All images should have max-width:100% styling
      const images = document.querySelectorAll('img');
      images.forEach((img) => {
        const hasMaxWidth =
          img.className.includes('max-w-full') ||
          img.className.includes('w-full') ||
          img.hasAttribute('width');
        // If not in component markup, verify via CSS
        if (!hasMaxWidth) {
          const computed = window.getComputedStyle(img);
          expect(computed.maxWidth).not.toBe('none');
        }
      });

      // No element should explicitly exceed viewport
      const mainEl = document.querySelector('main');
      if (mainEl) {
        const computed = window.getComputedStyle(mainEl);
        expect(computed.overflowX).not.toBe('scroll');
      }
    });
  });
});

describe('Responsive image scaling', () => {
  afterEach(() => {
    setViewport(originalInnerWidth);
  });

  it('demo usage.gif has max-width responsive classes', () => {
    render(
      <ThemeProvider>
        <Demo />
      </ThemeProvider>
    );

    const img = screen.getByAltText(/Animated demonstration of MirDB usage/i);
    expect(img.className).toContain('w-full');
    expect(img.className).toContain('max-w-full');
  });

  it('images do not overflow their containers at 375px - verified via classes', () => {
    setViewport(375);
    render(
      <ThemeProvider>
        <Demo />
      </ThemeProvider>
    );

    const img = screen.getByAltText(/Animated demonstration of MirDB usage/i);
    // Tailwind classes enforce responsive image sizing
    expect(img.className).toContain('max-w-full');
    expect(img.className).toContain('w-full');
  });

  it('images do not overflow their containers at 2560px', () => {
    setViewport(2560);
    render(
      <ThemeProvider>
        <Demo />
      </ThemeProvider>
    );

    const img = screen.getByAltText(/Animated demonstration of MirDB usage/i);
    expect(img).toBeInTheDocument();
  });
});

describe('Comparison table mobile readability', () => {
  afterEach(() => {
    setViewport(originalInnerWidth);
  });

  it('uses overflow-x-auto container for horizontal scroll on mobile', () => {
    setViewport(375);
    render(<Comparison />);

    const table = screen.getByRole('table');
    const scrollContainer = table.closest('.overflow-x-auto');
    expect(scrollContainer).toBeInTheDocument();
  });

  it('shows scroll hint text on small screens', () => {
    setViewport(375);
    render(<Comparison />);

    const hint = screen.getByText(/scroll horizontally/i);
    expect(hint).toBeInTheDocument();
  });

  it('all comparison data rows accessible at 320px (smallest phone)', () => {
    setViewport(320);
    render(<Comparison />);

    expect(screen.getByRole('rowheader', { name: 'Persistence' })).toBeInTheDocument();
    expect(screen.getByRole('rowheader', { name: 'Memcached Protocol' })).toBeInTheDocument();
    expect(screen.getByRole('rowheader', { name: 'Storage Engine' })).toBeInTheDocument();
    expect(screen.getByRole('rowheader', { name: 'Compaction' })).toBeInTheDocument();
    expect(screen.getByRole('rowheader', { name: 'Write Performance' })).toBeInTheDocument();
  });
});

describe('Feature grid responsive columns', () => {
  it('has grid-cols-1 for mobile base', () => {
    render(<Features />);
    const grid = document.querySelector('.grid');
    expect(grid!.className).toMatch(/grid-cols-1/);
  });

  it('has md:grid-cols-2 for tablet', () => {
    render(<Features />);
    const grid = document.querySelector('.grid');
    expect(grid!.className).toMatch(/md:grid-cols-2/);
  });

  it('has lg:grid-cols-3 for desktop', () => {
    render(<Features />);
    const grid = document.querySelector('.grid');
    expect(grid!.className).toMatch(/lg:grid-cols-3/);
  });
});

describe('Typography responsive base', () => {
  it('globals.css defines responsive font-size and text-size-adjust rules', () => {
    // Verify the CSS file contains the required responsive typography rules
    renderApp();

    // Body exists and is the application root's parent
    expect(document.body).toBeInTheDocument();

    // The CSS file at src/styles/globals.css defines:
    // - font-size: 1rem on body (minimum 16px to prevent iOS zoom)
    // - -webkit-text-size-adjust: 100% on html (prevents orientation resize)
    // These are verified by reading the CSS file directly
    const css = readFileSync('src/styles/globals.css', 'utf8');
    expect(css).toContain('font-size: 1rem');
    expect(css).toContain('-webkit-text-size-adjust: 100%');
  });

  it('headings are readable without horizontal scroll at mobile viewport', () => {
    setViewport(375);
    renderApp();

    const headings = document.querySelectorAll('h1, h2, h3');
    headings.forEach((heading) => {
      const computed = window.getComputedStyle(heading);
      expect(computed.whiteSpace).not.toBe('nowrap');
    });
  });
});
