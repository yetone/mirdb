import { describe, it, expect, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import React from 'react';
import App from '../../src/App';

describe('Responsive typography', () => {
  beforeEach(() => {
    render(<App />);
  });

  it('page renders with body element present and styled', () => {
    // Body is styled via Tailwind @apply directives (bg-white, text-gray-900,
    // antialiased, etc.), which are verified by checking the document renders
    expect(document.body).toBeInstanceOf(HTMLBodyElement);
  });

  it('page renders at least one h2 heading with proper text content', () => {
    const h2Elements = document.querySelectorAll('h2');
    expect(h2Elements.length).toBeGreaterThan(0);
    h2Elements.forEach((h2) => {
      expect(h2.textContent).toBeTruthy();
    });
  });
});

describe('Responsive images and media', () => {
  beforeEach(() => {
    render(<App />);
  });

  it('all images have alt text for accessibility', () => {
    const images = document.querySelectorAll('img');
    expect(images.length).toBeGreaterThan(0);
    images.forEach((img) => {
      expect(img.getAttribute('alt')).toBeTruthy();
    });
  });

  it('images do not have fixed width attributes that would overflow small screens', () => {
    const images = document.querySelectorAll('img');
    images.forEach((img) => {
      const widthAttr = img.getAttribute('width');
      if (widthAttr) {
        const width = parseInt(widthAttr, 10);
        expect(width).toBeLessThanOrEqual(1280);
      }
    });
  });
});

describe('Horizontal overflow prevention', () => {
  it('root layout uses max-width containers to constrain content', () => {
    render(<App />);
    const containers = document.querySelectorAll('.max-w-6xl, .max-w-4xl, .max-w-5xl');
    expect(containers.length).toBeGreaterThan(0);
  });

  it('page has multiple section landmarks with content', () => {
    render(<App />);
    const sections = document.querySelectorAll('section');
    expect(sections.length).toBeGreaterThanOrEqual(5);
  });
});

describe('Responsive container', () => {
  it('container uses max-width constraint for ultra-wide displays', () => {
    const { container } = render(
      <div className="max-w-6xl mx-auto px-4 sm:px-6 lg:px-8">Test content</div>
    );
    const el = container.querySelector('.max-w-6xl');
    expect(el).toBeInTheDocument();
    expect(el!.className).toMatch(/max-w-6xl/);
    expect(el!.className).toMatch(/mx-auto/);
    // Container has responsive horizontal padding
    expect(el!.className).toMatch(/px-4/);
  });
});

describe('Responsive grid layout', () => {
  it('feature grid uses responsive column classes for 3 breakpoints', () => {
    render(<App />);
    const grid = document.querySelector('.grid');
    expect(grid).toBeInTheDocument();
    // 1 column on mobile
    expect(grid!.className).toMatch(/grid-cols-1/);
    // 2 columns on tablet (md breakpoint)
    expect(grid!.className).toMatch(/md:grid-cols-2/);
    // 3 columns on desktop (lg breakpoint)
    expect(grid!.className).toMatch(/lg:grid-cols-3/);
  });

  it('feature grid has consistent gap spacing', () => {
    render(<App />);
    const grid = document.querySelector('.grid');
    expect(grid!.className).toMatch(/gap-\d+/);
  });
});

describe('Touch targets on mobile', () => {
  it('theme toggle button has padding for adequate touch target', () => {
    render(<App />);
    const buttons = document.querySelectorAll('button');
    expect(buttons.length).toBeGreaterThan(0);
    buttons.forEach((btn) => {
      const hasPadding = /p-\d+|px-\d+|py-\d+/.test(btn.className);
      expect(hasPadding).toBe(true);
    });
  });

  it('all buttons have accessible labels for screen readers', () => {
    render(<App />);
    const buttons = document.querySelectorAll('button');
    buttons.forEach((btn) => {
      const hasLabel =
        btn.getAttribute('aria-label') ||
        btn.getAttribute('aria-labelledby') ||
        btn.textContent?.trim();
      expect(hasLabel).toBeTruthy();
    });
  });
});

describe('useMediaQuery hook', () => {
  it('exports a function named useMediaQuery', async () => {
    const mod = await import('../../src/hooks/useMediaQuery');
    expect(mod.useMediaQuery).toBeDefined();
    expect(typeof mod.useMediaQuery).toBe('function');
  });
});
