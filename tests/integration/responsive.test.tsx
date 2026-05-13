import { describe, it, expect, afterEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import App from '../../src/App';

function setViewport(width: number) {
  Object.defineProperty(window, 'innerWidth', {
    writable: true,
    configurable: true,
    value: width,
  });
  window.dispatchEvent(new Event('resize'));
}

afterEach(() => {
  setViewport(1280);
});

describe('Responsive layout across devices', () => {
  describe('Desktop viewport (1280px)', () => {
    it('renders all major sections at 1280px viewport width', () => {
      setViewport(1280);
      render(<App />);

      const sections = document.querySelectorAll('section');
      expect(sections.length).toBeGreaterThanOrEqual(5);

      expect(screen.getByRole('region', { name: /key features/i })).toBeInTheDocument();
      expect(screen.getByRole('table')).toBeInTheDocument();
    });

    it('feature grid uses 3-column layout on desktop (lg breakpoint)', () => {
      setViewport(1280);
      render(<App />);
      const grid = document.querySelector('.grid');
      expect(grid).toBeInTheDocument();
      expect(grid!.className).toMatch(/lg:grid-cols-3/);
    });

    it('content sections are constrained with max-width containers', () => {
      setViewport(1280);
      render(<App />);
      const containers = document.querySelectorAll('.max-w-6xl, .max-w-4xl');
      expect(containers.length).toBeGreaterThan(0);
    });
  });

  describe('Tablet viewport (800px)', () => {
    it('renders all major sections at 800px viewport width', () => {
      setViewport(800);
      render(<App />);

      const sections = document.querySelectorAll('section');
      expect(sections.length).toBeGreaterThanOrEqual(5);

      expect(screen.getByRole('region', { name: /key features/i })).toBeInTheDocument();
      expect(screen.getByRole('table')).toBeInTheDocument();
    });

    it('feature grid uses 2-column layout on tablet (md breakpoint)', () => {
      setViewport(800);
      render(<App />);
      const grid = document.querySelector('.grid');
      expect(grid).toBeInTheDocument();
      expect(grid!.className).toMatch(/md:grid-cols-2/);
    });
  });

  describe('Mobile viewport (375px)', () => {
    it('renders all major sections at 375px viewport width', () => {
      setViewport(375);
      render(<App />);

      const sections = document.querySelectorAll('section');
      expect(sections.length).toBeGreaterThanOrEqual(5);

      expect(screen.getByRole('region', { name: /key features/i })).toBeInTheDocument();
      expect(screen.getByRole('table')).toBeInTheDocument();
    });

    it('feature grid defaults to single-column layout on mobile', () => {
      setViewport(375);
      render(<App />);
      const grid = document.querySelector('.grid');
      expect(grid).toBeInTheDocument();
      expect(grid!.className).toMatch(/grid-cols-1/);
    });

    it('comparison table has horizontal scroll container on mobile', () => {
      setViewport(375);
      render(<App />);
      const table = screen.getByRole('table');
      expect(table).toBeInTheDocument();
      const scrollContainer = table.closest('.overflow-x-auto');
      expect(scrollContainer).toBeInTheDocument();
    });

    it('comparison table shows scroll hint text on small screens', () => {
      setViewport(375);
      render(<App />);
      const hint = screen.getByText(/scroll/i);
      expect(hint).toBeInTheDocument();
    });
  });

  describe('Extreme viewports', () => {
    it('renders all sections at 320px (small phone)', () => {
      setViewport(320);
      render(<App />);

      const sections = document.querySelectorAll('section');
      expect(sections.length).toBeGreaterThanOrEqual(5);

      expect(screen.getByRole('region', { name: /key features/i })).toBeInTheDocument();
      expect(screen.getByRole('table')).toBeInTheDocument();
    });

    it('content uses max-width containers at 2560px (large desktop)', () => {
      setViewport(2560);
      render(<App />);

      const containers = document.querySelectorAll('.max-w-6xl, .max-w-4xl');
      expect(containers.length).toBeGreaterThan(0);
    });

    it('all sections render without errors at any supported viewport width', () => {
      const widths = [320, 375, 414, 768, 800, 1024, 1280, 1440, 1920, 2560];
      widths.forEach((width) => {
        setViewport(width);
        const { unmount } = render(<App />);

        // Verify at least the key sections are present
        expect(screen.getByRole('banner')).toBeInTheDocument();
        expect(screen.getByRole('contentinfo')).toBeInTheDocument();
        expect(screen.getByRole('table')).toBeInTheDocument();

        // Feature grid should have at least 1 column class
        const grid = document.querySelector('.grid');
        expect(grid).toBeInTheDocument();
        expect(grid!.className).toMatch(/grid-cols-1|grid-cols-2|grid-cols-3/);

        unmount();
      });
    });
  });

  describe('Navigation across viewports', () => {
    it('header is present at mobile, tablet, and desktop widths', () => {
      const widths = [375, 800, 1280];
      widths.forEach((width) => {
        setViewport(width);
        const { unmount } = render(<App />);
        expect(screen.getByRole('banner')).toBeInTheDocument();
        unmount();
      });
    });

    it('skip-to-content link is present for keyboard accessibility', () => {
      render(<App />);
      const skipLink = screen.getByText(/skip to main content/i);
      expect(skipLink).toBeInTheDocument();
    });

    it('theme toggle button is accessible on all viewport sizes', () => {
      const widths = [375, 800, 1280];
      widths.forEach((width) => {
        setViewport(width);
        const { unmount } = render(<App />);
        const buttons = document.querySelectorAll('button');
        const toggleBtn = Array.from(buttons).find(
          (btn) => btn.getAttribute('aria-label')?.includes('Switch')
        );
        expect(toggleBtn).toBeTruthy();
        expect(toggleBtn!.getAttribute('aria-label')).toBeTruthy();
        unmount();
      });
    });
  });

  describe('Image responsiveness', () => {
    it('images have alt text for accessibility', () => {
      render(<App />);
      const images = document.querySelectorAll('img');
      images.forEach((img) => {
        expect(img.getAttribute('alt')).toBeTruthy();
      });
    });

    it('images use CSS classes that prevent overflow', () => {
      render(<App />);
      const images = document.querySelectorAll('img');
      expect(images.length).toBeGreaterThanOrEqual(2);
      // Content images (not badge images) should have responsive sizing
      const contentImages = Array.from(images).filter(
        (img) => !img.className.includes('h-5')
      );
      // At least one content image should use responsive classes
      const responsiveImages = Array.from(images).filter((img) =>
        /w-full|max-w-full|h-auto|object-contain/.test(img.className)
      );
      expect(responsiveImages.length).toBeGreaterThanOrEqual(1);
    });
  });

  describe('Footer is accessible', () => {
    it('footer renders on all viewport sizes', () => {
      const widths = [375, 800, 1280];
      widths.forEach((width) => {
        setViewport(width);
        const { unmount } = render(<App />);
        expect(screen.getByRole('contentinfo')).toBeInTheDocument();
        unmount();
      });
    });
  });
});
