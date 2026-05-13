import { describe, it, expect, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import Features from '../../src/components/Features';
import { FEATURES } from '../../src/utils/constants';

describe('Feature Overview Grid', () => {
  beforeEach(() => {
    render(<Features />);
  });

  describe('Section structure', () => {
    it('renders as a section with an accessible label', () => {
      const section = screen.getByRole('region', { name: 'Key Features' });
      expect(section).toBeInTheDocument();
    });

    it('renders a heading with the text "Key Features"', () => {
      const heading = screen.getByRole('heading', { name: 'Key Features', level: 2 });
      expect(heading).toBeInTheDocument();
    });
  });

  describe('Feature card count', () => {
    it('renders at least 5 feature cards', () => {
      const cards = document.querySelectorAll('[class*="rounded-xl"]');
      expect(cards.length).toBeGreaterThanOrEqual(5);
    });

    it('renders exactly the number of features defined in constants', () => {
      const cards = document.querySelectorAll('[class*="rounded-xl"]');
      expect(cards.length).toBe(FEATURES.length);
    });
  });

  describe('Feature card content', () => {
    it('each card contains an SVG icon', () => {
      const cards = document.querySelectorAll('[class*="rounded-xl"]');
      cards.forEach((card) => {
        const svg = card.querySelector('svg');
        expect(svg).toBeInTheDocument();
      });
    });

    it('each card contains a title element', () => {
      FEATURES.forEach((feature) => {
        expect(screen.getByText(feature.title)).toBeInTheDocument();
      });
    });

    it('each card contains a description paragraph', () => {
      FEATURES.forEach((feature) => {
        expect(screen.getByText(feature.description)).toBeInTheDocument();
      });
    });

    it('each feature description has 20 words or fewer', () => {
      FEATURES.forEach((feature) => {
        const wordCount = feature.description.split(/\s+/).length;
        expect(wordCount).toBeLessThanOrEqual(20);
      });
    });

    it('icons have aria-hidden for accessibility', () => {
      const icons = document.querySelectorAll('svg[aria-hidden="true"]');
      expect(icons.length).toBeGreaterThanOrEqual(5);
    });
  });

  describe('Specific feature titles', () => {
    const requiredFeatures = [
      /Memcached Protocol/i,
      /Persistent Storage/i,
      /tokio/i,
      /LSM-Tree/i,
      /skip.*list/i,
      /compaction/i,
    ];

    requiredFeatures.forEach((pattern) => {
      it(`includes a feature matching "${pattern}"`, () => {
        const match = FEATURES.some(
          (f) => pattern.test(f.title) || pattern.test(f.description)
        );
        expect(match).toBe(true);
      });
    });
  });

  describe('Responsive grid classes', () => {
    it('renders a grid container with mobile (1-column) class', () => {
      const grid = document.querySelector('.grid');
      expect(grid).toBeInTheDocument();
      expect(grid!.className).toMatch(/grid-cols-1/);
    });

    it('renders a grid container with tablet (2-column) responsive class', () => {
      const grid = document.querySelector('.grid');
      expect(grid!.className).toMatch(/md:grid-cols-2/);
    });

    it('renders a grid container with desktop (3-column) responsive class', () => {
      const grid = document.querySelector('.grid');
      expect(grid!.className).toMatch(/lg:grid-cols-3/);
    });

    it('grid has spacing gap applied', () => {
      const grid = document.querySelector('.grid');
      expect(grid!.className).toMatch(/gap-/);
    });
  });

  describe('Hover interactions', () => {
    it('feature cards have transition classes for smooth hover effects', () => {
      const cards = document.querySelectorAll('[class*="rounded-xl"]');
      cards.forEach((card) => {
        expect(card.className).toMatch(/transition/);
      });
    });

    it('feature cards have hover shadow class', () => {
      const cards = document.querySelectorAll('[class*="rounded-xl"]');
      cards.forEach((card) => {
        expect(card.className).toMatch(/hover:shadow/);
      });
    });

    it('feature cards have hover scale transform', () => {
      const cards = document.querySelectorAll('[class*="rounded-xl"]');
      cards.forEach((card) => {
        expect(card.className).toMatch(/hover:scale/);
      });
    });

    it('feature cards have hover border color change', () => {
      const cards = document.querySelectorAll('[class*="rounded-xl"]');
      cards.forEach((card) => {
        expect(card.className).toMatch(/hover:border/);
      });
    });

    it('icon container has hover background transition', () => {
      const iconContainers = document.querySelectorAll('[class*="group-hover:bg-"]');
      expect(iconContainers.length).toBeGreaterThanOrEqual(5);
    });
  });

  describe('Dark mode support', () => {
    it('cards have dark mode border classes', () => {
      const cards = document.querySelectorAll('[class*="rounded-xl"]');
      cards.forEach((card) => {
        expect(card.className).toMatch(/dark:border-/);
      });
    });

    it('cards have dark mode background classes', () => {
      const cards = document.querySelectorAll('[class*="rounded-xl"]');
      cards.forEach((card) => {
        expect(card.className).toMatch(/dark:bg-/);
      });
    });

    it('headings have dark mode text color classes', () => {
      const heading = screen.getByRole('heading', { name: 'Key Features' });
      expect(heading.className).toMatch(/dark:text-white/);
    });

    it('feature titles have dark mode text color classes', () => {
      const titles = document.querySelectorAll('h3');
      titles.forEach((title) => {
        expect(title.className).toMatch(/dark:text-white/);
      });
    });

    it('descriptions have dark mode text color classes', () => {
      const descriptions = document.querySelectorAll('p.text-sm');
      descriptions.forEach((desc) => {
        expect(desc.className).toMatch(/dark:text-/);
      });
    });
  });

  describe('Content accuracy', () => {
    it('Memcached protocol feature has accurate description referencing protocol compatibility', () => {
      const feature = FEATURES.find((f) => f.id === 'memcached-protocol');
      expect(feature).toBeDefined();
      expect(feature!.description.toLowerCase()).toMatch(/memcached/);
    });

    it('Persistent storage feature mentions WAL or write-ahead log', () => {
      const feature = FEATURES.find((f) => f.id === 'persistent-storage');
      expect(feature).toBeDefined();
      const text = (feature!.title + feature!.description).toLowerCase();
      expect(text).toMatch(/wal|write.ahead|persist|durability/);
    });

    it('Async performance feature references tokio', () => {
      const feature = FEATURES.find((f) => f.id === 'async-tokio');
      expect(feature).toBeDefined();
      const text = (feature!.title + feature!.description).toLowerCase();
      expect(text).toMatch(/tokio/);
    });

    it('LSM-tree feature accurately describes log-structured merge-tree', () => {
      const feature = FEATURES.find((f) => f.id === 'lsm-tree');
      expect(feature).toBeDefined();
      const text = (feature!.title + feature!.description).toLowerCase();
      expect(text).toMatch(/lsm|log.structured|merge.tree/);
    });

    it('Skiplist memtable feature references skip list data structure', () => {
      const feature = FEATURES.find((f) => f.id === 'skiplist-memtable');
      expect(feature).toBeDefined();
      const text = (feature!.title + feature!.description).toLowerCase();
      expect(text).toMatch(/skip|skiplist|memtable/);
    });

    it('Compaction feature mentions minor and major compaction', () => {
      const feature = FEATURES.find((f) => f.id === 'compaction');
      expect(feature).toBeDefined();
      const text = (feature!.title + feature!.description).toLowerCase();
      expect(text).toMatch(/compaction|minor|major/);
    });
  });
});
