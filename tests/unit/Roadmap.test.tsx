import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import Roadmap from '../../src/components/Roadmap';

describe('Roadmap & CI Status', () => {
  describe('CircleCI badge rendering', () => {
    it('renders a CircleCI badge image with correct src and links to CircleCI project', () => {
      render(<Roadmap />);

      const link = screen.getByRole('link', {
        name: 'View CircleCI build status for MirDB',
      });
      expect(link).toBeInTheDocument();
      expect(link).toHaveAttribute('href', 'https://circleci.com/gh/yetone/mirdb');

      const img = link.querySelector('img');
      expect(img).toBeInTheDocument();
      expect(img).toHaveAttribute('src', 'https://circleci.com/gh/yetone/mirdb.svg');
    });

    it('badge image has meaningful alt text', () => {
      render(<Roadmap />);

      const img = screen.getByAltText('CircleCI build status');
      expect(img).toBeInTheDocument();
      expect(img.tagName).toBe('IMG');
    });

    it('badge link opens in a new tab', async () => {
      const user = userEvent.setup();
      render(<Roadmap />);

      const link = screen.getByRole('link', {
        name: 'View CircleCI build status for MirDB',
      });
      expect(link).toHaveAttribute('target', '_blank');
      expect(link).toHaveAttribute('rel', 'noopener noreferrer');
    });
  });

  describe('Roadmap section structure', () => {
    it('renders a section with accessible label for roadmap', () => {
      render(<Roadmap />);

      const section = screen.getByRole('region', {
        name: 'Project Roadmap and Build Status',
      });
      expect(section).toBeInTheDocument();
    });

    it('displays the section heading', () => {
      render(<Roadmap />);

      expect(
        screen.getByRole('heading', { name: /Roadmap.*Project Status/i })
      ).toBeInTheDocument();
    });
  });

  describe('Completed items display', () => {
    it('shows tokio async networking as a completed feature', () => {
      render(<Roadmap />);

      expect(screen.getByText('Tokio Async Networking')).toBeInTheDocument();
    });

    it('shows Memcached protocol support as a completed feature', () => {
      render(<Roadmap />);

      expect(screen.getByText('Memcached Protocol Support')).toBeInTheDocument();
    });

    it('shows skip list memtable as a completed feature', () => {
      render(<Roadmap />);

      expect(screen.getByText('Skip List Memtable')).toBeInTheDocument();
    });

    it('shows minor compaction as a completed feature', () => {
      render(<Roadmap />);

      expect(screen.getByText('Minor Compaction')).toBeInTheDocument();
    });

    it('shows major compaction as a completed feature', () => {
      render(<Roadmap />);

      expect(screen.getByText('Major Compaction')).toBeInTheDocument();
    });

    it('each completed item has a visual completion indicator', () => {
      render(<Roadmap />);

      const completedLabels = screen.getAllByText('Completed');
      // 5 completed items + 1 section header "Completed" = 6
      expect(completedLabels.length).toBeGreaterThanOrEqual(5);
    });

    it('completed items have a green background style', () => {
      render(<Roadmap />);

      const section = screen.getByRole('region', {
        name: 'Project Roadmap and Build Status',
      });
      const completedItems = section.querySelectorAll('.bg-green-50');
      // 5 completed items
      expect(completedItems.length).toBe(5);
    });
  });

  describe('Planned items display', () => {
    it('shows Raft Consensus as a planned feature', () => {
      render(<Roadmap />);

      expect(screen.getByText('Raft Consensus')).toBeInTheDocument();
    });

    it('planned item has a visual indicator showing it is not complete', () => {
      render(<Roadmap />);

      // There should be at least one "Planned" label
      const plannedLabels = screen.getAllByText('Planned');
      // 1 planned item + 1 section header "Planned" = 2
      expect(plannedLabels.length).toBeGreaterThanOrEqual(2);
    });

    it('planned items have an amber background style', () => {
      render(<Roadmap />);

      const section = screen.getByRole('region', {
        name: 'Project Roadmap and Build Status',
      });
      const plannedItems = section.querySelectorAll('.bg-amber-50');
      expect(plannedItems.length).toBe(1);
    });
  });

  describe('Visual distinction between completed and planned', () => {
    it('uses Check icon for completed items and Clock icon for planned items (not color-only)', () => {
      render(<Roadmap />);

      // Completed section header uses CheckCircle icon
      const completedHeadings = screen.getAllByText('Completed');
      expect(completedHeadings.length).toBeGreaterThan(0);

      // Planned section header uses Circle icon
      const plannedHeadings = screen.getAllByText('Planned');
      expect(plannedHeadings.length).toBeGreaterThan(0);

      // Status indicators use different icons (Check vs Clock)
      const section = screen.getByRole('region', {
        name: 'Project Roadmap and Build Status',
      });

      // Completed items use Check SVG icon
      const checkIcons = section.querySelectorAll('.text-green-600 svg');
      expect(checkIcons.length).toBeGreaterThan(0);

      // Planned items use Clock SVG icon
      const clockIcons = section.querySelectorAll('.text-amber-600 svg');
      expect(clockIcons.length).toBeGreaterThan(0);
    });

    it('provides text labels ("Completed" / "Planned") alongside icons for accessibility', () => {
      render(<Roadmap />);

      // Text labels are present and not hidden from screen readers
      const completedTexts = screen.getAllByText('Completed');
      const plannedTexts = screen.getAllByText('Planned');

      expect(completedTexts.length).toBeGreaterThan(0);
      expect(plannedTexts.length).toBeGreaterThan(0);
    });

    it('completed and planned items have distinct background colors (green vs amber)', () => {
      render(<Roadmap />);

      const section = screen.getByRole('region', {
        name: 'Project Roadmap and Build Status',
      });
      const greenItems = section.querySelectorAll('.bg-green-50');
      const amberItems = section.querySelectorAll('.bg-amber-50');

      expect(greenItems.length).toBeGreaterThan(0);
      expect(amberItems.length).toBeGreaterThan(0);
    });
  });

  describe('Roadmap data completeness', () => {
    it('renders exactly 5 completed items', () => {
      render(<Roadmap />);

      const section = screen.getByRole('region', {
        name: 'Project Roadmap and Build Status',
      });
      const completedItems = section.querySelectorAll('.bg-green-50');
      expect(completedItems.length).toBe(5);
    });

    it('renders exactly 1 planned item', () => {
      render(<Roadmap />);

      const section = screen.getByRole('region', {
        name: 'Project Roadmap and Build Status',
      });
      const plannedItems = section.querySelectorAll('.bg-amber-50');
      expect(plannedItems.length).toBe(1);
    });
  });
});
