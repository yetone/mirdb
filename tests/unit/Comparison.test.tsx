import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import Comparison from '../../src/components/Comparison';

describe('Comparison Table', () => {
  describe('Table structure', () => {
    it('renders a comparison section with an accessible label', () => {
      render(<Comparison />);
      const section = screen.getByRole('region', { name: 'Feature Comparison' });
      expect(section).toBeInTheDocument();
    });

    it('contains a structured table with row headers and columns for MirDB, memcached, and Redis', () => {
      render(<Comparison />);
      const table = screen.getByRole('table');
      expect(table).toBeInTheDocument();

      expect(screen.getByRole('columnheader', { name: 'Feature' })).toBeInTheDocument();
      expect(screen.getByRole('columnheader', { name: 'MirDB' })).toBeInTheDocument();
      expect(screen.getByRole('columnheader', { name: 'memcached' })).toBeInTheDocument();
      expect(screen.getByRole('columnheader', { name: 'Redis' })).toBeInTheDocument();
    });

    it('has a screen-reader-only caption describing the table', () => {
      render(<Comparison />);
      const table = screen.getByRole('table');
      const caption = table.querySelector('caption');
      expect(caption).toBeInTheDocument();
      expect(caption?.textContent).toContain('Feature comparison');
    });
  });

  describe('Comparison dimensions', () => {
    it('includes persistence as a comparison dimension', () => {
      render(<Comparison />);
      expect(screen.getByRole('rowheader', { name: 'Persistence' })).toBeInTheDocument();
    });

    it('includes Memcached Protocol as a comparison dimension', () => {
      render(<Comparison />);
      expect(screen.getByRole('rowheader', { name: 'Memcached Protocol' })).toBeInTheDocument();
    });

    it('includes storage engine type as a comparison dimension', () => {
      render(<Comparison />);
      expect(screen.getByRole('rowheader', { name: 'Storage Engine' })).toBeInTheDocument();
    });

    it('includes compaction as a comparison dimension', () => {
      render(<Comparison />);
      expect(screen.getByRole('rowheader', { name: 'Compaction' })).toBeInTheDocument();
    });

    it('includes write performance as a comparison dimension', () => {
      render(<Comparison />);
      expect(screen.getByRole('rowheader', { name: 'Write Performance' })).toBeInTheDocument();
    });
  });

  describe('Factual accuracy', () => {
    it('shows MirDB as having persistence (positive indicator)', () => {
      render(<Comparison />);
      const persistenceRow = screen.getByRole('rowheader', { name: 'Persistence' }).closest('tr');
      expect(persistenceRow).toBeInTheDocument();
      const cells = persistenceRow!.querySelectorAll('td');
      expect(cells[0].textContent).toMatch(/Yes/);
    });

    it('shows memcached as NOT having persistence', () => {
      render(<Comparison />);
      const persistenceRow = screen.getByRole('rowheader', { name: 'Persistence' }).closest('tr');
      const cells = persistenceRow!.querySelectorAll('td');
      expect(cells[1].textContent).toMatch(/No/);
    });

    it('shows Redis as NOT supporting Memcached protocol', () => {
      render(<Comparison />);
      const protocolRow = screen.getByRole('rowheader', { name: 'Memcached Protocol' }).closest('tr');
      const cells = protocolRow!.querySelectorAll('td');
      expect(cells[2].textContent).toMatch(/No/);
    });

    it('shows MirDB as supporting Memcached protocol', () => {
      render(<Comparison />);
      const protocolRow = screen.getByRole('rowheader', { name: 'Memcached Protocol' }).closest('tr');
      const cells = protocolRow!.querySelectorAll('td');
      expect(cells[0].textContent).toMatch(/Yes/);
    });

    it('shows MirDB as having compaction support', () => {
      render(<Comparison />);
      const compactionRow = screen.getByRole('rowheader', { name: 'Compaction' }).closest('tr');
      const cells = compactionRow!.querySelectorAll('td');
      expect(cells[0].textContent).toMatch(/Yes/);
    });

    it('describes MirDB storage engine as LSM-tree', () => {
      render(<Comparison />);
      const engineRow = screen.getByRole('rowheader', { name: 'Storage Engine' }).closest('tr');
      const cells = engineRow!.querySelectorAll('td');
      expect(cells[0].textContent).toContain('LSM-tree');
    });
  });

  describe('Visual differentiation', () => {
    it('uses SVG check mark icon for positive features', () => {
      render(<Comparison />);
      const persistenceRow = screen.getByRole('rowheader', { name: 'Persistence' }).closest('tr');
      const mirdbCell = persistenceRow!.querySelectorAll('td')[0];
      const svg = mirdbCell.querySelector('svg');
      expect(svg).toBeInTheDocument();
    });

    it('uses SVG X/cross icon for negative features', () => {
      render(<Comparison />);
      const persistenceRow = screen.getByRole('rowheader', { name: 'Persistence' }).closest('tr');
      const memcachedCell = persistenceRow!.querySelectorAll('td')[1];
      const svg = memcachedCell.querySelector('svg');
      expect(svg).toBeInTheDocument();
    });

    it('provides text labels alongside icons so color is not the sole differentiator', () => {
      render(<Comparison />);
      const allYesLabels = screen.getAllByText('Yes');
      const allNoLabels = screen.getAllByText('No');
      expect(allYesLabels.length).toBeGreaterThan(0);
      expect(allNoLabels.length).toBeGreaterThan(0);
    });

    it('uses semantic color classes for positive indicators', () => {
      render(<Comparison />);
      const persistenceRow = screen.getByRole('rowheader', { name: 'Persistence' }).closest('tr');
      const mirdbCell = persistenceRow!.querySelectorAll('td')[0];
      const indicator = mirdbCell.querySelector('span');
      expect(indicator?.className).toMatch(/green/);
    });
  });
});
