/**
 * Unit tests for HowItWorks component.
 * Owner: Scenario 3 - How It Works Section
 *
 * Tests:
 * 1. Architecture diagram is displayed with proper alt text
 * 2. Comparison table contains rows comparing MirDB and memcached
 * 3. Table has proper headers with scope attributes for accessibility
 */

import React from 'react';
import { render, screen } from '@testing-library/react';
import { HowItWorks } from '@/components/sections/HowItWorks';

describe('HowItWorks Component', () => {
  describe('Test Case 1: Architecture diagram with alt text', () => {
    it('should render the architecture diagram with proper alt text describing LSM tree flow', () => {
      render(<HowItWorks />);

      const diagram = screen.getByRole('img', { name: /LSM Tree Architecture/i });
      expect(diagram).toBeInTheDocument();

      // Verify alt text describes the flow
      expect(diagram).toHaveAttribute(
        'alt',
        expect.stringContaining('Memtable')
      );
      expect(diagram).toHaveAttribute(
        'alt',
        expect.stringContaining('Immutable Memtables')
      );
      expect(diagram).toHaveAttribute(
        'alt',
        expect.stringContaining('SSTable')
      );
      expect(diagram).toHaveAttribute(
        'alt',
        expect.stringContaining('Write-Ahead Log')
      );
    });

    it('should have proper image source pointing to architecture diagram', () => {
      render(<HowItWorks />);

      const diagram = screen.getByRole('img', { name: /LSM Tree Architecture/i });
      expect(diagram).toHaveAttribute('src', '/images/architecture-diagram.svg');
    });
  });

  describe('Test Case 2: Comparison table with MirDB vs memcached', () => {
    it('should render comparison table with MirDB and memcached columns', () => {
      render(<HowItWorks />);

      // Check for table headers
      expect(screen.getByRole('columnheader', { name: /Feature/i })).toBeInTheDocument();
      expect(screen.getByRole('columnheader', { name: /MirDB/i })).toBeInTheDocument();
      expect(screen.getByRole('columnheader', { name: /Memcached/i })).toBeInTheDocument();
    });

    it('should display Data Persistence as a key differentiator', () => {
      render(<HowItWorks />);

      // Check for persistence row
      expect(screen.getByText('Data Persistence')).toBeInTheDocument();
      expect(screen.getByText(/Yes - LSM tree with WAL/i)).toBeInTheDocument();
      expect(screen.getByText(/No - Memory only/i)).toBeInTheDocument();
    });

    it('should display Protocol compatibility information', () => {
      render(<HowItWorks />);

      expect(screen.getByText('Protocol')).toBeInTheDocument();
      expect(screen.getByText(/Memcached compatible/i)).toBeInTheDocument();
    });

    it('should display Language comparison', () => {
      render(<HowItWorks />);

      expect(screen.getByText('Language')).toBeInTheDocument();
      expect(screen.getByText('Rust')).toBeInTheDocument();
      expect(screen.getByText('C')).toBeInTheDocument();
    });

    it('should display Data Survives Restart comparison', () => {
      render(<HowItWorks />);

      expect(screen.getByText('Data Survives Restart')).toBeInTheDocument();
      // Check for Yes and No values in cells
      const cells = screen.getAllByRole('cell');
      const yesCell = cells.find(cell => cell.textContent === 'Yes');
      const noCell = cells.find(cell => cell.textContent === 'No');
      expect(yesCell).toBeInTheDocument();
      expect(noCell).toBeInTheDocument();
    });

    it('should display Use Case comparison', () => {
      render(<HowItWorks />);

      expect(screen.getByText('Use Case')).toBeInTheDocument();
      expect(screen.getByText(/Persistent caching, primary storage/i)).toBeInTheDocument();
      expect(screen.getByText(/Volatile caching only/i)).toBeInTheDocument();
    });
  });

  describe('Test Case 3: Table accessibility with scope attributes', () => {
    it('should have column headers with scope="col" for screen readers', () => {
      render(<HowItWorks />);

      const headers = screen.getAllByRole('columnheader');
      expect(headers.length).toBe(3);

      headers.forEach(header => {
        expect(header).toHaveAttribute('scope', 'col');
      });
    });

    it('should render table element for proper semantic structure', () => {
      render(<HowItWorks />);

      const table = screen.getByRole('table');
      expect(table).toBeInTheDocument();
    });

    it('should have thead and tbody sections', () => {
      render(<HowItWorks />);

      const table = screen.getByRole('table');
      const thead = table.querySelector('thead');
      const tbody = table.querySelector('tbody');

      expect(thead).toBeInTheDocument();
      expect(tbody).toBeInTheDocument();
    });
  });

  describe('Section structure', () => {
    it('should have correct section id for navigation', () => {
      const { container } = render(<HowItWorks />);

      const section = container.querySelector('#how-it-works');
      expect(section).toBeInTheDocument();
    });

    it('should display section heading', () => {
      render(<HowItWorks />);

      expect(screen.getByRole('heading', { name: /How It Works/i, level: 2 })).toBeInTheDocument();
    });

    it('should display LSM tree explanation text', () => {
      render(<HowItWorks />);

      expect(screen.getByText(/Log-Structured Merge/i)).toBeInTheDocument();
      expect(screen.getByText(/persistent storage/i)).toBeInTheDocument();
    });
  });
});
