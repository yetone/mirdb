import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import Comparison from '../../src/components/Comparison';

describe('Comparison - Mobile responsiveness', () => {
  const originalInnerWidth = window.innerWidth;

  afterEach(() => {
    Object.defineProperty(window, 'innerWidth', {
      writable: true,
      configurable: true,
      value: originalInnerWidth,
    });
    window.dispatchEvent(new Event('resize'));
  });

  it('renders comparison table with horizontal scroll container at 375px viewport width', () => {
    Object.defineProperty(window, 'innerWidth', {
      writable: true,
      configurable: true,
      value: 375,
    });
    window.dispatchEvent(new Event('resize'));

    render(<Comparison />);

    const table = screen.getByRole('table');
    expect(table).toBeInTheDocument();

    const scrollContainer = table.closest('.overflow-x-auto');
    expect(scrollContainer).toBeInTheDocument();

    expect(screen.getByRole('rowheader', { name: 'Persistence' })).toBeInTheDocument();
    expect(screen.getByRole('rowheader', { name: 'Memcached Protocol' })).toBeInTheDocument();
    expect(screen.getByRole('rowheader', { name: 'Storage Engine' })).toBeInTheDocument();
    expect(screen.getByRole('rowheader', { name: 'Compaction' })).toBeInTheDocument();
    expect(screen.getByRole('rowheader', { name: 'Write Performance' })).toBeInTheDocument();

    expect(screen.getByRole('columnheader', { name: 'MirDB' })).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: 'memcached' })).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: 'Redis' })).toBeInTheDocument();
  });

  it('shows a scroll hint on small screens', () => {
    Object.defineProperty(window, 'innerWidth', {
      writable: true,
      configurable: true,
      value: 375,
    });
    window.dispatchEvent(new Event('resize'));

    render(<Comparison />);

    const hint = screen.getByText(/scroll horizontally/i);
    expect(hint).toBeInTheDocument();
  });

  it('renders all data cells accessible even at narrow width', () => {
    Object.defineProperty(window, 'innerWidth', {
      writable: true,
      configurable: true,
      value: 375,
    });
    window.dispatchEvent(new Event('resize'));

    render(<Comparison />);

    const yesIndicators = screen.getAllByText('Yes');
    const noIndicators = screen.getAllByText('No');

    expect(yesIndicators.length).toBeGreaterThanOrEqual(3);
    expect(noIndicators.length).toBeGreaterThanOrEqual(3);
  });
});
