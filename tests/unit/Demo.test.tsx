import React from 'react';
import { render, screen } from '@testing-library/react';
import { describe, it, expect } from 'vitest';
import Demo from '../../src/components/Demo';
import { ThemeProvider } from '../../src/components/ThemeToggle';

function renderWithProviders(ui: React.ReactElement) {
  return render(<ThemeProvider>{ui}</ThemeProvider>);
}

describe('Demo - usage.gif rendering', () => {
  it('renders the demo section with correct heading', () => {
    renderWithProviders(<Demo />);
    expect(screen.getByRole('heading', { name: /See MirDB in Action/i })).toBeInTheDocument();
  });

  it('renders usage.gif with correct src attribute', () => {
    renderWithProviders(<Demo />);
    const img = screen.getByAltText(/Animated demonstration of MirDB usage/i);
    expect(img).toBeInTheDocument();
    expect(img).toHaveAttribute('src', '/assets/usage.gif');
  });

  it('usage.gif has lazy loading attribute', () => {
    renderWithProviders(<Demo />);
    const img = screen.getByAltText(/Animated demonstration of MirDB usage/i);
    expect(img).toHaveAttribute('loading', 'lazy');
  });

  it('usage.gif is in an img tag (not background image) so it animates', () => {
    renderWithProviders(<Demo />);
    const img = screen.getByAltText(/Animated demonstration of MirDB usage/i);
    expect(img.tagName).toBe('IMG');
  });
});

describe('Demo - captions', () => {
  it('renders at least one caption element', () => {
    renderWithProviders(<Demo />);
    const captionsList = screen.getByLabelText('Demo command explanations');
    expect(captionsList).toBeInTheDocument();
    expect(captionsList.children.length).toBeGreaterThanOrEqual(1);
  });

  it('renders all 4 demo captions', () => {
    renderWithProviders(<Demo />);
    const items = screen.getAllByRole('listitem');
    expect(items.length).toBe(4);
  });

  it('captions describe commands related to the demo', () => {
    renderWithProviders(<Demo />);
    expect(screen.getByText(/Connect to MirDB/i)).toBeInTheDocument();
    expect(screen.getByText(/SET/i)).toBeInTheDocument();
    expect(screen.getByText(/GET/i)).toBeInTheDocument();
    expect(screen.getByText(/DELETE/i)).toBeInTheDocument();
  });

  it('captions are numbered for step-by-step walkthrough', () => {
    renderWithProviders(<Demo />);
    expect(screen.getByText('1')).toBeInTheDocument();
    expect(screen.getByText('2')).toBeInTheDocument();
    expect(screen.getByText('3')).toBeInTheDocument();
    expect(screen.getByText('4')).toBeInTheDocument();
  });
});
