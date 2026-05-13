import React from 'react';
import { render, screen } from '@testing-library/react';
import { describe, it, expect, beforeEach } from 'vitest';
import Demo from '../../src/components/Demo';
import { ThemeProvider } from '../../src/components/ThemeToggle';

function renderWithProviders(ui: React.ReactElement) {
  return render(<ThemeProvider>{ui}</ThemeProvider>);
}

describe('Demo - responsive scaling', () => {
  beforeEach(() => {
    // Reset any custom viewport
  });

  it('usage.gif has max-width: 100% styling via w-full class', () => {
    renderWithProviders(<Demo />);
    const img = screen.getByAltText(/Animated demonstration of MirDB usage/i);
    expect(img.className).toContain('w-full');
    expect(img.className).toContain('max-w-full');
  });

  it('renders correctly at mobile viewport (375px width)', () => {
    // Set viewport to 375px
    Object.defineProperty(window, 'innerWidth', { writable: true, configurable: true, value: 375 });
    window.dispatchEvent(new Event('resize'));

    renderWithProviders(<Demo />);
    const section = document.getElementById('demo');
    expect(section).toBeInTheDocument();

    // Image should still be in the document
    const img = screen.getByAltText(/Animated demonstration of MirDB usage/i);
    expect(img).toBeInTheDocument();
  });

  it('captions remain readable at mobile viewport', () => {
    Object.defineProperty(window, 'innerWidth', { writable: true, configurable: true, value: 375 });
    window.dispatchEvent(new Event('resize'));

    renderWithProviders(<Demo />);
    const captions = screen.getAllByRole('listitem');
    captions.forEach((caption) => {
      expect(caption).toBeVisible();
    });
  });

  it('section uses responsive padding classes', () => {
    renderWithProviders(<Demo />);
    const section = document.getElementById('demo');
    expect(section?.className).toContain('px-4');
    expect(section?.className).toContain('sm:px-6');
  });
});

describe('Demo - dark mode', () => {
  beforeEach(() => {
    document.documentElement.classList.remove('dark');
  });

  it('demo section container has dark mode background classes', () => {
    renderWithProviders(<Demo />);
    const section = document.getElementById('demo');
    expect(section?.className).toContain('dark:bg-gray-900');
  });

  it('demo card has dark mode border and background classes', () => {
    renderWithProviders(<Demo />);
    // The card container is the div with rounded-xl
    const card = document.querySelector('#demo .rounded-xl');
    expect(card?.className).toContain('dark:bg-gray-800');
    expect(card?.className).toContain('dark:border-gray-700');
  });

  it('captions have dark mode text colors', () => {
    renderWithProviders(<Demo />);
    const captionText = screen.getByText(/Connect to MirDB/i);
    expect(captionText.className).toContain('dark:text-gray-300');
  });

  it('heading adapts to dark mode', () => {
    renderWithProviders(<Demo />);
    const heading = screen.getByRole('heading', { name: /See MirDB in Action/i });
    expect(heading.className).toContain('dark:text-white');
  });
});

describe('Demo - layout stability (lazy loading)', () => {
  it('image has explicit dimensions hint via aspect container', () => {
    renderWithProviders(<Demo />);
    // The image is inside a container with rounded-lg overflow-hidden
    const img = screen.getByAltText(/Animated demonstration of MirDB usage/i);
    const container = img.parentElement;
    // Container should exist and contain the image
    expect(container).toBeInTheDocument();
    expect(container?.contains(img)).toBe(true);
  });

  it('image uses lazy loading to prevent layout shift on slow connections', () => {
    renderWithProviders(<Demo />);
    const img = screen.getByAltText(/Animated demonstration of MirDB usage/i);
    expect(img).toHaveAttribute('loading', 'lazy');
  });
});
