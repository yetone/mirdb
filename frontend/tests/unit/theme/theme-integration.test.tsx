/**
 * Integration tests for theme system.
 * Covers REQ-12 (localStorage persistence, no flash, component adaptation).
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import React from 'react';
import App from '../../../src/App';

describe('Theme Integration', () => {
  beforeEach(() => {
    document.documentElement.classList.remove('dark');
    window.localStorage.clear();
    vi.stubGlobal(
      'matchMedia',
      vi.fn((query: string) => ({
        matches: false,
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      }))
    );
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('renders app with default light theme', () => {
    render(<App />);
    expect(document.documentElement.classList.contains('dark')).toBe(false);
  });

  it('renders app in dark theme when localStorage has dark', () => {
    window.localStorage.setItem('theme', 'dark');
    render(<App />);
    expect(document.documentElement.classList.contains('dark')).toBe(true);
  });

  it('theme toggle is visible in the app', () => {
    render(<App />);
    expect(screen.getByTestId('theme-toggle-button')).toBeInTheDocument();
  });

  it('clicking toggle switches theme and persists to localStorage', async () => {
    const user = userEvent.setup();
    render(<App />);
    expect(window.localStorage.getItem('theme')).toBe('light');
    await user.click(screen.getByTestId('theme-toggle-button'));
    expect(document.documentElement.classList.contains('dark')).toBe(true);
    expect(window.localStorage.getItem('theme')).toBe('dark');
  });

  it('clicking toggle twice returns to light theme', async () => {
    const user = userEvent.setup();
    render(<App />);
    await user.click(screen.getByTestId('theme-toggle-button'));
    await user.click(screen.getByTestId('theme-toggle-button'));
    expect(document.documentElement.classList.contains('dark')).toBe(false);
    expect(window.localStorage.getItem('theme')).toBe('light');
  });

  it('dark theme persists across re-renders', async () => {
    const user = userEvent.setup();
    const { unmount } = render(<App />);
    await user.click(screen.getByTestId('theme-toggle-button'));
    expect(window.localStorage.getItem('theme')).toBe('dark');
    unmount();
    document.documentElement.classList.remove('dark');
    render(<App />);
    expect(document.documentElement.classList.contains('dark')).toBe(true);
  });

  it('header contains the theme toggle', () => {
    render(<App />);
    const header = screen.getByTestId('layout-header');
    expect(header).toContainElement(screen.getByTestId('theme-toggle-button'));
  });

  it('app container has layout root applied', () => {
    render(<App />);
    const appContainer = document.querySelector('.layout');
    expect(appContainer).toBeInTheDocument();
  });
});
