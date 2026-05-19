import { describe, it, expect, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from '../../../src/contexts/ThemeContext';
import { ThemeToggle } from '../../../src/components/shared/ThemeToggle';
import { SUPPORTED_THEMES, DEFAULT_THEME } from '../../../src/utils/constants';

describe('ThemeToggle', () => {
  beforeEach(() => {
    localStorage.clear();
    document.documentElement.removeAttribute('data-theme');
  });

  it('renders theme toggle button', () => {
    render(
      <ThemeProvider>
        <ThemeToggle />
      </ThemeProvider>
    );

    expect(screen.getByTestId('theme-toggle')).toBeInTheDocument();
    expect(screen.getByTestId('theme-toggle-button')).toBeInTheDocument();
  });

  it('displays current theme on the toggle button', () => {
    localStorage.setItem('theme', 'light');
    render(
      <ThemeProvider>
        <ThemeToggle />
      </ThemeProvider>
    );

    expect(screen.getByTestId('theme-toggle-button').textContent).toContain('light');
  });

  it('opens theme menu when toggle button is clicked', () => {
    render(
      <ThemeProvider>
        <ThemeToggle />
      </ThemeProvider>
    );

    expect(screen.queryByTestId('theme-menu')).not.toBeInTheDocument();

    fireEvent.click(screen.getByTestId('theme-toggle-button'));

    expect(screen.getByTestId('theme-menu')).toBeInTheDocument();
    for (const theme of SUPPORTED_THEMES) {
      expect(screen.getByTestId(`theme-option-${theme}`)).toBeInTheDocument();
    }
  });

  it('changes theme when a theme option is selected', () => {
    render(
      <ThemeProvider>
        <ThemeToggle />
      </ThemeProvider>
    );

    fireEvent.click(screen.getByTestId('theme-toggle-button'));
    fireEvent.click(screen.getByTestId('theme-option-cyberpunk'));

    expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk');
    expect(screen.getByTestId('theme-toggle-button').textContent).toContain('cyberpunk');
  });

  it('closes menu after selecting a theme', () => {
    render(
      <ThemeProvider>
        <ThemeToggle />
      </ThemeProvider>
    );

    fireEvent.click(screen.getByTestId('theme-toggle-button'));
    expect(screen.getByTestId('theme-menu')).toBeInTheDocument();

    fireEvent.click(screen.getByTestId('theme-option-light'));
    expect(screen.queryByTestId('theme-menu')).not.toBeInTheDocument();
  });

  it('cycles through all themes without errors', () => {
    render(
      <ThemeProvider>
        <ThemeToggle />
      </ThemeProvider>
    );

    for (const theme of SUPPORTED_THEMES) {
      fireEvent.click(screen.getByTestId('theme-toggle-button'));
      fireEvent.click(screen.getByTestId(`theme-option-${theme}`));
      expect(document.documentElement.getAttribute('data-theme')).toBe(theme);
    }
  });

  it('has proper ARIA attributes for accessibility', () => {
    render(
      <ThemeProvider>
        <ThemeToggle />
      </ThemeProvider>
    );

    const button = screen.getByTestId('theme-toggle-button');
    expect(button).toHaveAttribute('aria-label', 'Toggle theme menu');
    expect(button).toHaveAttribute('aria-expanded', 'false');
    expect(button).toHaveAttribute('aria-haspopup', 'listbox');

    fireEvent.click(button);
    expect(button).toHaveAttribute('aria-expanded', 'true');

    const listbox = screen.getByRole('listbox');
    expect(listbox).toHaveAttribute('aria-label', 'Select theme');
  });
});
