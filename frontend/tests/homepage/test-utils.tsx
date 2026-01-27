/**
 * Shared test utilities for homepage tests.
 * Created by the first scenario builder.
 *
 * Expected exports:
 * - renderWithProviders(component) - wraps with Router, Theme, Auth contexts
 * - mockNavigate - mocked useNavigate function
 * - mockTheme - mock theme context values
 * - createMatchMedia(width) - mock matchMedia for responsive tests
 */
import React, { ReactElement } from 'react';
import { render, RenderOptions } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';

interface WrapperProps {
  children: React.ReactNode;
}

const AllProviders = ({ children }: WrapperProps) => {
  return <BrowserRouter>{children}</BrowserRouter>;
};

export const renderWithProviders = (
  ui: ReactElement,
  options?: Omit<RenderOptions, 'wrapper'>
) => {
  return render(ui, { wrapper: AllProviders, ...options });
};

export const mockNavigate = vi.fn();

export const mockTheme = {
  theme: 'light',
  setTheme: vi.fn(),
};

export const createMatchMedia = (width: number) => {
  return (query: string): MediaQueryList => {
    const matches = query.includes('min-width')
      ? width >= parseInt(query.match(/\d+/)?.[0] || '0')
      : width <= parseInt(query.match(/\d+/)?.[0] || '0');

    return {
      matches,
      media: query,
      onchange: null,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      dispatchEvent: vi.fn(),
    };
  };
};

export const setViewportWidth = (width: number) => {
  Object.defineProperty(window, 'innerWidth', {
    writable: true,
    configurable: true,
    value: width,
  });
  window.matchMedia = createMatchMedia(width);
  window.dispatchEvent(new Event('resize'));
};
