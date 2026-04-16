/**
 * Unit Test Setup for Homepage Components.
 *
 * Provides utilities for testing homepage components with proper context providers.
 */

import '@testing-library/jest-dom';
import { render, RenderOptions } from '@testing-library/react';
import React, { ReactNode } from 'react';
import { BrowserRouter } from 'react-router-dom';
import { AuthProvider } from '../../../src/contexts/AuthContext';
import { ThemeProvider } from '../../../src/contexts/ThemeContext';

// Mock IntersectionObserver for Framer Motion's viewport features
class MockIntersectionObserver implements IntersectionObserver {
  readonly root: Element | null = null;
  readonly rootMargin: string = '';
  readonly thresholds: ReadonlyArray<number> = [];

  constructor(
    private callback: IntersectionObserverCallback,
    _options?: IntersectionObserverInit
  ) {}

  observe(_target: Element): void {
    // Simulate immediate intersection for testing
    this.callback(
      [
        {
          isIntersecting: true,
          boundingClientRect: {} as DOMRectReadOnly,
          intersectionRatio: 1,
          intersectionRect: {} as DOMRectReadOnly,
          rootBounds: null,
          target: {} as Element,
          time: Date.now(),
        },
      ],
      this
    );
  }

  unobserve(_target: Element): void {}
  disconnect(): void {}
  takeRecords(): IntersectionObserverEntry[] {
    return [];
  }
}

global.IntersectionObserver = MockIntersectionObserver;

interface WrapperProps {
  children: ReactNode;
}

function AllProviders({ children }: WrapperProps) {
  return (
    <BrowserRouter>
      <ThemeProvider>
        <AuthProvider>
          {children}
        </AuthProvider>
      </ThemeProvider>
    </BrowserRouter>
  );
}

/**
 * Custom render function that wraps components with providers
 */
export function renderWithProviders(
  ui: React.ReactElement,
  options?: Omit<RenderOptions, 'wrapper'>
) {
  return render(ui, { wrapper: AllProviders, ...options });
}

interface MockAuthContextValue {
  isAuthenticated: boolean;
  username: string | null;
  login: () => void;
  logout: () => void;
}

export function mockAuthContext(overrides?: Partial<MockAuthContextValue>): MockAuthContextValue {
  return {
    isAuthenticated: false,
    username: null,
    login: () => {},
    logout: () => {},
    ...overrides,
  };
}

export * from '@testing-library/react';
