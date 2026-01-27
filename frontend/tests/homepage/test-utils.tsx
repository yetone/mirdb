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

import { ReactNode } from 'react';
import { render, RenderOptions } from '@testing-library/react';
import { BrowserRouter, MemoryRouter } from 'react-router-dom';
import { vi } from 'vitest';

// Mock navigate function
export const mockNavigate = vi.fn();

// Mock useNavigate hook
vi.mock('react-router-dom', async () => {
  const actual = await vi.importActual('react-router-dom');
  return {
    ...actual,
    useNavigate: () => mockNavigate,
  };
});

// Theme context mock values
export const mockTheme = {
  theme: 'light',
  setTheme: vi.fn(),
};

interface ProvidersProps {
  children: ReactNode;
  initialRoute?: string;
}

// Wrapper with all providers
function AllProviders({ children, initialRoute = '/' }: ProvidersProps) {
  return (
    <MemoryRouter initialEntries={[initialRoute]}>
      {children}
    </MemoryRouter>
  );
}

interface CustomRenderOptions extends Omit<RenderOptions, 'wrapper'> {
  initialRoute?: string;
}

// Custom render function with providers
export function renderWithProviders(
  ui: React.ReactElement,
  options: CustomRenderOptions = {}
) {
  const { initialRoute, ...renderOptions } = options;

  return render(ui, {
    wrapper: ({ children }) => (
      <AllProviders initialRoute={initialRoute}>{children}</AllProviders>
    ),
    ...renderOptions,
  });
}

// Create matchMedia mock for responsive tests
export function createMatchMedia(width: number) {
  return (query: string): MediaQueryList => ({
    matches: query.includes(`min-width: ${width}`) ||
             (query.includes('min-width') &&
              parseInt(query.match(/\d+/)?.[0] || '0') <= width),
    media: query,
    onchange: null,
    addListener: vi.fn(),
    removeListener: vi.fn(),
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    dispatchEvent: vi.fn(() => false),
  });
}

// Set viewport width for responsive tests
export const setViewportWidth = (width: number) => {
  Object.defineProperty(window, 'innerWidth', {
    writable: true,
    configurable: true,
    value: width,
  });
  window.matchMedia = createMatchMedia(width);
  window.dispatchEvent(new Event('resize'));
};

// Reset all mocks between tests
export function resetMocks() {
  mockNavigate.mockClear();
  mockTheme.setTheme.mockClear();
}

export { render };
