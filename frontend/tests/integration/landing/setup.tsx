/**
 * Test Setup for Landing Page Integration Tests
 *
 * This file is created by the first scenario builder and
 * provides integration test utilities and setup.
 */
import { ReactElement } from 'react';
import { render, RenderOptions } from '@testing-library/react';
import { MemoryRouter, MemoryRouterProps } from 'react-router-dom';
import { ThemeProvider } from '../../../src/contexts/ThemeContext';
import { AuthProvider } from '../../../src/contexts/AuthContext';

interface WrapperOptions {
  initialEntries?: MemoryRouterProps['initialEntries'];
}

// Custom render that wraps components with full app providers
function createWrapper({ initialEntries = ['/'] }: WrapperOptions = {}) {
  return function Wrapper({ children }: { children: React.ReactNode }) {
    return (
      <MemoryRouter initialEntries={initialEntries}>
        <ThemeProvider>
          <AuthProvider>{children}</AuthProvider>
        </ThemeProvider>
      </MemoryRouter>
    );
  };
}

function customRender(
  ui: ReactElement,
  options?: Omit<RenderOptions, 'wrapper'> & WrapperOptions
) {
  const { initialEntries, ...renderOptions } = options || {};
  return render(ui, {
    wrapper: createWrapper({ initialEntries }),
    ...renderOptions,
  });
}

// Re-export everything from testing-library
export * from '@testing-library/react';
export { customRender as render };
