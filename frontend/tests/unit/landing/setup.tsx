/**
 * Test Setup for Landing Page Unit Tests
 *
 * This file is created by the first scenario builder and
 * provides shared test utilities and setup.
 */
import { ReactElement } from 'react';
import { render, RenderOptions } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import { ThemeProvider } from '../../../src/contexts/ThemeContext';

interface WrapperProps {
  children: React.ReactNode;
}

// Custom render that wraps components with necessary providers
function AllProviders({ children }: WrapperProps) {
  return (
    <BrowserRouter>
      <ThemeProvider>{children}</ThemeProvider>
    </BrowserRouter>
  );
}

function customRender(
  ui: ReactElement,
  options?: Omit<RenderOptions, 'wrapper'>
) {
  return render(ui, { wrapper: AllProviders, ...options });
}

// Re-export everything from testing-library
export * from '@testing-library/react';
export { customRender as render };
