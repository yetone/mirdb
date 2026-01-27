/**
 * Shared test utilities for homepage tests.
 *
 * Provides:
 * - Custom render function with providers (Router, Auth, Theme)
 * - Mock data for testing
 * - Common test helpers
 */

import React, { ReactElement } from 'react';
import { render, RenderOptions } from '@testing-library/react';
import { BrowserRouter, MemoryRouter } from 'react-router-dom';
import { vi } from 'vitest';

interface WrapperProps {
  children: React.ReactNode;
}

interface CustomRenderOptions extends Omit<RenderOptions, 'wrapper'> {
  initialRoute?: string;
  useMemoryRouter?: boolean;
}

const AllProviders: React.FC<WrapperProps> = ({ children }) => {
  return <BrowserRouter>{children}</BrowserRouter>;
};

export const renderWithRouter = (
  ui: React.ReactElement,
  options: CustomRenderOptions = {}
) => {
  const { initialRoute = '/', useMemoryRouter = false, ...renderOptions } = options;

  if (useMemoryRouter) {
    return render(
      <MemoryRouter initialEntries={[initialRoute]}>{ui}</MemoryRouter>,
      renderOptions
    );
  }

  return render(ui, { wrapper: AllProviders, ...renderOptions });
};

const customRender = (
  ui: ReactElement,
  options?: Omit<RenderOptions, 'wrapper'>
) => render(ui, { wrapper: AllProviders, ...options });

export const mockNavigate = vi.fn();

// Mock analytics data for testing
export const mockAnalyticsData = {
  totalClicks: 12847,
  uniqueVisitors: 8392,
  topReferrers: [
    { name: 'Google', value: 4521 },
    { name: 'Twitter', value: 2834 },
    { name: 'Facebook', value: 1923 },
    { name: 'Direct', value: 3569 },
  ],
  browsers: [
    { name: 'Chrome', value: 45 },
    { name: 'Firefox', value: 25 },
    { name: 'Safari', value: 20 },
    { name: 'Edge', value: 10 },
  ],
  locations: [
    { name: 'United States', value: 5234 },
    { name: 'United Kingdom', value: 2145 },
    { name: 'Germany', value: 1823 },
    { name: 'Canada', value: 1456 },
    { name: 'Australia', value: 1189 },
  ],
  clicksOverTime: [
    { date: 'Mon', clicks: 1200 },
    { date: 'Tue', clicks: 1800 },
    { date: 'Wed', clicks: 2100 },
    { date: 'Thu', clicks: 1950 },
    { date: 'Fri', clicks: 2400 },
    { date: 'Sat', clicks: 1700 },
    { date: 'Sun', clicks: 1697 },
  ],
};

export * from '@testing-library/react';
export { customRender as render };
