/**
 * Test setup utilities for homepage tests.
 *
 * This file provides common utilities and helpers for testing
 * homepage components.
 */

import { ReactNode } from 'react';

/**
 * Create a mock feature for testing
 */
export function createMockFeature(overrides: Partial<{
  id: string;
  title: string;
  description: string;
  icon: ReactNode;
}> = {}) {
  return {
    id: 'test-feature',
    title: 'Test Feature',
    description: 'Test description for the feature',
    icon: <span data-testid="mock-icon">Icon</span>,
    ...overrides,
  };
}

/**
 * Default test features for FeaturesSection testing
 */
export const testFeatures = [
  createMockFeature({ id: 'feature-1', title: 'URL Shortening', description: 'Shorten your URLs' }),
  createMockFeature({ id: 'feature-2', title: 'Click Analytics', description: 'Track your clicks' }),
  createMockFeature({ id: 'feature-3', title: 'Link Management', description: 'Manage your links' }),
];
