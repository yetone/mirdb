/**
 * React Router mocking utilities for tests.
 * Owner: First scenario builder
 *
 * Expected exports:
 * - createMockRouter(initialEntries?: string[]): MemoryRouter config
 * - mockNavigate: jest.Mock
 * - mockLocation: jest.Mock
 */

import { vi } from 'vitest';

export const mockNavigate = vi.fn();
export const mockLocation = vi.fn();

export function createMockRouter(initialEntries: string[] = ['/']) {
  return {
    initialEntries,
    initialIndex: 0,
  };
}
