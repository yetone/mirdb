/**
 * Test Setup and Utilities
 *
 * Shared test configuration:
 * - React Testing Library setup
 * - Mock providers (Theme, Auth contexts)
 * - Custom render utilities
 * - Common test data fixtures
 */

import '@testing-library/jest-dom';
import { cleanup } from '@testing-library/react';
import { afterEach, vi } from 'vitest';

// Mock IntersectionObserver for framer-motion's whileInView feature
const mockIntersectionObserver = vi.fn();
mockIntersectionObserver.mockReturnValue({
  observe: vi.fn(),
  unobserve: vi.fn(),
  disconnect: vi.fn(),
});
window.IntersectionObserver = mockIntersectionObserver;

// Mock ResizeObserver for framer-motion
const mockResizeObserver = vi.fn();
mockResizeObserver.mockReturnValue({
  observe: vi.fn(),
  unobserve: vi.fn(),
  disconnect: vi.fn(),
});
window.ResizeObserver = mockResizeObserver;

// Cleanup after each test
afterEach(() => {
  cleanup();
});
