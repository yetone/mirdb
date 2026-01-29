import '@testing-library/jest-dom/vitest';
import { vi } from 'vitest';

// Mock the clipboard API globally for all tests
const mockWriteText = vi.fn().mockResolvedValue(undefined);

Object.defineProperty(navigator, 'clipboard', {
  value: {
    writeText: mockWriteText,
  },
  writable: true,
  configurable: true,
});
