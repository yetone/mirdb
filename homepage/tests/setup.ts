import '@testing-library/jest-dom';
import { vi } from 'vitest';

// Mock clipboard API for testing
Object.assign(navigator, {
  clipboard: {
    writeText: vi.fn().mockResolvedValue(undefined),
    readText: vi.fn().mockResolvedValue(''),
  },
});
