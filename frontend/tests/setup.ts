/**
 * Test setup file for Vitest with React Testing Library
 *
 * This file is executed before each test file and sets up:
 * - Jest-DOM matchers for DOM assertions
 * - Global cleanup after each test
 */
import '@testing-library/jest-dom';
import { cleanup } from '@testing-library/react';
import { afterEach } from 'vitest';

// Cleanup after each test case
afterEach(() => {
  cleanup();
});
