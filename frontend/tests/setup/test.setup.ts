import '@testing-library/jest-dom';
import { vi } from 'vitest';
import React from 'react';

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => {
  const MotionDiv = React.forwardRef(function MotionDiv(
    props: React.HTMLAttributes<HTMLDivElement> & Record<string, unknown>,
    ref: React.Ref<HTMLDivElement>
  ) {
    const { initial, animate, exit, whileHover, whileTap, transition, ...rest } = props;
    return React.createElement('div', { ...rest, ref });
  });

  const MotionButton = React.forwardRef(function MotionButton(
    props: React.HTMLAttributes<HTMLButtonElement> & Record<string, unknown>,
    ref: React.Ref<HTMLButtonElement>
  ) {
    const { initial, animate, exit, whileHover, whileTap, transition, ...rest } = props;
    return React.createElement('button', { ...rest, ref });
  });

  return {
    motion: {
      div: MotionDiv,
      button: MotionButton,
    },
    AnimatePresence: ({ children }: { children: React.ReactNode }) => children,
  };
});

// Mock localStorage with a real implementation for tests
const localStorageStore: Record<string, string> = {};
const localStorageMock = {
  getItem: vi.fn((key: string) => localStorageStore[key] || null),
  setItem: vi.fn((key: string, value: string) => {
    localStorageStore[key] = value;
  }),
  removeItem: vi.fn((key: string) => {
    delete localStorageStore[key];
  }),
  clear: vi.fn(() => {
    Object.keys(localStorageStore).forEach(key => delete localStorageStore[key]);
  }),
};

Object.defineProperty(window, 'localStorage', { value: localStorageMock });

// Mock matchMedia
Object.defineProperty(window, 'matchMedia', {
  writable: true,
  value: vi.fn().mockImplementation((query) => ({
    matches: false,
    media: query,
    onchange: null,
    addListener: vi.fn(),
    removeListener: vi.fn(),
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    dispatchEvent: vi.fn(),
  })),
});

// Mock ResizeObserver
class ResizeObserverMock {
  observe = vi.fn();
  unobserve = vi.fn();
  disconnect = vi.fn();
}

window.ResizeObserver = ResizeObserverMock;

// Mock IntersectionObserver
class IntersectionObserverMock {
  observe = vi.fn();
  unobserve = vi.fn();
  disconnect = vi.fn();
}

window.IntersectionObserver = IntersectionObserverMock as unknown as typeof IntersectionObserver;

// Mock canvas for BackgroundEffect
HTMLCanvasElement.prototype.getContext = vi.fn().mockReturnValue({
  clearRect: vi.fn(),
  beginPath: vi.fn(),
  arc: vi.fn(),
  fill: vi.fn(),
  fillStyle: '',
});
