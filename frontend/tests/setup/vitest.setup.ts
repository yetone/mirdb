import '@testing-library/jest-dom';
import { vi } from 'vitest';
import React from 'react';

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => {
  // Create a generic motion component factory that strips framer-motion props
  const createMotionComponent = (tag: string) =>
    React.forwardRef(function MotionComponent(
      props: React.HTMLAttributes<HTMLElement> & Record<string, unknown>,
      ref: React.Ref<HTMLElement>
    ) {
      const { initial, animate, exit, whileHover, whileTap, transition, variants, ...rest } = props;
      return React.createElement(tag, { ...rest, ref });
    });

  return {
    motion: {
      div: createMotionComponent('div'),
      button: createMotionComponent('button'),
      h1: createMotionComponent('h1'),
      h2: createMotionComponent('h2'),
      h3: createMotionComponent('h3'),
      p: createMotionComponent('p'),
      span: createMotionComponent('span'),
      section: createMotionComponent('section'),
      footer: createMotionComponent('footer'),
      header: createMotionComponent('header'),
      nav: createMotionComponent('nav'),
      a: createMotionComponent('a'),
      ul: createMotionComponent('ul'),
      li: createMotionComponent('li'),
      main: createMotionComponent('main'),
      article: createMotionComponent('article'),
      aside: createMotionComponent('aside'),
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
