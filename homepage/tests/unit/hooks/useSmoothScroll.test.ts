/**
 * Tests for useSmoothScroll hook.
 * Owner: Scenario 15 - Smooth Scroll Navigation
 */
import { renderHook, act } from '@testing-library/react';
import { useSmoothScroll, scrollToSection } from '@/hooks/useSmoothScroll';

// Mock window.scrollTo
const mockScrollTo = jest.fn();

// Mock history.pushState
const mockPushState = jest.fn();

describe('scrollToSection utility', () => {
  beforeEach(() => {
    jest.clearAllMocks();

    // Mock window.scrollTo
    Object.defineProperty(window, 'scrollTo', {
      value: mockScrollTo,
      writable: true,
    });

    // Mock window.scrollY
    Object.defineProperty(window, 'scrollY', {
      value: 0,
      writable: true,
    });

    // Mock history.pushState
    Object.defineProperty(window.history, 'pushState', {
      value: mockPushState,
      writable: true,
    });

    // Clear any existing test elements
    document.body.innerHTML = '';
  });

  it('should scroll to element with specified ID', () => {
    // Create a target element
    const targetElement = document.createElement('div');
    targetElement.id = 'features';
    document.body.appendChild(targetElement);

    // Mock getBoundingClientRect
    jest.spyOn(targetElement, 'getBoundingClientRect').mockReturnValue({
      top: 500,
      bottom: 600,
      left: 0,
      right: 100,
      width: 100,
      height: 100,
      x: 0,
      y: 500,
      toJSON: () => {},
    });

    scrollToSection('features');

    expect(mockScrollTo).toHaveBeenCalledWith({
      top: 500,
      behavior: 'smooth',
    });
  });

  it('should update URL hash after scrolling', () => {
    const targetElement = document.createElement('div');
    targetElement.id = 'quickstart';
    document.body.appendChild(targetElement);

    jest.spyOn(targetElement, 'getBoundingClientRect').mockReturnValue({
      top: 800,
      bottom: 900,
      left: 0,
      right: 100,
      width: 100,
      height: 100,
      x: 0,
      y: 800,
      toJSON: () => {},
    });

    scrollToSection('quickstart');

    expect(mockPushState).toHaveBeenCalledWith(null, '', '#quickstart');
  });

  it('should not scroll if element does not exist', () => {
    scrollToSection('nonexistent');

    expect(mockScrollTo).not.toHaveBeenCalled();
    expect(mockPushState).not.toHaveBeenCalled();
  });

  it('should apply offset when scrolling', () => {
    const targetElement = document.createElement('div');
    targetElement.id = 'performance';
    document.body.appendChild(targetElement);

    jest.spyOn(targetElement, 'getBoundingClientRect').mockReturnValue({
      top: 1000,
      bottom: 1100,
      left: 0,
      right: 100,
      width: 100,
      height: 100,
      x: 0,
      y: 1000,
      toJSON: () => {},
    });

    scrollToSection('performance', { offset: 80 });

    expect(mockScrollTo).toHaveBeenCalledWith({
      top: 920, // 1000 - 80
      behavior: 'smooth',
    });
  });

  it('should use specified scroll behavior', () => {
    const targetElement = document.createElement('div');
    targetElement.id = 'comparison';
    document.body.appendChild(targetElement);

    jest.spyOn(targetElement, 'getBoundingClientRect').mockReturnValue({
      top: 1200,
      bottom: 1300,
      left: 0,
      right: 100,
      width: 100,
      height: 100,
      x: 0,
      y: 1200,
      toJSON: () => {},
    });

    scrollToSection('comparison', { behavior: 'instant' });

    expect(mockScrollTo).toHaveBeenCalledWith({
      top: 1200,
      behavior: 'instant',
    });
  });
});

describe('useSmoothScroll hook', () => {
  beforeEach(() => {
    jest.clearAllMocks();

    Object.defineProperty(window, 'scrollTo', {
      value: mockScrollTo,
      writable: true,
    });

    Object.defineProperty(window, 'scrollY', {
      value: 0,
      writable: true,
    });

    Object.defineProperty(window.history, 'pushState', {
      value: mockPushState,
      writable: true,
    });

    document.body.innerHTML = '';
  });

  it('should return a scrollTo function', () => {
    const { result } = renderHook(() => useSmoothScroll());

    expect(result.current.scrollTo).toBeDefined();
    expect(typeof result.current.scrollTo).toBe('function');
  });

  it('should scroll to target element when scrollTo is called', () => {
    const targetElement = document.createElement('div');
    targetElement.id = 'features';
    document.body.appendChild(targetElement);

    jest.spyOn(targetElement, 'getBoundingClientRect').mockReturnValue({
      top: 500,
      bottom: 600,
      left: 0,
      right: 100,
      width: 100,
      height: 100,
      x: 0,
      y: 500,
      toJSON: () => {},
    });

    const { result } = renderHook(() => useSmoothScroll());

    act(() => {
      result.current.scrollTo('features');
    });

    expect(mockScrollTo).toHaveBeenCalledWith({
      top: 500,
      behavior: 'smooth',
    });
  });

  it('should update URL hash when scrollTo is called', () => {
    const targetElement = document.createElement('div');
    targetElement.id = 'quickstart';
    document.body.appendChild(targetElement);

    jest.spyOn(targetElement, 'getBoundingClientRect').mockReturnValue({
      top: 800,
      bottom: 900,
      left: 0,
      right: 100,
      width: 100,
      height: 100,
      x: 0,
      y: 800,
      toJSON: () => {},
    });

    const { result } = renderHook(() => useSmoothScroll());

    act(() => {
      result.current.scrollTo('quickstart');
    });

    expect(mockPushState).toHaveBeenCalledWith(null, '', '#quickstart');
  });

  it('should use provided options for scrolling', () => {
    const targetElement = document.createElement('div');
    targetElement.id = 'performance';
    document.body.appendChild(targetElement);

    jest.spyOn(targetElement, 'getBoundingClientRect').mockReturnValue({
      top: 1000,
      bottom: 1100,
      left: 0,
      right: 100,
      width: 100,
      height: 100,
      x: 0,
      y: 1000,
      toJSON: () => {},
    });

    const { result } = renderHook(() =>
      useSmoothScroll({ offset: 60, behavior: 'smooth' })
    );

    act(() => {
      result.current.scrollTo('performance');
    });

    expect(mockScrollTo).toHaveBeenCalledWith({
      top: 940, // 1000 - 60
      behavior: 'smooth',
    });
  });

  it('should handle non-existent elements gracefully', () => {
    const { result } = renderHook(() => useSmoothScroll());

    // Should not throw
    act(() => {
      result.current.scrollTo('nonexistent');
    });

    expect(mockScrollTo).not.toHaveBeenCalled();
  });

  it('should maintain stable scrollTo reference across re-renders', () => {
    const { result, rerender } = renderHook(() => useSmoothScroll());

    const firstScrollTo = result.current.scrollTo;

    rerender();

    const secondScrollTo = result.current.scrollTo;

    expect(firstScrollTo).toBe(secondScrollTo);
  });
});
