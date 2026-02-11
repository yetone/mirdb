/**
 * Unit tests for useScrollTo hook.
 * Owner: Scenario 14 - Smooth Scroll Navigation
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { renderHook, act } from '@testing-library/react'
import { useScrollTo, scrollToElement } from '../../../src/hooks/useScrollTo'

// Mock window.scrollTo
const mockScrollTo = vi.fn()

beforeEach(() => {
  vi.clearAllMocks()

  // Mock window.scrollTo
  Object.defineProperty(window, 'scrollTo', {
    value: mockScrollTo,
    writable: true,
  })

  // Mock window.scrollY
  Object.defineProperty(window, 'scrollY', {
    value: 0,
    writable: true,
  })
})

afterEach(() => {
  vi.clearAllMocks()
  document.body.innerHTML = ''
})

describe('useScrollTo Hook', () => {
  describe('initialization', () => {
    it('should provide scrollTo function', () => {
      const { result } = renderHook(() => useScrollTo())

      expect(typeof result.current.scrollTo).toBe('function')
    })

    it('should accept custom header offset', () => {
      const { result } = renderHook(() => useScrollTo({ headerOffset: 100 }))

      expect(typeof result.current.scrollTo).toBe('function')
    })

    it('should accept custom scroll behavior', () => {
      const { result } = renderHook(() => useScrollTo({ behavior: 'auto' }))

      expect(typeof result.current.scrollTo).toBe('function')
    })
  })

  describe('scrollTo function', () => {
    it('should scroll to element with default header offset (64px)', () => {
      // Create a target element
      const targetElement = document.createElement('div')
      targetElement.id = 'quick-start'
      document.body.appendChild(targetElement)

      // Mock getBoundingClientRect
      targetElement.getBoundingClientRect = vi.fn(() => ({
        top: 500,
        bottom: 600,
        left: 0,
        right: 100,
        width: 100,
        height: 100,
        x: 0,
        y: 500,
        toJSON: () => {},
      }))

      const { result } = renderHook(() => useScrollTo())

      act(() => {
        result.current.scrollTo('quick-start')
      })

      expect(mockScrollTo).toHaveBeenCalledWith({
        top: 500 - 64, // element position - default header offset
        behavior: 'smooth',
      })
    })

    it('should scroll to element with custom header offset', () => {
      const targetElement = document.createElement('div')
      targetElement.id = 'features'
      document.body.appendChild(targetElement)

      targetElement.getBoundingClientRect = vi.fn(() => ({
        top: 800,
        bottom: 900,
        left: 0,
        right: 100,
        width: 100,
        height: 100,
        x: 0,
        y: 800,
        toJSON: () => {},
      }))

      const { result } = renderHook(() => useScrollTo({ headerOffset: 80 }))

      act(() => {
        result.current.scrollTo('features')
      })

      expect(mockScrollTo).toHaveBeenCalledWith({
        top: 800 - 80,
        behavior: 'smooth',
      })
    })

    it('should scroll with auto behavior when specified', () => {
      const targetElement = document.createElement('div')
      targetElement.id = 'demo'
      document.body.appendChild(targetElement)

      targetElement.getBoundingClientRect = vi.fn(() => ({
        top: 300,
        bottom: 400,
        left: 0,
        right: 100,
        width: 100,
        height: 100,
        x: 0,
        y: 300,
        toJSON: () => {},
      }))

      const { result } = renderHook(() => useScrollTo({ behavior: 'auto' }))

      act(() => {
        result.current.scrollTo('demo')
      })

      expect(mockScrollTo).toHaveBeenCalledWith({
        top: 300 - 64,
        behavior: 'auto',
      })
    })

    it('should handle element ID with hash prefix', () => {
      const targetElement = document.createElement('div')
      targetElement.id = 'about'
      document.body.appendChild(targetElement)

      targetElement.getBoundingClientRect = vi.fn(() => ({
        top: 200,
        bottom: 300,
        left: 0,
        right: 100,
        width: 100,
        height: 100,
        x: 0,
        y: 200,
        toJSON: () => {},
      }))

      const { result } = renderHook(() => useScrollTo())

      act(() => {
        result.current.scrollTo('#about')
      })

      expect(mockScrollTo).toHaveBeenCalledWith({
        top: 200 - 64,
        behavior: 'smooth',
      })
    })

    it('should warn and not scroll when element not found', () => {
      const consoleSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})
      const { result } = renderHook(() => useScrollTo())

      act(() => {
        result.current.scrollTo('nonexistent')
      })

      expect(mockScrollTo).not.toHaveBeenCalled()
      expect(consoleSpy).toHaveBeenCalledWith(
        'useScrollTo: Element with id "nonexistent" not found'
      )

      consoleSpy.mockRestore()
    })

    it('should account for current scroll position', () => {
      // Set current scroll position
      Object.defineProperty(window, 'scrollY', {
        value: 100,
        writable: true,
      })

      const targetElement = document.createElement('div')
      targetElement.id = 'test-section'
      document.body.appendChild(targetElement)

      targetElement.getBoundingClientRect = vi.fn(() => ({
        top: 400, // Relative to viewport
        bottom: 500,
        left: 0,
        right: 100,
        width: 100,
        height: 100,
        x: 0,
        y: 400,
        toJSON: () => {},
      }))

      const { result } = renderHook(() => useScrollTo())

      act(() => {
        result.current.scrollTo('test-section')
      })

      expect(mockScrollTo).toHaveBeenCalledWith({
        top: 400 + 100 - 64, // element position + scrollY - header offset
        behavior: 'smooth',
      })
    })
  })

  describe('memoization', () => {
    it('should return stable scrollTo function when options do not change', () => {
      const { result, rerender } = renderHook(() => useScrollTo())

      const firstScrollTo = result.current.scrollTo

      rerender()

      expect(result.current.scrollTo).toBe(firstScrollTo)
    })

    it('should return new scrollTo function when headerOffset changes', () => {
      const { result, rerender } = renderHook(
        ({ headerOffset }) => useScrollTo({ headerOffset }),
        { initialProps: { headerOffset: 64 } }
      )

      const firstScrollTo = result.current.scrollTo

      rerender({ headerOffset: 80 })

      expect(result.current.scrollTo).not.toBe(firstScrollTo)
    })
  })
})

describe('scrollToElement standalone function', () => {
  it('should scroll to element with default options', () => {
    const targetElement = document.createElement('div')
    targetElement.id = 'target'
    document.body.appendChild(targetElement)

    targetElement.getBoundingClientRect = vi.fn(() => ({
      top: 600,
      bottom: 700,
      left: 0,
      right: 100,
      width: 100,
      height: 100,
      x: 0,
      y: 600,
      toJSON: () => {},
    }))

    scrollToElement('target')

    expect(mockScrollTo).toHaveBeenCalledWith({
      top: 600 - 64,
      behavior: 'smooth',
    })
  })

  it('should scroll to element with custom options', () => {
    const targetElement = document.createElement('div')
    targetElement.id = 'custom-target'
    document.body.appendChild(targetElement)

    targetElement.getBoundingClientRect = vi.fn(() => ({
      top: 300,
      bottom: 400,
      left: 0,
      right: 100,
      width: 100,
      height: 100,
      x: 0,
      y: 300,
      toJSON: () => {},
    }))

    scrollToElement('custom-target', { headerOffset: 100, behavior: 'auto' })

    expect(mockScrollTo).toHaveBeenCalledWith({
      top: 300 - 100,
      behavior: 'auto',
    })
  })

  it('should handle hash prefix in element ID', () => {
    const targetElement = document.createElement('div')
    targetElement.id = 'hash-target'
    document.body.appendChild(targetElement)

    targetElement.getBoundingClientRect = vi.fn(() => ({
      top: 200,
      bottom: 300,
      left: 0,
      right: 100,
      width: 100,
      height: 100,
      x: 0,
      y: 200,
      toJSON: () => {},
    }))

    scrollToElement('#hash-target')

    expect(mockScrollTo).toHaveBeenCalledWith({
      top: 200 - 64,
      behavior: 'smooth',
    })
  })

  it('should warn when element not found', () => {
    const consoleSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})

    scrollToElement('missing-element')

    expect(mockScrollTo).not.toHaveBeenCalled()
    expect(consoleSpy).toHaveBeenCalledWith(
      'scrollToElement: Element with id "missing-element" not found'
    )

    consoleSpy.mockRestore()
  })
})
