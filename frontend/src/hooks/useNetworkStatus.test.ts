import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { renderHook, act } from '@testing-library/react'
import { useNetworkStatus } from './useNetworkStatus'

/**
 * Integration tests for useNetworkStatus hook
 * Test Case 3: Test content API failure - handles network state detection
 */
describe('useNetworkStatus Hook', () => {
  beforeEach(() => {
    vi.stubGlobal('navigator', {
      ...window.navigator,
      onLine: true,
    })
  })

  afterEach(() => {
    vi.unstubAllGlobals()
  })

  it('returns true when online', () => {
    const { result } = renderHook(() => useNetworkStatus())
    expect(result.current.isOnline).toBe(true)
  })

  it('returns false when offline', () => {
    vi.stubGlobal('navigator', {
      ...window.navigator,
      onLine: false,
    })

    const { result } = renderHook(() => useNetworkStatus())
    expect(result.current.isOnline).toBe(false)
  })

  it('updates when going offline', async () => {
    const { result } = renderHook(() => useNetworkStatus())
    expect(result.current.isOnline).toBe(true)

    await act(async () => {
      vi.stubGlobal('navigator', {
        ...window.navigator,
        onLine: false,
      })
      window.dispatchEvent(new Event('offline'))
    })

    expect(result.current.isOnline).toBe(false)
  })

  it('updates when coming online', async () => {
    vi.stubGlobal('navigator', {
      ...window.navigator,
      onLine: false,
    })

    const { result } = renderHook(() => useNetworkStatus())
    expect(result.current.isOnline).toBe(false)

    await act(async () => {
      vi.stubGlobal('navigator', {
        ...window.navigator,
        onLine: true,
      })
      window.dispatchEvent(new Event('online'))
    })

    expect(result.current.isOnline).toBe(true)
  })

  it('cleans up event listeners on unmount', () => {
    const addEventSpy = vi.spyOn(window, 'addEventListener')
    const removeEventSpy = vi.spyOn(window, 'removeEventListener')

    const { unmount } = renderHook(() => useNetworkStatus())

    expect(addEventSpy).toHaveBeenCalledWith('online', expect.any(Function))
    expect(addEventSpy).toHaveBeenCalledWith('offline', expect.any(Function))

    unmount()

    expect(removeEventSpy).toHaveBeenCalledWith('online', expect.any(Function))
    expect(removeEventSpy).toHaveBeenCalledWith('offline', expect.any(Function))

    addEventSpy.mockRestore()
    removeEventSpy.mockRestore()
  })
})
