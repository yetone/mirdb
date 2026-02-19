# Copy to Clipboard React Hook

## Overview

A reusable React hook for clipboard copy functionality with visual feedback. The hook uses the modern Clipboard API and provides auto-resetting state to show "Copied!" feedback that disappears after a configurable delay.

## When to Use This Skill

Use this skill when users request:

- Copy-to-clipboard buttons for code snippets
- Copy functionality for URLs, API keys, or other text
- Any feature that needs "Copied!" visual feedback

## Implementation

### The Hook

```typescript
import { useState, useCallback } from 'react'

interface UseCopyToClipboardReturn {
  copy: (text: string) => Promise<boolean>
  copied: boolean
  error: Error | null
}

export function useCopyToClipboard(resetDelay = 2000): UseCopyToClipboardReturn {
  const [copied, setCopied] = useState(false)
  const [error, setError] = useState<Error | null>(null)

  const copy = useCallback(async (text: string): Promise<boolean> => {
    if (!navigator?.clipboard) {
      const err = new Error('Clipboard API not available')
      setError(err)
      return false
    }

    try {
      await navigator.clipboard.writeText(text)
      setCopied(true)
      setError(null)

      setTimeout(() => {
        setCopied(false)
      }, resetDelay)

      return true
    } catch (err) {
      const error = err instanceof Error ? err : new Error('Failed to copy')
      setError(error)
      setCopied(false)
      return false
    }
  }, [resetDelay])

  return { copy, copied, error }
}
```

### Usage in a Component

```tsx
import { useCopyToClipboard } from '@/hooks/useCopyToClipboard'

function CopyButton({ text }: { text: string }) {
  const { copy, copied } = useCopyToClipboard()

  return (
    <button onClick={() => copy(text)}>
      {copied ? 'Copied!' : 'Copy'}
    </button>
  )
}
```

## Testing Pattern

Testing clipboard operations requires mocking the Clipboard API and using fake timers:

```typescript
import { vi, beforeEach, afterEach } from 'vitest'
import { renderHook, act } from '@testing-library/react'
import { useCopyToClipboard } from './useCopyToClipboard'

describe('useCopyToClipboard', () => {
  const mockWriteText = vi.fn()
  const originalClipboard = navigator.clipboard

  beforeEach(() => {
    vi.useFakeTimers()
    Object.defineProperty(navigator, 'clipboard', {
      value: { writeText: mockWriteText },
      writable: true,
      configurable: true,
    })
    mockWriteText.mockResolvedValue(undefined)
  })

  afterEach(() => {
    vi.useRealTimers()
    vi.clearAllMocks()
    Object.defineProperty(navigator, 'clipboard', {
      value: originalClipboard,
      writable: true,
      configurable: true,
    })
  })

  it('copies text and resets after delay', async () => {
    const { result } = renderHook(() => useCopyToClipboard(1000))

    await act(async () => {
      await result.current.copy('test')
    })

    expect(result.current.copied).toBe(true)

    act(() => {
      vi.advanceTimersByTime(1000)
    })

    expect(result.current.copied).toBe(false)
  })
})
```

## Best Practices

- Use `useCallback` to memoize the copy function
- Provide error handling for browsers without Clipboard API
- Auto-reset the copied state so users can copy again
- Default reset delay of 2000ms works well for UX
- In tests, use `await Promise.resolve()` chains when using fake timers with async operations
