/**
 * Copy Button Component
 * Owner: Scenario 5 - Copy to Clipboard Functionality
 *
 * Reusable copy-to-clipboard button with visual feedback.
 *
 * Behavior:
 * - Shows "Copy" by default
 * - Shows "Copied!" for 2 seconds after successful copy
 * - Handles errors gracefully
 */

import React from 'react'
import { useClipboard } from '@/hooks/useClipboard'
import { CopyButtonProps } from '@/types/home'

/**
 * CopyButton - A button that copies text to clipboard with visual feedback
 */
export const CopyButton: React.FC<CopyButtonProps> = ({
  text,
  className = '',
  onCopy,
  onError,
}) => {
  const { copy, copied, error } = useClipboard()

  const handleClick = async () => {
    const success = await copy(text)
    if (success) {
      onCopy?.()
    } else if (error) {
      onError?.(error)
    }
  }

  // Call onError when error state changes
  React.useEffect(() => {
    if (error && onError) {
      onError(error)
    }
  }, [error, onError])

  return (
    <button
      type="button"
      onClick={handleClick}
      className={`btn btn-sm ${copied ? 'btn-success' : 'btn-primary'} ${className}`}
      aria-label={copied ? 'Copied to clipboard' : 'Copy to clipboard'}
    >
      {copied ? 'Copied!' : 'Copy'}
    </button>
  )
}

export default CopyButton
