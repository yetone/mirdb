/**
 * Copy Button Component
 * Owner: Scenario 4 - Quick Start Section
 *
 * Button to copy text to clipboard:
 * - Shows "Copy" icon by default
 * - Shows "Copied!" feedback
 * - Accessible label
 */
import { useCopyToClipboard } from '@/hooks/useCopyToClipboard'
import { cn } from '@/utils/cn'
import styles from './CopyButton.module.css'

export interface CopyButtonProps {
  text: string
  className?: string
  'aria-label'?: string
}

export function CopyButton({ text, className, 'aria-label': ariaLabel }: CopyButtonProps) {
  const { copy, copied } = useCopyToClipboard()

  const handleClick = () => {
    copy(text)
  }

  return (
    <button
      type="button"
      className={cn(styles.copyButton, copied && styles.copied, className)}
      onClick={handleClick}
      aria-label={ariaLabel || 'Copy to clipboard'}
      title={copied ? 'Copied!' : 'Copy to clipboard'}
    >
      {copied ? (
        <>
          <svg
            xmlns="http://www.w3.org/2000/svg"
            width="16"
            height="16"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
            aria-hidden="true"
            className={styles.icon}
          >
            <polyline points="20 6 9 17 4 12" />
          </svg>
          <span className={styles.label}>Copied!</span>
        </>
      ) : (
        <>
          <svg
            xmlns="http://www.w3.org/2000/svg"
            width="16"
            height="16"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
            aria-hidden="true"
            className={styles.icon}
          >
            <rect x="9" y="9" width="13" height="13" rx="2" ry="2" />
            <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1" />
          </svg>
          <span className={styles.label}>Copy</span>
        </>
      )}
    </button>
  )
}
