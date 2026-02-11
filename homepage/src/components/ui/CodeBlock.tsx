/**
 * Code block component with syntax highlighting and copy button.
 * Owner: Scenario 5 - Quick Start Section
 *
 * Requirements:
 * - REQ-9: Display code examples
 * - US-2: Copy-to-clipboard with visual feedback
 */

import { useCopyToClipboard } from '../../hooks/useCopyToClipboard'
import { classNames } from '../../utils'
import './CodeBlock.css'

interface CodeBlockProps {
  code: string
  language?: string
  title?: string
  className?: string
}

export function CodeBlock({ code, language = 'bash', title, className }: CodeBlockProps) {
  const { copy, copied } = useCopyToClipboard()

  const handleCopy = () => {
    copy(code)
  }

  return (
    <div className={classNames('code-block', className)}>
      {title && (
        <div className="code-block__header">
          <span className="code-block__title">{title}</span>
          <span className="code-block__language">{language}</span>
        </div>
      )}
      <div className="code-block__container">
        <pre className="code-block__pre">
          <code className={`code-block__code language-${language}`}>
            {code}
          </code>
        </pre>
        <button
          type="button"
          className={classNames('code-block__copy', copied && 'code-block__copy--copied')}
          onClick={handleCopy}
          aria-label={copied ? 'Copied to clipboard' : 'Copy code to clipboard'}
        >
          {copied ? (
            <>
              <svg
                className="code-block__icon"
                width="16"
                height="16"
                viewBox="0 0 16 16"
                fill="none"
                xmlns="http://www.w3.org/2000/svg"
                aria-hidden="true"
              >
                <path
                  d="M13.5 4.5L6 12L2.5 8.5"
                  stroke="currentColor"
                  strokeWidth="2"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                />
              </svg>
              <span className="code-block__copy-text">Copied!</span>
            </>
          ) : (
            <>
              <svg
                className="code-block__icon"
                width="16"
                height="16"
                viewBox="0 0 16 16"
                fill="none"
                xmlns="http://www.w3.org/2000/svg"
                aria-hidden="true"
              >
                <rect
                  x="5"
                  y="5"
                  width="9"
                  height="9"
                  rx="1"
                  stroke="currentColor"
                  strokeWidth="1.5"
                />
                <path
                  d="M11 5V3C11 2.44772 10.5523 2 10 2H3C2.44772 2 2 2.44772 2 3V10C2 10.5523 2.44772 11 3 11H5"
                  stroke="currentColor"
                  strokeWidth="1.5"
                />
              </svg>
              <span className="code-block__copy-text">Copy</span>
            </>
          )}
        </button>
      </div>
    </div>
  )
}
