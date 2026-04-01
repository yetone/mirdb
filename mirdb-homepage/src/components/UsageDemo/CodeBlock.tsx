/**
 * Code Block Component with copy functionality.
 * Owner: Scenario 2 - Usage Demo Section
 *
 * Features:
 * - Terminal-style appearance (dark background, monospace font)
 * - Syntax highlighting for memcached commands
 * - Copy-to-clipboard button
 * - Visual feedback on copy success
 */

import { useState, useCallback } from 'react'

interface CodeBlockProps {
  code: string
  language?: string
  showCopyButton?: boolean
}

export function CodeBlock({ code, language = 'shell', showCopyButton = true }: CodeBlockProps) {
  const [copied, setCopied] = useState(false)

  const handleCopy = useCallback(async () => {
    try {
      await navigator.clipboard.writeText(code)
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    } catch {
      // Fallback for older browsers
      const textarea = document.createElement('textarea')
      textarea.value = code
      document.body.appendChild(textarea)
      textarea.select()
      document.execCommand('copy')
      document.body.removeChild(textarea)
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    }
  }, [code])

  const highlightCode = (text: string): React.ReactNode => {
    const lines = text.split('\n')
    return lines.map((line, index) => {
      const highlighted = highlightLine(line)
      return (
        <span key={index}>
          {highlighted}
          {index < lines.length - 1 && '\n'}
        </span>
      )
    })
  }

  const highlightLine = (line: string): React.ReactNode => {
    // Highlight memcached commands
    if (line.startsWith('set ')) {
      const parts = line.split(' ')
      return (
        <>
          <span className="text-green-400 font-semibold">{parts[0]}</span>
          <span className="text-cyan-300"> {parts[1]}</span>
          <span className="text-yellow-300"> {parts.slice(2).join(' ')}</span>
        </>
      )
    }

    if (line.startsWith('get ')) {
      const parts = line.split(' ')
      return (
        <>
          <span className="text-green-400 font-semibold">{parts[0]}</span>
          <span className="text-cyan-300"> {parts.slice(1).join(' ')}</span>
        </>
      )
    }

    if (line.startsWith('delete ')) {
      const parts = line.split(' ')
      return (
        <>
          <span className="text-green-400 font-semibold">{parts[0]}</span>
          <span className="text-cyan-300"> {parts.slice(1).join(' ')}</span>
        </>
      )
    }

    // Highlight responses
    if (line === 'STORED' || line === 'DELETED' || line === 'END') {
      return <span className="text-purple-400 font-semibold">{line}</span>
    }

    // Highlight VALUE response
    if (line.startsWith('VALUE ')) {
      const parts = line.split(' ')
      return (
        <>
          <span className="text-purple-400 font-semibold">{parts[0]}</span>
          <span className="text-cyan-300"> {parts[1]}</span>
          <span className="text-yellow-300"> {parts.slice(2).join(' ')}</span>
        </>
      )
    }

    // Highlight shell prompt
    if (line.startsWith('$ ')) {
      return (
        <>
          <span className="text-gray-500">$</span>
          <span className="text-blue-400">{line.slice(1)}</span>
        </>
      )
    }

    // Highlight connection messages
    if (line.startsWith('Trying ') || line.startsWith('Connected ')) {
      return <span className="text-gray-500">{line}</span>
    }

    return <span>{line}</span>
  }

  return (
    <div
      className="code-block relative rounded-lg overflow-hidden bg-gray-900 dark:bg-gray-950 border border-gray-700"
      data-testid="code-block"
      data-language={language}
    >
      <div className="flex items-center justify-between px-4 py-2 bg-gray-800 dark:bg-gray-900 border-b border-gray-700">
        <div className="flex space-x-2">
          <div className="w-3 h-3 rounded-full bg-red-500"></div>
          <div className="w-3 h-3 rounded-full bg-yellow-500"></div>
          <div className="w-3 h-3 rounded-full bg-green-500"></div>
        </div>
        <span className="text-xs text-gray-400 uppercase tracking-wider">{language}</span>
        {showCopyButton && (
          <button
            onClick={handleCopy}
            className="text-gray-400 hover:text-white transition-colors text-sm flex items-center gap-1"
            aria-label={copied ? 'Copied!' : 'Copy code'}
            data-testid="copy-button"
          >
            {copied ? (
              <>
                <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                </svg>
                <span>Copied!</span>
              </>
            ) : (
              <>
                <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" />
                </svg>
                <span>Copy</span>
              </>
            )}
          </button>
        )}
      </div>
      <pre className="code-block-content p-4 overflow-x-auto font-mono text-sm leading-relaxed text-gray-100">
        <code data-testid="code-content">{highlightCode(code)}</code>
      </pre>
    </div>
  )
}
