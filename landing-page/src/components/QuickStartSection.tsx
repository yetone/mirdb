import { useState, useCallback } from 'react'

// Code snippet content based on PRD requirements
const CODE_SNIPPET = `# Start MirDB server
mirdb -c config.toml

# Connect with any memcached client
telnet localhost 12333
set mykey 0 0 5
hello
STORED
get mykey
VALUE mykey 0 5
hello
END`

interface CodeToken {
  type: 'comment' | 'command' | 'string' | 'output' | 'text'
  content: string
}

// Parse and tokenize the code for syntax highlighting
function tokenizeLine(line: string): CodeToken[] {
  const tokens: CodeToken[] = []

  // Comments (lines starting with #)
  if (line.trim().startsWith('#')) {
    tokens.push({ type: 'comment', content: line })
    return tokens
  }

  // Output lines (STORED, VALUE, END, or lines starting with numbers)
  if (['STORED', 'END'].includes(line.trim()) || line.match(/^VALUE\s/)) {
    tokens.push({ type: 'output', content: line })
    return tokens
  }

  // Plain text output (like 'hello' responses)
  if (line.trim() === 'hello') {
    tokens.push({ type: 'text', content: line })
    return tokens
  }

  // Commands (mirdb, telnet, set, get)
  const commandMatch = line.match(/^(mirdb|telnet|set|get)\s/)
  if (commandMatch) {
    const commandEnd = commandMatch[0].length
    tokens.push({ type: 'command', content: line.slice(0, commandEnd - 1) })
    tokens.push({ type: 'string', content: line.slice(commandEnd - 1) })
    return tokens
  }

  // Default: plain text
  tokens.push({ type: 'text', content: line })
  return tokens
}

// Check icon SVG component
function CheckIcon() {
  return (
    <svg
      data-testid="check-icon"
      width="16"
      height="16"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <polyline points="20 6 9 17 4 12" />
    </svg>
  )
}

// Copy icon SVG component
function CopyIcon() {
  return (
    <svg
      width="16"
      height="16"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <rect x="9" y="9" width="13" height="13" rx="2" ry="2" />
      <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1" />
    </svg>
  )
}

export default function QuickStartSection() {
  const [copied, setCopied] = useState(false)

  const handleCopy = useCallback(async () => {
    try {
      await navigator.clipboard.writeText(CODE_SNIPPET)
      setCopied(true)

      // Reset the copied state after 2 seconds
      setTimeout(() => {
        setCopied(false)
      }, 2000)
    } catch (err) {
      console.error('Failed to copy:', err)
    }
  }, [])

  const lines = CODE_SNIPPET.split('\n')

  return (
    <section className="quick-start-section" aria-labelledby="quick-start-title">
      <h2 id="quick-start-title">Quick Start</h2>
      <p className="quick-start-description">
        Get started with MirDB in seconds. It works seamlessly with any memcached client.
      </p>

      <div className="code-container">
        <div
          data-testid="code-block"
          className="code-block syntax-highlight"
        >
          <button
            onClick={handleCopy}
            className="copy-button"
            aria-label={copied ? 'Copied!' : 'Copy code'}
            data-copied={copied ? 'true' : 'false'}
          >
            {copied ? (
              <>
                <CheckIcon />
                <span>Copied!</span>
              </>
            ) : (
              <>
                <CopyIcon />
                <span>Copy</span>
              </>
            )}
          </button>

          <pre>
            <code role="code">
              {lines.map((line, lineIndex) => {
                const tokens = tokenizeLine(line)
                return (
                  <div key={lineIndex} className="code-line">
                    {tokens.map((token, tokenIndex) => (
                      <span key={tokenIndex} className={token.type}>
                        {token.content}
                      </span>
                    ))}
                    {'\n'}
                  </div>
                )
              })}
            </code>
          </pre>
        </div>
      </div>

      <style>{`
        .quick-start-section {
          padding: 4rem 2rem;
          background: #1a1a2e;
          color: #ffffff;
        }

        #quick-start-title {
          font-size: 2rem;
          margin-bottom: 1rem;
          text-align: center;
        }

        .quick-start-description {
          text-align: center;
          color: #a0a0b0;
          margin-bottom: 2rem;
        }

        .code-container {
          max-width: 700px;
          margin: 0 auto;
        }

        .code-block {
          position: relative;
          background: #0d1117;
          border-radius: 8px;
          padding: 1.5rem;
          overflow-x: auto;
        }

        .code-block pre {
          margin: 0;
          font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
          font-size: 14px;
          line-height: 1.6;
        }

        .code-block code {
          display: block;
        }

        .code-line {
          display: block;
          min-height: 1.4em;
        }

        /* Syntax highlighting colors */
        .comment {
          color: #6a9955;
        }

        .command {
          color: #569cd6;
          font-weight: bold;
        }

        .string {
          color: #ce9178;
        }

        .output {
          color: #4ec9b0;
        }

        .text {
          color: #d4d4d4;
        }

        .copy-button {
          position: absolute;
          top: 0.75rem;
          right: 0.75rem;
          display: flex;
          align-items: center;
          gap: 0.5rem;
          padding: 0.5rem 0.75rem;
          background: #21262d;
          border: 1px solid #30363d;
          border-radius: 6px;
          color: #c9d1d9;
          cursor: pointer;
          font-size: 12px;
          transition: all 0.2s ease;
        }

        .copy-button:hover {
          background: #30363d;
          border-color: #8b949e;
        }

        .copy-button[data-copied="true"] {
          color: #3fb950;
          border-color: #3fb950;
        }
      `}</style>
    </section>
  )
}
