/**
 * Usage Section Component
 * Owner: Scenario 5 - Usage Demonstration
 *
 * Shows how to use MirDB:
 * - Embedded usage.gif demonstration
 * - Supported Memcached commands table
 * - Example command snippets with copy
 * - Default configuration reference
 *
 * Requirements: REQ-6, REQ-8
 * User Story: US-4
 */

import { useState, useCallback } from 'react'
import { commands, defaultConfig } from '@/data/commands'
import { CommandTable } from './CommandTable'
import styles from './Usage.module.css'

interface CodeBlockProps {
  code: string
  language?: string
  'data-testid'?: string
}

function CodeBlock({ code, language = 'shell', 'data-testid': testId }: CodeBlockProps) {
  const [copied, setCopied] = useState(false)

  const handleCopy = useCallback(async () => {
    try {
      await navigator.clipboard.writeText(code)
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    } catch (err) {
      console.error('Failed to copy:', err)
    }
  }, [code])

  return (
    <div className={styles.codeBlock} data-testid={testId}>
      <div className={styles.codeHeader}>
        <span className={styles.codeLanguage}>{language}</span>
        <button
          type="button"
          className={`${styles.copyButton} ${copied ? styles.copyButtonSuccess : ''}`}
          onClick={handleCopy}
          aria-label={copied ? 'Copied to clipboard' : 'Copy to clipboard'}
          data-testid={`${testId}-copy-button`}
        >
          {copied ? (
            <>
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                <polyline points="20 6 9 17 4 12" />
              </svg>
              Copied!
            </>
          ) : (
            <>
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                <rect x="9" y="9" width="13" height="13" rx="2" ry="2" />
                <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1" />
              </svg>
              Copy
            </>
          )}
        </button>
      </div>
      <pre className={styles.pre}>
        <code className={styles.code}>{code}</code>
      </pre>
    </div>
  )
}

const exampleCommands = [
  {
    id: 'set-example',
    title: 'Set a value',
    code: `# Set a key with 60 second TTL
set mykey 0 60 5
hello
STORED`,
  },
  {
    id: 'get-example',
    title: 'Get a value',
    code: `# Retrieve a value by key
get mykey
VALUE mykey 0 5
hello
END`,
  },
  {
    id: 'delete-example',
    title: 'Delete a key',
    code: `# Delete a key
delete mykey
DELETED`,
  },
  {
    id: 'info-example',
    title: 'Get server info',
    code: `# Query database statistics
info
... server statistics ...`,
  },
]

export function Usage() {
  return (
    <section id="usage" className={styles.usage} aria-labelledby="usage-heading">
      <div className={styles.container}>
        <h2 id="usage-heading" className={styles.heading}>
          Usage
        </h2>
        <p className={styles.description}>
          MirDB is fully compatible with the Memcached text protocol.
          Use any existing Memcached client to connect and interact with your data.
        </p>

        {/* Usage GIF Demonstration */}
        <div className={styles.demoContainer}>
          <img
            src="/assets/usage.gif"
            alt="MirDB terminal usage demonstration showing set, get, and delete operations"
            className={styles.demoImage}
            data-testid="usage-gif"
            loading="lazy"
          />
        </div>

        {/* Supported Commands Table */}
        <h3 className={styles.subheading}>Supported Commands</h3>
        <CommandTable commands={commands} />

        {/* Example Commands */}
        <h3 className={styles.subheading}>Command Examples</h3>
        <div className={styles.examplesGrid}>
          {exampleCommands.map((example) => (
            <div key={example.id}>
              <CodeBlock
                code={example.code}
                language="shell"
                data-testid={`example-${example.id}`}
              />
            </div>
          ))}
        </div>

        {/* Default Configuration */}
        <div className={styles.configSection}>
          <h3 className={styles.subheading}>Default Configuration</h3>
          <p className={styles.description} style={{ marginBottom: 'var(--spacing-4)' }}>
            Configure MirDB using a TOML configuration file. Below are the default settings:
          </p>
          <CodeBlock
            code={defaultConfig}
            language="toml"
            data-testid="config-block"
          />
        </div>
      </div>
    </section>
  )
}
