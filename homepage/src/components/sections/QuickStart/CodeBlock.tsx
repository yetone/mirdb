/**
 * CodeBlock Component
 * Owner: Scenario 4 - Quick Start Section
 *
 * Renders code with syntax highlighting and optional copy button.
 * Supports shell/bash language highlighting.
 */
import { CopyButton } from '@/components/ui/CopyButton/CopyButton'
import { cn } from '@/utils/cn'
import styles from './QuickStart.module.css'

export interface CodeBlockProps {
  code: string
  language?: 'shell' | 'bash' | 'toml' | 'text'
  copyable?: boolean
  title?: string
  className?: string
}

export function CodeBlock({
  code,
  language = 'shell',
  copyable = true,
  title,
  className,
}: CodeBlockProps) {
  // Simple syntax highlighting for shell commands
  const highlightCode = (code: string, lang: string): JSX.Element => {
    if (lang === 'shell' || lang === 'bash') {
      return highlightShell(code)
    }
    if (lang === 'toml') {
      return highlightToml(code)
    }
    return <>{code}</>
  }

  const highlightShell = (code: string): JSX.Element => {
    const lines = code.split('\n')
    return (
      <>
        {lines.map((line, i) => {
          const trimmed = line.trim()
          // Comments
          if (trimmed.startsWith('#')) {
            return (
              <span key={i}>
                <span className={styles.comment}>{line}</span>
                {i < lines.length - 1 && '\n'}
              </span>
            )
          }
          // Commands
          const parts = line.match(/^(\s*)(\$?\s*)(.*)$/)
          if (parts) {
            const [, indent, prompt, rest] = parts
            const tokens = tokenizeShellCommand(rest)
            return (
              <span key={i}>
                {indent}
                {prompt && <span className={styles.prompt}>{prompt}</span>}
                {tokens}
                {i < lines.length - 1 && '\n'}
              </span>
            )
          }
          return (
            <span key={i}>
              {line}
              {i < lines.length - 1 && '\n'}
            </span>
          )
        })}
      </>
    )
  }

  const tokenizeShellCommand = (command: string): JSX.Element => {
    // Match command, flags, strings, and other tokens
    const regex = /("(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'|--?\w+(?:=\S+)?|\S+)/g
    const tokens = command.match(regex) || []
    let isFirstToken = true

    return (
      <>
        {tokens.map((token, i) => {
          const before = i === 0 ? '' : ' '

          // String
          if (token.startsWith('"') || token.startsWith("'")) {
            return (
              <span key={i}>
                {before}
                <span className={styles.string}>{token}</span>
              </span>
            )
          }

          // Flag
          if (token.startsWith('-')) {
            return (
              <span key={i}>
                {before}
                <span className={styles.flag}>{token}</span>
              </span>
            )
          }

          // Command (first token)
          if (isFirstToken) {
            isFirstToken = false
            return (
              <span key={i}>
                {before}
                <span className={styles.command}>{token}</span>
              </span>
            )
          }

          return (
            <span key={i}>
              {before}
              {token}
            </span>
          )
        })}
      </>
    )
  }

  const highlightToml = (code: string): JSX.Element => {
    const lines = code.split('\n')
    return (
      <>
        {lines.map((line, i) => {
          const trimmed = line.trim()
          // Comments
          if (trimmed.startsWith('#')) {
            return (
              <span key={i}>
                <span className={styles.comment}>{line}</span>
                {i < lines.length - 1 && '\n'}
              </span>
            )
          }
          // Key-value pairs
          const match = line.match(/^(\s*)(\w+)(\s*=\s*)(.*)$/)
          if (match) {
            const [, indent, key, equals, value] = match
            return (
              <span key={i}>
                {indent}
                <span className={styles.key}>{key}</span>
                {equals}
                <span className={styles.value}>{value}</span>
                {i < lines.length - 1 && '\n'}
              </span>
            )
          }
          return (
            <span key={i}>
              {line}
              {i < lines.length - 1 && '\n'}
            </span>
          )
        })}
      </>
    )
  }

  return (
    <div className={cn(styles.codeBlock, className)} data-language={language}>
      {title && <div className={styles.codeBlockTitle}>{title}</div>}
      <div className={styles.codeBlockWrapper}>
        <pre className={styles.pre}>
          <code className={styles.code}>{highlightCode(code, language)}</code>
        </pre>
        {copyable && (
          <CopyButton
            text={code.replace(/^\$\s*/gm, '')}
            className={styles.copyButton}
            aria-label={`Copy ${title || 'code'} to clipboard`}
          />
        )}
      </div>
    </div>
  )
}
