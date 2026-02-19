# Shell Syntax Highlighting

## Overview

A lightweight approach to syntax highlighting shell commands in React without external dependencies. Uses simple tokenization to apply CSS classes for different token types (commands, flags, strings, comments).

## When to Use This Skill

Use this skill when users request:

- Code blocks for shell/bash commands
- Syntax highlighting without heavy libraries like Prism.js or highlight.js
- Custom syntax highlighting for documentation sites

## Implementation

### Highlight Function

```typescript
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
        // Commands with optional $ prompt
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

        // Flag (starts with -)
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
```

### CSS Styling

```css
/* Shell syntax highlighting colors */
.comment {
  color: #64748b;
  font-style: italic;
}

.command {
  color: #22d3ee;
}

.flag {
  color: #fbbf24;
}

.string {
  color: #a5f3fc;
}

.prompt {
  color: #22c55e;
  user-select: none;
}
```

## Token Types

| Token Type | Pattern | Example | Color |
|------------|---------|---------|-------|
| Comment | Starts with `#` | `# This is a comment` | Gray italic |
| Command | First token | `cargo`, `npm`, `git` | Cyan |
| Flag | Starts with `-` | `--release`, `-p` | Yellow |
| String | Quoted | `"hello"`, `'world'` | Light cyan |
| Prompt | `$` prefix | `$ ` | Green |

## Best Practices

- Keep the tokenizer simple - don't try to parse all shell syntax
- Use CSS custom properties for colors to support theming
- Apply `user-select: none` to prompts so they aren't copied
- Strip prompts when implementing copy functionality
- Test with common shell patterns: git, npm, cargo, docker commands
