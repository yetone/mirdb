/**
 * Code Block Component
 * Owner: Scenario 3 - Installation Instructions
 *
 * Expected exports:
 * - CodeBlock: React.FC<CodeBlockProps> - Syntax highlighted code display
 */

export interface CodeBlockProps {
  code: string
  language?: string
  showCopy?: boolean
}

export function CodeBlock({ code }: CodeBlockProps) {
  return (
    <pre>
      <code>{code}</code>
    </pre>
  )
}
