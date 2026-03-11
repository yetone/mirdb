/**
 * Interactive Demo Unit Tests
 * Owner: Scenario 3 - Interactive Demo Section
 *
 * Test coverage:
 * - Test Case 1: Render InteractiveDemo component with code examples and copy buttons
 * - Test Case 2: Check SET command example with correct format
 * - Test Case 3: Check GET command example with correct format
 * - Test Case 4: Check DELETE command example with correct format
 * - Test Case 5: Click copy button on SET example (covered in E2E tests)
 * - Test Case 6: Verify code syntax highlighting
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { InteractiveDemo, CodeBlock, getCommandExamples } from '../../../src/components/InteractiveDemo.js'
import { CopyButton } from '../../../src/components/CopyButton.js'

describe('InteractiveDemo', () => {
  let container

  beforeEach(() => {
    container = document.createElement('div')
    container.innerHTML = InteractiveDemo()
    document.body.appendChild(container)
  })

  afterEach(() => {
    document.body.innerHTML = ''
  })

  describe('Test Case 1: Render InteractiveDemo component', () => {
    it('should render the demo section with correct structure', () => {
      const demoSection = container.querySelector('[data-testid="interactive-demo"]')
      expect(demoSection).toBeTruthy()
      expect(demoSection.id).toBe('interactive-demo')
    })

    it('should display the section title "Try It Out"', () => {
      const title = container.querySelector('.demo-title')
      expect(title).toBeTruthy()
      expect(title.textContent.trim()).toBe('Try It Out')
    })

    it('should display a descriptive subtitle', () => {
      const subtitle = container.querySelector('.demo-subtitle')
      expect(subtitle).toBeTruthy()
      expect(subtitle.textContent).toContain('memcached text protocol')
    })

    it('should render three command examples (SET, GET, DELETE)', () => {
      const commandGrid = container.querySelector('[data-testid="command-grid"]')
      expect(commandGrid).toBeTruthy()

      const examples = container.querySelectorAll('[data-testid^="command-example-"]')
      expect(examples.length).toBe(3)
    })

    it('should render copy buttons for each command', () => {
      const copyButtons = container.querySelectorAll('.copy-button')
      // Each command has a copy button (3 commands total)
      expect(copyButtons.length).toBeGreaterThanOrEqual(3)
    })

    it('should have proper accessibility attributes', () => {
      const demoSection = container.querySelector('[data-testid="interactive-demo"]')
      expect(demoSection.getAttribute('aria-labelledby')).toBe('demo-heading')

      const commandGrid = container.querySelector('[data-testid="command-grid"]')
      expect(commandGrid.getAttribute('role')).toBe('list')
      expect(commandGrid.getAttribute('aria-label')).toBe('Command examples')
    })
  })

  describe('Test Case 2: Check SET command example', () => {
    it('should display SET command example', () => {
      const setExample = container.querySelector('[data-testid="command-example-set"]')
      expect(setExample).toBeTruthy()
    })

    it('should display correct SET command format', () => {
      const setExample = container.querySelector('[data-testid="command-example-set"]')
      const codeBlock = setExample.querySelector('[data-testid="code-block-set"]')
      expect(codeBlock).toBeTruthy()

      const code = codeBlock.querySelector('code')
      // Check that the command contains the expected parts
      expect(code.textContent).toContain('set')
      expect(code.textContent).toContain('mykey')
      expect(code.textContent).toContain('0')
      expect(code.textContent).toContain('5')
      expect(code.textContent).toContain('myval')
    })

    it('should display "STORED" as expected response', () => {
      const setExample = container.querySelector('[data-testid="command-example-set"]')
      const responseBlock = setExample.querySelector('[data-testid="code-block-set-response"]')
      expect(responseBlock).toBeTruthy()

      const code = responseBlock.querySelector('code')
      expect(code.textContent).toContain('STORED')
    })

    it('should have a copy button with correct aria-label', () => {
      const setExample = container.querySelector('[data-testid="command-example-set"]')
      const copyButton = setExample.querySelector('[data-testid="copy-button-set"]')
      expect(copyButton).toBeTruthy()
      expect(copyButton.getAttribute('aria-label')).toBe('Copy SET command')
    })
  })

  describe('Test Case 3: Check GET command example', () => {
    it('should display GET command example', () => {
      const getExample = container.querySelector('[data-testid="command-example-get"]')
      expect(getExample).toBeTruthy()
    })

    it('should display correct GET command format', () => {
      const getExample = container.querySelector('[data-testid="command-example-get"]')
      const codeBlock = getExample.querySelector('[data-testid="code-block-get"]')
      expect(codeBlock).toBeTruthy()

      const code = codeBlock.querySelector('code')
      expect(code.textContent).toContain('get')
      expect(code.textContent).toContain('mykey')
    })

    it('should display expected VALUE response', () => {
      const getExample = container.querySelector('[data-testid="command-example-get"]')
      const responseBlock = getExample.querySelector('[data-testid="code-block-get-response"]')
      expect(responseBlock).toBeTruthy()

      const code = responseBlock.querySelector('code')
      expect(code.textContent).toContain('VALUE')
      expect(code.textContent).toContain('mykey')
      expect(code.textContent).toContain('0')
      expect(code.textContent).toContain('5')
      expect(code.textContent).toContain('myval')
      expect(code.textContent).toContain('END')
    })
  })

  describe('Test Case 4: Check DELETE command example', () => {
    it('should display DELETE command example', () => {
      const deleteExample = container.querySelector('[data-testid="command-example-delete"]')
      expect(deleteExample).toBeTruthy()
    })

    it('should display correct DELETE command format', () => {
      const deleteExample = container.querySelector('[data-testid="command-example-delete"]')
      const codeBlock = deleteExample.querySelector('[data-testid="code-block-delete"]')
      expect(codeBlock).toBeTruthy()

      const code = codeBlock.querySelector('code')
      expect(code.textContent).toContain('delete')
      expect(code.textContent).toContain('mykey')
    })

    it('should display "DELETED" as expected response', () => {
      const deleteExample = container.querySelector('[data-testid="command-example-delete"]')
      const responseBlock = deleteExample.querySelector('[data-testid="code-block-delete-response"]')
      expect(responseBlock).toBeTruthy()

      const code = responseBlock.querySelector('code')
      expect(code.textContent).toContain('DELETED')
    })
  })

  describe('Test Case 6: Verify code syntax highlighting', () => {
    it('should apply syntax highlighting to code blocks', () => {
      const codeBlocks = container.querySelectorAll('.syntax-highlighted')
      expect(codeBlocks.length).toBeGreaterThan(0)
    })

    it('should highlight command keywords', () => {
      const setExample = container.querySelector('[data-testid="command-example-set"]')
      const codeBlock = setExample.querySelector('[data-testid="code-block-set"]')
      const code = codeBlock.querySelector('code')

      // Check for syntax highlighting span
      const keywordSpan = code.querySelector('.syntax-keyword')
      expect(keywordSpan).toBeTruthy()
      expect(keywordSpan.textContent).toBe('set')
    })

    it('should highlight key names in commands', () => {
      const setExample = container.querySelector('[data-testid="command-example-set"]')
      const codeBlock = setExample.querySelector('[data-testid="code-block-set"]')
      const code = codeBlock.querySelector('code')

      const keySpan = code.querySelector('.syntax-key')
      expect(keySpan).toBeTruthy()
      expect(keySpan.textContent).toBe('mykey')
    })

    it('should highlight numbers in commands', () => {
      const setExample = container.querySelector('[data-testid="command-example-set"]')
      const codeBlock = setExample.querySelector('[data-testid="code-block-set"]')
      const code = codeBlock.querySelector('code')

      const numberSpans = code.querySelectorAll('.syntax-number')
      expect(numberSpans.length).toBeGreaterThan(0)
    })

    it('should highlight success responses', () => {
      const setExample = container.querySelector('[data-testid="command-example-set"]')
      const responseBlock = setExample.querySelector('[data-testid="code-block-set-response"]')
      const code = responseBlock.querySelector('code')

      const successSpan = code.querySelector('.syntax-success')
      expect(successSpan).toBeTruthy()
      expect(successSpan.textContent).toBe('STORED')
    })

    it('should show \\r\\n newline indicators', () => {
      const setExample = container.querySelector('[data-testid="command-example-set"]')
      const codeBlock = setExample.querySelector('[data-testid="code-block-set"]')
      const code = codeBlock.querySelector('code')

      const newlineSpans = code.querySelectorAll('.syntax-newline')
      expect(newlineSpans.length).toBeGreaterThan(0)
    })
  })

  describe('Command Examples Data', () => {
    it('should export getCommandExamples function', () => {
      const examples = getCommandExamples()
      expect(Array.isArray(examples)).toBe(true)
      expect(examples.length).toBe(3)
    })

    it('should have correct structure for each command', () => {
      const examples = getCommandExamples()

      examples.forEach(example => {
        expect(example).toHaveProperty('id')
        expect(example).toHaveProperty('name')
        expect(example).toHaveProperty('description')
        expect(example).toHaveProperty('command')
        expect(example).toHaveProperty('response')
      })
    })
  })
})

describe('CodeBlock', () => {
  it('should render a code block with provided code', () => {
    const html = CodeBlock({ code: 'test code', id: 'test' })
    const container = document.createElement('div')
    container.innerHTML = html

    const codeBlock = container.querySelector('[data-testid="code-block-test"]')
    expect(codeBlock).toBeTruthy()

    const code = codeBlock.querySelector('code')
    expect(code.textContent).toContain('test code')
  })

  it('should include copy button when copyable is true', () => {
    const html = CodeBlock({ code: 'test code', id: 'test', copyable: true })
    const container = document.createElement('div')
    container.innerHTML = html

    const copyButton = container.querySelector('.copy-button')
    expect(copyButton).toBeTruthy()
  })

  it('should not include copy button when copyable is false', () => {
    const html = CodeBlock({ code: 'test code', id: 'test', copyable: false })
    const container = document.createElement('div')
    container.innerHTML = html

    const copyButton = container.querySelector('.copy-button')
    expect(copyButton).toBeFalsy()
  })

  it('should have the correct language attribute', () => {
    const html = CodeBlock({ code: 'test', id: 'test', language: 'memcached' })
    const container = document.createElement('div')
    container.innerHTML = html

    const codeBlock = container.querySelector('[data-testid="code-block-test"]')
    expect(codeBlock.getAttribute('data-language')).toBe('memcached')
  })
})

describe('CopyButton', () => {
  it('should render a button with correct data attributes', () => {
    const html = CopyButton({ text: 'copy me', id: 'test' })
    const container = document.createElement('div')
    container.innerHTML = html

    const button = container.querySelector('[data-testid="copy-button-test"]')
    expect(button).toBeTruthy()
    expect(button.getAttribute('data-copy-text')).toBe('copy me')
  })

  it('should have accessible aria-label', () => {
    const html = CopyButton({ text: 'copy me', id: 'test', label: 'Copy command' })
    const container = document.createElement('div')
    container.innerHTML = html

    const button = container.querySelector('[data-testid="copy-button-test"]')
    expect(button.getAttribute('aria-label')).toBe('Copy command')
  })

  it('should escape HTML special characters in text', () => {
    const html = CopyButton({ text: '<script>alert("xss")</script>', id: 'xss-test' })

    // Check raw HTML string has escaped characters (before DOM parsing)
    // The escapeHtml function should convert < to &lt; and > to &gt;
    expect(html).toContain('&lt;script&gt;')
    expect(html).not.toContain('data-copy-text="<script>')

    // Verify the HTML can be safely parsed without executing scripts
    const container = document.createElement('div')
    container.innerHTML = html

    // Verify no script elements were created during parsing
    const scripts = container.querySelectorAll('script')
    expect(scripts.length).toBe(0)

    // Button should still exist and be accessible
    const button = container.querySelector('[data-testid="copy-button-xss-test"]')
    expect(button).toBeTruthy()
  })
})
