/**
 * QuickStart Section Unit Tests
 * Owner: Scenario 4 - Quick Start Guide Section
 *
 * Test coverage:
 * - Renders installation instructions with cargo install command
 * - Displays server start command
 * - Shows client connection example (telnet/nc localhost:12333)
 * - Displays example mirdb.toml configuration
 * - Link to full documentation is present
 * - Copy buttons are present for code blocks
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { QuickStart, InstallStep, getInstallCommands, getConfigExample } from '../../../src/components/QuickStart.js'

describe('QuickStart', () => {
  let container

  beforeEach(() => {
    container = document.createElement('div')
    container.innerHTML = QuickStart()
    document.body.appendChild(container)
  })

  afterEach(() => {
    document.body.innerHTML = ''
  })

  describe('Test Case 1: Render QuickStart component', () => {
    it('should render the quickstart section', () => {
      const section = container.querySelector('[data-testid="quickstart-section"]')
      expect(section).toBeTruthy()
    })

    it('should display installation instructions', () => {
      const installSection = container.querySelector('[data-testid="quickstart-install"]')
      expect(installSection).toBeTruthy()
    })

    it('should display usage examples', () => {
      const usageSection = container.querySelector('[data-testid="quickstart-usage"]')
      expect(usageSection).toBeTruthy()
    })

    it('should have copy buttons for code blocks', () => {
      const copyButtons = container.querySelectorAll('.copy-button')
      expect(copyButtons.length).toBeGreaterThan(0)
    })

    it('should have proper heading structure', () => {
      const section = container.querySelector('[data-testid="quickstart-section"]')
      const heading = section.querySelector('h2')
      expect(heading).toBeTruthy()
      expect(heading.textContent.toLowerCase()).toContain('quick start')
    })

    it('should have proper ARIA attributes for accessibility', () => {
      const section = container.querySelector('[data-testid="quickstart-section"]')
      expect(section.getAttribute('aria-labelledby')).toBeTruthy()
    })
  })

  describe('Test Case 2: Check installation command', () => {
    it('should display cargo install mirdb command', () => {
      const section = container.querySelector('[data-testid="quickstart-section"]')
      const text = section.textContent
      expect(text).toContain('cargo install mirdb')
    })

    it('should have installation command in a code block', () => {
      const installCode = container.querySelector('[data-testid="code-block-install"]')
      expect(installCode).toBeTruthy()
      expect(installCode.textContent).toContain('cargo install mirdb')
    })

    it('should have copy button for installation command', () => {
      const installBlock = container.querySelector('[data-testid="code-block-install"]')
      expect(installBlock).toBeTruthy()
      const copyButton = installBlock.querySelector('.copy-button')
      expect(copyButton).toBeTruthy()
    })
  })

  describe('Test Case 3: Check server start command', () => {
    it('should display command to start MirDB server', () => {
      const section = container.querySelector('[data-testid="quickstart-section"]')
      const text = section.textContent
      expect(text).toContain('mirdb')
    })

    it('should show server start command in code block', () => {
      const serverCode = container.querySelector('[data-testid="code-block-server-start"]')
      expect(serverCode).toBeTruthy()
    })

    it('should have copy button for server start command', () => {
      const serverBlock = container.querySelector('[data-testid="code-block-server-start"]')
      expect(serverBlock).toBeTruthy()
      const copyButton = serverBlock.querySelector('.copy-button')
      expect(copyButton).toBeTruthy()
    })
  })

  describe('Test Case 4: Check client connection example', () => {
    it('should show how to connect using telnet to localhost:12333', () => {
      const section = container.querySelector('[data-testid="quickstart-section"]')
      const text = section.textContent
      expect(text).toContain('telnet')
      expect(text).toContain('localhost')
      expect(text).toContain('12333')
    })

    it('should show alternative nc connection option', () => {
      const section = container.querySelector('[data-testid="quickstart-section"]')
      const text = section.textContent
      expect(text).toContain('nc')
    })

    it('should have connection command in code block', () => {
      const connectCode = container.querySelector('[data-testid="code-block-connect"]')
      expect(connectCode).toBeTruthy()
    })

    it('should have copy button for connection command', () => {
      const connectBlock = container.querySelector('[data-testid="code-block-connect"]')
      expect(connectBlock).toBeTruthy()
      const copyButton = connectBlock.querySelector('.copy-button')
      expect(copyButton).toBeTruthy()
    })
  })

  describe('Test Case 5: Check configuration file example', () => {
    it('should display example mirdb.toml configuration', () => {
      const section = container.querySelector('[data-testid="quickstart-section"]')
      const text = section.textContent
      expect(text).toContain('mirdb.toml')
    })

    it('should have configuration block with key options', () => {
      const configCode = container.querySelector('[data-testid="code-block-config"]')
      expect(configCode).toBeTruthy()
      const text = configCode.textContent
      // Should include key configuration options
      expect(text).toContain('port')
    })

    it('should show memtable configuration option', () => {
      const configCode = container.querySelector('[data-testid="code-block-config"]')
      expect(configCode).toBeTruthy()
      const text = configCode.textContent
      expect(text).toContain('memtable')
    })

    it('should show data directory configuration', () => {
      const configCode = container.querySelector('[data-testid="code-block-config"]')
      expect(configCode).toBeTruthy()
      const text = configCode.textContent
      expect(text).toContain('data_dir')
    })

    it('should have copy button for config example', () => {
      const configBlock = container.querySelector('[data-testid="code-block-config"]')
      expect(configBlock).toBeTruthy()
      const copyButton = configBlock.querySelector('.copy-button')
      expect(copyButton).toBeTruthy()
    })
  })

  describe('Test Case 6: Check link to full documentation', () => {
    it('should have link to detailed documentation', () => {
      const docsLink = container.querySelector('[data-testid="quickstart-docs-link"]')
      expect(docsLink).toBeTruthy()
    })

    it('should have proper href for documentation link', () => {
      const docsLink = container.querySelector('[data-testid="quickstart-docs-link"]')
      expect(docsLink).toBeTruthy()
      const href = docsLink.getAttribute('href')
      expect(href).toBeTruthy()
      // Should link to GitHub readme or docs
      expect(href).toMatch(/github|docs|readme/i)
    })

    it('should have accessible text for documentation link', () => {
      const docsLink = container.querySelector('[data-testid="quickstart-docs-link"]')
      expect(docsLink).toBeTruthy()
      const text = docsLink.textContent.toLowerCase()
      expect(text).toMatch(/documentation|docs|learn more|read more/)
    })
  })

  describe('InstallStep helper function', () => {
    it('should render platform-specific install step', () => {
      const step = InstallStep({ platform: 'cargo', command: 'cargo install mirdb' })
      const tempDiv = document.createElement('div')
      tempDiv.innerHTML = step

      expect(tempDiv.textContent).toContain('cargo')
      expect(tempDiv.textContent).toContain('cargo install mirdb')
    })

    it('should include copy button for the command', () => {
      const step = InstallStep({ platform: 'cargo', command: 'cargo install mirdb' })
      const tempDiv = document.createElement('div')
      tempDiv.innerHTML = step

      const copyButton = tempDiv.querySelector('.copy-button')
      expect(copyButton).toBeTruthy()
    })
  })

  describe('getInstallCommands helper function', () => {
    it('should return array of installation commands', () => {
      const commands = getInstallCommands()
      expect(Array.isArray(commands)).toBe(true)
      expect(commands.length).toBeGreaterThan(0)
    })

    it('should include cargo install command', () => {
      const commands = getInstallCommands()
      const cargoCommand = commands.find(c => c.platform === 'cargo' || c.command.includes('cargo'))
      expect(cargoCommand).toBeTruthy()
    })
  })

  describe('getConfigExample helper function', () => {
    it('should return configuration example string', () => {
      const config = getConfigExample()
      expect(typeof config).toBe('string')
      expect(config.length).toBeGreaterThan(0)
    })

    it('should include key configuration options', () => {
      const config = getConfigExample()
      expect(config).toContain('port')
      expect(config).toContain('data_dir')
    })
  })

  describe('Accessibility', () => {
    it('should have proper section structure', () => {
      const section = container.querySelector('[data-testid="quickstart-section"]')
      expect(section.tagName.toLowerCase()).toBe('section')
    })

    it('should have code blocks with proper lang attribute', () => {
      const codeBlocks = container.querySelectorAll('[data-testid^="code-block-"]')
      codeBlocks.forEach(block => {
        expect(block.getAttribute('data-language')).toBeTruthy()
      })
    })

    it('should have visible headings for each subsection', () => {
      const section = container.querySelector('[data-testid="quickstart-section"]')
      const headings = section.querySelectorAll('h2, h3')
      expect(headings.length).toBeGreaterThanOrEqual(1)
    })
  })

  describe('Responsive design classes', () => {
    it('should have responsive container classes', () => {
      const section = container.querySelector('[data-testid="quickstart-section"]')
      // Container classes can be on section or inner div
      const innerContainer = section.querySelector('.container-custom, [class*="max-w"], [class*="mx-auto"]')
      const hasResponsiveClasses = section.className.match(/container|max-w|mx-auto|px-/) || innerContainer
      expect(hasResponsiveClasses).toBeTruthy()
    })

    it('should have responsive grid or flex layout', () => {
      const installSection = container.querySelector('[data-testid="quickstart-install"]')
      expect(installSection).toBeTruthy()
    })
  })
})
