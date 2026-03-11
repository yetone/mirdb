/**
 * Protocol Documentation Preview Unit Tests
 * Owner: Scenario 5 - Protocol Documentation Preview
 *
 * Test coverage:
 * - Renders protocol documentation section with command table
 * - Lists SET, GET, DELETE commands with descriptions
 * - Displays correct SET command syntax
 * - Displays correct GET command syntax with response format
 * - Lists all response codes
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { ProtocolDocs, CommandReference, ResponseCodes, CommandRow, ResponseCodeRow } from '../../../src/components/ProtocolDocs.js'

describe('ProtocolDocs', () => {
  let container

  beforeEach(() => {
    container = document.createElement('div')
    container.innerHTML = ProtocolDocs()
    document.body.appendChild(container)
  })

  afterEach(() => {
    document.body.innerHTML = ''
  })

  describe('Test Case 1: Render ProtocolDocs component', () => {
    it('should render protocol documentation section with command table/list displayed', () => {
      const protocolDocs = container.querySelector('[data-testid="protocol-docs"]')
      expect(protocolDocs).toBeTruthy()

      const commandTable = container.querySelector('[data-testid="command-table"]')
      expect(commandTable).toBeTruthy()

      const commandList = container.querySelector('[data-testid="command-list"]')
      expect(commandList).toBeTruthy()

      // Verify section structure
      const sectionTitle = container.querySelector('.section-title')
      expect(sectionTitle).toBeTruthy()
      expect(sectionTitle.textContent.trim()).toBe('Protocol Documentation')

      const sectionSubtitle = container.querySelector('.section-subtitle')
      expect(sectionSubtitle).toBeTruthy()
      expect(sectionSubtitle.textContent).toContain('memcached text protocol')
    })

    it('should have proper container and layout classes', () => {
      const protocolDocs = container.querySelector('[data-testid="protocol-docs"]')
      expect(protocolDocs.classList.contains('container-custom')).toBe(true)
      expect(protocolDocs.classList.contains('max-w-7xl')).toBe(true)
    })

    it('should have command reference section', () => {
      const commandReference = container.querySelector('[data-testid="command-reference"]')
      expect(commandReference).toBeTruthy()

      const heading = commandReference.querySelector('h3')
      expect(heading.textContent.trim()).toBe('Supported Commands')
    })
  })

  describe('Test Case 2: Check supported commands list', () => {
    it('should list SET, GET, DELETE commands with brief descriptions', () => {
      // Check SET command
      const setRow = container.querySelector('[data-testid="command-row-set"]')
      expect(setRow).toBeTruthy()
      const setName = setRow.querySelector('.command-name')
      expect(setName.textContent.trim()).toBe('SET')
      const setDescription = setRow.querySelector('.command-description')
      expect(setDescription.textContent).toContain('Store')

      // Check GET command
      const getRow = container.querySelector('[data-testid="command-row-get"]')
      expect(getRow).toBeTruthy()
      const getName = getRow.querySelector('.command-name')
      expect(getName.textContent.trim()).toBe('GET')
      const getDescription = getRow.querySelector('.command-description')
      expect(getDescription.textContent).toContain('Retrieve')

      // Check DELETE command
      const deleteRow = container.querySelector('[data-testid="command-row-delete"]')
      expect(deleteRow).toBeTruthy()
      const deleteName = deleteRow.querySelector('.command-name')
      expect(deleteName.textContent.trim()).toBe('DELETE')
      const deleteDescription = deleteRow.querySelector('.command-description')
      expect(deleteDescription.textContent).toContain('Remove')
    })

    it('should display all 8 commands in the table', () => {
      const commandRows = container.querySelectorAll('[data-testid^="command-row-"]')
      expect(commandRows.length).toBe(8)
    })

    it('should have each command with name, syntax, and description columns', () => {
      const firstRow = container.querySelector('[data-testid="command-row-set"]')
      expect(firstRow.querySelector('.command-name')).toBeTruthy()
      expect(firstRow.querySelector('.command-syntax')).toBeTruthy()
      expect(firstRow.querySelector('.command-description')).toBeTruthy()
    })
  })

  describe('Test Case 3: Check SET command syntax', () => {
    it('should display correct SET command syntax format', () => {
      const setRow = container.querySelector('[data-testid="command-row-set"]')
      const syntaxCell = setRow.querySelector('.command-syntax code')

      // Verify the syntax matches memcached protocol
      const syntax = syntaxCell.textContent.trim()
      expect(syntax).toContain('set')
      expect(syntax).toContain('<key>')
      expect(syntax).toContain('<flags>')
      expect(syntax).toContain('<exptime>')
      expect(syntax).toContain('<bytes>')
      expect(syntax).toContain('[noreply]')
      expect(syntax).toContain('\\r\\n')
      expect(syntax).toContain('<data>')
    })

    it('should have proper formatting for SET syntax', () => {
      const setRow = container.querySelector('[data-testid="command-row-set"]')
      const syntaxCode = setRow.querySelector('.command-syntax code')

      // Verify code styling classes
      expect(syntaxCode.classList.contains('font-mono')).toBe(true)
      expect(syntaxCode.classList.contains('bg-gray-100')).toBe(true)
    })
  })

  describe('Test Case 4: Check GET command syntax', () => {
    it('should display correct GET command syntax with response format', () => {
      const getRow = container.querySelector('[data-testid="command-row-get"]')
      const syntaxCell = getRow.querySelector('.command-syntax')

      // Check main syntax
      const mainSyntax = syntaxCell.querySelector('code')
      expect(mainSyntax.textContent.trim()).toContain('get')
      expect(mainSyntax.textContent.trim()).toContain('<key>')
      expect(mainSyntax.textContent.trim()).toContain('\\r\\n')

      // Check response format is displayed
      const responseSection = syntaxCell.querySelector('.command-response')
      expect(responseSection).toBeTruthy()

      const responseCode = responseSection.querySelector('code')
      expect(responseCode.textContent).toContain('VALUE')
      expect(responseCode.textContent).toContain('<key>')
      expect(responseCode.textContent).toContain('<flags>')
      expect(responseCode.textContent).toContain('<bytes>')
      expect(responseCode.textContent).toContain('<data>')
      expect(responseCode.textContent).toContain('END')
    })

    it('should have response label for GET command', () => {
      const getRow = container.querySelector('[data-testid="command-row-get"]')
      const responseLabel = getRow.querySelector('.command-response span')
      expect(responseLabel).toBeTruthy()
      expect(responseLabel.textContent).toContain('Response')
    })
  })

  describe('Test Case 5: Check response codes reference', () => {
    it('should list all required response codes', () => {
      const responseCodes = container.querySelector('[data-testid="response-codes"]')
      expect(responseCodes).toBeTruthy()

      // Verify all required response codes are present
      const requiredCodes = ['STORED', 'NOT_STORED', 'EXISTS', 'NOT_FOUND', 'ERROR', 'DELETED', 'VALUE', 'END']

      requiredCodes.forEach(code => {
        const codeRow = container.querySelector(`[data-testid="response-code-${code.toLowerCase()}"]`)
        expect(codeRow, `Response code ${code} should be present`).toBeTruthy()

        const codeCell = codeRow.querySelector('.response-code')
        expect(codeCell.textContent.trim()).toBe(code)

        const descriptionCell = codeRow.querySelector('.response-description')
        expect(descriptionCell.textContent.trim().length).toBeGreaterThan(0)
      })
    })

    it('should have proper response codes table structure', () => {
      const responseCodesTable = container.querySelector('[data-testid="response-codes-table"]')
      expect(responseCodesTable).toBeTruthy()

      const thead = responseCodesTable.querySelector('thead')
      expect(thead).toBeTruthy()

      const headers = thead.querySelectorAll('th')
      expect(headers.length).toBe(2)
      expect(headers[0].textContent.trim()).toBe('Code')
      expect(headers[1].textContent.trim()).toBe('Description')
    })

    it('should have 8 response code rows', () => {
      const responseCodeRows = container.querySelectorAll('[data-testid^="response-code-"]')
      expect(responseCodeRows.length).toBe(8)
    })

    it('should have Response Codes section heading', () => {
      const responseCodes = container.querySelector('[data-testid="response-codes"]')
      const heading = responseCodes.querySelector('h3')
      expect(heading.textContent.trim()).toBe('Response Codes')
    })
  })
})

describe('CommandRow', () => {
  it('should render a command row with all required fields', () => {
    const props = {
      name: 'TEST',
      syntax: 'test <key>\\r\\n',
      description: 'Test command description'
    }

    const rowHtml = CommandRow(props)
    const container = document.createElement('tbody')
    container.innerHTML = rowHtml

    const row = container.querySelector('[data-testid="command-row-test"]')
    expect(row).toBeTruthy()
    expect(row.querySelector('.command-name').textContent.trim()).toBe('TEST')
    expect(row.querySelector('.command-description').textContent.trim()).toBe('Test command description')
  })

  it('should render response section when response is provided', () => {
    const props = {
      name: 'FETCH',
      syntax: 'fetch <key>\\r\\n',
      description: 'Fetch command',
      response: 'DATA <value>\\r\\n'
    }

    const rowHtml = CommandRow(props)
    const container = document.createElement('tbody')
    container.innerHTML = rowHtml

    const responseSection = container.querySelector('.command-response')
    expect(responseSection).toBeTruthy()
    expect(responseSection.textContent).toContain('DATA')
  })
})

describe('ResponseCodeRow', () => {
  it('should render a response code row', () => {
    const props = {
      code: 'SUCCESS',
      description: 'Operation completed successfully'
    }

    const rowHtml = ResponseCodeRow(props)
    const container = document.createElement('tbody')
    container.innerHTML = rowHtml

    const row = container.querySelector('[data-testid="response-code-success"]')
    expect(row).toBeTruthy()
    expect(row.querySelector('.response-code').textContent.trim()).toBe('SUCCESS')
    expect(row.querySelector('.response-description').textContent.trim()).toBe('Operation completed successfully')
  })
})

describe('CommandReference', () => {
  it('should render command reference with provided commands', () => {
    const commands = [
      { name: 'CMD1', syntax: 'cmd1\\r\\n', description: 'First command' },
      { name: 'CMD2', syntax: 'cmd2\\r\\n', description: 'Second command' }
    ]

    const html = CommandReference({ commands })
    const container = document.createElement('div')
    container.innerHTML = html

    const commandRef = container.querySelector('[data-testid="command-reference"]')
    expect(commandRef).toBeTruthy()

    const rows = container.querySelectorAll('[data-testid^="command-row-"]')
    expect(rows.length).toBe(2)
  })
})

describe('ResponseCodes', () => {
  it('should render response codes with provided codes', () => {
    const responseCodes = [
      { code: 'OK', description: 'All good' },
      { code: 'FAIL', description: 'Something failed' }
    ]

    const html = ResponseCodes({ responseCodes })
    const container = document.createElement('div')
    container.innerHTML = html

    const codesSection = container.querySelector('[data-testid="response-codes"]')
    expect(codesSection).toBeTruthy()

    const rows = container.querySelectorAll('[data-testid^="response-code-"]')
    expect(rows.length).toBe(2)
  })
})
