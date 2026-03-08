/**
 * Unit tests for links data.
 * Owner: Scenario 6 - External Links and Resources
 *
 * Test Cases:
 * - TC1: Check GitHub link presence (href contains 'github.com')
 * - TC3: Check documentation link presence
 * - TC4: Check protocol reference link
 */

import { describe, it, expect } from 'vitest'
import { links, GITHUB_URL, DOCS_URL, PROTOCOL_URL, getLinkByType } from './links'

describe('Links Data', () => {
  // Test Case 1: Check GitHub link presence
  describe('GitHub Link', () => {
    it('GITHUB_URL contains github.com', () => {
      expect(GITHUB_URL).toContain('github.com')
    })

    it('links array contains a GitHub link', () => {
      const githubLink = links.find(link => link.type === 'github')
      expect(githubLink).toBeDefined()
      expect(githubLink?.url).toContain('github.com')
    })

    it('GitHub link has proper label', () => {
      const githubLink = getLinkByType('github')
      expect(githubLink).toBeDefined()
      expect(githubLink?.label).toBe('GitHub')
    })
  })

  // Test Case 3: Check documentation link presence
  describe('Documentation Link', () => {
    it('DOCS_URL is defined and not empty', () => {
      expect(DOCS_URL).toBeDefined()
      expect(DOCS_URL.length).toBeGreaterThan(0)
    })

    it('links array contains a documentation link', () => {
      const docsLink = links.find(link => link.type === 'docs')
      expect(docsLink).toBeDefined()
      expect(docsLink?.label).toContain('Documentation')
    })

    it('documentation link points to README', () => {
      const docsLink = getLinkByType('docs')
      expect(docsLink).toBeDefined()
      expect(docsLink?.url.toLowerCase()).toContain('readme')
    })
  })

  // Test Case 4: Check protocol reference link
  describe('Protocol Reference Link', () => {
    it('PROTOCOL_URL is defined and contains memcached', () => {
      expect(PROTOCOL_URL).toBeDefined()
      expect(PROTOCOL_URL.toLowerCase()).toContain('memcached')
    })

    it('links array contains a protocol link', () => {
      const protocolLink = links.find(link => link.type === 'protocol')
      expect(protocolLink).toBeDefined()
    })

    it('protocol link points to protocol specification', () => {
      const protocolLink = getLinkByType('protocol')
      expect(protocolLink).toBeDefined()
      expect(protocolLink?.url.toLowerCase()).toContain('protocol')
    })
  })

  // Utility function tests
  describe('getLinkByType', () => {
    it('returns correct link for github type', () => {
      const link = getLinkByType('github')
      expect(link).toBeDefined()
      expect(link?.type).toBe('github')
    })

    it('returns undefined for non-existent type', () => {
      const link = getLinkByType('other')
      expect(link).toBeUndefined()
    })
  })

  // Links array structure
  describe('Links Array Structure', () => {
    it('contains at least 3 external links', () => {
      expect(links.length).toBeGreaterThanOrEqual(3)
    })

    it('all links have required properties', () => {
      links.forEach(link => {
        expect(link).toHaveProperty('label')
        expect(link).toHaveProperty('url')
        expect(link).toHaveProperty('type')
        expect(link.label.length).toBeGreaterThan(0)
        expect(link.url.length).toBeGreaterThan(0)
      })
    })

    it('all links have valid URLs starting with https://', () => {
      links.forEach(link => {
        expect(link.url).toMatch(/^https:\/\//)
      })
    })
  })
})
