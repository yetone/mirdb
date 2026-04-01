/**
 * Unit tests for useSmoothScroll hook and smooth scroll configuration.
 * Owner: Scenario 14 - Smooth Scroll Navigation
 *
 * Test Cases:
 * 1. CSS scroll-behavior property verification
 * 2. Section anchor links verification
 * 3. useSmoothScroll hook functionality
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { renderHook, act } from '@testing-library/react'
import { useSmoothScroll } from './useSmoothScroll'
import * as fs from 'fs'
import * as path from 'path'

describe('Smooth Scroll Navigation', () => {
  describe('Test Case 1: CSS scroll-behavior property', () => {
    it('should have scroll-behavior: smooth defined in globals.css', () => {
      const cssPath = path.join(__dirname, '../styles/globals.css')
      const cssContent = fs.readFileSync(cssPath, 'utf-8')

      // Check that html element has scroll-behavior: smooth
      expect(cssContent).toMatch(/html\s*\{[^}]*scroll-behavior:\s*smooth/s)
    })

    it('should define scroll-behavior on html element', () => {
      const cssPath = path.join(__dirname, '../styles/globals.css')
      const cssContent = fs.readFileSync(cssPath, 'utf-8')

      // Verify the exact rule structure
      const hasHtmlSmoothScroll = cssContent.includes('html {') &&
        cssContent.includes('scroll-behavior: smooth')

      expect(hasHtmlSmoothScroll).toBe(true)
    })
  })

  describe('Test Case 3: Section anchor links', () => {
    it('should have Features section with id="features"', () => {
      const featuresPath = path.join(__dirname, '../components/Features/Features.tsx')
      const content = fs.readFileSync(featuresPath, 'utf-8')

      expect(content).toMatch(/id=["']features["']/)
    })

    it('should have UsageDemo section with id="usage"', () => {
      const usagePath = path.join(__dirname, '../components/UsageDemo/UsageDemo.tsx')
      const content = fs.readFileSync(usagePath, 'utf-8')

      expect(content).toMatch(/id=["']usage["']/)
    })

    it('should have Roadmap section with id="roadmap"', () => {
      const roadmapPath = path.join(__dirname, '../components/Roadmap/Roadmap.tsx')
      const content = fs.readFileSync(roadmapPath, 'utf-8')

      expect(content).toMatch(/id=["']roadmap["']/)
    })

    it('should have Navigation links matching section ids', () => {
      const navPath = path.join(__dirname, '../components/Navigation/Navigation.tsx')
      const content = fs.readFileSync(navPath, 'utf-8')

      // Check that navigation has links to all major sections
      expect(content).toMatch(/href:\s*['"]#features['"]/)
      expect(content).toMatch(/href:\s*['"]#usage['"]/)
      expect(content).toMatch(/href:\s*['"]#roadmap['"]/)
    })
  })

  describe('useSmoothScroll hook', () => {
    let mockElement: HTMLElement
    let scrollIntoViewMock: ReturnType<typeof vi.fn>
    let scrollToMock: ReturnType<typeof vi.fn>

    beforeEach(() => {
      scrollIntoViewMock = vi.fn()
      mockElement = document.createElement('div')
      mockElement.id = 'test-section'
      mockElement.scrollIntoView = scrollIntoViewMock
      document.body.appendChild(mockElement)

      scrollToMock = vi.fn()
      window.scrollTo = scrollToMock
    })

    afterEach(() => {
      document.body.removeChild(mockElement)
      vi.restoreAllMocks()
    })

    it('should return scrollTo and scrollToTop functions', () => {
      const { result } = renderHook(() => useSmoothScroll())

      expect(result.current.scrollTo).toBeInstanceOf(Function)
      expect(result.current.scrollToTop).toBeInstanceOf(Function)
    })

    it('should scroll to element by id without hash', () => {
      const { result } = renderHook(() => useSmoothScroll())

      act(() => {
        result.current.scrollTo('test-section')
      })

      expect(scrollIntoViewMock).toHaveBeenCalledWith({
        behavior: 'smooth',
        block: 'start',
      })
    })

    it('should scroll to element by id with hash prefix', () => {
      const { result } = renderHook(() => useSmoothScroll())

      act(() => {
        result.current.scrollTo('#test-section')
      })

      expect(scrollIntoViewMock).toHaveBeenCalledWith({
        behavior: 'smooth',
        block: 'start',
      })
    })

    it('should not throw when element does not exist', () => {
      const { result } = renderHook(() => useSmoothScroll())

      expect(() => {
        act(() => {
          result.current.scrollTo('non-existent-element')
        })
      }).not.toThrow()
    })

    it('should scroll to top of page', () => {
      const { result } = renderHook(() => useSmoothScroll())

      act(() => {
        result.current.scrollToTop()
      })

      expect(scrollToMock).toHaveBeenCalledWith({
        top: 0,
        behavior: 'smooth',
      })
    })
  })
})
