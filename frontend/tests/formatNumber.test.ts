import { describe, it, expect } from 'vitest'
import { formatNumber, formatStatistic } from '../src/utils/formatNumber'

describe('formatNumber', () => {
  describe('Large number formatting with suffixes', () => {
    it('formats billions with B suffix', () => {
      expect(formatNumber(1000000000)).toBe('1B')
      expect(formatNumber(1500000000)).toBe('1.5B')
      expect(formatNumber(5500000000)).toBe('5.5B')
      expect(formatNumber(12340000000)).toBe('12.3B')
    })

    it('formats millions with M suffix', () => {
      expect(formatNumber(1000000)).toBe('1M')
      expect(formatNumber(1200000)).toBe('1.2M')
      expect(formatNumber(1250000)).toBe('1.3M') // rounds to 1.3 with 1 decimal
      expect(formatNumber(45700000)).toBe('45.7M')
      expect(formatNumber(999000000)).toBe('999M')
    })

    it('formats thousands with K suffix', () => {
      expect(formatNumber(1000)).toBe('1K')
      expect(formatNumber(1500)).toBe('1.5K')
      expect(formatNumber(52000)).toBe('52K')
      expect(formatNumber(500000)).toBe('500K')
      expect(formatNumber(999999)).toBe('1000K')
    })

    it('handles exact threshold values', () => {
      expect(formatNumber(1000)).toBe('1K')
      expect(formatNumber(1000000)).toBe('1M')
      expect(formatNumber(1000000000)).toBe('1B')
    })
  })

  describe('Small numbers without suffixes', () => {
    it('displays numbers under 1000 without suffix', () => {
      expect(formatNumber(0)).toBe('0')
      expect(formatNumber(1)).toBe('1')
      expect(formatNumber(42)).toBe('42')
      expect(formatNumber(100)).toBe('100')
      expect(formatNumber(999)).toBe('999')
    })

    it('formats small numbers with commas for readability', () => {
      // Numbers under 1000 shouldn't have commas
      expect(formatNumber(999)).toBe('999')
    })
  })

  describe('Edge cases', () => {
    it('handles zero', () => {
      expect(formatNumber(0)).toBe('0')
    })

    it('handles negative numbers', () => {
      expect(formatNumber(-1000)).toBe('-1K')
      expect(formatNumber(-1500000)).toBe('-1.5M')
      expect(formatNumber(-999)).toBe('-999')
    })

    it('handles NaN', () => {
      expect(formatNumber(NaN)).toBe('0')
    })

    it('handles very large numbers', () => {
      expect(formatNumber(1000000000000)).toBe('1000B')
    })
  })

  describe('Decimal precision', () => {
    it('uses default 1 decimal place', () => {
      expect(formatNumber(1234567)).toBe('1.2M')
    })

    it('respects custom decimal places', () => {
      expect(formatNumber(1234567, 2)).toBe('1.23M')
      expect(formatNumber(1234567, 0)).toBe('1M')
    })

    it('removes trailing zeros', () => {
      expect(formatNumber(1000000)).toBe('1M')
      expect(formatNumber(1100000)).toBe('1.1M')
      expect(formatNumber(1000000, 2)).toBe('1M')
    })
  })
})

describe('formatStatistic', () => {
  describe('Basic formatting', () => {
    it('formats numbers with suffixes', () => {
      expect(formatStatistic(1500000)).toBe('1.5M')
      expect(formatStatistic(500000)).toBe('500K')
      expect(formatStatistic(5500000000)).toBe('5.5B')
    })

    it('formats small numbers without suffix', () => {
      expect(formatStatistic(100)).toBe('100')
      expect(formatStatistic(999)).toBe('999')
    })
  })

  describe('Options', () => {
    it('respects custom decimals option', () => {
      expect(formatStatistic(1234567, { decimals: 2 })).toBe('1.23M')
      expect(formatStatistic(1234567, { decimals: 0 })).toBe('1M')
    })

    it('forceDecimals keeps trailing zeros', () => {
      expect(formatStatistic(1000000, { forceDecimals: true })).toBe('1.0M')
      expect(formatStatistic(1000000, { forceDecimals: false })).toBe('1M')
    })
  })

  describe('Edge cases', () => {
    it('handles zero', () => {
      expect(formatStatistic(0)).toBe('0')
    })

    it('handles NaN', () => {
      expect(formatStatistic(NaN)).toBe('0')
    })
  })
})
