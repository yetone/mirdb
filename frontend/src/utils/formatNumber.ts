/**
 * Formats a number for display with human-readable suffixes.
 * Examples:
 *   - 1500 -> "1.5K"
 *   - 1200000 -> "1.2M"
 *   - 5500000000 -> "5.5B"
 *   - 999 -> "999"
 *
 * @param num - The number to format
 * @param decimals - Number of decimal places to show (default: 1)
 * @returns A formatted string representation of the number
 */
export function formatNumber(num: number, decimals: number = 1): string {
  if (num === null || num === undefined || isNaN(num)) {
    return '0'
  }

  const absNum = Math.abs(num)

  // Define thresholds and suffixes
  const tiers = [
    { threshold: 1e9, suffix: 'B' },
    { threshold: 1e6, suffix: 'M' },
    { threshold: 1e3, suffix: 'K' },
  ]

  for (const tier of tiers) {
    if (absNum >= tier.threshold) {
      const value = num / tier.threshold
      // Remove trailing zeros after decimal point
      const formatted = value.toFixed(decimals).replace(/\.0+$/, '').replace(/(\.\d*?)0+$/, '$1')
      return `${formatted}${tier.suffix}`
    }
  }

  // For numbers less than 1000, return as-is with comma formatting
  return num.toLocaleString('en-US', { maximumFractionDigits: 0 })
}

/**
 * Formats a number with compact notation for statistics display.
 * Similar to formatNumber but with more flexible options.
 *
 * @param num - The number to format
 * @param options - Formatting options
 * @returns A formatted string representation of the number
 */
export function formatStatistic(
  num: number,
  options: {
    decimals?: number
    forceDecimals?: boolean
  } = {}
): string {
  const { decimals = 1, forceDecimals = false } = options

  if (num === null || num === undefined || isNaN(num)) {
    return '0'
  }

  const absNum = Math.abs(num)

  const tiers = [
    { threshold: 1e9, suffix: 'B' },
    { threshold: 1e6, suffix: 'M' },
    { threshold: 1e3, suffix: 'K' },
  ]

  for (const tier of tiers) {
    if (absNum >= tier.threshold) {
      const value = num / tier.threshold
      let formatted = value.toFixed(decimals)

      if (!forceDecimals) {
        // Remove unnecessary trailing zeros
        formatted = formatted.replace(/\.0+$/, '').replace(/(\.\d*?)0+$/, '$1')
      }

      return `${formatted}${tier.suffix}`
    }
  }

  return num.toLocaleString('en-US', { maximumFractionDigits: 0 })
}

export default formatNumber
