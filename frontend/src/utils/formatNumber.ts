/**
 * Formats a number for display in statistics.
 * - Numbers < 1000: displayed as-is with commas (e.g., 999)
 * - Numbers >= 1000 and < 1,000,000: displayed with commas (e.g., 1,234 or 999,999)
 * - Numbers >= 1,000,000: abbreviated with suffix (e.g., 1.2M, 15.5M)
 * - Numbers >= 1,000,000,000: abbreviated with B suffix (e.g., 1.5B)
 */
export function formatStatNumber(num: number): string {
  if (num >= 1_000_000_000) {
    const billions = num / 1_000_000_000
    return billions % 1 === 0
      ? `${billions.toFixed(0)}B`
      : `${billions.toFixed(1)}B`
  }

  if (num >= 1_000_000) {
    const millions = num / 1_000_000
    return millions % 1 === 0
      ? `${millions.toFixed(0)}M`
      : `${millions.toFixed(1)}M`
  }

  return num.toLocaleString('en-US')
}

export default formatStatNumber
