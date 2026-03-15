/**
 * Hero Section Component.
 * Owner: Scenario 1 - Homepage Hero Section and Branding
 *
 * Responsibilities:
 * - Display product name (MirDB) and logo
 * - Show compelling tagline and value proposition
 * - Render prominent "Get Started" CTA button
 *
 * Requirements:
 * - REQ-1: Product name, tagline, value proposition
 * - REQ-2: Get Started CTA
 * - US-1: Core features at a glance
 */

import { Button } from '@/components/ui/Button'
import { PRODUCT_NAME, TAGLINE, VALUE_PROPOSITION, DOCS_URL } from '@/utils/constants'

export function Hero() {
  return (
    <section
      aria-label="Hero"
      role="region"
      className="min-h-screen flex items-center justify-center bg-gradient-to-br from-gray-50 to-gray-100 dark:from-gray-900 dark:to-gray-800"
    >
      <div className="container py-20 text-center">
        <div className="max-w-4xl mx-auto">
          <img
            src="/logo.gif"
            alt="MirDB Logo"
            className="w-32 h-32 mx-auto mb-8 rounded-lg shadow-lg"
          />

          <h1 className="text-5xl md:text-7xl font-bold text-gray-900 dark:text-white mb-6">
            {PRODUCT_NAME}
          </h1>

          <p className="text-xl md:text-2xl text-primary-600 dark:text-primary-400 font-medium mb-4">
            {TAGLINE}
          </p>

          <p className="text-lg text-gray-600 dark:text-gray-300 mb-10 max-w-2xl mx-auto">
            {VALUE_PROPOSITION}
          </p>

          <div className="flex flex-col sm:flex-row gap-4 justify-center">
            <Button
              href={DOCS_URL}
              size="lg"
              aria-label="Get Started with MirDB"
            >
              Get Started
            </Button>

            <Button
              href="https://github.com/yetone/mirdb"
              variant="outline"
              size="lg"
              aria-label="View MirDB on GitHub"
            >
              View on GitHub
            </Button>
          </div>
        </div>
      </div>
    </section>
  )
}
