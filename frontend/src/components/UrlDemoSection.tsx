import { useState, useCallback } from 'react'
import { motion, useReducedMotion } from 'framer-motion'
import { HiClipboardCopy, HiCheckCircle, HiLink, HiExternalLink } from 'react-icons/hi'
import GlassMorphismCard from './GlassMorphismCard'

interface UrlDemoSectionProps {
  className?: string
}

// Generate a demo shortened URL based on input
const generateDemoShortUrl = (inputUrl: string): string => {
  if (!inputUrl.trim()) {
    return ''
  }
  // Create a simple hash-like string from the input for demo purposes
  const hash = inputUrl
    .split('')
    .reduce((acc, char) => acc + char.charCodeAt(0), 0)
    .toString(36)
    .slice(0, 6)
    .toLowerCase()
  return `short.url/${hash}`
}

export default function UrlDemoSection({ className = '' }: UrlDemoSectionProps) {
  const [inputUrl, setInputUrl] = useState('')
  const [shortenedUrl, setShortenedUrl] = useState('')
  const [copied, setCopied] = useState(false)
  const [hasInteracted, setHasInteracted] = useState(false)

  const shouldReduceMotion = useReducedMotion()

  const handleInputChange = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value
    setInputUrl(value)
    setHasInteracted(true)
    setCopied(false)

    // Generate demo output as user types
    const demoUrl = generateDemoShortUrl(value)
    setShortenedUrl(demoUrl)
  }, [])

  const handleCopyClick = useCallback(async () => {
    if (shortenedUrl) {
      try {
        await navigator.clipboard.writeText(`https://${shortenedUrl}`)
        setCopied(true)
        setTimeout(() => setCopied(false), 2000)
      } catch {
        // Fallback for browsers without clipboard API
        setCopied(true)
        setTimeout(() => setCopied(false), 2000)
      }
    }
  }, [shortenedUrl])

  const animationProps = shouldReduceMotion
    ? { initial: { opacity: 1, y: 0 }, animate: { opacity: 1, y: 0 }, transition: { duration: 0 } }
    : { initial: { opacity: 0, y: 20 }, animate: { opacity: 1, y: 0 } }

  return (
    <section
      id="demo"
      className={`py-20 px-4 sm:px-6 lg:px-8 bg-gradient-to-b from-base-100 to-base-200 ${className}`}
      aria-labelledby="demo-heading"
      data-testid="url-demo-section"
    >
      <div className="max-w-4xl mx-auto">
        <motion.div
          className="text-center mb-12"
          {...animationProps}
          transition={shouldReduceMotion ? { duration: 0 } : { duration: 0.6 }}
        >
          <h2
            id="demo-heading"
            className="text-3xl sm:text-4xl font-bold mb-4"
            data-testid="demo-heading"
          >
            Try It Out
          </h2>
          <p className="text-base-content/70 text-lg max-w-2xl mx-auto" data-testid="demo-description">
            See how easy it is to shorten URLs. Enter any URL below for a quick preview.
          </p>
          <p className="text-sm text-base-content/50 mt-2" data-testid="demo-disclaimer">
            This is a demo preview. Sign up for a free account to create real shortened URLs.
          </p>
        </motion.div>

        <motion.div
          {...animationProps}
          transition={shouldReduceMotion ? { duration: 0 } : { duration: 0.6, delay: 0.2 }}
        >
          <GlassMorphismCard className="p-8">
            <div className="space-y-6">
              {/* Input Section */}
              <div className="space-y-2">
                <label
                  htmlFor="demo-url-input"
                  className="block text-sm font-medium text-base-content"
                >
                  Enter your long URL
                </label>
                <div className="relative">
                  <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
                    <HiLink className="h-5 w-5 text-base-content/50" aria-hidden="true" />
                  </div>
                  <input
                    id="demo-url-input"
                    type="url"
                    value={inputUrl}
                    onChange={handleInputChange}
                    placeholder="https://example.com/your-very-long-url-goes-here"
                    className="input input-bordered w-full pl-10 text-base"
                    data-testid="demo-url-input"
                    aria-describedby="demo-input-hint"
                  />
                </div>
                <p id="demo-input-hint" className="text-xs text-base-content/50">
                  Paste any URL to see a preview of the shortened version
                </p>
              </div>

              {/* Output Section */}
              <div
                className={`space-y-2 transition-opacity duration-300 ${
                  hasInteracted ? 'opacity-100' : 'opacity-50'
                }`}
                data-testid="demo-output-section"
              >
                <label className="block text-sm font-medium text-base-content">
                  Your shortened URL (preview)
                </label>
                <div className="flex gap-2">
                  <div
                    className="flex-1 flex items-center gap-2 px-4 py-3 bg-base-200 rounded-lg border border-base-300 min-h-[48px]"
                    data-testid="demo-output-container"
                  >
                    {shortenedUrl ? (
                      <>
                        <HiExternalLink className="h-5 w-5 text-primary flex-shrink-0" aria-hidden="true" />
                        <span
                          className="text-primary font-medium break-all"
                          data-testid="demo-shortened-url"
                        >
                          https://{shortenedUrl}
                        </span>
                      </>
                    ) : (
                      <span className="text-base-content/40" data-testid="demo-output-placeholder">
                        Your short URL will appear here
                      </span>
                    )}
                  </div>
                  <button
                    type="button"
                    onClick={handleCopyClick}
                    disabled={!shortenedUrl}
                    className="btn btn-primary flex-shrink-0 gap-2"
                    data-testid="demo-copy-button"
                    aria-label={copied ? 'Copied to clipboard' : 'Copy shortened URL'}
                  >
                    {copied ? (
                      <>
                        <HiCheckCircle className="h-5 w-5" aria-hidden="true" />
                        <span className="hidden sm:inline">Copied!</span>
                      </>
                    ) : (
                      <>
                        <HiClipboardCopy className="h-5 w-5" aria-hidden="true" />
                        <span className="hidden sm:inline">Copy</span>
                      </>
                    )}
                  </button>
                </div>
              </div>

              {/* Demo Notice */}
              <div
                className="text-center pt-4 border-t border-base-300"
                data-testid="demo-cta-section"
              >
                <p className="text-sm text-base-content/60 mb-3">
                  Ready to create real shortened URLs with full analytics?
                </p>
                <a
                  href="/register"
                  className="btn btn-outline btn-primary btn-sm"
                  data-testid="demo-signup-cta"
                >
                  Sign Up Free
                </a>
              </div>
            </div>
          </GlassMorphismCard>
        </motion.div>
      </div>
    </section>
  )
}
