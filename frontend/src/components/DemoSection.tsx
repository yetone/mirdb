import { useState } from 'react'
import { motion } from 'framer-motion'
import { Link } from 'react-router-dom'

interface DemoState {
  url: string
  error: string
  previewUrl: string | null
  isLoading: boolean
  showRegistrationPrompt: boolean
}

const isValidUrl = (urlString: string): boolean => {
  if (!urlString.trim()) {
    return false
  }
  try {
    const url = new URL(urlString)
    return url.protocol === 'http:' || url.protocol === 'https:'
  } catch {
    return false
  }
}

const generatePreviewCode = (): string => {
  const chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
  let code = ''
  for (let i = 0; i < 6; i++) {
    code += chars.charAt(Math.floor(Math.random() * chars.length))
  }
  return code
}

const DemoSection = () => {
  const [state, setState] = useState<DemoState>({
    url: '',
    error: '',
    previewUrl: null,
    isLoading: false,
    showRegistrationPrompt: false,
  })

  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setState((prev) => ({
      ...prev,
      url: e.target.value,
      error: '',
      previewUrl: null,
      showRegistrationPrompt: false,
    }))
  }

  const handleShorten = () => {
    const trimmedUrl = state.url.trim()

    // Validation: Check for empty input
    if (!trimmedUrl) {
      setState((prev) => ({
        ...prev,
        error: 'Please enter a URL',
        previewUrl: null,
        showRegistrationPrompt: false,
      }))
      return
    }

    // Validation: Check for valid URL format
    if (!isValidUrl(trimmedUrl)) {
      setState((prev) => ({
        ...prev,
        error: 'Please enter a valid URL (must start with http:// or https://)',
        previewUrl: null,
        showRegistrationPrompt: false,
      }))
      return
    }

    // Simulate loading
    setState((prev) => ({ ...prev, isLoading: true, error: '' }))

    // Generate preview URL (simulated)
    setTimeout(() => {
      const previewCode = generatePreviewCode()
      const previewUrl = `linkshort.io/${previewCode}`

      setState((prev) => ({
        ...prev,
        isLoading: false,
        previewUrl,
        showRegistrationPrompt: true,
      }))
    }, 500)
  }

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      handleShorten()
    }
  }

  return (
    <section
      id="demo"
      data-testid="demo-section"
      className="py-20 px-4 bg-gradient-to-br from-secondary/10 via-base-100 to-primary/10"
    >
      <div className="container mx-auto max-w-3xl">
        <motion.div
          className="text-center mb-12"
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5 }}
        >
          <h2 className="text-3xl md:text-4xl font-bold mb-4">
            Try It Now
          </h2>
          <p className="text-base-content/70 max-w-xl mx-auto">
            See how easy it is to shorten your URLs. Enter a link below to preview how it works.
          </p>
        </motion.div>

        <motion.div
          className="card bg-base-100 shadow-xl"
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5, delay: 0.2 }}
        >
          <div className="card-body">
            {/* Input Section */}
            <div className="flex flex-col sm:flex-row gap-3">
              <input
                type="url"
                data-testid="demo-input"
                placeholder="Enter your long URL here..."
                className={`input input-bordered flex-1 ${state.error ? 'input-error' : ''}`}
                value={state.url}
                onChange={handleInputChange}
                onKeyDown={handleKeyDown}
                aria-label="URL to shorten"
                aria-describedby={state.error ? 'demo-error' : undefined}
              />
              <button
                data-testid="demo-shorten-button"
                className={`btn btn-primary ${state.isLoading ? 'loading' : ''}`}
                onClick={handleShorten}
                disabled={state.isLoading}
              >
                {state.isLoading ? 'Shortening...' : 'Shorten'}
              </button>
            </div>

            {/* Error Message */}
            {state.error && (
              <motion.div
                id="demo-error"
                data-testid="demo-error"
                className="alert alert-error mt-4"
                initial={{ opacity: 0, scale: 0.95 }}
                animate={{ opacity: 1, scale: 1 }}
                transition={{ duration: 0.2 }}
              >
                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  className="stroke-current shrink-0 h-6 w-6"
                  fill="none"
                  viewBox="0 0 24 24"
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth="2"
                    d="M10 14l2-2m0 0l2-2m-2 2l-2-2m2 2l2 2m7-2a9 9 0 11-18 0 9 9 0 0118 0z"
                  />
                </svg>
                <span>{state.error}</span>
              </motion.div>
            )}

            {/* Preview Result */}
            {state.previewUrl && (
              <motion.div
                data-testid="demo-preview"
                className="mt-6"
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ duration: 0.3 }}
              >
                <div className="bg-base-200 rounded-lg p-4">
                  <p className="text-sm text-base-content/70 mb-2">Preview of your shortened URL:</p>
                  <div className="flex items-center gap-2">
                    <code
                      data-testid="demo-preview-url"
                      className="flex-1 bg-base-300 px-3 py-2 rounded text-primary font-mono text-lg"
                    >
                      {state.previewUrl}
                    </code>
                  </div>
                </div>

                {/* Registration Prompt */}
                {state.showRegistrationPrompt && (
                  <motion.div
                    data-testid="demo-registration-prompt"
                    className="mt-4 text-center"
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 1 }}
                    transition={{ duration: 0.3, delay: 0.2 }}
                  >
                    <p className="text-base-content/70 mb-3">
                      This is a preview. Sign up to create real shortened URLs and track your analytics!
                    </p>
                    <Link
                      to="/register"
                      data-testid="demo-register-link"
                      className="btn btn-primary btn-sm"
                    >
                      Create Free Account
                    </Link>
                  </motion.div>
                )}
              </motion.div>
            )}
          </div>
        </motion.div>
      </div>
    </section>
  )
}

export default DemoSection
