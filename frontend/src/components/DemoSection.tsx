import { useState } from 'react'
import { motion } from 'framer-motion'
import { Link } from 'react-router-dom'

interface DemoState {
  url: string
  error: string | null
  showPrompt: boolean
}

const isValidUrl = (url: string): boolean => {
  try {
    const parsed = new URL(url)
    return parsed.protocol === 'http:' || parsed.protocol === 'https:'
  } catch {
    return false
  }
}

const DemoSection = () => {
  const [state, setState] = useState<DemoState>({
    url: '',
    error: null,
    showPrompt: false,
  })

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()

    // Reset error and prompt
    setState((prev) => ({ ...prev, error: null, showPrompt: false }))

    // Validate empty URL
    if (!state.url.trim()) {
      setState((prev) => ({
        ...prev,
        error: 'Please enter a URL',
      }))
      return
    }

    // Validate URL format
    if (!isValidUrl(state.url)) {
      setState((prev) => ({
        ...prev,
        error: 'Please enter a valid URL (e.g., https://example.com)',
      }))
      return
    }

    // Show register prompt for valid URL
    setState((prev) => ({ ...prev, showPrompt: true }))
  }

  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setState((prev) => ({
      ...prev,
      url: e.target.value,
      error: null,
      showPrompt: false,
    }))
  }

  return (
    <section
      id="demo"
      className="py-16 px-4 sm:px-6 lg:px-8 bg-base-100"
      data-testid="demo-section"
      aria-labelledby="demo-heading"
    >
      <div className="max-w-3xl mx-auto">
        <motion.div
          className="text-center mb-8"
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5 }}
        >
          <h2
            id="demo-heading"
            className="text-3xl font-bold text-base-content sm:text-4xl"
          >
            Try It Now
          </h2>
          <p className="mt-4 text-lg text-base-content/70">
            See how easy it is to shorten your URLs. Enter a link below to get started.
          </p>
        </motion.div>

        <motion.form
          onSubmit={handleSubmit}
          className="flex flex-col sm:flex-row gap-4"
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5, delay: 0.2 }}
        >
          <div className="flex-1">
            <input
              type="text"
              value={state.url}
              onChange={handleInputChange}
              placeholder="Enter your URL here (https://example.com)"
              className="input input-bordered w-full"
              data-testid="demo-url-input"
              aria-label="URL to shorten"
            />
          </div>
          <button
            type="submit"
            className="btn btn-primary"
            data-testid="demo-submit-button"
          >
            Shorten URL
          </button>
        </motion.form>

        {state.error && (
          <motion.div
            className="mt-4 alert alert-error"
            data-testid="demo-error-message"
            initial={{ opacity: 0, scale: 0.95 }}
            animate={{ opacity: 1, scale: 1 }}
            transition={{ duration: 0.2 }}
          >
            <svg
              xmlns="http://www.w3.org/2000/svg"
              className="h-5 w-5"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
              aria-hidden="true"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
              />
            </svg>
            <span>{state.error}</span>
          </motion.div>
        )}

        {state.showPrompt && (
          <motion.div
            className="mt-6 card bg-base-200 shadow-xl"
            data-testid="demo-register-prompt"
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.3 }}
          >
            <div className="card-body text-center">
              <div className="mb-4">
                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  className="h-12 w-12 mx-auto text-primary"
                  fill="none"
                  viewBox="0 0 24 24"
                  stroke="currentColor"
                  aria-hidden="true"
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={2}
                    d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1"
                  />
                </svg>
              </div>
              <h3 className="card-title justify-center text-xl">
                Ready to Shorten Your URL!
              </h3>
              <p className="text-base-content/70 mb-4">
                Create a free account to shorten{' '}
                <span className="font-mono text-sm bg-base-300 px-2 py-1 rounded">
                  {state.url}
                </span>{' '}
                and unlock powerful analytics.
              </p>
              <div className="card-actions justify-center">
                <Link to="/register" className="btn btn-primary">
                  Create Free Account
                </Link>
                <Link to="/login" className="btn btn-outline">
                  Login
                </Link>
              </div>
            </div>
          </motion.div>
        )}
      </div>
    </section>
  )
}

export default DemoSection
