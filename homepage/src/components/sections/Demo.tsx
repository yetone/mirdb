/**
 * Demo section displaying the usage GIF.
 * Owner: Scenario 3 - Usage Demo GIF Display
 *
 * Requirements:
 * - REQ-6: Display usage.gif from assets
 * - NFR-7: Appropriate alt text for accessibility
 * - Non-blocking load (lazy loading)
 */

import { useState } from 'react'
import './Demo.css'

export function Demo() {
  const [isLoaded, setIsLoaded] = useState(false)
  const [hasError, setHasError] = useState(false)

  const handleLoad = () => {
    setIsLoaded(true)
  }

  const handleError = () => {
    setHasError(true)
  }

  return (
    <section className="demo section" aria-labelledby="demo-heading">
      <div className="container">
        <h2 id="demo-heading" className="demo__heading">
          See MirDB in Action
        </h2>
        <p className="demo__description">
          Watch how easy it is to use MirDB with standard Memcached commands.
        </p>
        <div className="demo__content">
          {!isLoaded && !hasError && (
            <div className="demo__placeholder" aria-hidden="true">
              <div className="demo__spinner" />
              <span>Loading demo...</span>
            </div>
          )}
          {hasError && (
            <div className="demo__error" role="alert">
              <p>Failed to load demo. Please refresh the page.</p>
            </div>
          )}
          <img
            src="/assets/usage.gif"
            alt="MirDB usage demonstration showing Memcached protocol commands including set and get operations"
            className={`demo__image ${isLoaded ? 'demo__image--loaded' : ''}`}
            loading="lazy"
            onLoad={handleLoad}
            onError={handleError}
            width="800"
            height="500"
          />
        </div>
      </div>
    </section>
  )
}
