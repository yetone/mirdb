/**
 * Demo Section Component.
 * Owner: Scenario 4 - Demo & Social Proof
 *
 * Expected behavior:
 * - Visual mockup showing URL transformation
 * - "Long URL -> Short URL -> Analytics" flow visualization
 * - Realistic example URLs
 * - Clear input-output relationship
 * - Subtle animation or interactive element
 */

function DemoSection() {
  return (
    <section
      id="demo"
      className="py-16 lg:py-24 px-4 bg-base-100"
      data-testid="demo-section"
    >
      <div className="container mx-auto max-w-5xl">
        <div className="text-center mb-12">
          <h2 className="text-3xl md:text-4xl font-bold text-base-content mb-4">
            See How It Works
          </h2>
          <p className="text-base-content/80 max-w-2xl mx-auto">
            Transform your long URLs into short, trackable links in seconds.
          </p>
        </div>

        {/* URL Transformation Flow */}
        <div
          className="flex flex-col lg:flex-row items-center justify-center gap-4 lg:gap-6"
          data-testid="demo-transformation-flow"
        >
          {/* Step 1: Long URL */}
          <div
            className="w-full lg:w-auto lg:flex-1 bg-base-200 rounded-xl p-4 md:p-6 shadow-md"
            data-testid="demo-long-url"
          >
            <div className="flex items-center gap-2 mb-2">
              <span className="badge badge-primary badge-sm">1</span>
              <span className="text-sm font-semibold text-base-content">Your Long URL</span>
            </div>
            <div className="bg-base-100 rounded-lg p-3 border border-base-300">
              <code className="text-xs md:text-sm text-base-content/80 break-all">
                https://www.example.com/products/category/electronics/item/12345?utm_source=newsletter&utm_medium=email&utm_campaign=spring_sale
              </code>
            </div>
          </div>

          {/* Arrow Indicator 1 */}
          <div
            className="flex items-center justify-center py-2 lg:py-0"
            data-testid="demo-flow-indicator-1"
          >
            <svg
              className="w-8 h-8 text-primary rotate-90 lg:rotate-0"
              fill="none"
              stroke="currentColor"
              viewBox="0 0 24 24"
              aria-hidden="true"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M13 7l5 5m0 0l-5 5m5-5H6"
              />
            </svg>
          </div>

          {/* Step 2: Short URL */}
          <div
            className="w-full lg:w-auto lg:flex-1 bg-primary/10 rounded-xl p-4 md:p-6 shadow-md border-2 border-primary/20"
            data-testid="demo-short-url"
          >
            <div className="flex items-center gap-2 mb-2">
              <span className="badge badge-primary badge-sm">2</span>
              <span className="text-sm font-semibold text-base-content">Short Link</span>
            </div>
            <div className="bg-base-100 rounded-lg p-3 border border-primary/30">
              <code className="text-lg md:text-xl font-bold text-primary">
                shrt.io/abc123
              </code>
            </div>
          </div>

          {/* Arrow Indicator 2 */}
          <div
            className="flex items-center justify-center py-2 lg:py-0"
            data-testid="demo-flow-indicator-2"
          >
            <svg
              className="w-8 h-8 text-primary rotate-90 lg:rotate-0"
              fill="none"
              stroke="currentColor"
              viewBox="0 0 24 24"
              aria-hidden="true"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M13 7l5 5m0 0l-5 5m5-5H6"
              />
            </svg>
          </div>

          {/* Step 3: Analytics */}
          <div
            className="w-full lg:w-auto lg:flex-1 bg-base-200 rounded-xl p-4 md:p-6 shadow-md"
            data-testid="demo-analytics"
          >
            <div className="flex items-center gap-2 mb-2">
              <span className="badge badge-primary badge-sm">3</span>
              <span className="text-sm font-semibold text-base-content">Track Analytics</span>
            </div>
            <div className="bg-base-100 rounded-lg p-3 border border-base-300">
              <div className="grid grid-cols-3 gap-2 text-center">
                <div>
                  <div className="text-lg md:text-xl font-bold text-primary">2.4K</div>
                  <div className="text-xs text-base-content/80">Clicks</div>
                </div>
                <div>
                  <div className="text-lg md:text-xl font-bold text-primary">12</div>
                  <div className="text-xs text-base-content/80">Countries</div>
                </div>
                <div>
                  <div className="text-lg md:text-xl font-bold text-primary">67%</div>
                  <div className="text-xs text-base-content/80">Mobile</div>
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Additional Context */}
        <div className="text-center mt-8 text-base-content/80 text-sm">
          <p>Your links are ready to share instantly. No sign-up required to try!</p>
        </div>
      </div>
    </section>
  );
}

export default DemoSection;
