import { motion } from 'framer-motion'
import { Link } from 'react-router-dom'

const FinalCTASection = () => {
  return (
    <section
      id="final-cta"
      data-testid="final-cta-section"
      className="py-20 px-4 bg-gradient-to-br from-primary/10 via-base-100 to-secondary/10"
    >
      <div className="container mx-auto">
        <motion.div
          className="text-center max-w-3xl mx-auto"
          initial={{ opacity: 0, y: 30 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.6 }}
        >
          <h2 className="text-3xl md:text-4xl lg:text-5xl font-bold mb-6 bg-gradient-to-r from-primary to-secondary bg-clip-text text-transparent">
            Ready to Start Tracking Your Links?
          </h2>
          <p
            data-testid="final-cta-subheadline"
            className="text-lg md:text-xl text-base-content/70 mb-10 max-w-2xl mx-auto"
          >
            Join thousands of users who trust our platform to shorten URLs and track every click
            with powerful analytics. Create your free account today.
          </p>
          <motion.div
            initial={{ opacity: 0, scale: 0.9 }}
            whileInView={{ opacity: 1, scale: 1 }}
            viewport={{ once: true }}
            transition={{ duration: 0.4, delay: 0.3 }}
          >
            <Link
              to="/register"
              data-testid="final-cta-button"
              className="btn btn-primary btn-lg text-lg px-8"
            >
              Start Shortening URLs Today
              <svg
                xmlns="http://www.w3.org/2000/svg"
                fill="none"
                viewBox="0 0 24 24"
                strokeWidth={2}
                stroke="currentColor"
                className="w-5 h-5 ml-2"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  d="M13.5 4.5L21 12m0 0l-7.5 7.5M21 12H3"
                />
              </svg>
            </Link>
          </motion.div>
        </motion.div>
      </div>
    </section>
  )
}

export default FinalCTASection
