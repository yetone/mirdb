import { motion } from 'framer-motion'
import { Link } from 'react-router-dom'

export default function HeroSection() {
  return (
    <section className="min-h-[80vh] flex items-center justify-center px-4 sm:px-6 lg:px-8 bg-gradient-to-b from-base-100 to-base-200">
      <div className="max-w-4xl mx-auto text-center">
        <motion.h1
          className="text-4xl sm:text-5xl lg:text-6xl font-bold text-base-content mb-6"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6 }}
        >
          Shorten Links,{' '}
          <span className="text-primary">Amplify Reach</span>
        </motion.h1>

        <motion.p
          className="text-lg sm:text-xl text-base-content/70 mb-8 max-w-2xl mx-auto"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6, delay: 0.1 }}
        >
          Transform your long URLs into powerful, trackable short links.
          Gain insights with comprehensive analytics and manage all your links
          from one intuitive dashboard.
        </motion.p>

        <motion.div
          className="flex flex-col sm:flex-row gap-4 justify-center"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6, delay: 0.2 }}
        >
          <Link
            to="/register"
            className="btn btn-primary btn-lg"
          >
            Get Started Free
          </Link>
          <Link
            to="/login"
            className="btn btn-outline btn-lg"
          >
            Sign In
          </Link>
        </motion.div>
      </div>
    </section>
  )
}
