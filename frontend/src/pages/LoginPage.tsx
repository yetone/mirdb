import { Link } from 'react-router-dom'
import { motion } from 'framer-motion'
import BackgroundEffect from '../components/BackgroundEffect'
import FuturisticButton from '../components/FuturisticButton'

interface LoginPageProps {
  'data-testid'?: string
}

export default function LoginPage({ 'data-testid': testId }: LoginPageProps) {
  return (
    <div
      data-testid={testId || 'login-page'}
      className="min-h-screen flex items-center justify-center relative overflow-hidden"
    >
      <BackgroundEffect />

      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.4 }}
        className="relative z-10 w-full max-w-md mx-4"
      >
        <div className="bg-base-100/80 backdrop-blur-md rounded-2xl shadow-xl border border-base-300 p-8">
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            transition={{ delay: 0.1 }}
            className="text-center mb-8"
          >
            <Link
              to="/"
              className="text-2xl font-bold text-base-content hover:text-primary transition-colors"
              data-testid="login-logo"
            >
              URL Shortener
            </Link>
            <h1
              className="text-xl font-semibold text-base-content mt-4"
              data-testid="login-heading"
            >
              Welcome Back
            </h1>
            <p className="text-base-content/60 mt-2">
              Sign in to manage your short links
            </p>
          </motion.div>

          <form className="space-y-6" data-testid="login-form">
            <div>
              <label
                htmlFor="email"
                className="block text-sm font-medium text-base-content mb-2"
              >
                Email
              </label>
              <input
                type="email"
                id="email"
                name="email"
                className="w-full px-4 py-3 rounded-lg bg-base-200 border border-base-300 text-base-content placeholder-base-content/50 focus:outline-none focus:ring-2 focus:ring-primary focus:border-transparent transition-all"
                placeholder="you@example.com"
                data-testid="login-email"
              />
            </div>

            <div>
              <label
                htmlFor="password"
                className="block text-sm font-medium text-base-content mb-2"
              >
                Password
              </label>
              <input
                type="password"
                id="password"
                name="password"
                className="w-full px-4 py-3 rounded-lg bg-base-200 border border-base-300 text-base-content placeholder-base-content/50 focus:outline-none focus:ring-2 focus:ring-primary focus:border-transparent transition-all"
                placeholder="Enter your password"
                data-testid="login-password"
              />
            </div>

            <FuturisticButton
              as="button"
              type="submit"
              variant="primary"
              className="w-full"
              data-testid="login-submit"
            >
              Sign In
            </FuturisticButton>
          </form>

          <motion.p
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            transition={{ delay: 0.3 }}
            className="text-center mt-6 text-base-content/60"
          >
            Don't have an account?{' '}
            <Link
              to="/register"
              className="text-primary hover:underline font-medium"
              data-testid="login-register-link"
            >
              Sign up
            </Link>
          </motion.p>
        </div>
      </motion.div>
    </div>
  )
}
