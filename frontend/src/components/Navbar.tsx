import { Link } from 'react-router-dom'
import { motion } from 'framer-motion'

interface NavbarProps {
  'data-testid'?: string
}

export default function Navbar({ 'data-testid': testId }: NavbarProps) {
  return (
    <motion.header
      initial={{ opacity: 0, y: -20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.4 }}
      data-testid={testId || 'navbar'}
      className="fixed top-0 left-0 right-0 z-50 bg-base-100/80 backdrop-blur-md border-b border-base-300"
    >
      <nav className="container mx-auto px-4 py-4 flex items-center justify-between">
        <Link
          to="/"
          className="text-xl font-bold text-base-content hover:text-primary transition-colors"
          data-testid="navbar-logo"
        >
          URL Shortener
        </Link>

        <div className="flex items-center gap-4">
          <Link
            to="/login"
            className="text-base-content/80 hover:text-primary transition-colors font-medium"
            data-testid="navbar-login"
          >
            Login
          </Link>
          <Link
            to="/register"
            className="bg-primary text-primary-content px-4 py-2 rounded-lg font-medium hover:bg-primary/80 transition-colors"
            data-testid="navbar-get-started"
          >
            Get Started
          </Link>
        </div>
      </nav>
    </motion.header>
  )
}
