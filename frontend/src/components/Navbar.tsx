import { Link } from 'react-router-dom'
import { FuturisticButton } from './FuturisticButton'

interface NavbarProps {
  onGetStarted?: () => void
  onSignIn?: () => void
}

export function Navbar({ onGetStarted, onSignIn }: NavbarProps) {
  return (
    <nav className="navbar bg-base-100/80 backdrop-blur-md sticky top-0 z-50 border-b border-base-300/50">
      <div className="container mx-auto px-4">
        <div className="flex-1">
          <Link to="/" className="btn btn-ghost text-xl font-bold">
            🔗 URLShortener
          </Link>
        </div>
        <div className="hidden md:flex items-center gap-4">
          <a href="#features" className="btn btn-ghost btn-sm">
            Features
          </a>
          <a href="#how-it-works" className="btn btn-ghost btn-sm">
            How It Works
          </a>
        </div>
        <div className="flex items-center gap-2 ml-4">
          <FuturisticButton
            variant="outline"
            size="sm"
            onClick={onSignIn}
            data-testid="sign-in-nav"
          >
            Sign In
          </FuturisticButton>
          <FuturisticButton
            variant="primary"
            size="sm"
            onClick={onGetStarted}
            data-testid="get-started-nav"
          >
            Get Started
          </FuturisticButton>
        </div>
      </div>
    </nav>
  )
}
