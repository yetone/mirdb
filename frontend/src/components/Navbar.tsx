import { Link } from 'react-router-dom'

export default function Navbar() {
  return (
    <nav data-testid="navbar" className="navbar bg-base-100 shadow-sm">
      <div className="flex-1">
        <Link to="/" className="btn btn-ghost text-xl">
          URL Shortener
        </Link>
      </div>
      <div className="flex-none gap-2">
        <Link to="/login" className="btn btn-ghost">
          Login
        </Link>
        <Link to="/register" className="btn btn-primary">
          Register
        </Link>
      </div>
    </nav>
  )
}
