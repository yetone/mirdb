import { useState, useEffect } from 'react'
import { Link } from 'react-router-dom'
import { SunIcon, MoonIcon } from '@heroicons/react/24/outline'

type Theme = 'light' | 'dark' | 'cyberpunk' | 'synthwave'

export default function Navbar() {
  const [theme, setTheme] = useState<Theme>(() => {
    const savedTheme = localStorage.getItem('theme') as Theme
    return savedTheme || 'light'
  })

  useEffect(() => {
    document.documentElement.setAttribute('data-theme', theme)
    localStorage.setItem('theme', theme)
  }, [theme])

  const toggleTheme = () => {
    setTheme((prevTheme) => (prevTheme === 'light' ? 'dark' : 'light'))
  }

  return (
    <nav
      data-testid="navbar"
      role="navigation"
      className="navbar bg-base-100 shadow-md px-4 sm:px-6 lg:px-8"
    >
      <div className="flex-1">
        <Link
          to="/"
          className="btn btn-ghost text-xl font-bold text-primary normal-case"
        >
          URL Shortener
        </Link>
      </div>
      <div className="flex-none gap-2">
        <button
          data-testid="theme-toggle"
          onClick={toggleTheme}
          aria-label={theme === 'light' ? 'Switch to dark mode' : 'Switch to light mode'}
          className="btn btn-ghost btn-circle"
        >
          {theme === 'light' ? (
            <MoonIcon data-testid="moon-icon" className="h-6 w-6" />
          ) : (
            <SunIcon data-testid="sun-icon" className="h-6 w-6" />
          )}
        </button>
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
