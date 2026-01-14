import { motion } from 'framer-motion'
import { Link } from 'react-router-dom'
import BackgroundEffect from '../components/BackgroundEffect'

const Register = () => {
  return (
    <>
      <BackgroundEffect />
      <div
        className="min-h-screen flex items-center justify-center bg-base-200 px-4"
        data-testid="register-page"
      >
        <motion.div
          className="w-full max-w-md"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.5 }}
        >
          <div className="card bg-base-100 shadow-xl">
            <div className="card-body">
              <h1 className="card-title text-2xl font-bold text-center justify-center mb-6">
                Create Account
              </h1>

              <form className="space-y-4" data-testid="register-form">
                <div className="form-control">
                  <label className="label" htmlFor="username">
                    <span className="label-text">Username</span>
                  </label>
                  <input
                    type="text"
                    id="username"
                    name="username"
                    placeholder="Enter your username"
                    className="input input-bordered w-full"
                    data-testid="register-username"
                    required
                  />
                </div>

                <div className="form-control">
                  <label className="label" htmlFor="email">
                    <span className="label-text">Email</span>
                  </label>
                  <input
                    type="email"
                    id="email"
                    name="email"
                    placeholder="Enter your email"
                    className="input input-bordered w-full"
                    data-testid="register-email"
                    required
                  />
                </div>

                <div className="form-control">
                  <label className="label" htmlFor="password">
                    <span className="label-text">Password</span>
                  </label>
                  <input
                    type="password"
                    id="password"
                    name="password"
                    placeholder="Enter your password"
                    className="input input-bordered w-full"
                    data-testid="register-password"
                    required
                  />
                </div>

                <div className="form-control mt-6">
                  <button
                    type="submit"
                    className="btn btn-primary w-full"
                    data-testid="register-submit"
                  >
                    Sign Up
                  </button>
                </div>
              </form>

              <div className="divider">OR</div>

              <p className="text-center text-base-content/70">
                Already have an account?{' '}
                <Link to="/login" className="link link-primary" data-testid="login-link">
                  Sign in
                </Link>
              </p>
            </div>
          </div>
        </motion.div>
      </div>
    </>
  )
}

export default Register
