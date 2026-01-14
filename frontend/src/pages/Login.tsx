import { motion } from 'framer-motion'
import { Link } from 'react-router-dom'
import BackgroundEffect from '../components/BackgroundEffect'

const Login = () => {
  return (
    <>
      <BackgroundEffect />
      <div
        className="min-h-screen flex items-center justify-center bg-base-200 px-4"
        data-testid="login-page"
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
                Welcome Back
              </h1>

              <form className="space-y-4" data-testid="login-form">
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
                    data-testid="login-username"
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
                    data-testid="login-password"
                    required
                  />
                </div>

                <div className="form-control mt-6">
                  <button
                    type="submit"
                    className="btn btn-primary w-full"
                    data-testid="login-submit"
                  >
                    Sign In
                  </button>
                </div>
              </form>

              <div className="divider">OR</div>

              <p className="text-center text-base-content/70">
                Don't have an account?{' '}
                <Link to="/register" className="link link-primary" data-testid="register-link">
                  Sign up
                </Link>
              </p>
            </div>
          </div>
        </motion.div>
      </div>
    </>
  )
}

export default Login
