import { Link } from 'react-router-dom'

export default function Login() {
  return (
    <div className="min-h-screen flex items-center justify-center bg-base-200">
      <div className="card w-96 bg-base-100 shadow-xl">
        <div className="card-body">
          <h2 className="card-title justify-center">Login</h2>
          <form className="space-y-4">
            <div className="form-control">
              <label className="label">
                <span className="label-text">Email</span>
              </label>
              <input type="email" className="input input-bordered" />
            </div>
            <div className="form-control">
              <label className="label">
                <span className="label-text">Password</span>
              </label>
              <input type="password" className="input input-bordered" />
            </div>
            <button type="submit" className="btn btn-primary w-full">Login</button>
          </form>
          <p className="text-center mt-4">
            Don't have an account? <Link to="/register" className="link link-primary">Register</Link>
          </p>
        </div>
      </div>
    </div>
  )
}
