import { Link } from 'react-router-dom'

function Login() {
  return (
    <div className="login-page" data-testid="login-page">
      <h1>Login</h1>
      <form>
        <div>
          <label htmlFor="email">Email</label>
          <input type="email" id="email" name="email" />
        </div>
        <div>
          <label htmlFor="password">Password</label>
          <input type="password" id="password" name="password" />
        </div>
        <button type="submit">Login</button>
      </form>
      <p>
        Don't have an account? <Link to="/register">Sign Up</Link>
      </p>
      <Link to="/">Back to Home</Link>
    </div>
  )
}

export default Login
