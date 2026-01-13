import { Link } from 'react-router-dom'

function Register() {
  return (
    <div className="register-page" data-testid="register-page">
      <h1>Sign Up</h1>
      <form>
        <div>
          <label htmlFor="name">Name</label>
          <input type="text" id="name" name="name" />
        </div>
        <div>
          <label htmlFor="email">Email</label>
          <input type="email" id="email" name="email" />
        </div>
        <div>
          <label htmlFor="password">Password</label>
          <input type="password" id="password" name="password" />
        </div>
        <button type="submit">Sign Up</button>
      </form>
      <p>
        Already have an account? <Link to="/login">Login</Link>
      </p>
      <Link to="/" data-testid="back-to-home-link">Back to Home</Link>
    </div>
  )
}

export default Register
