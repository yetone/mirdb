function Login() {
  return (
    <div className="min-h-screen flex items-center justify-center bg-base-200">
      <div className="card w-96 bg-base-100 shadow-xl">
        <div className="card-body">
          <h2 className="card-title text-2xl font-bold justify-center">Log In</h2>
          <form className="space-y-4">
            <div className="form-control">
              <label className="label">
                <span className="label-text">Email</span>
              </label>
              <input type="email" placeholder="email@example.com" className="input input-bordered" />
            </div>
            <div className="form-control">
              <label className="label">
                <span className="label-text">Password</span>
              </label>
              <input type="password" placeholder="********" className="input input-bordered" />
            </div>
            <div className="form-control mt-6">
              <button className="btn btn-primary">Log In</button>
            </div>
          </form>
        </div>
      </div>
    </div>
  )
}

export default Login
