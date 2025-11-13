from App.views import app, scheduler
from App.lockfile import cleanup_all_lockfiles

if __name__ == "__main__":
    cleanup_all_lockfiles()
    scheduler.init_app(app)
    scheduler.start()
    app.run(host='0.0.0.0', port=5001 ,debug=False)