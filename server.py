from jukebox import app
import sys

print("Starting Jukebox server...", file=sys.stderr)

if __name__ == '__main__':
    app.init()
    app.app.run(host='0.0.0.0', port=4000, debug=True, ssl_context=('cert.pem', 'key.pem'), use_reloader=False)
