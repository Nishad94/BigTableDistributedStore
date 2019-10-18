from flask import Flask

app = Flask(__name__)

@app.route('/')
def index():
    return "Hello, World!"

@app.route('/api/tables/', methods=['GET'])
def list_tables():
    pass


"""
Check https://blog.miguelgrinberg.com/post/designing-a-restful-api-with-python-and-flask for Flask tutorial

To run this server on command line, type: python tablet_server.py
"""
if __name__ == '__main__':
    app.run(debug=True)