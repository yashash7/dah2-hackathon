from flask import Flask, render_template, request
import csv
from backend_logic import data

'''The below line is used for getting the import name of the place the app is defined like the root module 
from where to look other resources like templates'''
app = Flask(__name__) 

# route is used to assign the urls of the pages to flask. / is for the home or main page
@app.route('/', methods=['POST', 'GET']) # 
def index():
    if request.method=='POST':
        if request.form['submit']=='add':
            return render_template('sample.html', res='Rules can be added!')
        elif request.form['submit']=='update':
            return render_template('sample.html', res='Rules can be updated!')
        elif request.form['submit']=='delete':
            return render_template('sample.html', res='Rules can be deleted!')
    return render_template('sample.html')

# Driver Code
# __name__ == 'main': is used then the code will be executed if the file was run directly and it will not run if it is imported
if __name__ == '__main__':
    app.run()