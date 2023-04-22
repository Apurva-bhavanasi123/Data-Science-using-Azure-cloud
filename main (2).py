import os
from azure.storage.blob import BlobServiceClient
from flask import Flask, render_template, request, url_for, redirect, flash, g, send_from_directory, current_app
from flask import Flask, request, send_file

from werkzeug.security import generate_password_hash, check_password_hash
from flask_sqlalchemy import SQLAlchemy
from flask_login import UserMixin, login_user, LoginManager, login_required, current_user, logout_user
from werkzeug.utils import secure_filename
import re
import random
import pandas as pd
from io import BytesIO


app = Flask(__name__)
blob_service_client = BlobServiceClient.from_connection_string('DefaultEndpointsProtocol=https;AccountName=cclast;AccountKey=PWtpOwmrwaKbduhnOVev17wmW9XtdfPH/GEy8HuyxbA1wO87dsqWytrvOSzt6o95jS2gFLOIgPru+AStryGNJQ==;EndpointSuffix=core.windows.net')
#blob_client = BlobServiceClient.from_connection_string(conn_str).get_blob_client(container=container_name, blob=blob_name)

# app.config['SECRET_KEY'] = 'yek-terces'
app.config['SECRET_KEY'] = 'group8-123'

app.config['SQLALCHEMY_DATABASE_URI']='sqlite:///users.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS']=False
ctx = app.app_context()
db = SQLAlchemy(app)
login_manager = LoginManager()
login_manager.init_app(app)

@login_manager.user_loader
def load_user(id):
    return UserDetails.query.get(int(id))

##CREATE TABLE IN DB
class UserDetails(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    userName = db.Column(db.String(50), unique=True)
    emailId = db.Column(db.String(50), unique=True)
    password = db.Column(db.String(50))
    firstName = db.Column(db.String(50))
    lastName = db.Column(db.String(50))
#Line below only required once, when creating DB.

ctx.push() 
db.create_all()
ctx.pop()
@app.route('/')
def index():
    return render_template("index.html")

@app.route('/upload', methods=['GET', 'POST'])
def upload_file():
    # Create a new blob container if it doesn't exist
    container_client = blob_service_client.get_container_client('cclastblob')
    # container_client.create_container()
    if request.method == 'POST':
        # check if the post request has the file part
        if 'file' not in request.files:
            return 'No file part'
        file = request.files['file']
        if file.filename == '':
            return 'No selected file'
        # upload the file to Azure Blob Storage
        blob_client = container_client.get_blob_client(file.filename)
        blob_client.upload_blob(file.read())
        print(file.read())
        return 'File uploaded successfully'
    return render_template('upload.html')



@app.route('/download/<filename>', methods=['GET'])
def download(filename):

    container_client = blob_service_client.get_container_client("cclastblob")
    blob_client = container_client.get_blob_client(filename)
    downloader = blob_client.download_blob()
    data = downloader.readall()
    file = BytesIO(data)
    print(file)
    # send the file as a byte stream using Flask's send_file function
    return send_file(
        file,
        mimetype='text/csv',
        as_attachment=True,
        download_name=filename
    )

@app.route('/register', methods=['GET','POST'])
def register():
    if request.method == 'POST':
        userName = request.form.get('userName')
        emailId = request.form.get('emailId')
        firstName = request.form.get('firstName')
        lastName = request.form.get('lastName')
        user = UserDetails.query.filter_by(userName=userName).first()
        if user:
            return redirect(url_for("register"))
        hash_and_salted_password = generate_password_hash(
        request.form.get('password'),
        method='pbkdf2:sha256',salt_length=8 )
        new_user = UserDetails(userName=userName,emailId=emailId, password=hash_and_salted_password, firstName=firstName, lastName=lastName )
        db.session.add(new_user)
        db.session.commit()
        # Log in and authenticate user after adding details to database.
        login_user(new_user)
        return redirect(url_for(("dashboard"),user=new_user))
    return render_template("registration.html")

@app.route('/login', methods=['GET','POST'])
def login():
    if request.method == 'POST':
        userName = request.form.get('userName')
        password = request.form.get('password')
        # Find user by emailId entered.
        user = UserDetails.query.filter_by(userName=userName).first()
        if not user:
            return redirect(url_for("register"))
        else:
               if check_password_hash(user.password, password):
                   login_user(user)
                   return redirect(url_for("dashboard"))
    return render_template("login.html")

@app.route('/dashboard')
@login_required
def dashboard():
    return render_template("dashboard.html", user=current_user)

@app.route('/datapull')
@login_required
def datapull():
    return render_template("datapull.html", user=current_user)

@app.route('/vizualization')
@login_required
def vizualization():
    return render_template("visualization.html", user=current_user)

@app.route('/sirfile', methods=['GET','POST'])
def sirfile():
    container_client = blob_service_client.get_container_client('cclastblob')

    if request.method == 'POST':
        hfile = request.files['hfile']
        # hhdf = pd.read_csv(hfile)
        blob_client = container_client.get_blob_client("households.csv")
        blob_client.upload_blob(hfile.read(),overwrite=True)
        print(hfile.read())

        # hhdf.columns = hhdf.columns.str.strip()
        # hhdf.to_csv('household.csv')
        tfile = request.files['tfile']
        # trdf = pd.read_csv(tfile)
        blob_client = container_client.get_blob_client("transactions.csv")
        blob_client.upload_blob(tfile.read(),overwrite=True)
        # trdf.columns = trdf.columns.str.strip()
        # trdf.to_csv('transactions.csv')
        pfile = request.files['pfile']
        # prdf = pd.read_csv(pfile)
        blob_client = container_client.get_blob_client("products.csv")
        blob_client.upload_blob(pfile.read(),overwrite=True)
        # prdf.columns = prdf.columns.str.strip()
        # prdf.to_csv('products.csv')
        return redirect(url_for('sirtable'))
    return render_template("sirfile.html")


@app.route('/sirtable', methods=['GET','POST'])
@login_required
def sirtable():
    if request.method == 'GET':
        # hhdf = pd.read_csv('household.csv')
        # trdf = pd.read_csv('transactions.csv')
        # prdf = pd.read_csv('products.csv')

        container_client = blob_service_client.get_container_client("cclastblob")
        blob_client = container_client.get_blob_client("households.csv")
        downloader = blob_client.download_blob()
        #data = downloader.readall()
        from io import StringIO
        data = downloader.content_as_text()  # Returns the blob contents as a string
        hhdf = pd.read_csv(StringIO(data))
        
        print("dstsd",hhdf)

        #hhdf = pd.DataFrame(data_list[1:], columns=data_list[0])

        
        
        #print("data is ",data.size)
        print("cos",hhdf.columns)
        hhdf.columns = hhdf.columns.str.strip()
        hhdf.columns = hhdf.columns.map(lambda x: x.strip().replace('\n', '').replace('\r', '').replace(' ',""))

        print("cos2",hhdf.columns)

# TRDF
        blob_client = container_client.get_blob_client("transactions.csv")
        downloader = blob_client.download_blob()
        data1 = downloader.content_as_text()  # Returns the blob contents as a string
        trdf = pd.read_csv(StringIO(data1))
        
        print("TRDF",trdf)        
        #print("data is ",data.size)
        print("cos",trdf.columns)
        trdf.columns = trdf.columns.str.strip()
        trdf.columns = trdf.columns.map(lambda x: x.strip().replace('\n', '').replace('\r', '').replace(' ',""))

        print("cos2",trdf.columns)



# PRDF
        blob_client = container_client.get_blob_client("products.csv")
        downloader = blob_client.download_blob()
        data2 = downloader.content_as_text()  # Returns the blob contents as a string
        prdf = pd.read_csv(StringIO(data2))
        
        print("PRDF",prdf)        
        #print("data is ",data.size)
        print("cos",prdf.columns)
        prdf.columns = prdf.columns.str.strip()
        prdf.columns = prdf.columns.map(lambda x: x.strip().replace('\n', '').replace('\r', '').replace(' ',""))

        print("cos2",prdf.columns)
# END
        


        func = open("templates/sirtable.html", "w")
        func.write("<!DOCTYPE html> <html lang='en'>\n")
        func.write("<head><meta charset='UTF-8'><title>Datapull for uploaded data</title></head>\n")
        func.write('<h4><a href={{url_for("dashboard")}} class="btn btn-primary btn-block btn-large">Back</a>\n')
        func.write('<a href={{url_for("logout")}} class="btn btn-primary btn-block btn-large">Logout</a></h4>')
        func.write('<body style="background-color: lightgray;">\n')
        func.write('<form action={{url_for("sirtable")}} method="POST" enctype="multipart/form-data">')
        func.write('<h1>Data pull for uploaded data</h1>\n')
        # hhdf = hhdf.iloc[:, 1:]
        # trdf = trdf.iloc[:, 1:]
        # prdf = prdf.iloc[:, 1:]
        
        samp = hhdf.sample()
        print(samp)
        hhnum = samp.iloc[0][0]
        print("HHNUM", hhnum)
        print("Coloumns", hhdf.columns)
        print("hhdf[HSHD_NUM]", hhdf['HSHD_NUM'])

        # print("hhdf[hhdf[HSHD_NUM]", hhdf[hhdf['HSHD_NUM']])
        ihhdf = hhdf[hhdf['HSHD_NUM'] == hhnum]
        jhtrdf = pd.merge(ihhdf, trdf, how='inner', on='HSHD_NUM')
        joindf = pd.merge(jhtrdf, prdf, how='inner', on='PRODUCT_NUM')
        joindf = joindf.sort_values(by=['BASKET_NUM', 'PURCHASE_', 'DEPARTMENT', 'COMMODITY', 'SPEND'])
        columns_arranged = ['HSHD_NUM', 'BASKET_NUM', 'PURCHASE_', 'PRODUCT_NUM', 'DEPARTMENT', 'COMMODITY', 'SPEND',
                            'UNITS', 'STORE_R', 'WEEK_NUM', 'YEAR', 'L', 'AGE_RANGE', 'MARITAL', 'INCOME_RANGE',
                            'HOMEOWNER',
                            'HSHD_COMPOSITION', 'HH_SIZE', 'CHILDREN']
        joindf = joindf[columns_arranged]
        html = joindf.to_html(table_id="table", index=False)
        hhnumbers = hhdf['HSHD_NUM'].tolist()
        hhnumbers.sort()
        func.write('<select name="hhnum" id="hh">\n')
        for i in hhnumbers:
            func.write(' <option value ="')
            func.write(str(i))
            func.write('"')
            if hhnum == i:
                func.write('selected>')
            else:
                func.write('>')
            func.write(str(i))
            func.write('</option >\n')
        func.write('</select>\n')
        func.write('<input type="submit" value="Select">')
        func.write(html)
        func.write('</body></html>')
        func.close()
        return render_template("sirtable.html", user=current_user)
    if request.method == 'POST':
        hhnum = int(request.form.get('hhnum'))
        container_client = blob_service_client.get_container_client("cclastblob")
        blob_client = container_client.get_blob_client("households.csv")
        downloader = blob_client.download_blob()
        #data = downloader.readall()
        from io import StringIO
        data = downloader.content_as_text()  # Returns the blob contents as a string
        hhdf = pd.read_csv(StringIO(data))
        
        print("dstsd",hhdf)

        #hhdf = pd.DataFrame(data_list[1:], columns=data_list[0])

        
        
        #print("data is ",data.size)
        print("cos",hhdf.columns)
        hhdf.columns = hhdf.columns.str.strip()
        hhdf.columns = hhdf.columns.map(lambda x: x.strip().replace('\n', '').replace('\r', '').replace(' ',""))

        print("cos2",hhdf.columns)

# TRDF
        blob_client = container_client.get_blob_client("transactions.csv")
        downloader = blob_client.download_blob()
        data1 = downloader.content_as_text()  # Returns the blob contents as a string
        trdf = pd.read_csv(StringIO(data1))
        
        print("TRDF",trdf)        
        #print("data is ",data.size)
        print("cos",trdf.columns)
        trdf.columns = trdf.columns.str.strip()
        trdf.columns = trdf.columns.map(lambda x: x.strip().replace('\n', '').replace('\r', '').replace(' ',""))

        print("cos2",trdf.columns)



# PRDF
        blob_client = container_client.get_blob_client("products.csv")
        downloader = blob_client.download_blob()
        data2 = downloader.content_as_text()  # Returns the blob contents as a string
        prdf = pd.read_csv(StringIO(data2))
        
        print("PRDF",prdf)        
        #print("data is ",data.size)
        print("cos",prdf.columns)
        prdf.columns = prdf.columns.str.strip()
        prdf.columns = prdf.columns.map(lambda x: x.strip().replace('\n', '').replace('\r', '').replace(' ',""))

        print("cos2",prdf.columns)

        
        name = "sirtable"
        name = name + str(hhnum)
        name1 = "templates/" + name
        name = name + ".html"
        name1 = name1 + ".html"
        func = open(name1, "w")
        func.write("<!DOCTYPE html> <html lang='en'>\n")
        func.write("<head><meta charset='UTF-8'><title>Datapull for uploaded data</title></head>\n")
        func.write('<h4><a href={{url_for("dashboard")}} class="btn btn-primary btn-block btn-large">Back</a>\n')
        func.write('<a href={{url_for("logout")}} class="btn btn-primary btn-block btn-large">Logout</a></h4>')
        func.write('<body style="background-color: lightgray;">\n')
        func.write('<form action={{url_for("sirtable")}} method="POST" enctype="multipart/form-data">')
        func.write('<h1>Data pull for uploaded data</h1>\n')
        
        hhdf.columns = hhdf.columns.str.strip()
        trdf.columns = trdf.columns.str.strip()
        prdf.columns = prdf.columns.str.strip()
        ihhdf = hhdf[hhdf['HSHD_NUM'] == hhnum]
        jhtrdf = pd.merge(ihhdf, trdf, how='inner', on='HSHD_NUM')
        joindf = pd.merge(jhtrdf, prdf, how='inner', on='PRODUCT_NUM')
        joindf = joindf.sort_values(by=['BASKET_NUM', 'PURCHASE_', 'DEPARTMENT', 'COMMODITY', 'SPEND'])
        columns_arranged = ['HSHD_NUM', 'BASKET_NUM', 'PURCHASE_', 'PRODUCT_NUM', 'DEPARTMENT', 'COMMODITY', 'SPEND',
                            'UNITS','STORE_R', 'WEEK_NUM', 'YEAR', 'L', 'AGE_RANGE', 'MARITAL', 'INCOME_RANGE', 'HOMEOWNER',
                            'HSHD_COMPOSITION', 'HH_SIZE', 'CHILDREN']
        joindf = joindf[columns_arranged]
        html = joindf.to_html(table_id="table", index=False)
        hhnumbers = hhdf['HSHD_NUM'].tolist()
        hhnumbers.sort()
        func.write('<select name="hhnum" id="hh">\n')
        for i in hhnumbers:
            func.write(' <option value ="')
            func.write(str(i))
            func.write('"')
            if hhnum == i:
                func.write('selected>')
            else:
                func.write('>')
            func.write(str(i))
            func.write('</option >\n')
        func.write('</select>\n')
        func.write('<input type="submit" value="Select">')
        func.write(html)
        func.write('\n</form></body></html>')
        func.close()
        return render_template(name, user=current_user)

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8000)
    

