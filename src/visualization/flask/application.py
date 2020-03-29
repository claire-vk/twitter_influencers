import json
from flask import Flask, render_template, request, session, redirect, url_for
# from models import db, User, Place
from forms import SignupForm, LoginForm, AddressForm
from flask.ext.sqlalchemy import SQLAlchemy
from werkzeug import generate_password_hash, check_password_hash
from sqlalchemy import create_engine





app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://flask:wizjysys@flaskest.csjkhjjygutf.us-east-1.rds.amazonaws.com:3306/flaskdb'
app.config['SQLALCHEMY_ECHO'] = True

db = SQLAlchemy(app)

class User(db.Model):
  __tablename__ = 'users'
  uid = db.Column(db.Integer, primary_key = True, autoincrement=True)
  firstname = db.Column(db.String(100))
  lastname = db.Column(db.String(100))
  email = db.Column(db.String(120), unique=True)
  pwdhash = db.Column(db.String(54))

  def __init__(self, firstname, lastname, email, password):
    self.firstname = firstname.title()
    self.lastname = lastname.title()
    self.email = email.lower()
    self.set_password(password)
     
  def set_password(self, password):
    self.pwdhash = generate_password_hash(password)

  def check_password(self, password):
    return check_password_hash(self.pwdhash, password)


class influencer(db.Model):
  __tablename__='influence_score'
  User_id = db.Column(db.Integer,primary_key=True)
  User_name = db.Column(db.String(100))
  Number_of_Followers=db.Column(db.Integer)
  Number_of_Retweets=db.Column(db.Integer)
  Number_of_Likes=db.Column(db.Integer)
  Influencer_Score = db.Column(db.Float)
  Sentiment_Score = db.Column(db.String(100))
  Img_URL = db.Column(db.String(100))
  Tweets = db.Column(db.String(100))


  def __init__(self,User_id,User_name ,Number_of_Followers,Number_of_Retweets,Number_of_Likes,Influencer_Score,Sentiment_Score,Img_URL):
    self.User_id = User_id
    self.User_name = User_name
    self.Number_of_Followers = Number_of_Followers
    self.Number_of_Retweets = Number_of_Retweets
    self.Number_of_Likes = Number_of_Likes
    self.Influencer_Score = Influencer_Score
    self.Sentiment_Score = Sentiment_Score
    self.Img_URL = Img_URL
    self.Tweets = Tweets







app.secret_key = "development-key"


@app.route('/')
def welcome():
    return render_template('welcome.html')

@app.route("/about")
def about():
  return render_template("about.html")

@app.route("/signup", methods=["GET", "POST"])
def signup():
  if 'email' in session:
    return redirect(url_for('gmap'))

  form = SignupForm()
  if request.method == "POST":
    if form.validate() == False:
      return render_template('signup.html', form=form)
    else:
      newuser = User(form.first_name.data, form.last_name.data, form.email.data, form.password.data)
      print newuser
      db.session.add(newuser)
      db.session.commit()

      session['email'] = newuser.email
      return redirect(url_for('gmap'))

  elif request.method == "GET":
    return render_template('signup.html', form=form)

@app.route("/login", methods=["GET", "POST"])
def login():
  if 'email' in session:
    return redirect(url_for('gmap'))

  form = LoginForm()

  if request.method == "POST":
    if form.validate() == False:
      return render_template("login.html", form=form)
    else:
      email = form.email.data 
      password = form.password.data 

      user = User.query.filter_by(email=email).first()
      if user is not None and user.check_password(password):
        session['email'] = form.email.data 
        return redirect(url_for('gmap'))
      else:
        return redirect(url_for('login'))

  elif request.method == 'GET':
    return render_template('login.html', form=form)

@app.route("/logout")
def logout():
  session.pop('email', None)
  return redirect(url_for('index'))


@app.route('/test')
def test():
  tweets = influencer.query.all()

  return render_template('test.html',tweets =tweets)




@app.route('/gmap',methods=['GET'])
def gmap():
    return render_template('gmap.html',result=json.dumps({"a":[{"o":1},{"o":2}]}, indent = 2))
#mapping
map={
    1:'Love',
    2:'Food',
    3:'Trump',
    4:'Travel',
    5:'New York',
    6:'Job',
    7:'Hillary',
    8:'Fashion',
    9:'LOL',
    10:'Vegas'
}


@app.route('/table')
def table():

  influ= influencer.query.all()
  print influ[0].Influencer_Score
  return render_template("table.html", influ = influ)




@app.route("/chart")
def chart():
    global labels,values
    labels = []
    values = []
    return render_template('chart.html', values=values, labels=labels)


@app.route('/refreshData')
def refresh_graph_data():
    global labels, values
    print("labels now: " + str(labels))
    print("data now: " + str(values))
    return jsonify(sLabel=labels, sData=values)


@app.route('/updateData', methods=['POST'])
def update_data_post():
    global labels, values
    if not request.form or 'data' not in request.form:
        return "error",400
    labels = ast.literal_eval(request.form['label'])
    values = ast.literal_eval(request.form['data'])
    print("labels received: " + str(labels))
    print("data received: " + str(values))
    return "success",201



if __name__ == '__main__':
    app.run(debug = True)
