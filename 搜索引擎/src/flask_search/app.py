from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    session,
    flash,
    send_file,
    jsonify,
)
import webbrowser
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash
from flask_session import Session
import sys
import os

# 将 search 模块所在的路径添加到 sys.path
sys.path.append("D:\\test\\ir\\project\\src")
import search  # 导入你的搜索模块
import show_photos

app = Flask(__name__)
app.config["SESSION_TYPE"] = "filesystem"  # 使用文件系统存储会话数据
app.secret_key = "your_secret_key"
Session(app)  # 启用 Flask-Session

# 配置数据库
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///users.db"  # 使用 SQLite 数据库
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)


# 数据库模型
class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    nickname = db.Column(db.String(100), unique=True, nullable=False)
    password = db.Column(db.String(200), nullable=False)
    college = db.Column(db.String(200), nullable=False)


class SearchHistory(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    search_query = db.Column(db.String(200), nullable=False)


# 根路由，默认重定向到登录页面
@app.route("/")
def index():
    return redirect(url_for("login"))  # 如果未登录，跳转到登录页面


# 注册页面
@app.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        nickname = request.form["nickname"]
        password = request.form["password"]
        college = request.form["college"]

        # 使用正确的哈希方法
        hashed_password = generate_password_hash(password, method="pbkdf2:sha256")

        # 检查用户是否已经存在
        existing_user = User.query.filter_by(nickname=nickname).first()
        if existing_user:
            return "User already exists, please choose a different nickname."

        # 创建新用户对象并存入数据库
        new_user = User(nickname=nickname, password=hashed_password, college=college)
        db.session.add(new_user)
        db.session.commit()

        # 注册成功后，重定向到登录页面
        return redirect(url_for("login"))

    return render_template("register.html")


# 登录页面
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        nickname = request.form["nickname"]
        password = request.form["password"]

        # 查找用户
        user = User.query.filter_by(nickname=nickname).first()

        # 检查密码是否匹配
        if user and check_password_hash(user.password, password):
            session["username"] = user.nickname  # 在会话中保存用户名
            session["user_id"] = user.id  # 在会话中保存用户ID
            session["college"] = user.college
            return redirect(url_for("search_page"))  # 登录成功，重定向到搜索页面
        else:
            return "Invalid credentials, please try again."  # 登录失败提示

    return render_template("login.html")


@app.route("/search", methods=["GET", "POST"])
def search_page():
    if "username" not in session:
        return redirect(url_for("login"))  # 如果未登录，重定向到登录页面

    user_id = session["user_id"]
    college = session["college"]
    results = None
    if request.method == "POST":
        query = request.form["query"]
        search_type = request.form["search_type"]  # 获取查询类型

        # 检查是否已经存在相同的搜索记录
        existing_record = SearchHistory.query.filter_by(
            user_id=user_id, search_query=query
        ).first()
        if not existing_record:
            # 保存搜索记录
            new_record = SearchHistory(user_id=user_id, search_query=query)
            db.session.add(new_record)
            db.session.commit()

        if search_type == "file":
            results = search.all_search(query, college, "file")  # 调用文件搜索
        else:
            results = search.all_search(query, college, None)  # 调用普通搜索

    # 获取用户的搜索记录
    search_history = SearchHistory.query.filter_by(user_id=user_id).all()
    search_history = [record.search_query for record in search_history]

    return render_template(
        "search.html", results=results, search_history=search_history
    )


@app.route("/home")
def home():
    url = request.args.get("url")
    # 调用 showphoto 模块生成网页快照
    snapshot_path = show_photos.searchphoto(url)
    # 返回快照文件地址
    if snapshot_path is None:
        flash("没有对应的快照")
        return redirect(url_for("search_page"))
    print(snapshot_path)
    file_url = "file:///" + snapshot_path
    webbrowser.open(file_url)
    return jsonify({"success": True, "file_url": file_url})


# 主程序
if __name__ == "__main__":
    with app.app_context():
        db.create_all()  # 确保数据库表已创建
    app.run(debug=True)
