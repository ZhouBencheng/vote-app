from flask import Flask, render_template, request, make_response, g
from redis import Redis
import os
import socket
import random
import json
import logging

# 从环境变量中获取选项A和选项B的值，默认为“Cats”和“Dogs”
option_a = os.getenv('OPTION_A', "Cats")
option_b = os.getenv('OPTION_B', "Dogs")
# 获取当前主机名
hostname = socket.gethostname()

# 创建Flask应用实例
app = Flask(__name__)

# 配置Gunicorn的错误日志记录器
gunicorn_error_logger = logging.getLogger('gunicorn.error')
app.logger.handlers.extend(gunicorn_error_logger.handlers)
app.logger.setLevel(logging.INFO)

# 获取Redis连接
def get_redis():
    # 如果全局对象g中没有redis属性，则创建一个新的Redis连接
    if not hasattr(g, 'redis'):
        g.redis = Redis(host="redis", db=0, socket_timeout=5)
    return g.redis

# 定义路由处理函数，处理根路径的GET和POST请求
@app.route("/", methods=['POST','GET'])
def hello():
    # 从请求的cookie中获取voter_id，如果不存在则生成一个新的
    voter_id = request.cookies.get('voter_id')
    if not voter_id:
        voter_id = hex(random.getrandbits(64))[2:-1]

    vote = None

    # 如果请求方法是POST，则处理投票
    if request.method == 'POST':
        redis = get_redis()  # 获取Redis连接
        vote = request.form['vote']  # 获取表单中的投票选项
        app.logger.info('Received vote for %s', vote)  # 记录收到的投票信息
        data = json.dumps({'voter_id': voter_id, 'vote': vote})  # 将投票信息转换为JSON格式
        redis.rpush('votes', data)  # 将投票信息推入Redis列表

    # 创建响应对象，渲染模板并设置cookie
    resp = make_response(render_template(
        'index.html',
        option_a=option_a,
        option_b=option_b,
        hostname=hostname,
        vote=vote,
    ))
    resp.set_cookie('voter_id', voter_id)  # 设置voter_id到cookie中
    return resp  # 返回响应对象

# 如果当前模块是主程序，则运行Flask应用
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80, debug=True, threaded=True)
