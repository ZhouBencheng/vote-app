// 引入所需的模块
var express = require('express'), // Express框架，用于创建Web服务器
    async = require('async'), // 异步控制流模块
    { Pool } = require('pg'), // PostgreSQL数据库连接池
    cookieParser = require('cookie-parser'), // 解析Cookie的中间件
    app = express(), // 创建Express应用实例
    server = require('http').Server(app), // 创建HTTP服务器
    io = require('socket.io')(server); // 创建Socket.IO实例，启用实时通信

// 设置服务器监听的端口，优先使用环境变量中的PORT值，否则默认为4000
var port = process.env.PORT || 4000;

// 监听Socket.IO的连接事件
io.on('connection', function (socket) {
  // 当有新客户端连接时，发送欢迎消息
  socket.emit('message', { text : 'Welcome!' });

  // 监听客户端的订阅事件
  socket.on('subscribe', function (data) {
    // 将客户端加入指定的频道
    socket.join(data.channel);
  });
});

// 创建PostgreSQL连接池
var pool = new Pool({
  connectionString: 'postgres://postgres:postgres@db/postgres' // 数据库连接字符串
});

// 使用async.retry方法尝试多次连接数据库
async.retry(
  {times: 1000, interval: 1000}, // 尝试1000次，每次间隔1秒
  function(callback) {
    pool.connect(function(err, client, done) {
      if (err) {
        console.error("Waiting for db"); // 如果连接失败，输出等待信息
      }
      callback(err, client); // 回调函数，传递错误和客户端对象
    });
  },
  function(err, client) {
    if (err) {
      return console.error("Giving up"); // 如果最终连接失败，输出放弃信息
    }
    console.log("Connected to db"); // 成功连接后输出信息
    getVotes(client); // 调用getVotes函数获取投票数据
  }
);

// 定义getVotes函数，用于从数据库中获取投票数据
function getVotes(client) {
  client.query('SELECT vote, COUNT(id) AS count FROM votes GROUP BY vote', [], function(err, result) {
    if (err) {
      console.error("Error performing query: " + err); // 查询出错时输出错误信息
    } else {
      var votes = collectVotesFromResult(result); // 处理查询结果
      io.sockets.emit("scores", JSON.stringify(votes)); // 通过Socket.IO广播投票结果
    }

    // 每隔1秒重新获取投票数据
    setTimeout(function() {getVotes(client) }, 1000);
  });
}

// 定义collectVotesFromResult函数，用于处理查询结果
function collectVotesFromResult(result) {
  var votes = {a: 0, b: 0}; // 初始化投票结果对象

  // 遍历查询结果，统计每个选项的投票数
  result.rows.forEach(function (row) {
    votes[row.vote] = parseInt(row.count);
  });

  return votes; // 返回投票结果
}

// 使用中间件
app.use(cookieParser()); // 使用cookieParser中间件解析Cookie
app.use(express.urlencoded()); // 使用express.urlencoded中间件解析URL编码的请求体
app.use(express.static(__dirname + '/views')); // 设置静态文件目录

// 定义根路径的GET请求处理
app.get('/', function (req, res) {
  res.sendFile(path.resolve(__dirname + '/views/index.html')); // 发送index.html文件
});

// 启动服务器并监听指定端口
server.listen(port, function () {
  var port = server.address().port;
  console.log('App running on port ' + port); // 输出服务器启动信息
});
