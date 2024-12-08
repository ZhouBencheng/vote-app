using System;
using System.Data.Common;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using Newtonsoft.Json;
using Npgsql;
using StackExchange.Redis;

namespace Worker
{
    public class Program
    {
        public static int Main(string[] args)
        {
            try
            {
                // 打开数据库连接
                var pgsql = OpenDbConnection("Server=db;Username=postgres;Password=postgres;");
                // 打开Redis连接
                var redisConn = OpenRedisConnection("redis");
                var redis = redisConn.GetDatabase();

                // Npgsql尚未实现Keep alive功能，使用推荐的解决方案
                var keepAliveCommand = pgsql.CreateCommand();
                keepAliveCommand.CommandText = "SELECT 1";

                // 定义匿名类型用于反序列化JSON
                var definition = new { vote = "", voter_id = "" };
                while (true)
                {
                    // 为了防止CPU占用过高，每100毫秒查询一次
                    Thread.Sleep(100);

                    // 如果Redis连接断开，则重新连接
                    if (redisConn == null || !redisConn.IsConnected) {
                        Console.WriteLine("Reconnecting Redis");
                        redisConn = OpenRedisConnection("redis");
                        redis = redisConn.GetDatabase();
                    }
                    // 从Redis中弹出一个投票信息
                    string json = redis.ListLeftPopAsync("votes").Result;
                    if (json != null)
                    {
                        // 反序列化投票信息
                        var vote = JsonConvert.DeserializeAnonymousType(json, definition);
                        Console.WriteLine($"Processing vote for '{vote.vote}' by '{vote.voter_id}'");
                        // 如果数据库连接断开，则重新连接
                        if (!pgsql.State.Equals(System.Data.ConnectionState.Open))
                        {
                            Console.WriteLine("Reconnecting DB");
                            pgsql = OpenDbConnection("Server=db;Username=postgres;Password=postgres;");
                        }
                        else
                        { 
                            // 正常处理投票
                            UpdateVote(pgsql, vote.voter_id, vote.vote);
                        }
                    }
                    else
                    {
                        // 执行Keep alive命令
                        keepAliveCommand.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                // 捕获异常并输出错误信息
                Console.Error.WriteLine(ex.ToString());
                return 1;
            }
        }

        // 打开数据库连接的方法
        private static NpgsqlConnection OpenDbConnection(string connectionString)
        {
            NpgsqlConnection connection;

            while (true)
            {
                try
                {
                    // 创建并打开数据库连接
                    connection = new NpgsqlConnection(connectionString);
                    connection.Open();
                    break;
                }
                catch (SocketException)
                {
                    // 如果连接失败，等待1秒后重试
                    Console.Error.WriteLine("Waiting for db");
                    Thread.Sleep(1000);
                }
                catch (DbException)
                {
                    // 如果连接失败，等待1秒后重试
                    Console.Error.WriteLine("Waiting for db");
                    Thread.Sleep(1000);
                }
            }

            Console.Error.WriteLine("Connected to db");

            // 创建投票表，如果不存在则创建
            var command = connection.CreateCommand();
            command.CommandText = @"CREATE TABLE IF NOT EXISTS votes (
                                        id VARCHAR(255) NOT NULL UNIQUE,
                                        vote VARCHAR(255) NOT NULL
                                    )";
            command.ExecuteNonQuery();

            return connection;
        }

        // 打开Redis连接的方法
        private static ConnectionMultiplexer OpenRedisConnection(string hostname)
        {
            // 使用IP地址解决特定问题
            var ipAddress = GetIp(hostname);
            Console.WriteLine($"Found redis at {ipAddress}");

            while (true)
            {
                try
                {
                    // 连接到Redis
                    Console.Error.WriteLine("Connecting to redis");
                    return ConnectionMultiplexer.Connect(ipAddress);
                }
                catch (RedisConnectionException)
                {
                    // 如果连接失败，等待1秒后重试
                    Console.Error.WriteLine("Waiting for redis");
                    Thread.Sleep(1000);
                }
            }
        }

        // 获取主机名对应的IP地址
        private static string GetIp(string hostname)
            => Dns.GetHostEntryAsync(hostname)
                .Result
                .AddressList
                .First(a => a.AddressFamily == AddressFamily.InterNetwork)
                .ToString();

        // 更新投票信息的方法
        private static void UpdateVote(NpgsqlConnection connection, string voterId, string vote)
        {
            var command = connection.CreateCommand();
            try
            {
                // 尝试插入新的投票记录
                command.CommandText = "INSERT INTO votes (id, vote) VALUES (@id, @vote)";
                command.Parameters.AddWithValue("@id", voterId);
                command.Parameters.AddWithValue("@vote", vote);
                command.ExecuteNonQuery();
            }
            catch (DbException)
            {
                // 如果插入失败，则更新已有记录
                command.CommandText = "UPDATE votes SET vote = @vote WHERE id = @id";
                command.ExecuteNonQuery();
            }
            finally
            {
                // 释放命令资源
                command.Dispose();
            }
        }
    }
}