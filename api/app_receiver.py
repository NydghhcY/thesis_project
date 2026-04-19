from flask import Flask, request, jsonify
import pymysql

app = Flask(__name__)

# ================= 数据库配置 =================
DB_CONFIG = {
    'host': '127.0.0.1', 
    'port': 3306,
    'user': 'root',
    'password': '123456', 
    'database': 'thesis_db',      
    'charset': 'utf8mb4',
    'autocommit': False  # 显式关闭自动提交，由我们手动控制事务
}

@app.route('/api/receive_papers', methods=['POST'])
def receive_papers():
    """接收宿主机爬虫发来的 JSON 数据并存入 MySQL"""
    data = request.json
    
    if not data or not isinstance(data, list):
        return jsonify({"status": "error", "message": "无效的数据格式"}), 400

    conn = None
    try:
        # 1. 建立连接
        conn = pymysql.connect(**DB_CONFIG)
        # 使用 DictCursor 可以在报错时打印更清晰的上下文
        cursor = conn.cursor()
        
        # 2. 准备插入 SQL (使用 INSERT IGNORE 以防万一你之后想恢复唯一索引)
        insert_sql = """
            INSERT INTO raw_paper_data (title, discipline, publish_year, authors, source)
            VALUES (%s, %s, %s, %s, %s)
        """
        
        # 3. 转换数据格式
        values = [
            (item.get('title'), item.get('discipline'), item.get('publish_year'), 
             item.get('authors'), item.get('source', 'Crossref_API')) 
            for item in data
        ]
        
        # 4. 执行核心写入逻辑 (根据你的要求加入 try-except 保护)
        try:
            cursor.executemany(insert_sql, values)
            conn.commit()
            inserted_count = cursor.rowcount
            print(f"[*] 成功接收并入库 {inserted_count} 条数据！")
            return jsonify({"status": "success", "inserted": inserted_count}), 200
            
        except Exception as e:
            # ❌ 数据库写入失败时，执行回滚，不影响下一个批次
            conn.rollback() 
            print(f"❌ 数据库写入失败（当前批次已回滚）: {e}")
            # 返回 500 告知爬虫这一批次有问题，但 Flask 进程不会挂掉
            return jsonify({"status": "error", "message": f"DB Error: {str(e)}"}), 500

    except Exception as e:
        print(f"[!] 系统连接错误: {e}")
        return jsonify({"status": "error", "message": "Server internal connection error"}), 500
    
    finally:
        # 无论成功失败，都关闭连接，释放虚拟机资源
        if conn:
            cursor.close()
            conn.close()

if __name__ == '__main__':
    print("🚀 App Receiver 已启动，正在监听宿主机发送的数据...")
    # 建议开启 threaded=True 以应对大数据并发
    app.run(host='0.0.0.0', port=5000, threaded=True)
