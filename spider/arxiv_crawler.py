import requests
import time
from datetime import datetime

# ================= 配置区 =================
RECEIVER_URL = "http://192.168.88.130:5000/api/receive_papers"

# OpenAlex 学科 ID
DISCIPLINE = {
    "Computer Science": "C41008148",
    "Physics": "C121332964",
    "Medicine": "C71924100",
    "Biology": "C86803240",
    "Chemistry": "C185592680",
    "Engineering": "C127413603",
    "Psychology": "C157449865",
    "Economics": "C162324750",
    "Sociology": "C144024400",
    "Geography": "C205649164",
    "Environmental Science": "C39432329",
    "Education": "C145420912",
    "History": "C95457728",
    "Philosophy": "C138885662",
    "Law": "C199539241"

}

DISCIPLINES = {
    "Artificial Intelligence": "C154945302",      # 综合 AI
    "Machine Learning": "C119857082",             # 机器学习 (核心算法)
    "Computer Vision": "C120314980",              # 计算机视觉 (图像识别)
    "Natural Language Processing": "C204321447", # NLP (大模型、翻译)
    "Data Mining": "C124101348"                   # 数据挖掘 (推荐系统)
}


def fetch_massive_data_v2(total_target=800000):
    session = requests.Session()
    # 填入你的邮箱，进入“礼貌池”，速度更快更稳
    session.headers.update({"User-Agent": "ThesisCollector/3.0 (mailto:nydghhcy@gmail.com)"})

    total_global_fetched = 0
    target_per_subject = total_target // len(DISCIPLINES)

    print(f"🚀 启动【游标模式】全自动化采集，目标总数: {total_target}")

    for subject_name, concept_id in DISCIPLINES.items():
        print(f"\n📂 正在进入学科领域: [{subject_name}]")

        subject_fetched = 0
        # 【核心修改 1】：初始化游标为 '*'
        current_cursor = "*"

        while subject_fetched < target_per_subject:
            # 【核心修改 2】：使用 cursor 参数代替 page 参数
            api_url = (
                f"https://api.openalex.org/works?"
                f"filter=concepts.id:{concept_id},publication_year:>2018,language:en"
                f"&per_page=200"
                f"&cursor={current_cursor}"
            )

            try:
                response = session.get(api_url, timeout=30)

                if response.status_code == 429:
                    print("🚨 触发频率限制，休眠 60 秒...")
                    time.sleep(60)
                    continue

                response.raise_for_status()
                data = response.json()

                # 【核心修改 3】：从返回的 meta 中获取下一个游标
                next_cursor = data.get('meta', {}).get('next_cursor')
                results = data.get('results', [])

                # 如果没有结果或游标没变，说明该学科抓完了
                if not results or not next_cursor or next_cursor == current_cursor:
                    print(f"✅ 学科 [{subject_name}] 数据已全部抓取完毕。")
                    break

                # 数据结构转换
                batch_data = []
                for paper in results:
                    title = paper.get('display_name') or "Untitled"
                    year = paper.get('publication_year') or 2026
                    authors_list = [auth.get('author', {}).get('display_name', '')
                                    for auth in paper.get('authorships', [])]
                    authors_str = ", ".join(authors_list[:3])

                    batch_data.append({
                        "title": title[:500],
                        "discipline": subject_name,
                        "publish_year": int(year),
                        "authors": authors_str,
                        "source": 'OpenAlex_Cursor'
                    })

                # 发送给 Master
                if batch_data:
                    try:
                        res = session.post(RECEIVER_URL, json=batch_data, timeout=20)
                        if res.status_code == 200:
                            actual_inserted = res.json().get('inserted', len(batch_data))
                            subject_fetched += actual_inserted
                            total_global_fetched += actual_inserted
                            print(f"   [{datetime.now().strftime('%H:%M:%S')}] "
                                  f"进度: {subject_fetched}/{target_per_subject} | 全局: {total_global_fetched}")
                        else:
                            print(f"   ❌ Master 错误: {res.status_code}")
                    except Exception as e:
                        print(f"   ❌ 发送失败: {e}")

                # 【核心修改 4】：更新游标，准备下一页
                current_cursor = next_cursor

                # 适当休眠，保护 API
                time.sleep(0.2)

            except Exception as e:
                print(f"   ❌ 采集异常: {e}")
                time.sleep(10)
                continue

    print(f"\n🎉 任务圆满完成！最终入库数据: {total_global_fetched} 条。")


if __name__ == "__main__":
    fetch_massive_data_v2(total_target=100000)