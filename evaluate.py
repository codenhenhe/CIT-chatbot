import pandas as pd
import ast
from datasets import Dataset
from ragas import evaluate
from ragas.metrics import (
    faithfulness, 
    answer_relevancy, 
    context_recall, 
    context_precision,
    answer_correctness
)

os.environ["OPENAI_API_KEY"] = "sk...."

# 1. Đọc file CSV đã tạo ở trên
df = pd.read_csv("eval_data.csv")

# 2. Chuyển cột contexts từ string thành list của string (RAGAS yêu cầu định dạng list)
# Vì CSV lưu list dưới dạng string "[...]", ta dùng ast.literal_eval để chuyển lại
df['contexts'] = df['contexts'].apply(ast.literal_eval)

# 3. Chuyển đổi sang định dạng Dataset của HuggingFace
eval_dataset = Dataset.from_pandas(df)

# 4. Cấu hình các chỉ số đánh giá (Cần thiết lập OpenAI/Gemini API Key trước đó)
metrics = [
    faithfulness,          # Tính trung thực (so với ngữ cảnh)
    answer_relevancy,      # Độ phù hợp (so với câu hỏi)
    context_recall,        # Độ đầy đủ (ngữ cảnh so với đáp án chuẩn)
    context_precision,     # Độ chính xác (ngữ cảnh tìm thấy có bám sát câu hỏi không)
    answer_correctness     # Độ chính xác của câu trả lời (so với Ground Truth)
]

# 5. Thực hiện đánh giá
# Lưu ý: Cần export OPENAI_API_KEY hoặc cấu hình LLM khác tại đây
results = evaluate(eval_dataset, metrics=metrics)

# 6. Xuất kết quả chi tiết ra file CSV mới để phân tích
df_results = results.to_pandas()
df_results.to_csv("ket_qua_ragas_cictbot.csv", index=False)

print("Đã hoàn thành đánh giá 16 câu hỏi! Kiểm tra file 'ket_qua_ragas_cictbot.csv'.")
print("Điểm trung bình hệ thống:", results)