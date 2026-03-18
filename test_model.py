import requests
import json

def test_ollama_streaming():
    url = "http://localhost:11434/api/generate"
    data = {
        "model": "qwen2.5-coder:3b-instruct",
        "prompt": "Giải thích ngắn gọn ý nghĩa của quan hệ YEU_CAU_TIEN_QUYET trong đồ thị môn học.",
        "stream": True 
    }

    print("--- Trợ lý đang trả lời: ---")
    
    with requests.post(url, json=data, stream=True) as response:
        for line in response.iter_lines():
            if line:
                # Mỗi dòng trả về là một đối tượng JSON riêng biệt
                chunk = json.loads(line.decode('utf-8'))
                content = chunk.get("response", "")
                print(content, end="", flush=True)
                
                if chunk.get("done"):
                    print("\n\n--- Kết thúc trả lời ---")

if __name__ == "__main__":
    test_ollama_streaming()