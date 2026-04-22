# Mô Tả Chi Tiết Quy Trình Nạp Dữ Liệu PDF → Neo4j

## Tổng Quan Kiến Trúc

Quy trình gồm **4 giai đoạn chính**:
1. **PDF Parsing** - Trích xuất văn bản và bảng từ PDF
2. **Entity Extraction** - Trích xuất thực thể đào tạo từ dữ liệu có cấu trúc
3. **Graph Building** - Xây dựng nodes và edges từ thực thể
4. **Neo4j Import** - Nạp vào cơ sở dữ liệu đồ thị

---

## GIAI ĐOẠN 1: PDF PARSING (curriculum_parser.py)

### 1.1 Khởi Tạo Parser
```python
parser = PDFParser(pdf_path)
parser_sections = parser.parse_pdf_by_structure()
```

### 1.2 Quy Trình Trích Xuất

#### a) Đọc PDF Trang Trang
- Duyệt qua từng trang của file PDF
- Trích xuất full text từ tất cả các trang
- Kết hợp thành một chuỗi văn bản liên tục

#### b) Tách Các Phần Tử Theo Hình Học
Cho mỗi trang, trích xuất:

**Bảng (Tables):**
- Sử dụng `pdfplumber.find_tables()` với cài đặt dòng/cột
- Chuẩn hóa các ô (cell normalization)
- Tự động xác định header row
- Điền các giá trị rỗng dựa trên dòng trước
- Output: `{"type": "table", "header": [...], "rows": [...]}`

**Văn Bản (Text):**
- Trích xuất từng từ từ các vị trí không thuộc bảng
- Nhóm các từ thành dòng theo "top" position
- Output: Các block text được sắp xếp theo trang

### 1.3 Phân Tích Cấu Trúc

**Regex Heading Detection:**
```python
HEADING_PATTERN = r"""
  ^(?P<heading>
    [IVXLCDM]+\.?           # I. II. III. IV. ...
    | \d+(?:\.\d+)*(?:\.)?  # 1. 2.1 3.1.2. ...
    | [a-z][\.)]?           # a. b) c ...
  )\s+(?P<title>[^\n]+)$
"""
```

Phát hiện các tiêu đề với mẫu:
- `I. Mục tiêu đào tạo`
- `1. Thông tin chung`
- `2.1 Chuẩn đầu ra`
- `a) Điều kiện tốt nghiệp`

### 1.4 Xây Dựng Cây Phân Cấp

Nhóm các section theo mức độ indent:
```
I. Mục tiêu đào tạo
  a) Mục tiêu chung
  b) Mục tiêu cụ thể
II. Program Objectives (PEOs)
  1. Mục tiêu 1
  2. Mục tiêu 2
```

**Output:** Cấu trúc cây với các nodes:
- `heading`: Mã định danh (I, 1, a)
- `title`: Tiêu đề
- `text_content`: Nội dung văn bản
- `tables`: Danh sách bảng con
- `blocks`: Các phần tử con (text/table)

---

## GIAI ĐOẠN 2: ENTITY EXTRACTION (curriculum_extractor.py)

### 2.1 Khởi Tạo Trạng Thái

```python
class GraphState:
    raw_text: str           # Toàn bộ text từ PDF
    sections: List[Dict]    # Cây phân cấp sections
    info_map: Dict[str,str] # Cache thông tin chính
```

### 2.2 Chuẩn Hóa Dữ Liệu

**Đầu Tiên: Xây dựng Info Map**
```python
def _build_info_map(state) -> None:
    # Lấy các thông tin cơ bản từ PDF:
    - ma_chuong_trinh (từ tiêu đề hoặc table)
    - tong_tin_chi (từ tiêu đề + sections)
    - thoi_gian_dao_tao (năm học)
    - ten_khoa (tên khoa)
```

**Regex Pattern Matching:**
```python
LEGAL_DOC_RE = r"""(Quyết định|Thông tư|Nghị quyết)\s+số\s+([0-9]+)
                    \s+ngày\s+([0-9]{1,2}\s+tháng...)"""
PLO_RE = r"\((PLO\d{1,3})\)"
CREDIT_RE = r"(\d{1,3})\s*(?:tín chỉ|TC)"
COURSE_CODE_RE = r"\b[A-Z]{1,5}\s*\d{2,}[A-Z0-9]*\b"
```

### 2.3 Trích Xuất Entities (Nodes)

#### Node: **ChuongTrinhDaoTao** (Program Center)
**Properties:**
- `ma_chuong_trinh`: Mã chương trình (VD: KHMT, ATTT, TTNT)
- `khoa`: Khóa/năm nhập học
- `he`: Hệ đào tạo (Chính quy, Vừa làm vừa học)
- `ngon_ngu`: Ngôn ngữ dạy (Tiếng Việt, Tiếng Anh)
- `tong_tin_chi`: Tổng số tín chỉ (VD: 120)
- `thoi_gian_dao_tao`: Thời gian học (VD: 4 năm)
- `thang_diem`: Thang điểm (10 hoặc 4)

**Trích xuất từ:**
- Tiêu đề của PDF
- Section "Thông tin chung"
- Các bảng tổng quan

#### Node: **HocPhan** (Courses)
**Properties:**
- `ma_hoc_phan`: Mã học phần (VD: CS101, MATH101)
- `ten_hoc_phan`: Tên học phần
- `so_tin_chi`: Số tín chỉ (1-5)
- `so_tiet_ly_thuyet`: Số tiết lý thuyết
- `so_tiet_thuc_hanh`: Số tiết thực hành
- `dieu_kien`: Có yêu cầu tiên quyết (true/false)
- `yeu_cau_stc_toi_thieu`: Tín chỉ tối thiểu yêu cầu
- `bat_buoc`: Bắt buộc hay tự chọn
- `tom_tat`: Tóm tắt nội dung
- `text`: Mô tả đầy đủ

**Trích xuất từ:**
- Bảng danh sách học phần
- Column headers: "Mã HP", "Tên HP", "TC", "LT", "TH", "Tiên quyết", "Ghi chú"

**Ví dụ:**
```
| Mã HP | Tên HP              | TC | LT  | TH | Tiên quyết |
|-------|---------------------|----|-----|----|-----------| 
| CS201 | Cấu trúc dữ liệu    | 3  | 30  | 30 | CS101     |
| MATH301 | Đại số tuyến tính | 4  | 45  | 15 |           |
```

#### Node: **KhoiKienThuc** (Knowledge Blocks)
**Properties:**
- `ma_khoi`: Mã khối (GD, CS, CORE)
- `ten_khoi`: Tên khối (GD Đại cương, Cơ sở ngành)
- `tong_tin_chi`: Tổng tín chỉ khối
- `tin_chi_bat_buoc`: Tín chỉ bắt buộc
- `tin_chi_tu_chon`: Tín chỉ tự chọn

**Trích xuất từ:**
- Section "Cấu trúc chương trình"
- Giải tích từ text: "...gồm X tín chỉ bắt buộc và Y tín chỉ tự chọn"

#### Node: **ChuanDauRa** (Learning Outcomes - PLOs)
**Properties:**
- `ma_chuan`: Mã chuẩn (PLO1, PLO2)
- `noi_dung`: Nội dung chuẩn đầu ra
- `nhom`: Nhóm (Kiến thức, Kỹ năng)
- `loai`: Loại (Kỹ năng cứng, Kỹ năng mềm)

**Trích xuất từ:**
- Section "Chuẩn đầu ra"
- Regex: `\((PLO\d{1,3})\)` bắt mã PLO
- Từng dòng là một PLO

#### Nodes Quản Lý Cơ Sở:
```
Khoa(ma_khoa, ten_khoa)
  ↑ Từ section "Thông tin chung"

BoMon(ma_bo_mon, ten_bo_mon)
  ↑ Từ section "Bộ môn quản lý"

Nganh(ma_nganh, ten_nganh_vi, ten_nganh_en)
  ↑ Từ tiêu đề PDF hoặc section "Ngành đào tạo"

TrinhDo(ma_trinh_do, ten_trinh_do)
  ↑ Xác định từ "Cấp bậc" (Đại học/Thạc sĩ/Tiến sĩ)
```

#### Nodes Chính Sách:
```
VanBanPhapLy(so, ten, loai, ngay_ban_hanh, co_quan_ban_hanh)
  ↑ Regex: "Quyết định số XXX/YYYY ngày..."

DoiTuongTuyenSinh(noi_dung)
  ↑ Section "Đối tượng tuyển sinh"

DieuKienTotNghiep(noi_dung)
  ↑ Section "Điều kiện tốt nghiệp"

MucTieuDaoTao(loai, noi_dung)
  ↑ Section "Mục tiêu đào tạo" (PEOs)

ViTriViecLam(noi_dung)
  ↑ Section "Cơ hội việc làm"
```

### 2.4 Chuẩn Hóa Văn Bản

**Hàm `normalize_token()`:**
1. Loại bỏ dấu: "Học Phần" → "Hoc Phan"
2. Chuẩn NFD: Tách dấu ASCII
3. Chuyển thường: "COURSE" → "course"
4. Loại bỏ ký tự đặc biệt: "Mã-HP_v2" → "Ma HP v2"
5. Thu gọn khoảng trắng: Nhiều space → 1 space

**Hàm `slugify_token()`:**
- Chuyển thành UPPERCASE, thay space bằng underscore
- "Học Phần Bắt Buộc" → "HOC_PHAN_BAT_BUOC"

### 2.5 Ghép Nối Các Aliases

**Từ Điển Thuật Ngữ (term_dictionary.py):**
```python
PROPERTY_ALIASES = {
    "ma_nganh": ["ma nganh", "ma so nganh", "major code"],
    "ten_nganh_vi": ["nganh", "ten nganh", "vietnamese major"],
    "so_tin_chi": ["so tin chi", "tong tin chi", "credits", "total credits"],
    "dieu_kien": ["dieu kien", "prerequisite", "prerequisites"],
    ...
}

TABLE_LABEL_ALIASES = {
    "ma_hoc_phan": ["ma hp", "ma hoc phan", "course id", "course code"],
    "ten_hoc_phan": ["ten hoc phan", "ten hp", "course title"],
    "so_tin_chi": ["tin chi", "tc", "credits"],
    ...
}
```

**Cơ chế Khớp (Matching):**
- Tính điểm tương đồng cho từng alias
- `"ma HP" → "ma hoc phan"` (score=80 - substring match)
- `"course id" → "ma hoc phan"` (score=70 - set match)
- Chọn alias có score cao nhất

### 2.6 Output JSON

```json
{
  "source": "data/pdf/ChuyenNganh_DaoTao/pdf/k51/64_7480202_AnToanThongTin.pdf",
  "status": "extracted",
  "extracted_at": "2026-04-16T10:30:45.123456+00:00",
  "results": {
    "ChuongTrinhDaoTao": {
      "items": [
        {
          "ma_chuong_trinh": "ATTT",
          "khoa": "K64",
          "he": "Chính quy",
          "tong_tin_chi": 120,
          "thoi_gian_dao_tao": 4,
          "text": "Chương trình An Toàn Thông Tin...",
          "embedding": [0.1, 0.2, ...]  // Thêm sau khi admin approve
        }
      ]
    },
    "HocPhan": {
      "items": [
        {
          "ma_hoc_phan": "CS201",
          "ten_hoc_phan": "Cấu trúc dữ liệu",
          "so_tin_chi": 3,
          "so_tiet_ly_thuyet": 30,
          "so_tiet_thuc_hanh": 30,
          "dieu_kien": true,
          "bat_buoc": true,
          "text": "Học phần CS201 - Cấu trúc dữ liệu..."
        }
      ]
    },
    "KhoiKienThuc": {
      "items": [
        {
          "ma_khoi": "GD",
          "ten_khoi": "Giáo dục đại cương",
          "tong_tin_chi": 20,
          "tin_chi_bat_buoc": 15,
          "tin_chi_tu_chon": 5
        }
      ]
    },
    "ChuanDauRa": {
      "items": [
        {
          "ma_chuan": "PLO1",
          "noi_dung": "Có kiến thức cơ bản về An Toàn Thông Tin",
          "nhom": "Kiến thức",
          "loai": "Kiến thức chuyên sâu"
        }
      ]
    },
    ... // Các node type khác
  },
  "relationships": [
    {
      "source": "ChuongTrinhDaoTao",
      "source_value": "ATTT",
      "type": "GOM",
      "target": "KhoiKienThuc",
      "target_value": "GD"
    },
    {
      "source": "KhoiKienThuc",
      "source_value": "GD",
      "type": "GOM",
      "target": "HocPhan",
      "target_value": "CS201"
    }
  ],
  "admin_review": {
    "approved": false,
    "reviewed_by": null,
    "reviewed_at": null,
    "notes": ""
  }
}
```

---

## GIAI ĐOẠN 3: GRAPH BUILDING (curriculum_graph.py)

### 3.1 Tải JSON Đã Trích Xuất

```python
data = _load_extracted_json(json_path)
results = data.get("results", {})        # {node_type: {items: [...]}}
relationships = data.get("relationships", [])
source_name = data.get("source") or Path(json_path).stem
```

### 3.2 Xây Dựng Node IDs

**Hàm `_make_node_id(node_type, item, index, program_hint)`:**

Ưu tiên thứ tự:
1. **Sử dụng ID field nếu có:**
   ```python
   ID_FIELDS = {
       "HocPhan": ["ma_hoc_phan"],        # CS201
       "Khoa": ["ma_khoa"],               # KHMT
       "VanBanPhapLy": ["so"],            # 123/YYYY
       "ChuongTrinhDaoTao": ["ma_chuong_trinh"],
   }
   ```

2. **Nếu không có, sinh ID từ program_hint:**
   ```
   ID = {program}_{node_type}_{index+1}
   VD: ATTT_HOCPHAN_1, ATTT_KHOA_1
   ```

**Ví dụ:**
```
HocPhan {"ma_hoc_phan": "CS201", ...}
  → ID = "CS201"

Khoa {"ma_khoa": "KHMT", ...}
  → ID = "KHMT"

ChuanDauRa {"ma_chuan": null, ...}  // Không có ma_chuan
  → ID = "ATTT_CHUANDAURA_1"
```

### 3.3 Xây Dựng Node Objects

```python
def _build_nodes_from_results(results, source_name):
    nodes = []
    index_map = {}  # {node_type: [{id, properties, ...}]}
    
    for node_type, payload in results.items():
        items = payload.get("items", [])
        
        for index, item in enumerate(items):
            # 1. Tạo node ID
            node_id = _make_node_id(node_type, item, index, program_hint)
            
            # 2. Xây dựng text mô tả
            text = _build_text_for_node(node_type, item) or item.get("text")
            
            # 3. Tạo node object
            node = {
                "id": node_id,
                "type": node_type,
                "properties": {
                    "text": text,
                    **item  # Tất cả properties từ item
                }
            }
            
            nodes.append(node)
            index_map.setdefault(node_type, []).append(node)
    
    return nodes, index_map
```

**Node Object Structure:**
```json
{
  "id": "CS201",
  "type": "HocPhan",
  "properties": {
    "text": "Học phần CS201 - Cấu trúc dữ liệu. So tin chi: 3...",
    "ma_hoc_phan": "CS201",
    "ten_hoc_phan": "Cấu trúc dữ liệu",
    "so_tin_chi": 3,
    "so_tiet_ly_thuyet": 30,
    "so_tiet_thuc_hanh": 30,
    "dieu_kien": true,
    "bat_buoc": true,
    "embedding": null  // Sẽ được thêm sau khi admin review + approve
  }
}
```

### 3.4 Xây Dựng Edges (Relationships)

#### a) **Edges Từ JSON Relationships:**

```python
def _build_edges_from_relationships(relationships, index_map):
    edges = []
    seen = set()  # Tránh duplicate
    
    for rel in relationships:
        source_type = rel.get("source")
        source_value = rel.get("source_value")
        rel_type = rel.get("type")
        target_type = rel.get("target")
        target_value = rel.get("target_value")
        
        # Resolve source node ID
        source_id = _resolve_node_id(source_type, source_value, index_map)
        target_id = _resolve_node_id(target_type, target_value, index_map)
        
        if source_id and target_id:
            key = (source_id, rel_type, target_id)
            if key not in seen:
                edges.append({
                    "source": source_id,
                    "type": rel_type,
                    "target": target_id
                })
                seen.add(key)
    
    return edges
```

**Ví dụ Resolution:**
```
Relationship:
  source_type: "ChuongTrinhDaoTao"
  source_value: "ATTT"
  → Tìm trong index_map node type "ChuongTrinhDaoTao"
  → Tìm item có ma_chuong_trinh = "ATTT"
  → Return node ID: "ATTT"

Edge:
  "ATTT" --[GOM]--> "CS201"
```

#### b) **Edges Suy Luận ETL (Inferred Edges):**

```python
def _infer_core_edges(index_map, existing_edges):
    """Bổ sung các quan hệ cốt lõi theo ETL"""
    
    program_ids = _node_ids(index_map, "ChuongTrinhDaoTao")
    major_ids = _node_ids(index_map, "Nganh")
    faculty_ids = _node_ids(index_map, "Khoa")
    dept_ids = _node_ids(index_map, "BoMon")
    
    # Theo ETL hierarchy:
    # CTDT --[THUOC_VE]--> Nganh
    # Nganh --[THUOC_VE]--> Khoa/BoMon
    
    for prog_id in program_ids:
        for major_id in major_ids:
            edges.append({
                "source": prog_id,
                "type": "THUOC_VE",
                "target": major_id
            })
    
    for major_id in major_ids:
        for faculty_id in faculty_ids + dept_ids:
            edges.append({
                "source": major_id,
                "type": "THUOC_VE",
                "target": faculty_id
            })
    
    return edges
```

### 3.5 Gắn Embeddings

```python
def _attach_embeddings(nodes, embedder):
    # 1. Trích xuất texts từ nodes
    texts = [node.get("properties", {}).get("text", "") for node in nodes]
    
    # 2. Compute embeddings batch
    embeddings = embedder.get_embedding_batch(texts)
    
    # 3. Gắn embedding vào mỗi node
    for idx, emb in enumerate(embeddings):
        nodes[idx]["properties"]["embedding"] = [float(dim) for dim in emb]
```

---

## GIAI ĐOẠN 4: NEO4J IMPORT (Neo4j Import)

### 4.1 Kết Nối Neo4j

```python
from app.scripts.neo4j_class import Neo4jConnector

db = Neo4jConnector(
    uri=os.getenv("NEO4J_URI"),           # bolt://localhost:7687
    username=os.getenv("NEO4J_USERNAME"),
    password=os.getenv("NEO4J_PASSWORD")
)
```

### 4.2 Tạo Nodes

**Cypher Query Pattern:**
```cypher
CREATE (node:NodeType {
  id: $id,
  text: $text,
  ma_hoc_phan: $ma_hoc_phan,
  ten_hoc_phan: $ten_hoc_phan,
  so_tin_chi: $so_tin_chi,
  ...
  embedding: $embedding
})
```

**Thực thi Batch:**
```python
for node in nodes:
    node_id = node["id"]
    node_type = node["type"]
    properties = node["properties"]
    
    query = f"""
    CREATE (n:{node_type} {{
        id: $id,
        text: $text,
        ...{all properties}...
        embedding: $embedding
    }})
    """
    db.run(query, **properties)
```

### 4.3 Tạo Relationships

**Cypher Query Pattern:**
```cypher
MATCH (source:HocPhan {id: $source_id})
MATCH (target:HocPhan {id: $target_id})
CREATE (source)-[r:YEU_CAU_TIEN_QUYET]->(target)
```

**Thực thi Batch:**
```python
for edge in edges:
    query = f"""
    MATCH (source {{id: $source_id}})
    MATCH (target {{id: $target_id}})
    CREATE (source)-[r:{edge['type']}]->(target)
    """
    db.run(query, 
        source_id=edge["source"],
        target_id=edge["target"]
    )
```

### 4.4 Indexes và Constraints

```cypher
-- Tìm kiếm nhanh theo ID
CREATE INDEX IF NOT EXISTS ON :HocPhan(id);
CREATE INDEX IF NOT EXISTS ON :ChuongTrinhDaoTao(id);
CREATE INDEX IF NOT EXISTS ON :KhoiKienThuc(id);

-- Tìm kiếm full-text
CREATE INDEX IF NOT EXISTS ON :HocPhan(text);

-- Constraint unique
CREATE CONSTRAINT IF NOT EXISTS ON (h:HocPhan) ASSERT h.id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS ON (k:KhoiKienThuc) ASSERT k.id IS UNIQUE;
```

---

## SCHEMA NEO4J CUỐI CÙNG

### Nodes

| Node Type | Mô Tả | Ví Dụ ID |
|-----------|-------|---------|
| **ChuongTrinhDaoTao** | Chương trình đào tạo (tâm) | ATTT, KHMT |
| **HocPhan** | Môn học/khóa học | CS201, MATH301 |
| **KhoiKienThuc** | Khối kiến thức (GD, Cơ sở, Chuyên môn) | GD, CS, CORE |
| **Khoa** | Khoa/Trường | KHMT, ATTT |
| **BoMon** | Bộ môn | CS_DEPT, MATH_DEPT |
| **Nganh** | Ngành đào tạo | ATTT, KHMT |
| **ChuanDauRa** | Learning Outcomes (PLOs) | PLO1, PLO2 |
| **VanBanPhapLy** | Quyết định/Thông tư ban hành | 123/YYYY |
| **DoiTuongTuyenSinh** | Đối tượng tuyển sinh | obj_1, obj_2 |
| **DieuKienTotNghiep** | Điều kiện tốt nghiệp | cond_1 |
| **MucTieuDaoTao** | Mục tiêu năm (PEOs) | peo_1, peo_2 |
| **ViTriViecLam** | Cơ hội việc làm | job_1, job_2 |
| **KhaNangHocTap** | Khả năng học tập | learning_1 |
| **DanhGiaKiemDinh** | Đánh giá chất lượng | eval_1 |
| **YeuCauTuChon** | Yêu cầu tự chọn | elec_req_1 |
| **NhomHocPhanTuChon** | Nhóm môn học tự chọn | AV, PV, N1 |

### Relationships (Edges)

```
ChuongTrinhDaoTao (CTDT)
├─[THUOC_VE]──→ Nganh
├─[THUOC_VE]──→ Khoa
├─[DAO_TAO]──→ TrinhDo
├─[CAP]──→ LoaiVanBang
├─[CO]──→ HinhThucDaoTao
├─[CO]──→ PhuongThucDaoTao
├─[CO]──→ DoiTuongTuyenSinh
├─[CO]──→ MucTieuDaoTao (PEOs)
├─[CO]──→ DanhGiaKiemDinh
├─[CO]──→ ViTriViecLam
├─[CO]──→ ChuanDauRa (PLOs)
├─[YEU_CAU]──→ DieuKienTotNghiep
├─[THAM_KHAO]──→ ChuanThamKhao
├─[DAT_DUOC]──→ KhaNangHocTap
├─[BAN_HANH_THEO]──→ VanBanPhapLy
├─[GOM]──→ KhoiKienThuc
└─[GOM]──→ HocPhan

Nganh
└─[THUOC_VE]──→ Khoa / BoMon

BoMon
└─[THUOC_VE]──→ Khoa

KhoiKienThuc
└─[GOM]──→ HocPhan

HocPhan
├─[YEU_CAU_TIEN_QUYET]──→ HocPhan (prerequisite)
└─[CO_THE_SONG_HANH]──→ HocPhan (concurrent)

YeuCauTuChon
├─[DOI_VOI]──→ NhomHocPhanTuChon
└─[GOM]──→ HocPhan

NhomHocPhanTuChon
└─[GOM]──→ HocPhan

ChuanDauRa
└─[PHAN_LOAI]──→ [Knowledge|Skill|Autonomy]
```

---

## WORKFLOW TOÀN BỘ

```
┌─────────────────────────────────────────────┐
│ 1. PDF Input                                │
│    /path/to/curriculum.pdf                 │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│ 2. PDF PARSING (curriculum_parser.py)      │
│    - pdfplumber.open()                     │
│    - Extract text + tables                 │
│    - Regex headings                        │
│    - Build section tree                    │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│ 3. ENTITY EXTRACTION (curriculum_extractor) │
│    - For each section:                     │
│      * Regex pattern matching              │
│      * Table parsing                       │
│      * Extract nodes                       │
│      * Normalize text                      │
│      * Extract relationships               │
│    - Output: JSON with results             │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│ 4. ADMIN REVIEW                            │
│    - Load JSON                             │
│    - Manual approval/notes                 │
│    - If approved: Add embeddings           │
│    - Save updated JSON                     │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│ 5. GRAPH BUILDING (curriculum_graph.py)    │
│    - Load extracted JSON                   │
│    - Build node IDs                        │
│    - Create node objects                   │
│    - Build edge relationships              │
│    - Infer missing edges                   │
│    - Attach embeddings                     │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│ 6. NEO4J IMPORT                            │
│    - Connect to Neo4j                      │
│    - CREATE nodes (batch)                  │
│    - CREATE relationships (batch)          │
│    - Create indexes                        │
│    - Create constraints                    │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│ 7. QUERY & RETRIEVAL                       │
│    - Similarity search (embeddings)        │
│    - Path queries                          │
│    - Pattern matching                      │
│    - Recommendations                       │
└─────────────────────────────────────────────┘
```

---

## CACHING & OPTIMIZATION

### Info Map Caching
```python
info_map = {
    "ma_chuong_trinh": "ATTT",
    "tong_tin_chi": "120",
    "thoi_gian_dao_tao": "4 years",
    "ten_khoa": "School of Information Security"
}
```

### Batch Processing
- **Nodes:** Tạo tất cả nodes trước
- **Relations:** Tạo relationships sau (sau khi tất cả nodes có ID)
- **Embeddings:** Compute batch qua model (nhanh hơn sequential)

### Normalization Caching
- Cache các kết quả `normalize_token()` 
- Tránh compute lại cùng string
- Lookup dictionary cho term aliases

---

## ERROR HANDLING

### Validation
```python
if not _is_empty_item(item):
    # Item có dữ liệu hợp lệ
    nodes.append(node)
else:
    # Skip empty items
    pass
```

### Deduplication
```python
seen = set((source_id, rel_type, target_id))
if key not in seen:
    edges.append(edge)
    seen.add(key)
```

### Fallback Mechanisms
```python
# Nếu không thể tìm chính xác node ID
if not exact_match:
    # Dùng program_hint + node_type + index
    node_id = f"{program_token}_{node_type_token}_{index+1}"
```

---

## MONITORING & LOGGING

- `extracted_at`: Timestamp UTC ISO
- `source`: Path đến file PDF gốc
- `status`: "extracted", "approved", "rejected"
- `admin_review`: Metadata người review
- `embedded_items`: Số lượng items có embedding

Điều này cho phép tracking đầy đủ từ PDF → JSON → Neo4j!
