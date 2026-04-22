import json
import re
import os
import unicodedata
from dotenv import load_dotenv
from typing import TYPE_CHECKING
from app.scripts.Embedding import EmbeddingModel

if TYPE_CHECKING:
    from app.scripts.neo4j_class import Neo4jConnector

load_dotenv(".env", override=True)

class QuyCheDocument:

    def __init__(self, json_path):
        with open(json_path, encoding="utf8") as f:
            self.data = json.load(f)

        self.metadata = self.data.get("metadata", {})
        self.raw = self.data

    def slugify(self, text):
        """Chuyển đổi văn bản thành dạng không dấu, viết liền để làm ID"""
        if not text: return ""
        text = text.replace("Đ", "D").replace("đ", "d")
        nfkd_form = unicodedata.normalize('NFKD', text)
        text = "".join([c for c in nfkd_form if not unicodedata.combining(c)])
        text = re.sub(r'[^a-zA-Z0-9]', '', text)
        return text

    def get_vanban_id(self):
        fname = self.metadata.get("ten_file", "vanban")
        raw_id = os.path.splitext(fname)[0]
        return self.slugify(raw_id)

    def get_vanban_name(self):
        try:
            t = self.raw["quyet_dinh_ban_hanh"]["cac_dieu"][0]["tieu_de"]
            m = re.search(r"Ban hành kèm theo Quyết định này\s+\"?(.+?)\"?$", t)
            return m.group(1) if m else t
        except:
            return "Văn bản quy chế"

# TRÍCH THAM CHIẾU

class ReferenceExtractor:
    
    REFS = [
        re.compile(r"Xem\s+Điều\s+(\d+)(?:\s+Khoản\s+(\d+))?(?:\s+Điểm\s+([a-z]))?", re.I),
        re.compile(r"Xem\s+Điểm\s+([a-z])\s+Khoản\s+(\d+)\s+Điều\s+(\d+)", re.I),
        re.compile(r"điểm\s+([a-z])\s+khoản\s+(\d+)\s+Điều\s+(\d+)", re.I),
    ]

    def extract(self, text):
        if not text: return []
        out = []

        for p in self.REFS:
            for m in p.finditer(text):
                g = m.groups()
                # Kiểm tra thứ tự các group dựa trên regex
                if "điểm" in text[m.start():m.start()+10].lower():
                    diem, khoan, dieu = g
                else:
                    dieu, khoan, diem = g

                out.append((dieu, khoan, diem))

        return out

# TẠO GRPAH NEO4J
class Neo4jGraphBuilder:
    
    def __init__(self, db_connector: "Neo4jConnector"):
        self.driver = db_connector.driver
        self.ref = ReferenceExtractor()
        self.emb = EmbeddingModel(device="cpu")

    async def build(self, document: QuyCheDocument):
        async with self.driver.session() as s:
            await s.execute_write(self._build_tx, document)

    def build_text(self, vb, chuong=None, dieu=None, khoan=None, diem=None, content=""):
        parts = [vb]
        if chuong: parts.append(chuong)
        if dieu: parts.append(f"Điều {dieu}")
        if khoan: parts.append(f"Khoản {khoan}")
        if diem: parts.append(f"Điểm {diem}")
        return f"{' - '.join(parts)}: {content}"

    def embed_text(self, text):
        vecs = self.emb.get_embedding_batch([text])
        return vecs[0] if vecs else None

    async def _delete_graph(self, tx, vb_id):
        await tx.run("""
        MATCH (v:VanBan {id:$id})
        OPTIONAL MATCH (v)-[:co_chuong]->(c)
        OPTIONAL MATCH (c)-[:co_dieu]->(d)
        OPTIONAL MATCH (d)-[:co_khoan]->(k)
        OPTIONAL MATCH (k)-[:co_diem]->(m)
        DETACH DELETE v,c,d,k,m
        """, id=vb_id)

    # Build graph
    async def _build_tx(self, tx, doc: QuyCheDocument):

        vb_id = doc.get_vanban_id()
        vb_name = doc.get_vanban_name()
        src = doc.metadata.get("ten_file")

        await self._delete_graph(tx, vb_id)

        dieu_index, khoan_index, diem_index = {}, {}, {}

        # 1. Tạo Node Văn Bản
        await tx.run("CREATE (:VanBan {id:$id, ten:$ten, nguon:$src})", id=vb_id, ten=vb_name, src=src)

        for chuong in doc.raw["quy_dinh_chi_tiet"]:
            c_slug = doc.slugify(chuong['chuong'])
            cid = f"{vb_id}_{c_slug}"

            # Embedding cho Chương
            text_chuong = self.build_text(vb_name, chuong=chuong["chuong"], content=chuong["ten"])
            vec_c = self.embed_text(text_chuong)

            await tx.run("""
            MATCH (v:VanBan {id:$vid})
            MERGE (c:Chuong {id:$id, ten:$ten, text_embed:$text, embedding:$vec})
            MERGE (v)-[:co_chuong]->(c)
            """, vid=vb_id, id=cid, ten=chuong["ten"], text=text_chuong, vec=vec_c)

            for dieu in chuong["cac_dieu"]:
                dso = dieu["id"].replace("Điều", "").strip()
                did = f"{vb_id}_Dieu{dso}"
                dieu_index[dso] = did

                # Embedding cho Điều (Gộp tiêu đề + nội dung nếu có)
                nd_dieu = dieu.get("noi_dung", "").strip()
                full_dieu_content = f"{dieu['tieu_de']}. {nd_dieu}".strip()
                text_dieu = self.build_text(vb_name, chuong=chuong["chuong"], dieu=dso, content=full_dieu_content)
                vec_d = self.embed_text(text_dieu)

                await tx.run("""
                MATCH (c:Chuong {id:$cid})
                MERGE (d:Dieu {id:$id, so:$so, tieu_de:$td, noi_dung:$nd, text_embed:$text, embedding:$vec})
                MERGE (c)-[:co_dieu]->(d)
                """, cid=cid, id=did, so=dso, td=dieu["tieu_de"], nd=nd_dieu, text=text_dieu, vec=vec_d)

                for k in dieu.get("cac_khoan", []):
                    kso = str(k["so"])
                    kid = f"{did}_Khoan{kso}"
                    khoan_index[(dso, kso)] = kid

                    # Context Augmentation: Điều + Khoản
                    full_khoan_content = f"({dieu['tieu_de']}) - Khoản {kso}: {k['noi_dung']}"
                    text_khoan = self.build_text(vb_name, chuong=chuong["chuong"], dieu=dso, khoan=kso, content=full_khoan_content)
                    vec_k = self.embed_text(text_khoan)

                    await tx.run("""
                    MATCH (d:Dieu {id:$did})
                    MERGE (k:Khoan {id:$id, so:$so, noi_dung:$nd, text_embed:$text, embedding:$vec})
                    MERGE (d)-[:co_khoan]->(k)
                    """, did=did, id=kid, so=kso, nd=k["noi_dung"], text=text_khoan, vec=vec_k)

                    for dm in k.get("cac_diem", []):
                        ky = dm["ky_hieu"]
                        mid = f"{kid}_Diem{doc.slugify(ky)}"
                        diem_index[(dso, kso, ky)] = mid

                        # Context Augmentation: Điều + Khoản + Điểm
                        full_diem_content = f"({dieu['tieu_de']} - {k['noi_dung']}) - Điểm {ky}: {dm['noi_dung']}"
                        text_diem = self.build_text(vb_name, chuong=chuong["chuong"], dieu=dso, khoan=kso, diem=ky, content=full_diem_content)
                        vec_m = self.embed_text(text_diem)

                        await tx.run("""
                        MATCH (k:Khoan {id:$kid})
                        MERGE (m:Diem {id:$id, ky_hieu:$ky, noi_dung:$nd, text_embed:$text, embedding:$vec})
                        MERGE (k)-[:co_diem]->(m)
                        """, kid=kid, id=mid, ky=ky, nd=dm["noi_dung"], text=text_diem, vec=vec_m)

        # 2. Xử lý Tham chiếu (Reference)
        await self._process_all_references(tx, doc, dieu_index, khoan_index, diem_index)

    async def _process_all_references(self, tx, doc, dieu_idx, khoan_idx, diem_idx):
        for chuong in doc.raw["quy_dinh_chi_tiet"]:
            for dieu in chuong["cac_dieu"]:
                dso = dieu["id"].replace("Điều", "").strip()
                
                # Tham chiếu từ Điều
                await self._link_refs(tx, dieu_idx[dso], f"{dieu.get('tieu_de')} {dieu.get('noi_dung')}", "Dieu", dieu_idx, khoan_idx, diem_idx)
                
                for k in dieu.get("cac_khoan", []):
                    kso = str(k["so"])
                    # Tham chiếu từ Khoản
                    await self._link_refs(tx, khoan_idx[(dso, kso)], k["noi_dung"], "Khoan", dieu_idx, khoan_idx, diem_idx)
                    
                    for dm in k.get("cac_diem", []):
                        # Tham chiếu từ Điểm
                        await self._link_refs(tx, diem_idx[(dso, kso, dm['ky_hieu'])], dm["noi_dung"], "Diem", dieu_idx, khoan_idx, diem_idx)

    async def _link_refs(self, tx, start_id, text, label, dieu_idx, khoan_idx, diem_idx):
        for r in self.ref.extract(text):
            # Ưu tiên map: Điểm -> Khoản -> Điều
            tgt = diem_idx.get(r) or khoan_idx.get((r[0], r[1])) or dieu_idx.get(r[0])
            if tgt:
                await tx.run(f"""
                MATCH (a:{label} {{id:$a}}), (b {{id:$b}})
                MERGE (a)-[:tham_chieu]->(b)
                """, a=start_id, b=tgt)