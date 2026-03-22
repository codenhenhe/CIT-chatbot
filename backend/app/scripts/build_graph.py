import json
import re
import os
from dotenv import load_dotenv
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.scripts.neo4j_class import Neo4jConnector

load_dotenv(".env.desktop", override=True)

class QuyCheDocument:

    def __init__(self, json_path):
        with open(json_path, encoding="utf8") as f:
            self.data = json.load(f)

        self.metadata = self.data.get("metadata", {})
        self.raw = self.data

    def get_vanban_id(self):
        """
        quychehocvu.pdf -> quychehocvu
        """
        fname = self.metadata.get("ten_file", "vanban")
        return os.path.splitext(fname)[0]

    def get_vanban_name(self):
        try:
            t = self.raw["quyet_dinh_ban_hanh"]["cac_dieu"][0]["tieu_de"]
            m = re.search(r"Ban hành kèm theo Quyết định này\s+\"?(.+?)\"?$", t)
            return m.group(1) if m else t
        except:
            return "Văn bản quy chế"

##################################
# REFERENCE EXTRACTOR (TRÍCH XUẤT THAM CHIẾU)
##################################

class ReferenceExtractor:

    REFS = [
        re.compile(r"Xem\s+Điều\s+(\d+)(?:\s+Khoản\s+(\d+))?(?:\s+Điểm\s+([a-z]))?", re.I),
        re.compile(r"Xem\s+Điểm\s+([a-z])\s+Khoản\s+(\d+)\s+Điều\s+(\d+)", re.I),
        re.compile(r"điểm\s+([a-z])\s+khoản\s+(\d+)\s+Điều\s+(\d+)", re.I),
    ]

    def extract(self, text):
        if not text:
            return []

        out = []

        for p in self.REFS:
            for m in p.finditer(text):
                g = m.groups()

                if len(g) == 3 and text[m.start():].lower().startswith("điểm"):
                    diem, khoan, dieu = g
                else:
                    dieu, khoan, diem = g

                out.append((dieu, khoan, diem))

        return out

##################################
# GRAPH BUILDER
##################################

class Neo4jGraphBuilder:

    def __init__(self, db_connector: "Neo4jConnector"):
        self.driver = db_connector.driver
        self.ref = ReferenceExtractor()

    async def build(self, document: QuyCheDocument):
        async with self.driver.session() as s:
            await s.execute_write(self._build_tx, document)

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

        # Nếu tồn tại thì xoá subtree cũ
        await self._delete_graph(tx, vb_id)

        # index local
        dieu_index = {}
        khoan_index = {}
        diem_index = {}

        # Tạo node Văn Bản

        await tx.run("""
        CREATE (:VanBan {
            id:$id,
            ten:$ten,
            nguon:$src
        })
        """, id=vb_id, ten=vb_name, src=src)

        # Tạo cây theo CHƯƠNG - ĐIỀU - KHOẢN - ĐIỂM

        for chuong in doc.raw["quy_dinh_chi_tiet"]:

            cid = f"{vb_id}_{chuong['chuong'].replace(' ', '')}"

            await tx.run("""
            MATCH (v:VanBan {id:$vid})
            MERGE (c:Chuong {id:$id, ten:$ten})
            MERGE (v)-[:co_chuong]->(c)
            """, vid=vb_id, id=cid, ten=chuong["ten"])

            for dieu in chuong["cac_dieu"]:

                dso = dieu["id"].replace("Điều", "").strip()
                did = f"{cid}_Dieu{dso}"
                dieu_index[dso] = did

                await tx.run("""
                MATCH (c:Chuong {id:$cid})
                MERGE (d:Dieu {id:$id, so:$so, tieu_de:$td})
                MERGE (c)-[:co_dieu]->(d)
                """, cid=cid, id=did, so=dso, td=dieu["tieu_de"])

                for k in dieu.get("cac_khoan", []):

                    kso = k["so"]
                    kid = f"{did}_Khoan{kso}"
                    khoan_index[(dso, kso)] = kid

                    await tx.run("""
                    MATCH (d:Dieu {id:$did})
                    MERGE (k:Khoan {id:$id, so:$so, noi_dung:$nd})
                    MERGE (d)-[:co_khoan]->(k)
                    """, did=did, id=kid, so=kso, nd=k["noi_dung"])

                    for dm in k.get("cac_diem", []):

                        ky = dm["ky_hieu"]
                        mid = f"{kid}_Diem{ky}"
                        diem_index[(dso, kso, ky)] = mid

                        await tx.run("""
                        MATCH (k:Khoan {id:$kid})
                        MERGE (m:Diem {id:$id, ky_hieu:$ky, noi_dung:$nd})
                        MERGE (k)-[:co_diem]->(m)
                        """, kid=kid, id=mid, ky=ky, nd=dm["noi_dung"])

        # Tạo các tham chiếu

        for chuong in doc.raw["quy_dinh_chi_tiet"]:
            for dieu in chuong["cac_dieu"]:

                dso = dieu["id"].replace("Điều", "").strip()
                did = dieu_index[dso]

                texts = [dieu.get("tieu_de", ""), dieu.get("noi_dung", "")]

                for t in texts:
                    for r in self.ref.extract(t):

                        tgt = (
                            diem_index.get(r)
                            or khoan_index.get((r[0], r[1]))
                            or dieu_index.get(r[0])
                        )

                        if tgt:
                            await tx.run("""
                            MATCH (a:Dieu {id:$a}), (b {id:$b})
                            MERGE (a)-[:tham_chieu]->(b)
                            """, a=did, b=tgt)

                for k in dieu.get("cac_khoan", []):

                    kso = k["so"]
                    kid = khoan_index[(dso, kso)]

                    for r in self.ref.extract(k["noi_dung"]):

                        tgt = (
                            diem_index.get(r)
                            or khoan_index.get((r[0], r[1]))
                            or dieu_index.get(r[0])
                        )

                        if tgt:
                            await tx.run("""
                            MATCH (a:Khoan {id:$a}), (b {id:$b})
                            MERGE (a)-[:tham_chieu]->(b)
                            """, a=kid, b=tgt)

                    for dm in k.get("cac_diem", []):

                        ky = dm["ky_hieu"]
                        mid = diem_index[(dso, kso, ky)]

                        for r in self.ref.extract(dm["noi_dung"]):

                            tgt = (
                                diem_index.get(r)
                                or khoan_index.get((r[0], r[1]))
                                or dieu_index.get(r[0])
                            )

                            if tgt:
                                await tx.run("""
                                MATCH (a:Diem {id:$a}), (b {id:$b})
                                MERGE (a)-[:tham_chieu]->(b)
                                """, a=mid, b=tgt)

##################################
# RUN
##################################

# if __name__ == "__main__":
#
#     doc = QuyCheDocument("data/processed/quychehocvu_VHVL.json")
#
#     # print("URI:", URI)
#     # print("USER:", USER)
#     # print("PASS:", PASS)
#     # print("DB:", DB)
#
#     builder = Neo4jGraphBuilder(URI, USER, PASS, DB)
#
#     builder.build(doc)
#     builder.close()
#
#     print("DONE")
