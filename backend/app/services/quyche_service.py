import logging
import os
import re
from typing import Any, Dict, List
from neo4j import GraphDatabase

from app.scripts.Embedding import EmbeddingModel
from app.services.quyche_llm_service import (
    QuyCheLLMService,
    VANBAN_DISPLAY,
)

logger = logging.getLogger("app.quyche")

ARABIC_TO_ROMAN = {
    1: "I", 2: "II", 3: "III", 4: "IV", 5: "V",
    6: "VI", 7: "VII", 8: "VIII", 9: "IX", 10: "X",
    11: "XI", 12: "XII", 13: "XIII", 14: "XIV", 15: "XV",
}


class Neo4jQuerier:
    def __init__(self):
        uri = os.getenv("NEO4J_URI")
        user = os.getenv("NEO4J_USERNAME")
        password = os.getenv("NEO4J_PASSWORD")
        database = os.getenv("NEO4J_DATABASE")
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.database = database

    def close(self):
        self.driver.close()

    def run(self, cypher: str, **params):
        with self.driver.session(database=self.database) as session:
            return [row.data() for row in session.run(cypher, **params)]

    def get_chuong(self, vb_id: str, roman: str) -> List[str]:
        cid = f"{vb_id}_{self._slugify('Chương ' + roman)}"
        rows = self.run(
            "MATCH (c:Chuong {id:$cid}) RETURN coalesce(c.text_embed, c.ten, '') AS text",
            cid=cid,
        )
        return [r["text"] for r in rows if r.get("text")]

    def get_dieu(self, vb_id: str, dso: str) -> List[str]:
        did = f"{vb_id}_Dieu{dso}"
        rows = self.run(
            """
            MATCH (d:Dieu {id:$did})
            OPTIONAL MATCH (d)-[:co_khoan]->(k:Khoan)
            OPTIONAL MATCH (k)-[:co_diem]->(m:Diem)
            WITH d,
                 collect(DISTINCT coalesce(k.text_embed, k.noi_dung, '')) AS khoan_texts,
                 collect(DISTINCT coalesce(m.text_embed, m.noi_dung, '')) AS diem_texts
            RETURN coalesce(d.text_embed, d.tieu_de, '') AS dieu_text,
                   khoan_texts, diem_texts
            """,
            did=did,
        )
        texts: List[str] = []
        for r in rows:
            if r.get("dieu_text"):
                texts.append(r["dieu_text"])
            texts += [t for t in r.get("khoan_texts", []) if t]
            texts += [t for t in r.get("diem_texts", []) if t]
        return texts

    def get_khoan(self, vb_id: str, dso: str, kso: str) -> List[str]:
        did = f"{vb_id}_Dieu{dso}"
        kid = f"{did}_Khoan{kso}"
        rows = self.run(
            """
            MATCH (k:Khoan {id:$kid})
            OPTIONAL MATCH (k)-[:co_diem]->(m:Diem)
            RETURN coalesce(k.text_embed, k.noi_dung, '') AS khoan_text,
                   collect(coalesce(m.text_embed, m.noi_dung, '')) AS diem_texts
            """,
            kid=kid,
        )
        texts: List[str] = []
        for r in rows:
            if r.get("khoan_text"):
                texts.append(r["khoan_text"])
            texts += [t for t in r.get("diem_texts", []) if t]
        return texts

    def get_diem(self, vb_id: str, dso: str, kso: str, dky: str) -> List[str]:
        did = f"{vb_id}_Dieu{dso}"
        kid = f"{did}_Khoan{kso}"
        mid = f"{kid}_Diem{self._slugify(dky)}"
        rows = self.run(
            "MATCH (m:Diem {id:$mid}) RETURN coalesce(m.text_embed, m.noi_dung, '') AS text",
            mid=mid,
        )
        return [r["text"] for r in rows if r.get("text")]

    def vector_search(self, vb_id: str, query_vec: List[float], top_k: int) -> List[str]:
        results = []
        for index_name in ["dieu_embedding_index", "khoan_embedding_index", "diem_embedding_index"]:
            rows = self.run(
                f"""
                CALL db.index.vector.queryNodes('{index_name}', $top_k, $qvec)
                YIELD node, score
                WHERE node.id STARTS WITH $vb_id
                RETURN node.id AS id,
                       coalesce(node.text_embed, node.noi_dung, node.tieu_de, '') AS text,
                       score
                """,
                qvec=query_vec,
                top_k=top_k,
                vb_id=vb_id,
            )
            for r in rows:
                if r.get("text"):
                    results.append((r["score"], r["text"], r["id"]))
        seen: Dict[str, tuple[float, str]] = {}
        for score, text, nid in results:
            if nid not in seen or score > seen[nid][0]:
                seen[nid] = (score, text)
        sorted_results = sorted(seen.values(), key=lambda x: x[0], reverse=True)[:top_k]
        return [text for _, text in sorted_results]

    @staticmethod
    def _slugify(text: str) -> str:
        if not text:
            return ""
        import unicodedata
        text = text.replace("Đ", "D").replace("đ", "d")
        nfkd = unicodedata.normalize("NFKD", text)
        text = "".join(c for c in nfkd if not unicodedata.combining(c))
        return re.sub(r"[^a-zA-Z0-9]", "", text)


class QuyCheService:
    """Service xử lý chatbot quy chế học vụ."""

    def __init__(self) -> None:
        self.querier = Neo4jQuerier()
        self.llm_service = QuyCheLLMService()
        self._embedding_model = None

    def close(self) -> None:
        self.querier.close()

    def _get_embedding_model(self) -> EmbeddingModel:
        if self._embedding_model is not None:
            return self._embedding_model
        self._embedding_model = EmbeddingModel(device=os.getenv("EMBEDDING_DEVICE", "cpu"))
        return self._embedding_model

    def _embed_text(self, text: str) -> List[float]:
        try:
            emb = self._get_embedding_model()
            vecs = emb.get_embedding_batch([text])
            return vecs[0] if vecs else []
        except Exception as e:
            logger.warning(f"[QuyCheService] Embedding error: {e}")
            return []

    def _extract_references(self, question: str) -> List[Dict[str, str]]:
        refs: List[Dict[str, str]] = []
        for m in re.finditer(r"[Cc]h[ưu]ơng\s+([IVXLCDM]+|\d+)", question):
            val = m.group(1)
            if val.isdigit():
                val = self._arabic_to_roman(int(val))
            refs.append({"type": "chuong", "value": val})

        for m in re.finditer(
            r"[Đđ]i[eềếệ]u\s+(\d+).*?[Kk]ho[aả]n\s+(\d+).*?[Đđ]i[eể]m\s+([a-zđ])",
            question, re.IGNORECASE,
        ):
            refs.append({"type": "diem", "dieu": m.group(1), "khoan": m.group(2), "diem": m.group(3)})

        for m in re.finditer(
            r"[Đđ]i[eềếệ]u\s+(\d+).*?[Kk]ho[aả]n\s+(\d+)",
            question, re.IGNORECASE,
        ):
            if not any(r["type"] == "diem" and r["dieu"] == m.group(1) and r["khoan"] == m.group(2) for r in refs):
                refs.append({"type": "khoan", "dieu": m.group(1), "khoan": m.group(2)})

        for m in re.finditer(r"[Đđ]i[eềếệ]u\s+(\d+)", question, re.IGNORECASE):
            dso = m.group(1)
            if not any(r.get("dieu") == dso for r in refs):
                refs.append({"type": "dieu", "dieu": dso})
        return refs

    def _arabic_to_roman(self, n: int) -> str:
        return ARABIC_TO_ROMAN.get(n, str(n))

    def _collect_context(self, vb_id: str, question: str, refs: List[Dict[str, str]]) -> str:
        texts: List[str] = []
        if refs:
            for ref in refs:
                if ref["type"] == "chuong":
                    texts += self.querier.get_chuong(vb_id, ref["value"])
                elif ref["type"] == "dieu":
                    texts += self.querier.get_dieu(vb_id, ref["dieu"])
                elif ref["type"] == "khoan":
                    texts += self.querier.get_khoan(vb_id, ref["dieu"], ref["khoan"])
                elif ref["type"] == "diem":
                    texts += self.querier.get_diem(vb_id, ref["dieu"], ref["khoan"], ref["diem"])
        else:
            vec = self._embed_text(question)
            if vec:
                texts = self.querier.vector_search(vb_id, vec, top_k=int(os.getenv("QUYCHE_VECTOR_TOP_K", "6")))
            else:
                logger.warning("[QuyCheService] Không lấy được embedding")

        seen = set()
        unique: List[str] = []
        for t in texts:
            if t and t not in seen:
                seen.add(t)
                unique.append(t)
        return "\n\n---\n\n".join(unique)

    async def handle_query(self, query: str, history: List[Dict[str, Any]]) -> Dict[str, Any]:
        question = self.llm_service.preprocess_question(query)
        if question != query:
            logger.info(f"[QuyCheService] Preprocess: '{query}' -> '{question}'")

        if self.llm_service.is_chitchat(question):
            answer = self.llm_service.answer_chitchat(question, history)
            return {"answer": answer, "vb_id": None, "vb_display": None, "refs": [], "chitchat": True}

        vb_id = self.llm_service.detect_vanban(question)
        vb_display = VANBAN_DISPLAY.get(vb_id, vb_id)
        refs = self._extract_references(question)
        if refs:
            logger.info(f"[QuyCheService] refs={refs}")

        context = self._collect_context(vb_id, question, refs)
        answer = self.llm_service.generate_answer(question, context, history, vb_display)
        return {
            "answer": answer,
            "vb_id": vb_id,
            "vb_display": vb_display,
            "refs": refs,
            "chitchat": False,
        }


quyche_service = QuyCheService()
