"use client"; // Bắt buộc cho Client Component

import { useEffect, useMemo, useRef, useState } from "react";
import { useRouter } from "next/navigation";

type UploadJobStatus = "queued" | "processing" | "completed" | "failed";

type UploadJob = {
  job_id: string;
  status: UploadJobStatus;
  category?: UploadCategory;
  result?: {
    ingestion_applied?: boolean;
    nodes?: number;
    edges?: number;
    message?: string;
    extraction_source?: string;
    extracted_text_length?: number;
    section_count?: number;
    extracted_preview?: string;
    json_path?: string;
  } | null;
  error?: string | null;
};

type ItemStatus = "ready" | "uploading" | "queued" | "processing" | "review" | "success" | "stored" | "error";

type UploadItem = {
  localId: string;
  key: string;
  file: File;
  status: ItemStatus;
  syncState?: "idle" | "syncing" | "synced";
  message: string;
  jobId?: string;
  extractionSource?: string;
  extractedTextLength?: number;
  sectionCount?: number;
  extractedPreview?: string;
  jsonPath?: string;
};

type UploadCategory =
  | "chuyen_nganh_dao_tao"
  | "quy_che_hoc_vu"
  | "huong_dan_thu_tuc"
  | "thong_bao_ke_hoach";

const CATEGORY_OPTIONS: { value: UploadCategory; label: string }[] = [
  { value: "chuyen_nganh_dao_tao", label: "Chuyên ngành đào tạo" },
  { value: "quy_che_hoc_vu", label: "Quy chế học vụ" },
  { value: "huong_dan_thu_tuc", label: "Hướng dẫn thủ tục" },
  { value: "thong_bao_ke_hoach", label: "Thông báo kế hoạch" },
];

const CATEGORY_LABEL_MAP: Record<UploadCategory, string> = {
  chuyen_nganh_dao_tao: "Chuyên ngành đào tạo",
  quy_che_hoc_vu: "Quy chế học vụ",
  huong_dan_thu_tuc: "Hướng dẫn thủ tục",
  thong_bao_ke_hoach: "Thông báo kế hoạch",
};

const API_BASE_URL = "http://localhost:8000";

// Hàm tiện ích để định dạng kích thước file
const formatBytes = (bytes: number, decimals = 2) => {
  if (bytes === 0) return "0 Bytes";
  const k = 1024;
  const dm = decimals < 0 ? 0 : decimals;
  const sizes = ["Bytes", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + " " + sizes[i];
};

export default function AdminKnowledgeManager() {
  const router = useRouter();
  const [uploadItems, setUploadItems] = useState<UploadItem[]>([]);
  const [dragOver, setDragOver] = useState(false);
  const [pageMessage, setPageMessage] = useState("");
  const [selectedCategory, setSelectedCategory] = useState<UploadCategory | "">("");
  const [selectedDetailItem, setSelectedDetailItem] = useState<UploadItem | null>(null);
  const [jsonEditorText, setJsonEditorText] = useState("");
  const [jsonEditorLoading, setJsonEditorLoading] = useState(false);
  const [jsonEditorDirty, setJsonEditorDirty] = useState(false);
  const [jsonEditorInfo, setJsonEditorInfo] = useState("");
  const [jsonEditorError, setJsonEditorError] = useState("");
  const [jsonConfirmLoading, setJsonConfirmLoading] = useState(false);
  const [jsonConfirmInfo, setJsonConfirmInfo] = useState("");
  const [jsonConfirmError, setJsonConfirmError] = useState("");
  const pollingRef = useRef<Record<string, number>>({});

  const isBusy = useMemo(
    () => uploadItems.some((item) => ["uploading", "queued", "processing"].includes(item.status)),
    [uploadItems]
  );

  const summary = useMemo(() => {
    const count = {
      ready: 0,
      uploading: 0,
      queued: 0,
      processing: 0,
      review: 0,
      success: 0,
      stored: 0,
      error: 0,
    };

    uploadItems.forEach((item) => {
      count[item.status] += 1;
    });

    return count;
  }, [uploadItems]);

  useEffect(() => {
    const token = localStorage.getItem("token");
    if (!token) {
      router.push("/admin/login");
    }
  }, [router]);

  useEffect(() => {
    return () => {
      Object.values(pollingRef.current).forEach((intervalId) => clearInterval(intervalId));
      pollingRef.current = {};
    };
  }, []);

  const updateItem = (localId: string, patch: Partial<UploadItem>) => {
    setUploadItems((prev) => prev.map((item) => (item.localId === localId ? { ...item, ...patch } : item)));
  };

  const patchItemEverywhere = (localId: string, patch: Partial<UploadItem>) => {
    updateItem(localId, patch);
    setSelectedDetailItem((prev) => (prev && prev.localId === localId ? { ...prev, ...patch } : prev));
  };

  const clearPolling = (localId: string) => {
    const intervalId = pollingRef.current[localId];
    if (intervalId) {
      clearInterval(intervalId);
      delete pollingRef.current[localId];
    }
  };

  const startPolling = (localId: string, jobId: string, token: string) => {
    const pollStatus = async () => {
      try {
        const response = await fetch(`${API_BASE_URL}/graph/upload/status/${jobId}`, {
          method: "GET",
          headers: {
            Authorization: `Bearer ${token}`,
          },
        });

        if (!response.ok) {
          if (response.status === 401) {
            updateItem(localId, { status: "error", message: "Phiên đăng nhập hết hạn." });
            clearPolling(localId);
            router.push("/admin/login");
            return;
          }
          throw new Error(`Không lấy được trạng thái: ${response.statusText}`);
        }

        const data: UploadJob = await response.json();
        const catLabel = data.category ? CATEGORY_LABEL_MAP[data.category] : "Chưa rõ";

        if (data.status === "queued") {
          updateItem(localId, {
            status: "queued",
            syncState: "idle",
            message: `Job ${jobId.slice(0, 8)} (${catLabel}) đang chờ trong hàng đợi.`,
          });
          return;
        }

        if (data.status === "processing") {
          updateItem(localId, {
            status: "processing",
            syncState: "idle",
            message: `Job ${jobId.slice(0, 8)} (${catLabel}) đang xử lý.`,
          });
          return;
        }

        if (data.status === "completed") {
          if (data.result?.ingestion_applied === false) {
            updateItem(localId, {
              status: "review",
              syncState: "idle",
              message: data.result?.message ?? "Đã trích xuất JSON, chờ admin xác nhận.",
              extractionSource: data.result?.extraction_source,
              extractedTextLength: data.result?.extracted_text_length,
              sectionCount: data.result?.section_count,
              extractedPreview: data.result?.extracted_preview,
              jsonPath: data.result?.json_path,
            });
            clearPolling(localId);
            return;
          }

          if (typeof data.result?.nodes === "number" && typeof data.result?.edges === "number") {
            updateItem(localId, {
              status: "success",
              syncState: "synced",
              message: `Xong: ${data.result.nodes} nodes, ${data.result.edges} edges.`,
              extractionSource: data.result?.extraction_source,
              extractedTextLength: data.result?.extracted_text_length,
              sectionCount: data.result?.section_count,
              extractedPreview: data.result?.extracted_preview,
              jsonPath: data.result?.json_path,
            });
          } else {
            updateItem(localId, {
              status: "success",
              syncState: "synced",
              message: data.result?.message ?? "Upload hoàn tất.",
              extractionSource: data.result?.extraction_source,
              extractedTextLength: data.result?.extracted_text_length,
              sectionCount: data.result?.section_count,
              extractedPreview: data.result?.extracted_preview,
              jsonPath: data.result?.json_path,
            });
          }
          clearPolling(localId);
          return;
        }

        if (data.status === "failed") {
          updateItem(localId, {
            status: "error",
            message: data.error ?? "Ingestion thất bại.",
          });
          clearPolling(localId);
        }
      } catch (error) {
        updateItem(localId, {
          status: "error",
          message: "Không thể cập nhật trạng thái job.",
        });
        clearPolling(localId);
        console.error("Poll status error:", error);
      }
    };

    pollStatus();
    const intervalId = window.setInterval(pollStatus, 2500);
    pollingRef.current[localId] = intervalId;
  };

  const appendFiles = (incomingFiles: FileList | File[]) => {
    const files = Array.from(incomingFiles);

    setUploadItems((prev) => {
      const existingKeys = new Set(prev.map((item) => item.key));
      const next = [...prev];

      files.forEach((f) => {
        const key = `${f.name}-${f.size}-${f.lastModified}`;
        if (existingKeys.has(key)) return;

        next.push({
          localId: crypto.randomUUID(),
          key,
          file: f,
          status: "ready",
          syncState: "idle",
          message: "Sẵn sàng upload.",
        });
      });

      return next;
    });
  };

  const handleDragOver = (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    setDragOver(true);
  };

  const handleDragLeave = () => {
    setDragOver(false);
  };

  const onFileDrop = (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    setDragOver(false);
    if (e.dataTransfer.files && e.dataTransfer.files.length > 0) {
      appendFiles(e.dataTransfer.files);
      setPageMessage("");
    }
  };

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files.length > 0) {
      appendFiles(e.target.files);
      setPageMessage("");
    }
  };

  const handleRemoveItem = (localId: string) => {
    clearPolling(localId);
    setUploadItems((prev) => prev.filter((item) => item.localId !== localId));
  };

  const openItemDetails = async (item: UploadItem) => {
    setSelectedDetailItem(item);
    setJsonEditorText("");
    setJsonEditorDirty(false);
    setJsonEditorInfo("");
    setJsonEditorError("");
    setJsonConfirmLoading(false);
    setJsonConfirmInfo("");
    setJsonConfirmError("");

    if (!item.jobId || !["success", "stored", "review"].includes(item.status)) {
      return;
    }

    const token = localStorage.getItem("token");
    if (!token) {
      setJsonEditorError("Phiên đăng nhập hết hạn. Vui lòng đăng nhập lại.");
      return;
    }

    setJsonEditorLoading(true);
    try {
      const response = await fetch(`${API_BASE_URL}/graph/upload/json/${item.jobId}`, {
        method: "GET",
        headers: { Authorization: `Bearer ${token}` },
      });

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(errorData?.detail || "Không thể tải JSON trích xuất.");
      }

      const payload = await response.json();
      const jsonText = JSON.stringify(payload.data ?? {}, null, 2);
      setJsonEditorText(jsonText);
      setJsonEditorInfo(`Đã tải JSON từ: ${payload.json_path}`);
      setJsonConfirmInfo(item.status === "review" ? "JSON đã sẵn sàng. Sửa xong thì lưu, rồi bấm xác nhận để nạp Neo4j." : "JSON đã sẵn sàng để xem hoặc lưu lại.");

      patchItemEverywhere(item.localId, { jsonPath: payload.json_path });
    } catch (error) {
      setJsonEditorError(error instanceof Error ? error.message : "Không thể tải JSON.");
    } finally {
      setJsonEditorLoading(false);
    }
  };

  const saveJsonEdits = async () => {
    if (!selectedDetailItem?.jobId) {
      setJsonEditorError("Không tìm thấy job_id để lưu JSON.");
      return;
    }

    const token = localStorage.getItem("token");
    if (!token) {
      setJsonEditorError("Phiên đăng nhập hết hạn. Vui lòng đăng nhập lại.");
      return;
    }

    let parsed: unknown;
    try {
      parsed = JSON.parse(jsonEditorText);
    } catch {
      setJsonEditorError("JSON không hợp lệ. Vui lòng kiểm tra lại cú pháp.");
      return;
    }

    setJsonEditorLoading(true);
    setJsonEditorError("");
    setJsonEditorInfo("");
    try {
      const response = await fetch(`${API_BASE_URL}/graph/upload/json/${selectedDetailItem.jobId}`, {
        method: "PUT",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${token}`,
        },
        body: JSON.stringify({ data: parsed }),
      });

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(errorData?.detail || "Không thể lưu JSON đã chỉnh sửa.");
      }

      const payload = await response.json();
      setJsonEditorDirty(false);
      setJsonEditorInfo(`Đã lưu thành công: ${payload.json_path}. Bây giờ có thể nhấn xác nhận để nạp Neo4j.`);
      setJsonConfirmInfo("JSON đã được lưu. Nhấn xác nhận để bắt đầu nạp Neo4j.");
    } catch (error) {
      setJsonEditorError(error instanceof Error ? error.message : "Lưu JSON thất bại.");
    } finally {
      setJsonEditorLoading(false);
    }
  };

  const confirmNeo4jImport = async () => {
    if (!selectedDetailItem?.jobId) {
      setJsonConfirmError("Không tìm thấy job_id để xác nhận nạp Neo4j.");
      return;
    }

    if (jsonEditorDirty) {
      setJsonConfirmError("Bạn cần lưu JSON trước khi xác nhận nạp Neo4j.");
      return;
    }

    const token = localStorage.getItem("token");
    if (!token) {
      setJsonConfirmError("Phiên đăng nhập hết hạn. Vui lòng đăng nhập lại.");
      return;
    }

    setJsonConfirmLoading(true);
    setJsonConfirmInfo("");
    setJsonConfirmError("");
    patchItemEverywhere(selectedDetailItem.localId, { syncState: "syncing" });

    try {
      const response = await fetch(`${API_BASE_URL}/graph/upload/confirm/${selectedDetailItem.jobId}`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${token}`,
        },
      });

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(errorData?.detail || "Không thể xác nhận nạp Neo4j.");
      }

      const payload = await response.json();
      setJsonConfirmInfo(payload.message ?? "Đã nạp Neo4j thành công.");
      patchItemEverywhere(selectedDetailItem.localId, {
        status: "success",
        syncState: "synced",
        message: payload.message ?? selectedDetailItem.message,
      });
    } catch (error) {
      patchItemEverywhere(selectedDetailItem.localId, { syncState: "idle" });
      setJsonConfirmError(error instanceof Error ? error.message : "Xác nhận nạp Neo4j thất bại.");
    } finally {
      setJsonConfirmLoading(false);
    }
  };

  const uploadSingleFile = async (item: UploadItem, token: string) => {
    updateItem(item.localId, { status: "uploading", message: "Đang upload file..." });

    const formData = new FormData();
    formData.append("file", item.file);
    formData.append("category", selectedCategory);

    const response = await fetch(`${API_BASE_URL}/graph/upload`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
      },
      body: formData,
    });

    const data = await response.json();

    if (!response.ok) {
      if (response.status === 401) {
        localStorage.removeItem("token");
        router.push("/admin/login");
        throw new Error("Phiên đăng nhập hết hạn. Vui lòng đăng nhập lại.");
      }
      throw new Error(data?.detail || `Lỗi server: ${response.statusText}`);
    }

    const returnedJobId = data?.job_id as string | undefined;
    if (!returnedJobId) {
      throw new Error("Server không trả về job_id");
    }

    updateItem(item.localId, {
      status: "queued",
      syncState: "idle",
      jobId: returnedJobId,
      message: `Đã upload, đang chờ xử lý (Job ${returnedJobId.slice(0, 8)}).`,
    });

    startPolling(item.localId, returnedJobId, token);
  };

  const handleIngest = async () => {
    if (!selectedCategory) {
      alert("Vui lòng chọn thể loại tài liệu!");
      return;
    }

    if (!uploadItems.length) {
      alert("Vui lòng chọn file!");
      return;
    }

    const token = localStorage.getItem("token");
    if (!token) {
      setPageMessage("Phiên đăng nhập đã hết hoặc chưa đăng nhập. Vui lòng đăng nhập lại.");
      router.push("/admin/login");
      return;
    }

    setPageMessage(`Bắt đầu upload ${uploadItems.length} file thuộc ${CATEGORY_LABEL_MAP[selectedCategory]}...`);

    const candidates = uploadItems.filter((item) => ["ready", "error"].includes(item.status));
    if (!candidates.length) {
      setPageMessage("Không có file nào ở trạng thái sẵn sàng để upload.");
      return;
    }

    try {
      await Promise.all(
        candidates.map(async (item) => {
          try {
            await uploadSingleFile(item, token);
          } catch (error) {
            updateItem(item.localId, {
              status: "error",
              message: error instanceof Error ? error.message : "Upload thất bại.",
            });
          }
        })
      );

      setPageMessage("Đã gửi file lên server. Hệ thống đang xử lý theo hàng đợi.");
    } catch (error) {
      setPageMessage("Có lỗi kết nối đến server.");
      console.error("Ingest error:", error);
    }
  };

  const getStatusBadge = (status: ItemStatus) => {
    if (status === "success") return "bg-emerald-50 text-emerald-700 border border-emerald-200";
    if (status === "stored") return "bg-slate-50 text-slate-600 border border-slate-200";
    if (status === "review") return "bg-amber-50 text-amber-700 border border-amber-200";
    if (status === "error") return "bg-rose-50 text-rose-700 border border-rose-200";
    if (status === "processing") return "bg-cyan-50 text-cyan-700 border border-cyan-200";
    if (status === "queued") return "bg-amber-50 text-amber-700 border border-amber-200";
    if (status === "uploading") return "bg-sky-50 text-sky-700 border border-sky-200";
    return "bg-slate-50 text-slate-600 border border-slate-200";
  };

  const getStatusLabel = (status: ItemStatus) => {
    if (status === "ready") return "Sẵn sàng";
    if (status === "uploading") return "Đang upload";
    if (status === "queued") return "Đang xếp hàng";
    if (status === "processing") return "Đang xử lý";
    if (status === "review") return "Chờ xác nhận";
    if (status === "success") return "Thành công";
    if (status === "stored") return "Đã lưu";
    return "Lỗi";
  };

  const getSyncBadge = (syncState?: UploadItem["syncState"]) => {
    if (syncState === "syncing") {
      return {
        label: "Đang đồng bộ",
        className: "bg-sky-50 text-sky-700 border-sky-200",
      };
    }
    if (syncState === "synced") {
      return {
        label: "Đã đồng bộ",
        className: "bg-emerald-50 text-emerald-700 border-emerald-200",
      };
    }
    return null;
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 via-blue-50 to-indigo-50 text-slate-900 p-5 md:p-8 font-sans">
      <div className="max-w-6xl mx-auto space-y-8">
        <header className="flex items-center justify-between pb-5 border-b border-slate-200/60">
          <div className="flex items-center gap-4">
            <div className="w-12 h-12 bg-gradient-to-br from-blue-600 to-indigo-700 rounded-2xl flex items-center justify-center text-white shadow-lg shadow-blue-200">
              <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4m0 5c0 2.21-3.582 4-8 4s-8-1.79-8-4" />
              </svg>
            </div>
            <div>
              <h1 className="text-2xl md:text-3xl font-bold tracking-tight text-slate-800">
                Quản lý dữ liệu
              </h1>
              <p className="text-sm text-slate-500 mt-1">
                Nạp tài liệu vào Neo4j Graph Database
              </p>
            </div>
          </div>
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-full bg-gradient-to-br from-blue-100 to-indigo-200 flex items-center justify-center text-blue-600">
              <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z" />
              </svg>
            </div>
            <span className="text-sm font-medium text-slate-600">Admin</span>
          </div>
        </header>

        <main className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          <section className="lg:col-span-2 space-y-6">
            <div className="bg-white/80 backdrop-blur-sm rounded-2xl shadow-sm border border-slate-200/60 p-6">
              <h2 className="text-lg font-semibold mb-1 text-slate-800">Thêm tài liệu mới</h2>
              <p className="text-slate-500 mb-6 text-sm">Kéo thả nhiều file PDF hoặc nhấn để chọn nhiều file.</p>

              <div className="mb-5">
                <label htmlFor="category-select" className="block text-sm font-semibold text-slate-700 mb-2">
                  Chọn thể loại tài liệu
                </label>
                <select
                  id="category-select"
                  value={selectedCategory}
                  onChange={(e) => setSelectedCategory(e.target.value as UploadCategory | "")}
                  className="w-full rounded-xl border border-slate-200 bg-slate-50 px-4 py-3 text-slate-700 focus:outline-none focus:ring-2 focus:ring-blue-400 focus:border-blue-400 transition-all"
                >
                  <option value="">-- Chọn thể loại --</option>
                  {CATEGORY_OPTIONS.map((option) => (
                    <option key={option.value} value={option.value}>
                      {option.label}
                    </option>
                  ))}
                </select>
              </div>

              <div
                onDragOver={handleDragOver}
                onDragLeave={handleDragLeave}
                onDrop={onFileDrop}
                className={`relative border-2 border-dashed rounded-2xl p-10 text-center transition-all ${
                  dragOver ? "border-blue-400 bg-blue-50" : "border-slate-200 hover:border-blue-300"
                }`}
              >
                <svg className={`w-14 h-14 mx-auto mb-4 ${dragOver ? "text-blue-500" : "text-slate-300"}`} fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                </svg>
                <label htmlFor="file-upload" className="cursor-pointer">
                  <span className="text-lg font-semibold text-blue-600">Kéo thả nhiều file PDF</span>
                  <span className="text-lg text-slate-500"> hoặc nhấn để chọn</span>
                  <input id="file-upload" type="file" multiple accept="application/pdf" onChange={handleFileChange} className="sr-only" />
                </label>
                <p className="mt-3 text-sm text-slate-400">Mỗi file tối đa 50MB, có thể upload cùng lúc nhiều file.</p>
              </div>

              {pageMessage && (
                <div className="mt-5 rounded-xl border border-blue-200 bg-blue-50 px-4 py-3 text-sm text-blue-700">
                  {pageMessage}
                </div>
              )}
            </div>

            <div className="bg-white/80 backdrop-blur-sm rounded-2xl shadow-sm border border-slate-200/60 p-6">
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-lg font-semibold text-slate-900">Danh sách file</h3>
                <span className="text-sm text-slate-500">{uploadItems.length} file</span>
              </div>

              {!uploadItems.length && (
                <div className="rounded-xl border border-dashed border-slate-300 px-4 py-8 text-center text-slate-500">
                  Chưa có file nào được chọn.
                </div>
              )}

              <div className="space-y-3 max-h-96 overflow-auto pr-1">
                {uploadItems.map((item) => (
                  <div key={item.localId} className="rounded-xl border border-slate-200/60 bg-white/60 px-4 py-3 hover:border-sky-200 hover:bg-white/80 transition-all">
                    <div className="flex items-start justify-between gap-3">
                      <div className="min-w-0">
                        <p className="font-medium text-slate-900 break-all leading-snug">{item.file.name}</p>
                        <p className="text-xs text-slate-500 mt-1">{formatBytes(item.file.size)}</p>
                      </div>

                      <div className="flex items-center gap-2">
                        <span className={`text-xs px-2.5 py-1 rounded-full font-medium ${getStatusBadge(item.status)}`}>
                          {getStatusLabel(item.status)}
                        </span>
                        {getSyncBadge(item.syncState) && (
                          <span
                            className={`text-[10px] px-2 py-1 rounded-full border font-medium ${getSyncBadge(item.syncState)?.className}`}
                          >
                            {getSyncBadge(item.syncState)?.label}
                          </span>
                        )}
                        {item.status !== "uploading" && item.status !== "queued" && item.status !== "processing" && (
                          <button onClick={() => handleRemoveItem(item.localId)} className="text-slate-400 hover:text-rose-500" aria-label="Remove file">
                            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" /></svg>
                          </button>
                        )}
                      </div>
                    </div>

                    <p className="text-xs text-slate-500 mt-2 truncate">{item.message}</p>

                    {(item.status === "success" || item.status === "stored" || item.status === "review") && (
                      <div className="mt-3 rounded-lg border border-slate-200/60 bg-slate-50/50 p-3">
                        <p className="text-[11px] font-semibold text-slate-700 mb-2">Thông tin trích xuất</p>
                        <div className="flex flex-wrap gap-2 text-[11px]">
                          <span className="px-2 py-1 rounded bg-white border border-slate-200 text-slate-700">
                            Nguồn: {item.extractionSource ?? "unknown"}
                          </span>
                          <span className="px-2 py-1 rounded bg-white border border-slate-200 text-slate-700">
                            Ký tự: {item.extractedTextLength ?? 0}
                          </span>
                          <span className="px-2 py-1 rounded bg-white border border-slate-200 text-slate-700">
                            Sections: {item.sectionCount ?? 0}
                          </span>
                        </div>

                        {item.extractedPreview && (
                          <div className="mt-2 text-[11px] text-slate-600 bg-white border border-slate-200 rounded p-2">
                            <p className="font-medium mb-1">Preview:</p>
                            <p className="line-clamp-3">{item.extractedPreview}</p>
                          </div>
                        )}

                        <div className="mt-2 flex items-center justify-between gap-2">
                          <p className="text-[11px] text-slate-500">
                            {item.jsonPath ? "Có JSON để mở, chỉnh sửa và xác nhận nạp Neo4j." : "Chưa có JSON để mở trực tiếp."}
                          </p>
                          <button
                            type="button"
                            onClick={() => openItemDetails(item)}
                            className="text-xs px-3 py-1.5 rounded-md bg-blue-500 text-white hover:bg-blue-600 cursor-pointer transition-all"
                          >
                            Xem chi tiết
                          </button>
                        </div>
                      </div>
                    )}
                  </div>
                ))}
              </div>
            </div>
          </section>

          <aside className="space-y-6">
            <div className="bg-white/80 backdrop-blur-sm rounded-2xl shadow-sm border border-slate-200/60 p-6 space-y-5">
              <h3 className="text-lg font-semibold text-slate-800">Bắt đầu xử lý</h3>
              <p className="text-sm text-slate-500">Upload nhiều file cùng lúc, backend sẽ xếp hàng xử lý tuần tự.</p>

              <button
                onClick={handleIngest}
                disabled={isBusy || !uploadItems.length}
                className={`w-full flex items-center justify-center gap-2 py-3.5 rounded-xl font-semibold text-white transition-all ${
                  isBusy || !uploadItems.length
                    ? "bg-slate-300 cursor-not-allowed"
                    : "bg-gradient-to-r from-blue-600 to-indigo-600 hover:from-blue-700 hover:to-indigo-700 shadow-lg shadow-blue-200"
                }`}
              >
                {isBusy ? (
                  <>
                    <div className="w-5 h-5 border-2 border-white border-t-transparent rounded-full animate-spin"></div>
                    Đang gửi và theo dõi jobs...
                  </>
                ) : (
                  <>
                    <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" /></svg>
                    Upload {uploadItems.length} file
                  </>
                )}
              </button>
            </div>

            <div className="bg-white/80 backdrop-blur-sm rounded-2xl shadow-sm border border-slate-200/60 p-6">
              <h4 className="text-sm font-semibold text-slate-800 mb-4">Tổng quan trạng thái</h4>
              <div className="grid grid-cols-2 gap-3 text-sm">
                <div className="rounded-lg bg-slate-50/50 border border-slate-200/50 px-3 py-2">
                  <p className="text-slate-500 text-xs">Sẵn sàng</p>
                  <p className="font-semibold text-slate-700">{summary.ready}</p>
                </div>
                <div className="rounded-lg bg-sky-50/50 border border-sky-200/50 px-3 py-2">
                  <p className="text-sky-600 text-xs">Đang upload</p>
                  <p className="font-semibold text-sky-700">{summary.uploading}</p>
                </div>
                <div className="rounded-lg bg-amber-50/50 border border-amber-200/50 px-3 py-2">
                  <p className="text-amber-600 text-xs">Đợi xử lý</p>
                  <p className="font-semibold text-amber-700">{summary.queued}</p>
                </div>
                <div className="rounded-lg bg-cyan-50/50 border border-cyan-200/50 px-3 py-2">
                  <p className="text-cyan-600 text-xs">Đang xử lý</p>
                  <p className="font-semibold text-cyan-700">{summary.processing}</p>
                </div>
                <div className="rounded-lg bg-emerald-50/50 border border-emerald-200/50 px-3 py-2">
                  <p className="text-emerald-600 text-xs">Thành công</p>
                  <p className="font-semibold text-emerald-700">{summary.success}</p>
                </div>
                <div className="rounded-lg bg-slate-50/50 border border-slate-200/50 px-3 py-2">
                  <p className="text-slate-500 text-xs">Đã lưu</p>
                  <p className="font-semibold text-slate-600">{summary.stored}</p>
                </div>
                <div className="rounded-lg bg-amber-50/50 border border-amber-200/50 px-3 py-2">
                  <p className="text-amber-600 text-xs">Chờ xác nhận</p>
                  <p className="font-semibold text-amber-700">{summary.review}</p>
                </div>
                <div className="rounded-lg bg-rose-50/50 border border-rose-200/50 px-3 py-2">
                  <p className="text-rose-600 text-xs">Lỗi</p>
                  <p className="font-semibold text-rose-700">{summary.error}</p>
                </div>
              </div>
            </div>
          </aside>
        </main>

        <footer className="text-center pt-6 border-t border-slate-200/60 text-slate-400 text-sm">
          <span className="inline-flex items-center gap-1">
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
            </svg>
            CIT GraphRAG
          </span>
        </footer>
      </div>

      {selectedDetailItem && (
        <div
          className="fixed inset-0 z-50 bg-slate-900/50 backdrop-blur-[1px] p-4 flex items-center justify-center"
          onClick={() => setSelectedDetailItem(null)}
        >
          <div
            className="w-full max-w-6xl max-h-[90vh] overflow-hidden rounded-2xl bg-white shadow-2xl border border-slate-200"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-center justify-between px-5 py-4 border-b border-slate-200">
              <div>
                <h3 className="text-lg font-semibold text-slate-900">Chi tiết trích xuất</h3>
                <p className="text-xs text-slate-500 mt-0.5 break-all leading-snug">{selectedDetailItem.file.name}</p>
              </div>
              <button
                type="button"
                onClick={() => setSelectedDetailItem(null)}
                className="text-slate-500 hover:text-slate-800"
                aria-label="Đóng"
              >
                <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" /></svg>
              </button>
            </div>

            <div className="p-5 overflow-y-auto max-h-[calc(90vh-70px)]">
              <div className="grid grid-cols-1 lg:grid-cols-12 gap-4">
                <div className="lg:col-span-7 rounded-lg border border-slate-200 bg-white p-3 space-y-3">
                  <div className="flex items-center justify-between">
                    <p className="text-xs text-slate-500">JSON trích xuất (có thể chỉnh sửa)</p>
                  </div>

                  {jsonEditorInfo && <p className="text-xs text-emerald-700">{jsonEditorInfo}</p>}
                  {jsonEditorError && <p className="text-xs text-red-600">{jsonEditorError}</p>}
                  {jsonConfirmInfo && <p className="text-xs text-blue-700">{jsonConfirmInfo}</p>}
                  {jsonConfirmError && <p className="text-xs text-rose-600">{jsonConfirmError}</p>}

                  {jsonEditorLoading && !jsonEditorText ? (
                    <p className="text-xs text-slate-500">Đang tải JSON...</p>
                  ) : (
                    <textarea
                      value={jsonEditorText}
                      onChange={(e) => {
                        setJsonEditorText(e.target.value);
                        setJsonEditorDirty(true);
                        setJsonEditorInfo("");
                        setJsonEditorError("");
                        if (selectedDetailItem?.localId) {
                          patchItemEverywhere(selectedDetailItem.localId, { syncState: "idle" });
                        }
                      }}
                      className="w-full min-h-[520px] rounded-md border border-slate-200 bg-slate-50 p-3 text-xs font-mono text-slate-700 focus:outline-none focus:ring-2 focus:ring-blue-400"
                      placeholder="JSON sẽ hiển thị ở đây khi job hoàn tất."
                    />
                  )}

                  <div className="flex items-center justify-end gap-2">
                    <button
                      type="button"
                      onClick={saveJsonEdits}
                      disabled={jsonEditorLoading || !jsonEditorDirty}
                      className={`text-xs px-3 py-1.5 rounded-md text-white ${
                        jsonEditorLoading || !jsonEditorDirty
                          ? "bg-slate-300 cursor-not-allowed"
                          : "bg-emerald-500 hover:bg-emerald-600"
                      }`}
                    >
                      {jsonEditorLoading ? "Đang lưu..." : "Lưu JSON"}
                    </button>
                    <button
                      type="button"
                      onClick={confirmNeo4jImport}
                      disabled={jsonConfirmLoading || jsonEditorDirty || selectedDetailItem?.status === "success"}
                      className={`text-xs px-3 py-1.5 rounded-md text-white ${
                        jsonConfirmLoading || jsonEditorDirty || selectedDetailItem?.status === "success"
                          ? "bg-slate-300 cursor-not-allowed"
                          : "bg-blue-500 hover:bg-blue-600"
                      }`}
                    >
                      {jsonConfirmLoading ? "Đang xác nhận..." : "Xác nhận nạp Neo4j"}
                    </button>
                  </div>
                </div>

                <div className="lg:col-span-5 space-y-4">
                  <div className="grid grid-cols-2 gap-3 text-sm">
                    <div className="rounded-lg border border-slate-200/60 bg-slate-50/50 p-3">
                      <p className="text-slate-500 text-xs">Trạng thái</p>
                      <p className="font-semibold text-slate-700">{getStatusLabel(selectedDetailItem.status)}</p>
                    </div>
                    <div className="rounded-lg border border-slate-200/60 bg-slate-50/50 p-3">
                      <p className="text-slate-500 text-xs">Kích thước</p>
                      <p className="font-semibold text-slate-700">{formatBytes(selectedDetailItem.file.size)}</p>
                    </div>
                    <div className="rounded-lg border border-slate-200/60 bg-slate-50/50 p-3">
                      <p className="text-slate-500 text-xs">Nguồn trích xuất</p>
                      <p className="font-semibold text-slate-700">{selectedDetailItem.extractionSource ?? "unknown"}</p>
                    </div>
                    <div className="rounded-lg border border-slate-200/60 bg-slate-50/50 p-3">
                      <p className="text-slate-500 text-xs">Số ký tự / Section</p>
                      <p className="font-semibold text-slate-700">{selectedDetailItem.extractedTextLength ?? 0} / {selectedDetailItem.sectionCount ?? 0}</p>
                    </div>
                  </div>

                  <div className="rounded-lg border border-slate-200/60 bg-slate-50/30 p-3">
                    <p className="text-xs text-slate-500 mb-2">Thông báo xử lý</p>
                    <p className="text-sm text-slate-700 whitespace-pre-wrap">{selectedDetailItem.message}</p>
                  </div>

                  <div className="rounded-lg border border-slate-200/60 bg-slate-50/30 p-3">
                    <p className="text-xs text-slate-500 mb-2">Nội dung trích xuất (preview)</p>
                    <pre className="text-xs text-slate-800 whitespace-pre-wrap leading-relaxed max-h-[320px] overflow-auto">
{selectedDetailItem.extractedPreview || "Chưa có dữ liệu preview."}
                    </pre>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
