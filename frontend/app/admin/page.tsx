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
  } | null;
  error?: string | null;
};

type ItemStatus = "ready" | "uploading" | "queued" | "processing" | "success" | "stored" | "error";

type UploadItem = {
  localId: string;
  key: string;
  file: File;
  status: ItemStatus;
  message: string;
  jobId?: string;
  extractionSource?: string;
  extractedTextLength?: number;
  sectionCount?: number;
  extractedPreview?: string;
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
            message: `Job ${jobId.slice(0, 8)} (${catLabel}) đang chờ trong hàng đợi.`,
          });
          return;
        }

        if (data.status === "processing") {
          updateItem(localId, {
            status: "processing",
            message: `Job ${jobId.slice(0, 8)} (${catLabel}) đang xử lý.`,
          });
          return;
        }

        if (data.status === "completed") {
          if (data.result?.ingestion_applied === false) {
            updateItem(localId, {
              status: "stored",
              message: data.result?.message ?? "Đã lưu file, chưa có pipeline ingestion cho thể loại này.",
            });
            clearPolling(localId);
            return;
          }

          if (typeof data.result?.nodes === "number" && typeof data.result?.edges === "number") {
            updateItem(localId, {
              status: "success",
              message: `Xong: ${data.result.nodes} nodes, ${data.result.edges} edges.`,
              extractionSource: data.result?.extraction_source,
              extractedTextLength: data.result?.extracted_text_length,
              sectionCount: data.result?.section_count,
              extractedPreview: data.result?.extracted_preview,
            });
          } else {
            updateItem(localId, {
              status: "success",
              message: data.result?.message ?? "Upload hoàn tất.",
              extractionSource: data.result?.extraction_source,
              extractedTextLength: data.result?.extracted_text_length,
              sectionCount: data.result?.section_count,
              extractedPreview: data.result?.extracted_preview,
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
    if (status === "success") return "bg-green-100 text-green-700";
    if (status === "stored") return "bg-slate-100 text-slate-700";
    if (status === "error") return "bg-red-100 text-red-700";
    if (status === "processing") return "bg-blue-100 text-blue-700";
    if (status === "queued") return "bg-amber-100 text-amber-700";
    if (status === "uploading") return "bg-indigo-100 text-indigo-700";
    return "bg-gray-100 text-gray-700";
  };

  const getStatusLabel = (status: ItemStatus) => {
    if (status === "ready") return "Sẵn sàng";
    if (status === "uploading") return "Đang upload";
    if (status === "queued") return "Đang xếp hàng";
    if (status === "processing") return "Đang xử lý";
    if (status === "success") return "Thành công";
    if (status === "stored") return "Đã lưu";
    return "Lỗi";
  };

  return (
    <div className="min-h-screen bg-slate-50 text-slate-900 p-5 md:p-8 font-sans">
      <div className="max-w-6xl mx-auto space-y-8">
        <header className="flex items-center justify-between pb-5 border-b border-slate-200">
          <div>
            <h1 className="text-3xl md:text-4xl font-extrabold tracking-tight text-slate-950">
              Nạp dữ liệu cho Neo4j Graph Database
            </h1>
            <p className="text-sm md:text-base text-slate-600 mt-2">
              Giao diện upload nhiều file, theo dõi realtime theo từng job.
            </p>
          </div>
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-full bg-slate-200 flex items-center justify-center text-slate-500 font-bold">L</div>
            <span className="text-sm font-medium text-slate-700">Admin1</span>
          </div>
        </header>

        <main className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          <section className="lg:col-span-2 space-y-6">
            <div className="bg-white rounded-2xl shadow-sm border border-slate-200 p-6">
              <h2 className="text-xl font-semibold mb-1 text-slate-900">Thêm tài liệu mới</h2>
              <p className="text-slate-500 mb-6">Kéo thả nhiều file PDF hoặc nhấn để chọn nhiều file.</p>

              <div className="mb-5">
                <label htmlFor="category-select" className="block text-sm font-semibold text-slate-700 mb-2">
                  Chọn thể loại tài liệu
                </label>
                <select
                  id="category-select"
                  value={selectedCategory}
                  onChange={(e) => setSelectedCategory(e.target.value as UploadCategory | "")}
                  className="w-full rounded-xl border border-slate-300 bg-white px-4 py-3 text-slate-700 focus:outline-none focus:ring-2 focus:ring-blue-500"
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
                  dragOver ? "border-blue-500 bg-blue-50" : "border-slate-300 hover:border-slate-400"
                }`}
              >
                <svg className={`w-14 h-14 mx-auto mb-4 ${dragOver ? "text-blue-500" : "text-slate-300"}`} fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                </svg>
                <label htmlFor="file-upload" className="cursor-pointer">
                  <span className="text-lg font-semibold text-blue-600">Kéo thả nhiều file PDF</span>
                  <span className="text-lg text-slate-600"> hoặc nhấn để chọn</span>
                  <input id="file-upload" type="file" multiple accept="application/pdf" onChange={handleFileChange} className="sr-only" />
                </label>
                <p className="mt-3 text-sm text-slate-400">Mỗi file tối đa 50MB, có thể upload cùng lúc nhiều file.</p>
              </div>

              {pageMessage && (
                <div className="mt-5 rounded-xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-700">
                  {pageMessage}
                </div>
              )}
            </div>

            <div className="bg-white rounded-2xl shadow-sm border border-slate-200 p-6">
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-lg font-semibold text-slate-900">Danh sách file</h3>
                <span className="text-sm text-slate-500">{uploadItems.length} file</span>
              </div>

              {!uploadItems.length && (
                <div className="rounded-xl border border-dashed border-slate-300 px-4 py-8 text-center text-slate-500">
                  Chưa có file nào được chọn.
                </div>
              )}

              <div className="space-y-3 max-h-90 overflow-auto pr-1">
                {uploadItems.map((item) => (
                  <div key={item.localId} className="rounded-xl border border-slate-200 bg-white px-4 py-3">
                    <div className="flex items-start justify-between gap-3">
                      <div className="min-w-0">
                        <p className="font-medium text-slate-900 truncate">{item.file.name}</p>
                        <p className="text-xs text-slate-500 mt-1">{formatBytes(item.file.size)}</p>
                      </div>

                      <div className="flex items-center gap-2">
                        <span className={`text-xs px-2.5 py-1 rounded-full font-medium ${getStatusBadge(item.status)}`}>
                          {getStatusLabel(item.status)}
                        </span>
                        {item.status !== "uploading" && item.status !== "queued" && item.status !== "processing" && (
                          <button onClick={() => handleRemoveItem(item.localId)} className="text-slate-400 hover:text-slate-700" aria-label="Remove file">
                            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" /></svg>
                          </button>
                        )}
                      </div>
                    </div>

                    <p className="text-xs text-slate-500 mt-2 truncate">{item.message}</p>

                    {(item.extractionSource || typeof item.extractedTextLength === "number") && (
                      <div className="mt-3 rounded-lg border border-slate-200 bg-slate-50 p-3">
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

                        <div className="mt-2 flex justify-end">
                          <button
                            type="button"
                            onClick={() => setSelectedDetailItem(item)}
                            className="text-xs px-3 py-1.5 rounded-md bg-blue-600 text-white hover:bg-blue-700 cursor-pointer"
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
            <div className="bg-white rounded-2xl shadow-sm border border-slate-200 p-6 space-y-5">
              <h3 className="text-lg font-semibold text-slate-900">Bắt đầu xử lý</h3>
              <p className="text-sm text-slate-500">Upload nhiều file cùng lúc, backend sẽ xếp hàng xử lý tuần tự.</p>

              <button
                onClick={handleIngest}
                disabled={isBusy || !uploadItems.length}
                className={`w-full flex items-center justify-center gap-2 py-3.5 rounded-xl font-semibold text-white transition-all ${
                  isBusy || !uploadItems.length
                    ? "bg-slate-400 cursor-not-allowed"
                    : "bg-blue-600 hover:bg-blue-700 shadow-sm shadow-blue-200"
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

            <div className="bg-white rounded-2xl shadow-sm border border-slate-200 p-6">
              <h4 className="text-sm font-semibold text-slate-900 mb-4">Tổng quan trạng thái</h4>
              <div className="grid grid-cols-2 gap-3 text-sm">
                <div className="rounded-lg bg-slate-50 border border-slate-200 px-3 py-2">
                  <p className="text-slate-500">Sẵn sàng</p>
                  <p className="font-semibold text-slate-900">{summary.ready}</p>
                </div>
                <div className="rounded-lg bg-slate-50 border border-slate-200 px-3 py-2">
                  <p className="text-slate-500">Đang upload</p>
                  <p className="font-semibold text-slate-900">{summary.uploading}</p>
                </div>
                <div className="rounded-lg bg-amber-50 border border-amber-200 px-3 py-2">
                  <p className="text-amber-700">Đợi xử lý</p>
                  <p className="font-semibold text-amber-900">{summary.queued}</p>
                </div>
                <div className="rounded-lg bg-blue-50 border border-blue-200 px-3 py-2">
                  <p className="text-blue-700">Đang xử lý</p>
                  <p className="font-semibold text-blue-900">{summary.processing}</p>
                </div>
                <div className="rounded-lg bg-green-50 border border-green-200 px-3 py-2">
                  <p className="text-green-700">Thành công</p>
                  <p className="font-semibold text-green-900">{summary.success}</p>
                </div>
                <div className="rounded-lg bg-slate-50 border border-slate-200 px-3 py-2">
                  <p className="text-slate-700">Đã lưu</p>
                  <p className="font-semibold text-slate-900">{summary.stored}</p>
                </div>
                <div className="rounded-lg bg-red-50 border border-red-200 px-3 py-2">
                  <p className="text-red-700">Lỗi</p>
                  <p className="font-semibold text-red-900">{summary.error}</p>
                </div>
              </div>
            </div>
          </aside>
        </main>

        <footer className="text-center pt-6 border-t border-slate-200 text-slate-400 text-sm">
          CIT GraphRAG
        </footer>
      </div>

      {selectedDetailItem && (
        <div
          className="fixed inset-0 z-50 bg-slate-900/50 backdrop-blur-[1px] p-4 flex items-center justify-center"
          onClick={() => setSelectedDetailItem(null)}
        >
          <div
            className="w-full max-w-3xl max-h-[85vh] overflow-hidden rounded-2xl bg-white shadow-2xl border border-slate-200"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-center justify-between px-5 py-4 border-b border-slate-200">
              <div>
                <h3 className="text-lg font-semibold text-slate-900">Chi tiết trích xuất</h3>
                <p className="text-xs text-slate-500 mt-0.5 truncate max-w-130">{selectedDetailItem.file.name}</p>
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

            <div className="p-5 overflow-y-auto max-h-[calc(85vh-70px)] space-y-4">
              <div className="grid grid-cols-2 gap-3 text-sm">
                <div className="rounded-lg border border-slate-200 bg-slate-50 p-3">
                  <p className="text-slate-500 text-xs">Trạng thái</p>
                  <p className="font-semibold text-slate-900">{getStatusLabel(selectedDetailItem.status)}</p>
                </div>
                <div className="rounded-lg border border-slate-200 bg-slate-50 p-3">
                  <p className="text-slate-500 text-xs">Kích thước</p>
                  <p className="font-semibold text-slate-900">{formatBytes(selectedDetailItem.file.size)}</p>
                </div>
                <div className="rounded-lg border border-slate-200 bg-slate-50 p-3">
                  <p className="text-slate-500 text-xs">Nguồn trích xuất</p>
                  <p className="font-semibold text-slate-900">{selectedDetailItem.extractionSource ?? "unknown"}</p>
                </div>
                <div className="rounded-lg border border-slate-200 bg-slate-50 p-3">
                  <p className="text-slate-500 text-xs">Số ký tự / Section</p>
                  <p className="font-semibold text-slate-900">{selectedDetailItem.extractedTextLength ?? 0} / {selectedDetailItem.sectionCount ?? 0}</p>
                </div>
              </div>

              <div className="rounded-lg border border-slate-200 bg-white p-3">
                <p className="text-xs text-slate-500 mb-2">Thông báo xử lý</p>
                <p className="text-sm text-slate-800 whitespace-pre-wrap">{selectedDetailItem.message}</p>
              </div>

              <div className="rounded-lg border border-slate-200 bg-white p-3">
                <p className="text-xs text-slate-500 mb-2">Nội dung trích xuất (preview)</p>
                <pre className="text-xs text-slate-800 whitespace-pre-wrap leading-relaxed max-h-[45vh] overflow-auto">
{selectedDetailItem.extractedPreview || "Chưa có dữ liệu preview."}
                </pre>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
